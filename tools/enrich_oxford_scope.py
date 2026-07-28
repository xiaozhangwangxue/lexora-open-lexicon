#!/usr/bin/env python3
"""Rate-limited, resumable enrichment using Lexora's public providers.

The worker only fills empty fields.  It keeps provider state in a separate
SQLite file so a stopped run can resume without touching completed terms.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build"
STATE = BUILD / "oxford-enrichment-state.sqlite"
EDGE_BASE = os.environ.get("LEXORA_EDGE_URL", "").rstrip("/")

def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

def j(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

def unique(values: list[str], limit: int = 40) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        value = " ".join(str(value).split()).strip()
        if value and value not in seen:
            seen.add(value); result.append(value)
        if len(result) >= limit:
            break
    return result

def init_state() -> sqlite3.Connection:
    db = sqlite3.connect(STATE)
    db.execute("""CREATE TABLE IF NOT EXISTS provider_state(
      term TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0, http_status INTEGER, last_error TEXT,
      updated_at TEXT NOT NULL, PRIMARY KEY(term,source))""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_provider_state_status ON provider_state(status,updated_at)")
    db.commit()
    return db

class HostGate:
    def __init__(self, interval: float, concurrency: int = 1):
        self.interval = interval
        self.lock = asyncio.Lock()
        self.last = 0.0
        self.semaphore = asyncio.Semaphore(max(1, concurrency))

    async def wait(self) -> None:
        async with self.lock:
            delta = time.monotonic() - self.last
            if delta < self.interval:
                await asyncio.sleep(self.interval - delta)
            self.last = time.monotonic()

async def request_json(client: httpx.AsyncClient, gate: HostGate, url: str, source: str, attempts: int = 3) -> tuple[Any | None, int | None, str | None]:
    for attempt in range(attempts):
        await gate.semaphore.acquire()
        try:
            await gate.wait()
            response = await client.get(url, headers={"User-Agent": "LexoraOpenLexicon/1.0 (open-data enrichment)"})
            if response.status_code == 200:
                return response.json(), 200, None
            if response.status_code in (429, 500, 502, 503, 504):
                retry = response.headers.get("retry-after")
                delay = float(retry) if retry and retry.isdigit() else min(60.0, 2 ** attempt)
                await asyncio.sleep(delay)
                continue
            return None, response.status_code, f"HTTP {response.status_code}"
        except (httpx.HTTPError, ValueError) as exc:
            if attempt + 1 < attempts:
                await asyncio.sleep(min(60.0, 2 ** attempt))
            else:
                return None, None, str(exc)
        finally:
            gate.semaphore.release()
    return None, None, f"{source} exhausted retries"

def dictionary_fields(data: Any) -> dict[str, Any]:
    if not isinstance(data, list): return {}
    definitions: list[str] = []; examples: list[str] = []; synonyms: list[str] = []; antonyms: list[str] = []
    us = ""; uk = ""
    for entry in data:
        for phonetic in entry.get("phonetics", []) or []:
            text = phonetic.get("text") or ""
            if not text: continue
            if not us: us = text
            if not uk: uk = text
        for meaning in entry.get("meanings", []) or []:
            for item in meaning.get("definitions", []) or []:
                if item.get("definition"): definitions.append(item["definition"])
                if item.get("example"): examples.append(item["example"])
            synonyms.extend((x.get("word", "") if isinstance(x, dict) else str(x)) for x in meaning.get("synonyms", []) or [])
            antonyms.extend((x.get("word", "") if isinstance(x, dict) else str(x)) for x in meaning.get("antonyms", []) or [])
    return {"definition": "\n".join(unique(definitions, 24)), "examples": unique(examples, 8), "synonyms": unique(synonyms), "antonyms": unique(antonyms), "us": us, "uk": uk}

def datamuse_fields(data: Any) -> dict[str, Any]:
    if not isinstance(data, list): return {}
    words = unique([x.get("word", "") for x in data if isinstance(x, dict)])
    definitions = unique([d for x in data if isinstance(x, dict) for d in x.get("defs", [])])
    scores = [float(x.get("score")) for x in data if isinstance(x, dict) and x.get("score") is not None]
    return {"words": words, "definition": "\n".join(definitions[:24]), "frequency": max(scores) if scores else 0.0}

def difficulty(score: float) -> str:
    if score >= 6.0: return "A1–A2"
    if score >= 4.8: return "B1–B2"
    if score >= 3.4: return "C1–C2"
    return "C2+"

def should_process_marker(raw: str | None, retry_after_hours: float) -> bool:
    """Resume efficiently without re-querying a recently attempted term."""
    marker = json.loads(raw or "{}")
    status = marker.get("status")
    if not status:
        return True
    if status == "completed":
        return False
    last_attempt = marker.get("lastAttempt")
    if not last_attempt:
        return True
    try:
        attempted_at = dt.datetime.fromisoformat(last_attempt)
        age = dt.datetime.now(dt.timezone.utc) - attempted_at
        return age.total_seconds() >= max(0.0, retry_after_hours) * 3600
    except ValueError:
        return True

async def enrich_term(client: httpx.AsyncClient, gates: dict[str, HostGate], term: str, state: sqlite3.Connection, existing: dict[str, Any]) -> dict[str, Any]:
    encoded = quote(term, safe="")
    urls = {
      "edge": f"{EDGE_BASE}/api/dictionary/full?term={encoded}" if EDGE_BASE else "",
      "dictionary": f"https://api.dictionaryapi.dev/api/v2/entries/en/{encoded}",
      "datamuse_related": f"https://api.datamuse.com/words?ml={encoded}&md=dfr&ipa=1&max=30",
      "datamuse_exact": f"https://api.datamuse.com/words?sp={encoded}&md=dfrp&ipa=1&max=8",
      "datamuse_synonyms": f"https://api.datamuse.com/words?rel_syn={encoded}&md=f&max=12",
      "datamuse_antonyms": f"https://api.datamuse.com/words?rel_ant={encoded}&max=12",
    }
    result: dict[str, Any] = {"_statuses": [], "_attempted": []}
    sources: list[str] = []
    needs_definition = not existing.get("definition")
    needs_phonetic = not existing.get("us") or not existing.get("uk")
    needs_examples = not existing.get("examples")
    if EDGE_BASE and (needs_definition or needs_phonetic or needs_examples):
        sources.append("edge")
    if needs_definition or needs_phonetic or needs_examples:
        sources.append("dictionary")
    if not existing.get("related"):
        sources.append("datamuse_related")
    if needs_definition:
        sources.append("datamuse_exact")
    if not existing.get("synonyms"):
        sources.append("datamuse_synonyms")
    if not existing.get("antonyms"):
        sources.append("datamuse_antonyms")
    async def fetch(source: str) -> tuple[str, Any | None, int | None, str | None]:
        gate = gates["datamuse"] if source.startswith("datamuse") else gates[source]
        data, status, error = await request_json(client, gate, urls[source], source)
        return source, data, status, error

    # Sources for one term are independent.  Fetching them together removes
    # the previous per-term serial bottleneck while HostGate still limits each
    # provider's request rate and in-flight concurrency.
    fetched = await asyncio.gather(*(fetch(source) for source in sources))
    for source, data, status, error in fetched:
        state.execute("""INSERT INTO provider_state(term,source,status,attempts,http_status,last_error,updated_at)
          VALUES(?,?,?,?,?,?,?) ON CONFLICT(term,source) DO UPDATE SET status=excluded.status,attempts=provider_state.attempts+1,http_status=excluded.http_status,last_error=excluded.last_error,updated_at=excluded.updated_at""",
          (term, source, "completed" if status == 200 else "failed", 1, status, error, now()))
        result["_statuses"].append("completed" if status == 200 else "failed")
        result["_attempted"].append(source)
        if source == "dictionary": result.update(dictionary_fields(data))
        elif source.startswith("datamuse"):
            parsed = datamuse_fields(data)
            result[source] = parsed
            if source == "datamuse_exact" and parsed.get("definition") and not result.get("definition"):
                result["definition"] = parsed["definition"]
            if source == "datamuse_related": result["related"] = parsed.get("words", [])
            if source == "datamuse_synonyms": result["synonyms"] = parsed.get("words", [])
            if source == "datamuse_antonyms": result["antonyms"] = parsed.get("words", [])
            result["frequency"] = max(float(result.get("frequency", 0)), float(parsed.get("frequency", 0)))
        elif source == "edge" and isinstance(data, dict) and data.get("word"):
            result.update({k: data.get(k) for k in ("definition", "definition_zh", "us_phonetic", "uk_phonetic", "synonyms", "antonyms", "examples", "examplesZh", "frequency", "difficulty") if data.get(k) is not None})
    return result

async def translate(client: httpx.AsyncClient, gate: HostGate, text: str) -> tuple[str, int | None, str | None]:
    if not text: return "", None, None
    url = "https://api.mymemory.translated.net/get?q=" + quote(text, safe="") + "&langpair=en|zh-CN"
    data, status, error = await request_json(client, gate, url, "translation")
    try:
        return str(data["responseData"]["translatedText"]), status, error
    except Exception:
        return "", status, error or "translation missing"

async def run(dataset: Path, limit: int, delay: float, workers: int, translation_delay: float, retry_after_hours: float) -> None:
    state = init_state()
    workers = max(1, workers)
    gates = {
        "edge": HostGate(delay, min(4, workers)),
        "dictionary": HostGate(delay, min(4, workers)),
        "datamuse": HostGate(delay, min(8, workers)),
        "translation": HostGate(max(delay, translation_delay), min(2, workers)),
    }
    client_timeout = httpx.Timeout(20.0, connect=10.0)
    client = httpx.AsyncClient(timeout=client_timeout, follow_redirects=True)
    try:
        db = sqlite3.connect(dataset)
        rows = db.execute("SELECT id,word,normalized_word,definition,definition_zh,us_phonetic,uk_phonetic,synonyms_json,antonyms_json,examples_json,related_words_json,frequency,difficulty,enrichment_json FROM entries ORDER BY id").fetchall()
        processed = 0
        async def process_row(row: tuple[Any, ...]) -> tuple[str, str]:
            entry_id, word, term, definition, definition_zh, us, uk, synonyms_json, antonyms_json, examples_json, related_json, freq, diff, enrich_json = row
            marker = json.loads(enrich_json or "{}")
            existing = {
                "definition": definition or "", "us": us or "", "uk": uk or "",
                "examples": json.loads(examples_json or "[]"),
                "synonyms": json.loads(synonyms_json or "[]"),
                "antonyms": json.loads(antonyms_json or "[]"),
                "related": json.loads(related_json or "[]"),
            }
            data = await enrich_term(client, gates, term, state, existing)
            definition = definition or data.get("definition", "")
            us = us or data.get("us", "") or data.get("us_phonetic", "")
            uk = uk or data.get("uk", "") or data.get("uk_phonetic", "")
            synonyms = unique([*json.loads(synonyms_json or "[]"), *data.get("synonyms", [])])
            antonyms = unique([*json.loads(antonyms_json or "[]"), *data.get("antonyms", [])])
            examples = unique([*json.loads(examples_json or "[]"), *data.get("examples", [])])
            related = unique([*json.loads(related_json or "[]"), *data.get("related", [])])
            zh = definition_zh
            if not zh and definition:
                zh, status, error = await translate(client, gates["translation"], definition[:1800])
                state.execute("""INSERT INTO provider_state(term,source,status,attempts,http_status,last_error,updated_at) VALUES(?,?,?,?,?,?,?)
                  ON CONFLICT(term,source) DO UPDATE SET status=excluded.status,attempts=provider_state.attempts+1,http_status=excluded.http_status,last_error=excluded.last_error,updated_at=excluded.updated_at""",
                  (term, "translation", "completed" if zh else "failed", 1, status, error, now()))
                data.setdefault("_statuses", []).append("completed" if zh else "failed")
            score = max(float(freq or 0), float(data.get("frequency", 0) or 0))
            statuses = data.pop("_statuses", [])
            attempted = data.pop("_attempted", [])
            marker_status = "completed" if not attempted or all(item == "completed" for item in statuses) else ("partial" if any(item == "completed" for item in statuses) else "not_found")
            marker = {"status": marker_status, "lastAttempt": now(), "sources": sorted(set(["open-data", "network"]))}
            db.execute("""UPDATE entries SET definition=?,definition_zh=?,us_phonetic=?,uk_phonetic=?,synonyms_json=?,antonyms_json=?,examples_json=?,related_words_json=?,frequency=?,difficulty=?,enrichment_json=? WHERE id=?""",
              (definition, zh, us, uk, j(synonyms), j(antonyms), j(examples), j(related), score, diff or difficulty(score), j(marker), entry_id))
            db.execute("""INSERT OR REPLACE INTO entries_fts(rowid,word,definition,definition_zh,examples,phrases)
              SELECT id,word,definition,definition_zh,examples_json,phrases_json FROM entries WHERE id=?""", (entry_id,))
            db.commit(); state.commit()
            return term, marker_status
        pending: list[tuple[Any, ...]] = []
        for row in rows:
            if limit and processed + len(pending) >= limit:
                break
            if not should_process_marker(row[-1], retry_after_hours):
                continue
            pending.append(row)
            if len(pending) < workers:
                continue
            results = await asyncio.gather(*(process_row(item) for item in pending), return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    print(f"enrichment-error={result!r}", flush=True)
                else:
                    processed += 1
                    if processed % 25 == 0:
                        print(f"enriched={processed} term={result[0]} status={result[1]}", flush=True)
            pending.clear()
        if pending:
            results = await asyncio.gather(*(process_row(item) for item in pending), return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    print(f"enrichment-error={result!r}", flush=True)
                else:
                    processed += 1
                    if processed % 25 == 0:
                        print(f"enriched={processed} term={result[0]} status={result[1]}", flush=True)
        db.close()
    finally:
        await client.aclose(); state.close()

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", type=Path, default=BUILD / "lexora-open-oxford-scope.sqlite")
    ap.add_argument("--limit", type=int, default=0, help="0 means all pending terms")
    ap.add_argument("--delay", type=float, default=0.5, help="minimum seconds between requests per provider host")
    ap.add_argument("--workers", type=int, default=16, help="number of terms processed concurrently")
    ap.add_argument("--translation-delay", type=float, default=1.0, help="minimum seconds between translation requests")
    ap.add_argument("--retry-after-hours", type=float, default=24.0, help="retry partial/not-found terms only after this many hours")
    args = ap.parse_args()
    asyncio.run(run(args.dataset, args.limit, args.delay, args.workers, args.translation_delay, args.retry_after_hours))

if __name__ == "__main__":
    main()
