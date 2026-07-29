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
import math
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
EDGE_TOKEN = os.environ.get("LEXORA_ORIGIN_TOKEN", "")

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


def merge_named_entries(
    current: Any,
    incoming: Any,
    limit: int = 40,
) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    positions: dict[str, int] = {}
    values = [
        *(current if isinstance(current, list) else []),
        *(incoming if isinstance(incoming, list) else []),
    ]
    for raw in values:
        if not isinstance(raw, dict):
            continue
        word = " ".join(str(raw.get("word") or "").split()).strip()
        definition = " ".join(
            str(raw.get("definition") or raw.get("meaning") or "").split()
        ).strip()
        if not word or not definition:
            continue
        key = word.lower()
        if key in positions:
            index = positions[key]
            if len(definition) > len(result[index]["definition"]):
                result[index]["definition"] = definition
            continue
        positions[key] = len(result)
        result.append({"word": word, "definition": definition})
        if len(result) >= limit:
            break
    return result


def normalize_phonetic(value: Any) -> str:
    """Normalize common legacy ECDICT symbols to Unicode IPA."""
    text = str(value or "").strip().strip("/")
    return (
        text.replace("ә", "ə")
        .replace(":", "ː")
        .replace("'", "ˈ")
        .replace("ˈˈ", "ˈ")
    )


def needs_phonetic_repair(value: Any) -> bool:
    """Treat empty and known legacy transcription forms as incomplete."""
    text = str(value or "").strip()
    return not text or "ә" in text or ":" in text


def needs_definition_translation(definition: Any, translation: Any) -> bool:
    """Detect missing or obviously abbreviated translations of long entries."""
    source = " ".join(str(definition or "").split()).strip()
    target = " ".join(str(translation or "").split()).strip()
    if not source:
        return False
    if not target:
        return True
    if len(source) <= 120:
        return False
    return len(target) < max(20, int(len(source) * 0.15))


def needs_frequency_repair(value: Any) -> bool:
    try:
        score = float(value or 0)
    except (TypeError, ValueError):
        return True
    # wordfreq and the exported manifest use the Zipf scale. Earlier
    # enrichment briefly stored Datamuse relevance scores, which can be in the
    # millions and must not survive into the public snapshots.
    return score > 10


def translation_chunks(text: str, limit: int = 450) -> list[str]:
    """Split long definitions without dropping content or breaking most words."""
    remaining = " ".join(str(text).split()).strip()
    if not remaining:
        return []
    chunk_count = max(1, (len(remaining) + limit - 1) // limit)
    chunks: list[str] = []
    while remaining and chunk_count > 0:
        if chunk_count == 1 or len(remaining) <= limit:
            chunks.append(remaining)
            break
        target = (len(remaining) + chunk_count - 1) // chunk_count
        minimum = max(1, len(remaining) - (chunk_count - 1) * limit)
        maximum = min(limit, len(remaining) - (chunk_count - 1))
        cut = remaining.rfind(" ", minimum, maximum + 1)
        if cut < minimum:
            cut = max(minimum, min(target, maximum))
        chunks.append(remaining[:cut].strip())
        remaining = remaining[cut:].strip()
        chunk_count -= 1
    return chunks


def init_state(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("""CREATE TABLE IF NOT EXISTS provider_state(
      term TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0, http_status INTEGER, last_error TEXT,
      updated_at TEXT NOT NULL, PRIMARY KEY(term,source))""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_provider_state_status ON provider_state(status,updated_at)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_provider_state_updated_at ON provider_state(updated_at)")
    db.commit()
    return db


def ensure_dataset_columns(database: sqlite3.Connection) -> None:
    columns = {
        str(row[1])
        for row in database.execute("PRAGMA table_info(entries)").fetchall()
    }
    for name in ("phrase_entries_json", "related_entries_json"):
        if name not in columns:
            database.execute(
                f"ALTER TABLE entries ADD COLUMN {name} "
                "TEXT NOT NULL DEFAULT '[]'"
            )
    database.commit()


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


class EdgeDictionaryBatcher:
    """Coalesce concurrent term lookups into one bounded edge invocation."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        gate: HostGate,
        batch_size: int = 8,
        flush_delay: float = 0.04,
    ):
        self.client = client
        self.gate = gate
        self.batch_size = max(1, min(8, batch_size))
        self.flush_delay = max(0.0, flush_delay)
        self.pending: list[
            tuple[str, asyncio.Future[tuple[Any | None, int | None, str | None]]]
        ] = []
        self.lock = asyncio.Lock()
        self.flush_task: asyncio.Task[None] | None = None

    async def request(
        self, term: str
    ) -> tuple[Any | None, int | None, str | None]:
        future: asyncio.Future[tuple[Any | None, int | None, str | None]] = (
            asyncio.get_running_loop().create_future()
        )
        async with self.lock:
            self.pending.append((term, future))
            if self.flush_task is None:
                self.flush_task = asyncio.create_task(self._flush())
        return await future

    async def _flush(self) -> None:
        if self.flush_delay:
            await asyncio.sleep(self.flush_delay)
        while True:
            async with self.lock:
                batch = self.pending[: self.batch_size]
                del self.pending[: self.batch_size]
                if not batch:
                    self.flush_task = None
                    return
            terms = [term for term, _ in batch]
            status: int | None = None
            error: str | None = None
            results: dict[str, Any] = {}
            await self.gate.semaphore.acquire()
            try:
                await self.gate.wait()
                response = await self.client.post(
                    f"{EDGE_BASE}/api/dictionary/batch",
                    json={"terms": terms},
                    headers={
                        "Accept": "application/json",
                        "User-Agent": "LexoraOpenLexicon/1.0 (open-data enrichment)",
                        **(
                            {"X-Lexora-Origin-Token": EDGE_TOKEN}
                            if EDGE_TOKEN
                            else {}
                        ),
                    },
                )
                status = response.status_code
                if response.status_code == 200:
                    payload = response.json()
                    raw_results = payload.get("results", {})
                    if isinstance(raw_results, dict):
                        results = raw_results
                else:
                    error = f"HTTP {response.status_code}"
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                error = str(exc)
            finally:
                self.gate.semaphore.release()

            for term, future in batch:
                item = results.get(term)
                item_status = (
                    int(item.get("status"))
                    if isinstance(item, dict) and item.get("status") is not None
                    else status
                )
                data = item.get("data") if isinstance(item, dict) else None
                item_error = error
                if item_status != 200:
                    if isinstance(data, dict) and data.get("error"):
                        item_error = str(data["error"])
                    data = None
                if not future.done():
                    future.set_result((data, item_status, item_error))


class EdgeTranslationBatcher:
    """Batch translations so dictionary enrichment stays within free quotas."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        gate: HostGate,
        batch_size: int = 8,
        flush_delay: float = 0.04,
    ):
        self.client = client
        self.gate = gate
        self.batch_size = max(1, min(32, batch_size))
        self.flush_delay = max(0.0, flush_delay)
        self.pending: list[
            tuple[str, asyncio.Future[tuple[str, int | None, str | None]]]
        ] = []
        self.lock = asyncio.Lock()
        self.flush_task: asyncio.Task[None] | None = None

    async def request(self, text: str) -> tuple[str, int | None, str | None]:
        future: asyncio.Future[tuple[str, int | None, str | None]] = (
            asyncio.get_running_loop().create_future()
        )
        async with self.lock:
            self.pending.append((text, future))
            if self.flush_task is None:
                self.flush_task = asyncio.create_task(self._flush())
        return await future

    async def _flush(self) -> None:
        if self.flush_delay:
            await asyncio.sleep(self.flush_delay)
        while True:
            async with self.lock:
                batch = self.pending[: self.batch_size]
                del self.pending[: self.batch_size]
                if not batch:
                    self.flush_task = None
                    return
            chunk_owners: list[int] = []
            texts: list[str] = []
            for owner, (text, _) in enumerate(batch):
                for chunk in translation_chunks(text):
                    chunk_owners.append(owner)
                    texts.append(chunk)
            translations: list[str] = []
            status: int | None = None
            error: str | None = None
            await self.gate.semaphore.acquire()
            try:
                await self.gate.wait()
                response = await self.client.post(
                    f"{EDGE_BASE}/api/translate/batch",
                    json={"texts": texts},
                    headers={
                        "Accept": "application/json",
                        "User-Agent": "LexoraOpenLexicon/1.0 (open-data enrichment)",
                        **(
                            {"X-Lexora-Origin-Token": EDGE_TOKEN}
                            if EDGE_TOKEN
                            else {}
                        ),
                    },
                )
                status = response.status_code
                if response.status_code == 200:
                    payload = response.json()
                    values = payload.get("translations", [])
                    if isinstance(values, list):
                        translations = [str(value or "").strip() for value in values]
                else:
                    error = f"HTTP {response.status_code}"
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                error = str(exc)
            finally:
                self.gate.semaphore.release()

            values_by_owner: list[list[str]] = [[] for _ in batch]
            for index, owner in enumerate(chunk_owners):
                if index < len(translations) and translations[index]:
                    values_by_owner[owner].append(translations[index])
            for index, (_, future) in enumerate(batch):
                value = "\n".join(values_by_owner[index]).strip()
                if not future.done():
                    future.set_result(
                        (
                            value,
                            status,
                            None if value else error or "translation missing",
                        )
                    )


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
    us = ""; uk = ""; fallback = ""
    for entry in data:
        entry_fallback = normalize_phonetic(entry.get("phonetic"))
        if entry_fallback and not fallback:
            fallback = entry_fallback
        for phonetic in entry.get("phonetics", []) or []:
            text = normalize_phonetic(phonetic.get("text"))
            if not text: continue
            audio = str(phonetic.get("audio") or "").lower()
            if ("-us." in audio or "/us/" in audio) and not us:
                us = text
            elif ("-uk." in audio or "/uk/" in audio) and not uk:
                uk = text
            elif not fallback:
                fallback = text
        for meaning in entry.get("meanings", []) or []:
            for item in meaning.get("definitions", []) or []:
                if item.get("definition"): definitions.append(item["definition"])
                if item.get("example"): examples.append(item["example"])
            synonyms.extend((x.get("word", "") if isinstance(x, dict) else str(x)) for x in meaning.get("synonyms", []) or [])
            antonyms.extend((x.get("word", "") if isinstance(x, dict) else str(x)) for x in meaning.get("antonyms", []) or [])
    return {
        "definition": "\n".join(unique(definitions, 24)),
        "examples": unique(examples, 8),
        "synonyms": unique(synonyms),
        "antonyms": unique(antonyms),
        "us": us or fallback or uk,
        "uk": uk or fallback or us,
    }

def datamuse_fields(data: Any) -> dict[str, Any]:
    if not isinstance(data, list): return {}
    words = unique([x.get("word", "") for x in data if isinstance(x, dict)])
    definitions = unique([d for x in data if isinstance(x, dict) for d in x.get("defs", [])])
    frequencies: list[float] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        for tag in item.get("tags", []) or []:
            if not isinstance(tag, str) or not tag.startswith("f:"):
                continue
            try:
                raw = float(tag[2:])
            except ValueError:
                continue
            if raw > 0:
                frequencies.append(3 + math.log10(raw))
    entries: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        word = " ".join(str(item.get("word") or "").split()).strip()
        if not word or word in seen:
            continue
        cleaned = unique(
            [
                str(value).split("\t", 1)[-1].strip()
                for value in item.get("defs", []) or []
                if str(value).split("\t", 1)[-1].strip()
            ],
            3,
        )
        if not cleaned:
            continue
        seen.add(word)
        entries.append({"word": word, "definition": "\n".join(cleaned)})
        if len(entries) >= 40:
            break
    return {
        "words": words,
        "entries": entries,
        "definition": "\n".join(definitions[:24]),
        "frequency": max(frequencies) if frequencies else 0.0,
    }


def edge_fields(data: Any) -> dict[str, Any]:
    """Normalize the aggregate response from Lexora's Cloudflare edge."""
    if not isinstance(data, dict):
        return {}
    result = dictionary_fields(data.get("dictionary"))
    exact = datamuse_fields(data.get("exact"))
    related = datamuse_fields(data.get("related"))
    synonyms = datamuse_fields(data.get("synonyms"))
    antonyms = datamuse_fields(data.get("antonyms"))
    related_words = related.get("words", [])
    if not result.get("definition"):
        result["definition"] = exact.get("definition", "")
    result["frequency"] = max(
        float(exact.get("frequency", 0) or 0),
        float(related.get("frequency", 0) or 0),
    )
    result["related"] = related_words
    result["phrases"] = [word for word in related_words if " " in word]
    result["related_entries"] = [
        item
        for item in related.get("entries", [])
        if " " not in str(item.get("word") or "")
    ]
    result["phrase_entries"] = [
        item
        for item in related.get("entries", [])
        if " " in str(item.get("word") or "")
    ]
    result["synonyms"] = synonyms.get("words", [])
    result["antonyms"] = antonyms.get("words", [])
    return result

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

async def enrich_term(
    client: httpx.AsyncClient,
    gates: dict[str, HostGate],
    term: str,
    state: sqlite3.Connection,
    existing: dict[str, Any],
    edge_batcher: EdgeDictionaryBatcher | None = None,
) -> dict[str, Any]:
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
    needs_phonetic = needs_phonetic_repair(existing.get("us")) or needs_phonetic_repair(existing.get("uk"))
    needs_examples = not existing.get("examples")
    needs_network = any(
        (
            needs_definition,
            needs_phonetic,
            needs_examples,
            not existing.get("related"),
            not existing.get("phrases"),
            not existing.get("synonyms"),
            not existing.get("antonyms"),
        )
    )
    if EDGE_BASE and needs_network:
        sources.append("edge")
    elif needs_definition or needs_phonetic or needs_examples:
        sources.append("dictionary")
    if not EDGE_BASE:
        if not existing.get("related") or not existing.get("phrases"):
            sources.append("datamuse_related")
        if needs_definition:
            sources.append("datamuse_exact")
        if not existing.get("synonyms"):
            sources.append("datamuse_synonyms")
        if not existing.get("antonyms"):
            sources.append("datamuse_antonyms")
    async def fetch(source: str) -> tuple[str, Any | None, int | None, str | None]:
        if source == "edge" and edge_batcher is not None:
            data, status, error = await edge_batcher.request(term)
            return source, data, status, error
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
            if source == "datamuse_related":
                result["related"] = parsed.get("words", [])
                result["phrases"] = [
                    word for word in parsed.get("words", []) if " " in word
                ]
            if source == "datamuse_synonyms": result["synonyms"] = parsed.get("words", [])
            if source == "datamuse_antonyms": result["antonyms"] = parsed.get("words", [])
            result["frequency"] = max(float(result.get("frequency", 0)), float(parsed.get("frequency", 0)))
        elif source == "edge":
            result.update(edge_fields(data))
    return result

async def translate(
    client: httpx.AsyncClient,
    gate: HostGate,
    text: str,
    translation_batcher: EdgeTranslationBatcher | None = None,
) -> tuple[str, int | None, str | None]:
    if not text: return "", None, None
    if EDGE_BASE and translation_batcher is not None:
        return await translation_batcher.request(text)
    if EDGE_BASE:
        for attempt in range(3):
            await gate.semaphore.acquire()
            try:
                await gate.wait()
                response = await client.post(
                    f"{EDGE_BASE}/api/translate/batch",
                    json={"texts": [text[:1800]]},
                    headers={
                        "User-Agent": "LexoraOpenLexicon/1.0 (open-data enrichment)",
                        **(
                            {"X-Lexora-Origin-Token": EDGE_TOKEN}
                            if EDGE_TOKEN
                            else {}
                        ),
                    },
                )
                if response.status_code == 200:
                    values = response.json().get("translations", [])
                    translated = str(values[0]).strip() if values else ""
                    return translated, 200, None if translated else "translation missing"
                if response.status_code not in (429, 500, 502, 503, 504):
                    return "", response.status_code, f"HTTP {response.status_code}"
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                if attempt == 2:
                    return "", None, str(exc)
            finally:
                gate.semaphore.release()
            await asyncio.sleep(min(8.0, 2 ** attempt))
        return "", None, "translation exhausted retries"
    url = "https://api.mymemory.translated.net/get?q=" + quote(text, safe="") + "&langpair=en|zh-CN"
    data, status, error = await request_json(client, gate, url, "translation")
    try:
        return str(data["responseData"]["translatedText"]), status, error
    except Exception:
        return "", status, error or "translation missing"

async def run(dataset: Path, state_path: Path, limit: int, delay: float, workers: int, translation_delay: float, retry_after_hours: float, start_id: int | None, end_id: int | None, shard_index: int | None, shard_count: int) -> None:
    state = init_state(state_path)
    workers = max(1, workers)
    gates = {
        "edge": HostGate(delay, min(4, workers)),
        "dictionary": HostGate(delay, min(4, workers)),
        "datamuse": HostGate(delay, min(8, workers)),
        "translation": HostGate(max(delay, translation_delay), min(2, workers)),
    }
    client_timeout = httpx.Timeout(20.0, connect=10.0)
    client = httpx.AsyncClient(timeout=client_timeout, follow_redirects=True)
    edge_batcher = (
        EdgeDictionaryBatcher(client, gates["edge"], batch_size=workers)
        if EDGE_BASE
        else None
    )
    translation_batcher = (
        EdgeTranslationBatcher(client, gates["translation"], batch_size=workers)
        if EDGE_BASE
        else None
    )
    try:
        db = sqlite3.connect(dataset)
        ensure_dataset_columns(db)
        if shard_index is not None:
            if shard_count < 1 or not 0 <= shard_index < shard_count:
                raise ValueError("--shard-index must be within [0, --shard-count)")
            min_id, max_id = db.execute("SELECT COALESCE(MIN(id), 0), COALESCE(MAX(id), -1) FROM entries").fetchone()
            total = max(0, max_id - min_id + 1)
            start_id = min_id + (total * shard_index) // shard_count
            end_id = min_id + (total * (shard_index + 1)) // shard_count - 1
        where: list[str] = []
        params: list[int] = []
        if start_id is not None:
            where.append("id >= ?"); params.append(start_id)
        if end_id is not None:
            where.append("id <= ?"); params.append(end_id)
        query = "SELECT id,word,normalized_word,definition,definition_zh,us_phonetic,uk_phonetic,synonyms_json,antonyms_json,examples_json,phrases_json,phrase_entries_json,related_words_json,related_entries_json,frequency,difficulty,enrichment_json FROM entries"
        if shard_index is not None:
            # Each rank is unique, so scanning the existing frequency index
            # lets both shards complete the future 20k snapshot first without
            # allocating a large temporary sort on a 1 GB micro instance.
            query = query.replace(
                " FROM entries",
                " FROM entries INDEXED BY idx_entries_freq",
            )
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY frequency_rank" if shard_index is not None else " ORDER BY id"
        # Stream the shard instead of materializing hundreds of thousands of
        # rows.  This keeps the worker within an E2.1.Micro's 1 GB memory
        # envelope and still allows the cursor to resume after each commit.
        rows = db.execute(query, params)
        processed = 0
        async def process_row(row: tuple[Any, ...]) -> tuple[str, str]:
            entry_id, word, term, definition, definition_zh, us, uk, synonyms_json, antonyms_json, examples_json, phrases_json, phrase_entries_json, related_json, related_entries_json, freq, diff, enrich_json = row
            marker = json.loads(enrich_json or "{}")
            existing = {
                "definition": definition or "",
                "us": "" if needs_phonetic_repair(us) else normalize_phonetic(us),
                "uk": "" if needs_phonetic_repair(uk) else normalize_phonetic(uk),
                "examples": json.loads(examples_json or "[]"),
                "phrases": json.loads(phrases_json or "[]"),
                "phrase_entries": json.loads(phrase_entries_json or "[]"),
                "synonyms": json.loads(synonyms_json or "[]"),
                "antonyms": json.loads(antonyms_json or "[]"),
                "related": json.loads(related_json or "[]"),
                "related_entries": json.loads(related_entries_json or "[]"),
            }
            data = await enrich_term(
                client,
                gates,
                term,
                state,
                existing,
                edge_batcher=edge_batcher,
            )
            definition = definition or data.get("definition", "")
            if needs_phonetic_repair(us):
                us = normalize_phonetic(
                    data.get("us", "") or data.get("us_phonetic", "")
                ) or normalize_phonetic(us)
            else:
                us = normalize_phonetic(us)
            if needs_phonetic_repair(uk):
                uk = normalize_phonetic(
                    data.get("uk", "") or data.get("uk_phonetic", "")
                ) or normalize_phonetic(uk)
            else:
                uk = normalize_phonetic(uk)
            synonyms = unique([*json.loads(synonyms_json or "[]"), *data.get("synonyms", [])])
            antonyms = unique([*json.loads(antonyms_json or "[]"), *data.get("antonyms", [])])
            examples = unique([*json.loads(examples_json or "[]"), *data.get("examples", [])])
            phrases = unique([*json.loads(phrases_json or "[]"), *data.get("phrases", [])])
            phrase_entries = merge_named_entries(
                json.loads(phrase_entries_json or "[]"),
                data.get("phrase_entries", []),
            )
            related = unique([*json.loads(related_json or "[]"), *data.get("related", [])])
            related_entries = merge_named_entries(
                json.loads(related_entries_json or "[]"),
                data.get("related_entries", []),
            )
            zh = definition_zh
            if needs_definition_translation(definition, zh):
                translated_zh, status, error = await translate(
                    client,
                    gates["translation"],
                    definition[:1800],
                    translation_batcher=translation_batcher,
                )
                if translated_zh:
                    zh = translated_zh
                state.execute("""INSERT INTO provider_state(term,source,status,attempts,http_status,last_error,updated_at) VALUES(?,?,?,?,?,?,?)
                  ON CONFLICT(term,source) DO UPDATE SET status=excluded.status,attempts=provider_state.attempts+1,http_status=excluded.http_status,last_error=excluded.last_error,updated_at=excluded.updated_at""",
                  (term, "translation", "completed" if translated_zh else "failed", 1, status, error, now()))
                data.setdefault("_statuses", []).append("completed" if translated_zh else "failed")
            base_frequency = 0.0 if needs_frequency_repair(freq) else float(freq or 0)
            score = max(
                base_frequency,
                float(data.get("frequency", 0) or 0),
            )
            statuses = data.pop("_statuses", [])
            attempted = data.pop("_attempted", [])
            marker_status = "completed" if not attempted or all(item == "completed" for item in statuses) else ("partial" if any(item == "completed" for item in statuses) else "not_found")
            marker = {"status": marker_status, "lastAttempt": now(), "sources": sorted(set(["open-data", "network"]))}
            db.execute("""UPDATE entries SET definition=?,definition_zh=?,us_phonetic=?,uk_phonetic=?,synonyms_json=?,antonyms_json=?,examples_json=?,phrases_json=?,phrase_entries_json=?,related_words_json=?,related_entries_json=?,frequency=?,difficulty=?,enrichment_json=? WHERE id=?""",
              (definition, zh, us, uk, j(synonyms), j(antonyms), j(examples), j(phrases), j(phrase_entries), j(related), j(related_entries), score, diff or difficulty(score), j(marker), entry_id))
            db.execute("""INSERT OR REPLACE INTO entries_fts(rowid,word,definition,definition_zh,examples,phrases)
              SELECT id,word,definition,definition_zh,examples_json,phrases_json FROM entries WHERE id=?""", (entry_id,))
            db.commit(); state.commit()
            return term, marker_status
        pending: list[tuple[Any, ...]] = []
        for row in rows:
            if limit and processed + len(pending) >= limit:
                break
            needs_quality_repair = (
                needs_phonetic_repair(row[5])
                or needs_phonetic_repair(row[6])
                or needs_definition_translation(row[3], row[4])
                or needs_frequency_repair(row[14])
            )
            if (
                not should_process_marker(row[-1], retry_after_hours)
                and not needs_quality_repair
            ):
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
    ap.add_argument("--state", type=Path, default=STATE, help="provider state SQLite path")
    ap.add_argument("--limit", type=int, default=0, help="0 means all pending terms")
    ap.add_argument("--delay", type=float, default=0.5, help="minimum seconds between requests per provider host")
    ap.add_argument("--workers", type=int, default=16, help="number of terms processed concurrently")
    ap.add_argument("--translation-delay", type=float, default=1.0, help="minimum seconds between translation requests")
    ap.add_argument("--retry-after-hours", type=float, default=24.0, help="retry partial/not-found terms only after this many hours")
    ap.add_argument("--start-id", type=int, default=None, help="inclusive entry ID start")
    ap.add_argument("--end-id", type=int, default=None, help="inclusive entry ID end")
    ap.add_argument("--shard-index", type=int, default=None, help="zero-based shard index")
    ap.add_argument("--shard-count", type=int, default=1, help="number of contiguous shards")
    args = ap.parse_args()
    if args.shard_index is not None and (args.start_id is not None or args.end_id is not None):
        ap.error("use either --shard-index/--shard-count or --start-id/--end-id, not both")
    asyncio.run(run(args.dataset, args.state, args.limit, args.delay, args.workers, args.translation_delay, args.retry_after_hours, args.start_id, args.end_id, args.shard_index, args.shard_count))

if __name__ == "__main__":
    main()
