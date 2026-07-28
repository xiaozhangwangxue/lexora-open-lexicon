#!/usr/bin/env python3
"""Build the two Lexora open lexicon SQLite snapshots.

The builder is deliberately streaming: the multi-gigabyte Wiktionary JSONL
dump is never loaded into memory.  ECDICT supplies the deterministic base
set and Chinese fields; English Wiktionary data augments the same headwords.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import json
import math
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "source"
BUILD = ROOT / "build"
WORD_RE = re.compile(r"^[A-Za-z][A-Za-z' .-]*$")

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS entries (
  id INTEGER PRIMARY KEY,
  word TEXT NOT NULL,
  normalized_word TEXT NOT NULL UNIQUE,
  pos TEXT,
  difficulty TEXT,
  frequency REAL,
  frequency_rank INTEGER,
  us_phonetic TEXT,
  uk_phonetic TEXT,
  definition TEXT,
  definition_zh TEXT,
  synonyms_json TEXT NOT NULL DEFAULT '[]',
  antonyms_json TEXT NOT NULL DEFAULT '[]',
  examples_json TEXT NOT NULL DEFAULT '[]',
  phrases_json TEXT NOT NULL DEFAULT '[]',
  related_words_json TEXT NOT NULL DEFAULT '[]',
  senses_json TEXT NOT NULL DEFAULT '[]',
  source_json TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_entries_word ON entries(word);
CREATE INDEX IF NOT EXISTS idx_entries_freq ON entries(frequency_rank, frequency);
CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
  word, definition, definition_zh, examples, phrases,
  tokenize='unicode61'
);
"""

def norm(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower()).replace("’", "'")

def clean_list(values: Iterable[str], limit: int = 40) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        value = re.sub(r"\s+", " ", str(value).strip())
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
        if len(out) >= limit:
            break
    return out

def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def frequency_score(word: str, row: dict[str, str]) -> float:
    """Return a higher-is-more-common score, with deterministic fallbacks."""
    try:
        from wordfreq import zipf_frequency  # type: ignore
        score = float(zipf_frequency(word, "en"))
        if score > 0:
            return score
    except Exception:
        pass
    try:
        frq = float(row.get("frq") or 0)
        if frq > 0:
            # Keep fallback-ranked entries below any wordfreq-observed entry;
            # ECDICT's frq is a rank, not a comparable Zipf score.
            return -math.log10(frq + 1)
    except ValueError:
        pass
    try:
        bnc = float(row.get("bnc") or 0)
        if bnc > 0:
            return -10.0 - math.log10(bnc + 1)
    except ValueError:
        pass
    return 0.0

def read_ecdict() -> dict[str, dict[str, Any]]:
    path = SOURCE / "ecdict.csv"
    records: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            word = (row.get("word") or "").strip()
            key = norm(word)
            if not key or not WORD_RE.match(word) or len(key) > 80:
                continue
            # Apostrophe/hyphen variants are accepted, but one canonical row wins.
            if key not in records or len((row.get("definition") or "")) > len(records[key].get("definition", "")):
                records[key] = {
                    "word": word,
                    "pos": row.get("pos") or "",
                    "definition": row.get("definition") or "",
                    "translation": row.get("translation") or "",
                    "phonetic": row.get("phonetic") or "",
                    "frq": row.get("frq") or "",
                    "bnc": row.get("bnc") or "",
                    "oxford": row.get("oxford") or "",
                }
    return records

def choose_records(records: dict[str, dict[str, Any]], count: int) -> tuple[list[str], dict[str, float]]:
    scores = {key: frequency_score(row["word"], row) for key, row in records.items()}
    eligible = [key for key, row in records.items() if row["definition"] or row["translation"]]
    eligible.sort(key=lambda key: (-scores[key], key))
    if len(eligible) < count:
        rest = sorted(set(records) - set(eligible))
        eligible.extend(rest)
    return eligible[:count], scores

def parse_sounds(sounds: list[dict[str, Any]]) -> tuple[str, str]:
    us: list[str] = []
    uk: list[str] = []
    for sound in sounds or []:
        ipa = sound.get("ipa") or sound.get("enpr")
        if not ipa:
            continue
        tags = {str(x).lower() for x in sound.get("tags", [])}
        if "us" in tags or "general-american" in tags or "american" in tags:
            us.append(str(ipa))
        elif "received-pronunciation" in tags or "british" in tags or "uk" in tags:
            uk.append(str(ipa))
    return (clean_list(us, 3)[0] if us else "", clean_list(uk, 3)[0] if uk else "")

def links(items: list[dict[str, Any]] | None) -> list[str]:
    return clean_list([(x.get("word") or "") for x in (items or [])])

def extract_kaikki(data: dict[str, Any]) -> dict[str, Any]:
    definitions: list[str] = []
    examples: list[str] = []
    synonyms: list[str] = links(data.get("synonyms"))
    antonyms: list[str] = links(data.get("antonyms"))
    related: list[str] = []
    senses: list[dict[str, Any]] = []
    for field in ("derived", "related", "hypernyms", "hyponyms", "coordinate_terms", "meronyms"):
        related.extend(links(data.get(field)))
    for sense in data.get("senses", []) or []:
        glosses = sense.get("glosses") or sense.get("raw_glosses") or []
        definitions.extend(str(x) for x in glosses)
        synonyms.extend(links(sense.get("synonyms")))
        antonyms.extend(links(sense.get("antonyms")))
        related.extend(links(sense.get("related")))
        for ex in sense.get("examples", []) or []:
            if ex.get("text") and ex.get("type", "example") == "example":
                examples.append(str(ex["text"]))
        if glosses:
            senses.append({"pos": data.get("pos") or "", "definitions": clean_list(glosses, 12)})
    us, uk = parse_sounds(data.get("sounds", []))
    return {
        "pos": data.get("pos") or "",
        "definition": "\n".join(clean_list(definitions, 24)),
        "examples": clean_list(examples, 12),
        "synonyms": clean_list(synonyms),
        "antonyms": clean_list(antonyms),
        "related": clean_list(related),
        "phrases": clean_list([x for x in related if " " in x or "-" in x]),
        "senses": senses[:24],
        "us": us,
        "uk": uk,
    }

def merge_kaikki(db: sqlite3.Connection, selected: set[str]) -> int:
    path = SOURCE / "enwiktionary-wiktextract.jsonl.gz"
    updated = 0
    current_key = None
    aggregate: dict[str, Any] | None = None
    def flush() -> None:
        nonlocal updated, aggregate, current_key
        if not current_key or not aggregate:
            return
        row = db.execute("SELECT pos,definition,us_phonetic,uk_phonetic,synonyms_json,antonyms_json,examples_json,phrases_json,related_words_json,senses_json,source_json FROM entries WHERE normalized_word=?", (current_key,)).fetchone()
        if not row:
            return
        def combine_json(index: int, new: list[str]) -> list[str]:
            old = json.loads(row[index] or "[]")
            return clean_list([*old, *new])
        sources = clean_list([*json.loads(row[10] or "[]"), "kaikki"])
        db.execute("""UPDATE entries SET pos=?, definition=?, us_phonetic=?, uk_phonetic=?,
          synonyms_json=?, antonyms_json=?, examples_json=?, phrases_json=?, related_words_json=?, senses_json=?, source_json=?
          WHERE normalized_word=?""", (
            row[0] or aggregate["pos"],
            "\n".join(clean_list([row[1] or "", aggregate["definition"]], 24)),
            row[2] or aggregate["us"], row[3] or aggregate["uk"],
            json_text(combine_json(4, aggregate["synonyms"])),
            json_text(combine_json(5, aggregate["antonyms"])),
            json_text(combine_json(6, aggregate["examples"])),
            json_text(combine_json(7, aggregate["phrases"])),
            json_text(combine_json(8, aggregate["related"])),
            json_text([*json.loads(row[9] or "[]"), *aggregate["senses"]]),
            json_text(sources), current_key))
        updated += 1
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            if data.get("lang_code") != "en":
                continue
            key = norm(str(data.get("word") or ""))
            if key not in selected:
                continue
            if key != current_key:
                flush()
                current_key = key
                aggregate = extract_kaikki(data)
            else:
                item = extract_kaikki(data)
                for k in ("definition",):
                    aggregate[k] = "\n".join(clean_list([aggregate[k], item[k]], 24))
                for k in ("synonyms", "antonyms", "examples", "related", "phrases", "senses"):
                    aggregate[k].extend(item[k])
                    aggregate[k] = clean_list(aggregate[k]) if k != "senses" else aggregate[k][:24]
                aggregate["us"] = aggregate["us"] or item["us"]
                aggregate["uk"] = aggregate["uk"] or item["uk"]
        flush()
    return updated

def make_db(name: str, keys: list[str], records: dict[str, dict[str, Any]], scores: dict[str, float], metadata: dict[str, Any]) -> Path:
    path = BUILD / name
    if path.exists():
        path.unlink()
    db = sqlite3.connect(path)
    db.executescript(SCHEMA)
    for rank, key in enumerate(keys, 1):
        row = records[key]
        db.execute("""INSERT INTO entries(word,normalized_word,pos,frequency,frequency_rank,definition,definition_zh,us_phonetic,uk_phonetic,phrases_json,source_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?)""", (row["word"], key, row["pos"], scores[key], rank, row["definition"], row["translation"], row["phonetic"], row["phonetic"], json_text([row["word"]] if " " in row["word"] or "-" in row["word"] else []), json_text(["ecdict"])))
    db.commit()
    merge_kaikki(db, set(keys))
    db.execute("DELETE FROM entries_fts")
    db.execute("""INSERT INTO entries_fts(rowid,word,definition,definition_zh,examples,phrases)
      SELECT id,word,definition,definition_zh,examples_json,phrases_json FROM entries""")
    db.execute("PRAGMA journal_mode=DELETE")
    db.commit()
    count = db.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
    db.close()
    metadata.update({"file": path.name, "rows": count, "bytes": path.stat().st_size, "sha256": sha256(path)})
    return path

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full-count", type=int, default=600_000)
    args = ap.parse_args()
    BUILD.mkdir(exist_ok=True)
    records = read_ecdict()
    keys, scores = choose_records(records, args.full_count)
    top_keys = keys[:20_000]
    stamp = dt.datetime.now(dt.timezone.utc).isoformat()
    manifest: dict[str, Any] = {"schema_version": 1, "built_at": stamp, "source": {"ecdict": "MIT", "kaikki": "Wiktionary/CC BY-SA", "frequency": "wordfreq Zipf with ECDICT fallback"}, "datasets": {}}
    make_db("lexora-english-600k.sqlite", keys, records, scores, manifest["datasets"].setdefault("full_600k", {}))
    make_db("lexora-frequency-20k.sqlite", top_keys, records, scores, manifest["datasets"].setdefault("frequency_20k", {}))
    (BUILD / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    report = {"ecdict_candidates": len(records), "full_rows": len(keys), "top_rows": len(top_keys), "full_size_bytes": manifest["datasets"]["full_600k"]["bytes"], "top_size_bytes": manifest["datasets"]["frequency_20k"]["bytes"]}
    (BUILD / "coverage-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
