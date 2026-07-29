#!/usr/bin/env python3
"""Build the uncapped open-data vocabulary scope used for Oxford-oriented
coverage.  This is intentionally separate from the original 600k snapshot.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from build_dataset import WORD_RE, SOURCE, BUILD, clean_list, extract_kaikki, frequency_score, json_text, norm

ROOT = Path(__file__).resolve().parents[1]
FULL_NAME = "lexora-open-oxford-scope.sqlite"
TOP_NAME = "lexora-open-oxford-frequency-20k.sqlite"
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
  phrase_entries_json TEXT NOT NULL DEFAULT '[]',
  related_words_json TEXT NOT NULL DEFAULT '[]',
  related_entries_json TEXT NOT NULL DEFAULT '[]',
  senses_json TEXT NOT NULL DEFAULT '[]',
  source_json TEXT NOT NULL DEFAULT '[]',
  scope_json TEXT NOT NULL DEFAULT '{}',
  enrichment_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_entries_word ON entries(word);
CREATE INDEX IF NOT EXISTS idx_entries_freq ON entries(frequency_rank, frequency);
CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(word,definition,definition_zh,examples,phrases,tokenize='unicode61');
"""

FIELDS = (
    "synonyms_json",
    "antonyms_json",
    "examples_json",
    "phrases_json",
    "phrase_entries_json",
    "related_words_json",
    "related_entries_json",
    "senses_json",
)

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def merge_values(old: str, new: list[str], limit: int = 40) -> str:
    try:
        values = json.loads(old or "[]")
    except Exception:
        values = []
    return json_text(clean_list([*values, *new], limit))

def insert_ecdict(db: sqlite3.Connection) -> int:
    count = 0
    with (SOURCE / "ecdict.csv").open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            word = (row.get("word") or "").strip()
            key = norm(word)
            if not key or len(key) > 120 or not WORD_RE.match(word):
                continue
            db.execute("""INSERT OR IGNORE INTO entries(
              word,normalized_word,pos,definition,definition_zh,us_phonetic,uk_phonetic,
              phrases_json,source_json,scope_json)
              VALUES(?,?,?,?,?,?,?,?,?,?)""", (
                word, key, row.get("pos") or "", row.get("definition") or "",
                row.get("translation") or "", row.get("phonetic") or "", row.get("phonetic") or "",
                json_text([word] if " " in word or "-" in word else []),
                json_text(["ecdict"]), json_text({"scope": "open-proxy", "ecdictOxfordFlag": row.get("oxford") or ""})))
            count += 1
            if count % 5000 == 0:
                db.commit()
    db.commit()
    return count

def flush_kaikki(db: sqlite3.Connection, key: str | None, aggregate: dict[str, Any] | None) -> None:
    if not key or not aggregate:
        return
    row = db.execute("""SELECT word,pos,definition,definition_zh,us_phonetic,uk_phonetic,
      synonyms_json,antonyms_json,examples_json,phrases_json,related_words_json,senses_json,
      source_json,scope_json FROM entries WHERE normalized_word=?""", (key,)).fetchone()
    if row:
        source = clean_list([*json.loads(row[12] or "[]"), "kaikki"])
        scope = json.loads(row[13] or "{}")
        scope["kaikki"] = True
        db.execute("""UPDATE entries SET pos=?,definition=?,us_phonetic=?,uk_phonetic=?,
          synonyms_json=?,antonyms_json=?,examples_json=?,phrases_json=?,related_words_json=?,
          senses_json=?,source_json=?,scope_json=? WHERE normalized_word=?""", (
            row[1] or aggregate["pos"],
            "\n".join(clean_list([row[2] or "", aggregate["definition"]], 40)),
            row[4] or aggregate["us"], row[5] or aggregate["uk"],
            merge_values(row[6], aggregate["synonyms"]), merge_values(row[7], aggregate["antonyms"]),
            merge_values(row[8], aggregate["examples"]), merge_values(row[9], aggregate["phrases"]),
            merge_values(row[10], aggregate["related"]), json_text([*json.loads(row[11] or "[]"), *aggregate["senses"]]),
            json_text(source), json_text(scope), key))
    else:
        db.execute("""INSERT INTO entries(word,normalized_word,pos,definition,us_phonetic,uk_phonetic,
          synonyms_json,antonyms_json,examples_json,phrases_json,related_words_json,senses_json,
          source_json,scope_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            key, key, aggregate["pos"], aggregate["definition"], aggregate["us"], aggregate["uk"],
            json_text(aggregate["synonyms"]), json_text(aggregate["antonyms"]), json_text(aggregate["examples"]),
            json_text(aggregate["phrases"]), json_text(aggregate["related"]), json_text(aggregate["senses"]),
            json_text(["kaikki"]), json_text({"scope": "open-proxy", "kaikki": True})))

def merge_kaikki(db: sqlite3.Connection) -> int:
    path = SOURCE / "enwiktionary-wiktextract.jsonl.gz"
    current: str | None = None
    aggregate: dict[str, Any] | None = None
    updated = 0
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            if data.get("lang_code") != "en":
                continue
            word = str(data.get("word") or "").strip()
            key = norm(word)
            if not key or len(key) > 120 or not WORD_RE.match(word):
                continue
            item = extract_kaikki(data)
            if key != current:
                flush_kaikki(db, current, aggregate)
                current, aggregate = key, item
                updated += 1
            else:
                aggregate["definition"] = "\n".join(clean_list([aggregate["definition"], item["definition"]], 40))
                for field in ("synonyms", "antonyms", "examples", "phrases", "related"):
                    aggregate[field] = clean_list([*aggregate[field], *item[field]])
                aggregate["senses"] = [*aggregate["senses"], *item["senses"]][:40]
                aggregate["us"] = aggregate["us"] or item["us"]
                aggregate["uk"] = aggregate["uk"] or item["uk"]
            if updated % 5000 == 0:
                db.commit()
    flush_kaikki(db, current, aggregate)
    db.commit()
    return updated

def assign_frequency(db: sqlite3.Connection) -> None:
    rows = db.execute("SELECT id,word FROM entries").fetchall()
    try:
        from wordfreq import zipf_frequency  # type: ignore
    except Exception:
        zipf_frequency = lambda _word, _lang: 0.0
    scored = [(float(zipf_frequency(word, "en")) or -20.0, row_id) for row_id, word in rows]
    scored.sort(key=lambda value: (-value[0], value[1]))
    db.execute("BEGIN")
    for rank, (score, row_id) in enumerate(scored, 1):
        db.execute("UPDATE entries SET frequency=?,frequency_rank=? WHERE id=?", (score, rank, row_id))
    db.commit()

def rebuild_fts(db: sqlite3.Connection) -> None:
    db.execute("DELETE FROM entries_fts")
    db.execute("""INSERT INTO entries_fts(rowid,word,definition,definition_zh,examples,phrases)
      SELECT id,word,definition,definition_zh,examples_json,phrases_json FROM entries""")
    db.commit()

def make_top(full: Path, top: Path) -> None:
    if top.exists():
        top.unlink()
    src = sqlite3.connect(full)
    src.row_factory = sqlite3.Row
    dst = sqlite3.connect(top)
    dst.executescript(SCHEMA)
    columns = ["word","normalized_word","pos","difficulty","frequency","frequency_rank","us_phonetic","uk_phonetic","definition","definition_zh","synonyms_json","antonyms_json","examples_json","phrases_json","phrase_entries_json","related_words_json","related_entries_json","senses_json","source_json","scope_json","enrichment_json"]
    query = "INSERT INTO entries(" + ",".join(columns) + ") VALUES(" + ",".join("?" for _ in columns) + ")"
    for row in src.execute("SELECT " + ",".join(columns) + " FROM entries ORDER BY frequency_rank LIMIT 20000"):
        dst.execute(query, tuple(row[column] for column in columns))
    rebuild_fts(dst)
    dst.execute("PRAGMA journal_mode=DELETE")
    dst.commit(); dst.close(); src.close()

def main() -> None:
    BUILD.mkdir(exist_ok=True)
    full = BUILD / FULL_NAME
    if full.exists():
        full.unlink()
    db = sqlite3.connect(full)
    db.executescript(SCHEMA)
    ecdict_count = insert_ecdict(db)
    kaikki_count = merge_kaikki(db)
    assign_frequency(db)
    rebuild_fts(db)
    db.execute("PRAGMA journal_mode=DELETE"); db.commit(); db.close()
    top = BUILD / TOP_NAME
    make_top(full, top)
    manifest = {
        "schema_version": 2,
        "scope": "open-data approximation of Oxford-oriented vocabulary",
        "built_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "candidate_sources": ["ECDICT", "English Wiktionary/Wiktextract", "wordfreq"],
        "counts": {"ecdict_rows_seen": ecdict_count, "kaikki_words_seen": kaikki_count},
        "datasets": {},
    }
    for key, path in (("scope", full), ("frequency_20k", top)):
        c = sqlite3.connect(path)
        manifest["datasets"][key] = {"file": path.name, "rows": c.execute("SELECT COUNT(*) FROM entries").fetchone()[0], "bytes": path.stat().st_size, "sha256": sha256(path)}
        c.close()
    (BUILD / "oxford-scope-manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
