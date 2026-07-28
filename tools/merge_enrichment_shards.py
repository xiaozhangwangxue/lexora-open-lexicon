#!/usr/bin/env python3
"""Merge disjoint enriched SQLite shard copies into the canonical database.

Each shard must contain the same schema and a non-overlapping ID range.  The
canonical database is updated transactionally and its FTS rows are refreshed.
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def merge(dataset: Path, shards: list[Path]) -> None:
    if not shards:
        raise ValueError("at least one shard is required")
    db = sqlite3.connect(dataset)
    try:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("BEGIN IMMEDIATE")
        seen: set[int] = set()
        columns = (
            "definition,definition_zh,us_phonetic,uk_phonetic,synonyms_json,"
            "antonyms_json,examples_json,related_words_json,frequency,difficulty,"
            "enrichment_json"
        )
        attached: list[str] = []
        for shard in shards:
            # Keep each attachment alive until the transaction is committed.
            # Detaching inside the write transaction can fail with
            # ``database ... is locked`` while SQLite still has a prepared
            # statement for the attached FTS table.
            alias = f"sharddb{len(attached)}"
            db.execute(f"ATTACH DATABASE ? AS {alias}", (str(shard),))
            attached.append(alias)
            rows = db.execute(f"SELECT id,{columns} FROM {alias}.entries ORDER BY id").fetchall()
            for row in rows:
                entry_id = int(row[0])
                if entry_id in seen:
                    raise ValueError(f"overlapping entry id {entry_id} in {shard}")
                seen.add(entry_id)
                db.execute(
                    f"UPDATE entries SET {','.join(c+'=?' for c in columns.split(','))} WHERE id=?",
                    (*row[1:], entry_id),
                )
                db.execute(
                    "INSERT OR REPLACE INTO entries_fts(rowid,word,definition,definition_zh,examples,phrases) "
                    "SELECT id,word,definition,definition_zh,examples_json,phrases_json FROM entries WHERE id=?",
                    (entry_id,),
                )
        db.commit()
        for alias in attached:
            db.execute(f"DETACH DATABASE {alias}")
        print(f"merged_rows={len(seen)}")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("shards", type=Path, nargs="+")
    args = parser.parse_args()
    merge(args.dataset, args.shards)


if __name__ == "__main__":
    main()
