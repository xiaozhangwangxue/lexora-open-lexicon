#!/usr/bin/env python3
"""Merge disjoint enriched SQLite shard copies into the canonical database.

Each shard must contain the same schema and a non-overlapping ID range.  The
canonical database is updated transactionally and its FTS rows are refreshed.
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


MUTABLE_COLUMNS = (
    "pos",
    "definition",
    "definition_zh",
    "us_phonetic",
    "uk_phonetic",
    "synonyms_json",
    "antonyms_json",
    "examples_json",
    "phrases_json",
    "phrase_entries_json",
    "related_words_json",
    "related_entries_json",
    "frequency",
    "difficulty",
    "enrichment_json",
)


def readonly_connection(path: Path) -> sqlite3.Connection:
    uri = f"{path.resolve().as_uri()}?mode=ro"
    database = sqlite3.connect(uri, uri=True)
    database.execute("PRAGMA query_only=ON")
    return database


def missing_entry_ids(
    database: sqlite3.Connection,
    entry_ids: list[int],
) -> list[int]:
    existing: set[int] = set()
    for offset in range(0, len(entry_ids), 900):
        chunk = entry_ids[offset:offset + 900]
        placeholders = ",".join("?" for _ in chunk)
        existing.update(
            int(row[0])
            for row in database.execute(
                f"SELECT id FROM entries WHERE id IN ({placeholders})",
                chunk,
            )
        )
    return [entry_id for entry_id in entry_ids if entry_id not in existing]


def merge(
    dataset: Path,
    shards: list[Path],
    batch_size: int = 1000,
) -> int:
    if not shards:
        raise ValueError("at least one shard is required")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    canonical_path = dataset.resolve()
    db = sqlite3.connect(dataset)
    try:
        db.execute("PRAGMA foreign_keys=ON")
        # Keep overlap tracking out of Python heap for multi-million-row merges.
        db.execute("PRAGMA temp_store=FILE")
        db.execute(
            """
            CREATE TEMP TABLE merged_entry_ids (
              id INTEGER PRIMARY KEY
            )
            """
        )
        db.execute("BEGIN IMMEDIATE")
        columns = ",".join(MUTABLE_COLUMNS)
        update_sql = (
            "UPDATE entries SET "
            + ",".join(f"{column}=?" for column in MUTABLE_COLUMNS)
            + " WHERE id=?"
        )
        fts_sql = (
            "INSERT OR REPLACE INTO entries_fts("
            "rowid,word,definition,definition_zh,examples,phrases"
            ") SELECT id,word,definition,definition_zh,"
            "examples_json,phrases_json FROM entries WHERE id=?"
        )
        merged_rows = 0

        for shard in shards:
            if shard.resolve() == canonical_path:
                raise ValueError("a shard cannot be the canonical dataset")
            shard_db = readonly_connection(shard)
            read_cursor: sqlite3.Cursor | None = None
            try:
                read_cursor = shard_db.execute(
                    f"SELECT id,{columns} FROM entries ORDER BY id"
                )
                read_cursor.arraysize = batch_size
                while True:
                    rows = read_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    entry_ids = [int(row[0]) for row in rows]
                    try:
                        db.executemany(
                            "INSERT INTO merged_entry_ids(id) VALUES(?)",
                            ((entry_id,) for entry_id in entry_ids),
                        )
                    except sqlite3.IntegrityError as error:
                        raise ValueError(
                            f"overlapping entry id in {shard}"
                        ) from error

                    updates = [
                        (*row[1:], entry_id)
                        for row, entry_id in zip(rows, entry_ids)
                    ]
                    update_cursor = db.executemany(update_sql, updates)
                    if update_cursor.rowcount != len(rows):
                        missing = missing_entry_ids(db, entry_ids)
                        detail = (
                            ",".join(str(entry_id) for entry_id in missing[:8])
                            if missing
                            else "unknown"
                        )
                        raise ValueError(
                            "shard entries did not match canonical rows: "
                            f"{detail} in {shard}"
                        )
                    db.executemany(
                        fts_sql,
                        ((entry_id,) for entry_id in entry_ids),
                    )
                    merged_rows += len(rows)
            finally:
                if read_cursor is not None:
                    read_cursor.close()
                shard_db.close()

        db.commit()
        print(f"merged_rows={merged_rows}")
        return merged_rows
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("shards", type=Path, nargs="+")
    args = parser.parse_args()
    merge(args.dataset, args.shards, args.batch_size)


if __name__ == "__main__":
    main()
