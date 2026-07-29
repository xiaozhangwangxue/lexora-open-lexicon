#!/usr/bin/env python3
"""Export a compact, mergeable delta from one enrichment shard."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

MUTABLE_COLUMNS = (
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


def shard_bounds(
    database: sqlite3.Connection, shard_index: int, shard_count: int
) -> tuple[int, int]:
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("shard index must be within shard count")
    min_id, max_id = database.execute(
        "SELECT COALESCE(MIN(id),0), COALESCE(MAX(id),-1) FROM entries"
    ).fetchone()
    total_ids = max(0, max_id - min_id + 1)
    start = min_id + (total_ids * shard_index) // shard_count
    end = min_id + (total_ids * (shard_index + 1)) // shard_count - 1
    return start, end


def export_delta(
    dataset: Path,
    output: Path,
    shard_index: int,
    shard_count: int,
    frequency_limit: int | None,
) -> int:
    if output.exists():
        raise FileExistsError(f"refusing to overwrite {output}")
    source = sqlite3.connect(f"file:{dataset}?mode=ro", uri=True)
    try:
        start, end = shard_bounds(source, shard_index, shard_count)
        clauses = ["id BETWEEN ? AND ?"]
        arguments: list[int] = [start, end]
        if frequency_limit is not None:
            clauses.append("frequency_rank <= ?")
            arguments.append(frequency_limit)
        where = " AND ".join(clauses)
        columns = ",".join(MUTABLE_COLUMNS)
        rows = source.execute(
            f"SELECT id,{columns} FROM entries WHERE {where} ORDER BY id",
            arguments,
        )

        destination = sqlite3.connect(output)
        try:
            definitions = ["id INTEGER PRIMARY KEY"]
            for column in MUTABLE_COLUMNS:
                kind = "REAL" if column == "frequency" else "TEXT"
                definitions.append(f"{column} {kind}")
            destination.execute(f"CREATE TABLE entries ({','.join(definitions)})")
            placeholders = ",".join("?" for _ in range(len(MUTABLE_COLUMNS) + 1))
            destination.executemany(
                f"INSERT INTO entries VALUES ({placeholders})",
                rows,
            )
            count = destination.execute("SELECT count(*) FROM entries").fetchone()[0]
            destination.execute("PRAGMA journal_mode=DELETE")
            destination.commit()
        finally:
            destination.close()
    finally:
        source.close()
    return int(count)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--frequency-limit", type=int)
    args = parser.parse_args()
    count = export_delta(
        args.dataset,
        args.output,
        args.shard_index,
        args.shard_count,
        args.frequency_limit,
    )
    print(f"exported_rows={count} output={args.output}")


if __name__ == "__main__":
    main()
