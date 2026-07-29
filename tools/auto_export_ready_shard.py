#!/usr/bin/env python3
"""Atomically export a shard once every requested entry is enriched."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from export_enrichment_shard import export_delta, shard_bounds

FINISHED_STATUSES = ("completed", "partial", "not_found")


def export_if_ready(
    dataset: Path,
    output: Path,
    shard_index: int,
    shard_count: int,
    frequency_limit: int | None,
) -> str:
    source = sqlite3.connect(f"file:{dataset}?mode=ro", uri=True)
    try:
        start, end = shard_bounds(source, shard_index, shard_count)
        clauses = ["id BETWEEN ? AND ?"]
        arguments: list[int] = [start, end]
        if frequency_limit is not None:
            clauses.append("frequency_rank <= ?")
            arguments.append(frequency_limit)
        where = " AND ".join(clauses)
        total = int(
            source.execute(
                f"SELECT count(*) FROM entries WHERE {where}", arguments
            ).fetchone()[0]
        )
        placeholders = ",".join("?" for _ in FINISHED_STATUSES)
        finished = int(
            source.execute(
                f"SELECT count(*) FROM entries WHERE {where} "
                f"AND json_extract(enrichment_json, '$.status') IN ({placeholders})",
                [*arguments, *FINISHED_STATUSES],
            ).fetchone()[0]
        )
    finally:
        source.close()

    if finished != total:
        return f"waiting finished={finished} total={total}"

    if output.exists():
        destination = sqlite3.connect(f"file:{output}?mode=ro", uri=True)
        try:
            exported = int(
                destination.execute("SELECT count(*) FROM entries").fetchone()[0]
            )
        finally:
            destination.close()
        if exported != total:
            raise RuntimeError(
                f"existing export has {exported} rows, expected {total}: {output}"
            )
        return f"ready rows={exported} output={output}"

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    if temporary.exists():
        temporary.unlink()
    try:
        exported = export_delta(
            dataset,
            temporary,
            shard_index,
            shard_count,
            frequency_limit,
        )
        if exported != total:
            raise RuntimeError(f"exported {exported} rows, expected {total}")
        temporary.replace(output)
    finally:
        if temporary.exists():
            temporary.unlink()
    return f"created rows={exported} output={output}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--frequency-limit", type=int)
    args = parser.parse_args()
    print(
        export_if_ready(
            args.dataset,
            args.output,
            args.shard_index,
            args.shard_count,
            args.frequency_limit,
        )
    )


if __name__ == "__main__":
    main()
