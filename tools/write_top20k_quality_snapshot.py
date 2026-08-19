#!/usr/bin/env python3
"""Atomically cache one shard's top-20k quality report.

The report is intentionally generated out of band.  API requests only read the
small JSON snapshot and never scan the live SQLite database.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

TOOLS = Path(__file__).resolve().parent
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

from top20k_quality import quality_report  # noqa: E402


def write_quality_snapshot(
    dataset: Path,
    output: Path,
    shard_index: int,
    shard_count: int,
    unresolved_limit: int = 8,
) -> dict[str, object]:
    report = quality_report(
        dataset,
        max_frequency_rank=20_000,
        shard_index=shard_index,
        shard_count=shard_count,
        unresolved_limit=max(0, unresolved_limit),
    )
    report["updatedAt"] = dt.datetime.now(dt.timezone.utc).isoformat()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(report, stream, ensure_ascii=False, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, default=2)
    parser.add_argument("--unresolved-limit", type=int, default=8)
    args = parser.parse_args()
    result = write_quality_snapshot(
        dataset=args.dataset,
        output=args.output,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
        unresolved_limit=args.unresolved_limit,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
