#!/usr/bin/env python3
"""Atomically cache one shard's exact progress for lightweight clients."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path

try:
    from oci_progress import progress
except ModuleNotFoundError:
    from tools.oci_progress import progress


def write_snapshot(
    dataset: Path,
    state: Path,
    output: Path,
    shard_index: int,
    shard_count: int,
) -> dict[str, object]:
    result = progress(
        dataset=dataset,
        state=state,
        start_id=None,
        end_id=None,
        shard_index=shard_index,
        shard_count=shard_count,
    )
    result["updatedAt"] = dt.datetime.now(dt.timezone.utc).isoformat()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, ensure_ascii=False, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, default=2)
    args = parser.parse_args()
    result = write_snapshot(
        dataset=args.dataset,
        state=args.state,
        output=args.output,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
