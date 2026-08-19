#!/usr/bin/env python3
"""Validate one complete fixed repair shard before starting any HTTP work."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path


def _read_only(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--repair-queue", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--tools", type=Path, required=True)
    args = parser.parse_args()

    sys.path.insert(0, str(args.tools.resolve()))
    from enrich_oxford_scope import (
        open_repair_queue,
        preflight_repair_queue_shard,
    )

    dataset = _read_only(args.dataset)
    queue = open_repair_queue(args.repair_queue)
    try:
        rows, metadata = preflight_repair_queue_shard(
            dataset,
            queue,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
        )
    finally:
        queue.close()
        dataset.close()

    expected_digest = os.environ.get("LEXORA_CANDIDATE_DIGEST", "")
    actual_digest = str(metadata.get("candidate_digest") or "")
    if not expected_digest or actual_digest != expected_digest:
        raise RuntimeError("repair queue candidate digest does not match release")
    print(
        json.dumps(
            {
                "candidateDigest": actual_digest,
                "preflightRows": len(rows),
                "shardCount": args.shard_count,
                "shardIndex": args.shard_index,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
