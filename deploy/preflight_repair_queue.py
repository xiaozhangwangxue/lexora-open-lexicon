#!/usr/bin/env python3
"""Validate one complete fixed repair shard before starting any HTTP work."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path


def _read_only(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)


def write_success_marker(path: Path, value: dict[str, object]) -> None:
    """Atomically publish readiness only after every preflight gate passed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--repair-queue", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--tools", type=Path, required=True)
    parser.add_argument("--release-id")
    parser.add_argument("--success-marker", type=Path)
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
    result: dict[str, object] = {
        "format": "lexora-top20k-preflight-v1",
        "candidateDigest": actual_digest,
        "releaseId": str(args.release_id or ""),
        "preflightRows": len(rows),
        "selectedOwnerRows": int(metadata["selected_owner_rows"]),
        "shardCount": args.shard_count,
        "shardIndex": args.shard_index,
        "processId": os.getpid(),
        "completedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    if args.success_marker is not None:
        if not args.release_id:
            raise RuntimeError("--release-id is required with --success-marker")
        write_success_marker(args.success_marker, result)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
