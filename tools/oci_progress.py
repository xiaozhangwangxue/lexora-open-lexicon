#!/usr/bin/env python3
"""Show resumable enrichment progress for one shard or the merged dataset.

The enrichment worker writes its durable marker into ``entries.enrichment_json``
and provider-level attempts into a separate SQLite state database.  This tool
only reads both databases, so it is safe to run while a worker is active.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path


def progress(dataset: Path, state: Path, start_id: int | None, end_id: int | None,
             shard_index: int | None, shard_count: int) -> dict[str, object]:
    db = sqlite3.connect(f"file:{dataset}?mode=ro", uri=True)
    try:
        if shard_index is not None:
            if shard_count < 1 or not 0 <= shard_index < shard_count:
                raise ValueError("--shard-index must be within [0, --shard-count)")
            min_id, max_id = db.execute(
                "SELECT COALESCE(MIN(id), 0), COALESCE(MAX(id), -1) FROM entries"
            ).fetchone()
            total_ids = max(0, max_id - min_id + 1)
            start_id = min_id + (total_ids * shard_index) // shard_count
            end_id = min_id + (total_ids * (shard_index + 1)) // shard_count - 1
        clauses: list[str] = []
        args: list[int] = []
        if start_id is not None:
            clauses.append("id >= ?"); args.append(start_id)
        if end_id is not None:
            clauses.append("id <= ?"); args.append(end_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        total = int(db.execute(f"SELECT count(*) FROM entries{where}", args).fetchone()[0])
        rows = db.execute(
            f"SELECT COALESCE(json_extract(enrichment_json, '$.status'), 'pending'), count(*) "
            f"FROM entries{where} GROUP BY 1 ORDER BY 1", args
        ).fetchall()
        statuses = {str(k): int(v) for k, v in rows}
    finally:
        db.close()

    state_db = sqlite3.connect(f"file:{state}?mode=ro", uri=True)
    try:
        provider_rows = state_db.execute(
            "SELECT status, count(*) FROM provider_state GROUP BY status ORDER BY status"
        ).fetchall()
        attempts = int(state_db.execute("SELECT COALESCE(sum(attempts),0) FROM provider_state").fetchone()[0])
    finally:
        state_db.close()
    finished = sum(v for k, v in statuses.items() if k in {"completed", "partial", "not_found"})
    return {
        "dataset": str(dataset), "state": str(state), "total": total,
        "finished": finished, "remaining": max(0, total - finished),
        "percent": round((finished / total * 100) if total else 100.0, 3),
        "entry_status": statuses,
        "provider_status": {str(k): int(v) for k, v in provider_rows},
        "provider_attempts": attempts,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", type=Path, required=True)
    ap.add_argument("--state", type=Path, required=True)
    ap.add_argument("--start-id", type=int)
    ap.add_argument("--end-id", type=int)
    ap.add_argument("--shard-index", type=int)
    ap.add_argument("--shard-count", type=int, default=4)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    if args.shard_index is not None and (args.start_id is not None or args.end_id is not None):
        ap.error("use either --shard-index/--shard-count or --start-id/--end-id, not both")
    result = progress(args.dataset, args.state, args.start_id, args.end_id, args.shard_index, args.shard_count)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return
    print(f"{result['finished']}/{result['total']} ({result['percent']}%)  remaining={result['remaining']}")
    print("entry_status=" + json.dumps(result["entry_status"], ensure_ascii=False, sort_keys=True))
    print("provider_status=" + json.dumps(result["provider_status"], ensure_ascii=False, sort_keys=True))
    print(f"provider_attempts={result['provider_attempts']}  checked_at={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")


if __name__ == "__main__":
    main()
