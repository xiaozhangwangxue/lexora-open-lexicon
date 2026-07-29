#!/usr/bin/env python3
"""Fail when an active enrichment shard has stopped making durable progress."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path


def parse_time(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--max-stale-minutes", type=float, default=45.0)
    args = parser.parse_args()

    if not args.state.is_file():
        raise SystemExit(f"state database is missing: {args.state}")
    database = sqlite3.connect(f"file:{args.state}?mode=ro", uri=True, timeout=10)
    try:
        row = database.execute(
            """
            SELECT term, source, status, updated_at
            FROM provider_state
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        database.close()
    if row is None:
        raise SystemExit("provider state has no durable progress rows")

    checked_at = dt.datetime.now(dt.timezone.utc)
    updated_at = parse_time(str(row[3]))
    stale_seconds = max(0.0, (checked_at - updated_at).total_seconds())
    result = {
        "term": row[0],
        "source": row[1],
        "status": row[2],
        "updatedAt": updated_at.isoformat(),
        "checkedAt": checked_at.isoformat(),
        "staleSeconds": round(stale_seconds, 1),
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if stale_seconds > max(60.0, args.max_stale_minutes * 60):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
