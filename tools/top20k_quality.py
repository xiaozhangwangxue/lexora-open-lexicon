#!/usr/bin/env python3
"""Report the exact quality gate used by the fast 20,000-entry lexicon."""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

from enrich_oxford_scope import entry_quality_gaps, is_phrase


def shard_bounds(
    database: sqlite3.Connection,
    shard_index: int | None,
    shard_count: int,
) -> tuple[int | None, int | None]:
    if shard_index is None:
        return None, None
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("--shard-index must be within [0, --shard-count)")
    minimum, maximum = database.execute(
        "SELECT COALESCE(MIN(id),0),COALESCE(MAX(id),-1) FROM entries"
    ).fetchone()
    total = max(0, int(maximum) - int(minimum) + 1)
    start = int(minimum) + (total * shard_index) // shard_count
    end = int(minimum) + (total * (shard_index + 1)) // shard_count - 1
    return start, end


def quality_report(
    dataset: Path,
    *,
    max_frequency_rank: int = 20_000,
    shard_index: int | None = None,
    shard_count: int = 2,
    unresolved_limit: int = 20,
) -> dict[str, Any]:
    database = sqlite3.connect(f"file:{dataset}?mode=ro", uri=True)
    try:
        start, end = shard_bounds(database, shard_index, shard_count)
        clauses = ["frequency_rank <= ?"]
        params: list[int] = [max_frequency_rank]
        if start is not None:
            clauses.append("id >= ?")
            params.append(start)
        if end is not None:
            clauses.append("id <= ?")
            params.append(end)
        rows = database.execute(
            "SELECT normalized_word,definition,definition_zh,us_phonetic,"
            "uk_phonetic,pos,COALESCE(json_extract(enrichment_json,"
            "'$.status'),'pending') FROM entries WHERE "
            + " AND ".join(clauses)
            + " ORDER BY frequency_rank,id",
            params,
        ).fetchall()
    finally:
        database.close()

    gaps = Counter()
    statuses = Counter()
    phrases = 0
    incomplete = 0
    unresolved: list[dict[str, Any]] = []
    for term, definition, translation, us, uk, pos, status in rows:
        phrase = is_phrase(term, pos)
        phrases += int(phrase)
        statuses[str(status)] += 1
        missing = entry_quality_gaps(
            term,
            definition,
            translation,
            us,
            uk,
            pos,
        )
        gaps.update(missing)
        if missing:
            incomplete += 1
            if len(unresolved) < unresolved_limit:
                unresolved.append(
                    {
                        "term": term,
                        "kind": "phrase" if phrase else "word",
                        "gaps": missing,
                        "status": status,
                    }
                )
    total = len(rows)
    complete = total - incomplete
    return {
        "dataset": str(dataset),
        "maxFrequencyRank": max_frequency_rank,
        "shardIndex": shard_index,
        "shardCount": shard_count if shard_index is not None else None,
        "total": total,
        "complete": complete,
        "incomplete": incomplete,
        "percent": round((complete / total * 100) if total else 100.0, 3),
        "terms": {"words": total - phrases, "phrases": phrases},
        "missing": dict(sorted(gaps.items())),
        "entryStatus": dict(sorted(statuses.items())),
        "unresolved": unresolved,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--max-frequency-rank", type=int, default=20_000)
    parser.add_argument("--shard-index", type=int)
    parser.add_argument("--shard-count", type=int, default=2)
    parser.add_argument("--unresolved-limit", type=int, default=20)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = quality_report(
        args.dataset,
        max_frequency_rank=args.max_frequency_rank,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
        unresolved_limit=max(0, args.unresolved_limit),
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return
    print(
        f"{report['complete']}/{report['total']} "
        f"({report['percent']}%)  incomplete={report['incomplete']}"
    )
    print("terms=" + json.dumps(report["terms"], ensure_ascii=False))
    print("missing=" + json.dumps(report["missing"], ensure_ascii=False))
    print("entry_status=" + json.dumps(report["entryStatus"], ensure_ascii=False))
    if report["unresolved"]:
        print("unresolved=" + json.dumps(report["unresolved"], ensure_ascii=False))


if __name__ == "__main__":
    main()
