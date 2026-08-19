#!/usr/bin/env python3
"""Report the exact quality gate used by the fast 20,000-entry lexicon."""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

from enrich_oxford_scope import (
    entry_quality_gaps,
    is_phrase,
    open_repair_queue,
    repair_queue_metadata,
)
from fast20k_contract import canonical_identity_digest


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
    candidate: Path | None = None,
    max_frequency_rank: int = 20_000,
    shard_index: int | None = None,
    shard_count: int = 2,
    unresolved_limit: int = 20,
) -> dict[str, Any]:
    database = sqlite3.connect(f"file:{dataset.resolve()}?mode=ro", uri=True)
    database.row_factory = sqlite3.Row
    candidate_db: sqlite3.Connection | None = None
    candidate_digest: str | None = None
    try:
        database.execute("PRAGMA query_only=ON")
        database.execute("BEGIN")
        if candidate is None:
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
                "'$.status'),'pending') AS status FROM entries INDEXED BY "
                "idx_entries_freq WHERE "
                + " AND ".join(clauses)
                + " ORDER BY frequency_rank,id",
                params,
            ).fetchall()
        else:
            if shard_index is None:
                raise ValueError("candidate quality requires --shard-index")
            candidate_db = open_repair_queue(candidate)
            metadata = repair_queue_metadata(candidate_db)
            if shard_count != int(metadata["shard_count"]):
                raise ValueError(
                    "candidate quality shard count mismatch: "
                    f"candidate={metadata['shard_count']} runtime={shard_count}"
                )
            if not 0 <= shard_index < shard_count:
                raise ValueError("--shard-index must be within [0, --shard-count)")
            candidate_digest = str(metadata["candidate_digest"])
            selected = candidate_db.execute(
                "SELECT canonical_id,canonical_identity_sha256 "
                "FROM fast20k_provenance WHERE (canonical_id % ?)=? "
                "ORDER BY selected_rank",
                (shard_count, shard_index),
            ).fetchall()
            rows = []
            for offset in range(0, len(selected), 900):
                page = selected[offset : offset + 900]
                ids = [int(row[0]) for row in page]
                identities = {int(row[0]): str(row[1]) for row in page}
                placeholders = ",".join("?" for _ in ids)
                found = database.execute(
                    "SELECT id,word,normalized_word,frequency_rank,source_json,"
                    "scope_json,definition,definition_zh,us_phonetic,uk_phonetic,"
                    "pos,COALESCE(json_extract(enrichment_json,'$.status'),"
                    "'pending') AS status FROM entries "
                    f"WHERE id IN ({placeholders})",
                    ids,
                ).fetchall()
                by_id = {int(row["id"]): row for row in found}
                for entry_id in ids:
                    row = by_id.get(entry_id)
                    if row is None:
                        raise ValueError(
                            f"candidate quality canonical row missing: id={entry_id}"
                        )
                    if canonical_identity_digest(dict(row)) != identities[entry_id]:
                        raise ValueError(
                            f"candidate quality identity mismatch: id={entry_id}"
                        )
                    rows.append(
                        (
                            row["normalized_word"],
                            row["definition"],
                            row["definition_zh"],
                            row["us_phonetic"],
                            row["uk_phonetic"],
                            row["pos"],
                            row["status"],
                        )
                    )
    finally:
        if database.in_transaction:
            database.rollback()
        database.close()
        if candidate_db is not None:
            candidate_db.close()

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
        "maxFrequencyRank": max_frequency_rank if candidate is None else None,
        "candidateDigest": candidate_digest,
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
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--max-frequency-rank", type=int, default=20_000)
    parser.add_argument("--shard-index", type=int)
    parser.add_argument("--shard-count", type=int, default=2)
    parser.add_argument("--unresolved-limit", type=int, default=20)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = quality_report(
        args.dataset,
        candidate=args.candidate,
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
