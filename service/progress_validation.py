"""Dependency-free validation for candidate-bound top-20k progress snapshots."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

TOP20K_SNAPSHOT_MAX_AGE_SECONDS = 3 * 3600
CANDIDATE_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")


def validate_top20k_quality_snapshot(
    snapshot: Path,
    dataset: Path,
    candidate: Path,
    *,
    shard_index: int,
    shard_count: int = 2,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    quality = json.loads(snapshot.read_text(encoding="utf-8"))
    quality_updated_at = dt.datetime.fromisoformat(
        str(quality["updatedAt"]).replace("Z", "+00:00")
    )
    if quality_updated_at.tzinfo is None:
        quality_updated_at = quality_updated_at.replace(tzinfo=dt.timezone.utc)
    checked_at = now or dt.datetime.now(dt.timezone.utc)
    age = (
        checked_at.astimezone(dt.timezone.utc)
        - quality_updated_at.astimezone(dt.timezone.utc)
    ).total_seconds()
    if age < 0 or age > TOP20K_SNAPSHOT_MAX_AGE_SECONDS:
        raise ValueError("quality snapshot is stale")

    def matches_identity(value: Any, file_stat: os.stat_result) -> bool:
        return (
            isinstance(value, dict)
            and type(value.get("device")) is int
            and type(value.get("inode")) is int
            and value["device"] == file_stat.st_dev
            and value["inode"] == file_stat.st_ino
        )

    dataset_stat = dataset.stat()
    if not matches_identity(quality["datasetIdentity"], dataset_stat):
        raise ValueError("quality snapshot belongs to a different dataset")
    candidate_before = candidate.stat()
    if not matches_identity(quality["candidateIdentity"], candidate_before):
        raise ValueError("quality snapshot belongs to a different candidate")

    raw_counts = (
        quality["total"],
        quality["complete"],
        quality["incomplete"],
    )
    if any(type(value) is not int or value < 0 for value in raw_counts):
        raise ValueError("quality snapshot counts are invalid")
    quality_total, quality_complete, quality_incomplete = raw_counts
    if quality_total <= 0 or quality_complete + quality_incomplete != quality_total:
        raise ValueError("quality counts do not add up")
    if (
        type(quality.get("qualityGateVersion")) is not int
        or quality["qualityGateVersion"] != 2
        or type(quality.get("shardIndex")) is not int
        or type(quality.get("shardCount")) is not int
        or quality["shardIndex"] != shard_index
        or quality["shardCount"] != shard_count
    ):
        raise ValueError("quality snapshot shard contract is invalid")
    candidate_digest = quality.get("candidateDigest")
    if (
        not isinstance(candidate_digest, str)
        or CANDIDATE_DIGEST_PATTERN.fullmatch(candidate_digest) is None
    ):
        raise ValueError("quality snapshot candidate digest is invalid")
    with closing(
        sqlite3.connect(
            f"file:{candidate.resolve()}?mode=ro",
            uri=True,
        )
    ) as candidate_db:
        row = candidate_db.execute(
            "SELECT candidate_digest FROM fast20k_metadata WHERE id=1"
        ).fetchone()
    candidate_after = candidate.stat()
    if (
        candidate_before.st_dev != candidate_after.st_dev
        or candidate_before.st_ino != candidate_after.st_ino
    ):
        raise ValueError("candidate was replaced during snapshot validation")
    if row is None or row[0] != candidate_digest:
        raise ValueError("quality snapshot candidate digest does not match")
    return quality
