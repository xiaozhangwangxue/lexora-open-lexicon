from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

try:
    from fastapi import HTTPException
    from service import server
except ModuleNotFoundError:
    HTTPException = None
    server = None


CANDIDATE_DIGEST = "a" * 64


def build_candidate(path: Path, digest: str = CANDIDATE_DIGEST) -> None:
    with closing(sqlite3.connect(path)) as database:
        database.execute(
            "CREATE TABLE fast20k_metadata(id INTEGER PRIMARY KEY, "
            "candidate_digest TEXT NOT NULL)"
        )
        database.execute(
            "INSERT INTO fast20k_metadata(id,candidate_digest) VALUES(1,?)",
            (digest,),
        )
        database.commit()


def quality_payload(
    dataset: Path,
    candidate: Path,
    *,
    shard_index: int,
    updated_at: datetime | None = None,
) -> dict[str, object]:
    dataset_stat = dataset.stat()
    candidate_stat = candidate.stat()
    return {
        "total": 10,
        "complete": 8,
        "incomplete": 2,
        "terms": {"words": 7, "phrases": 3},
        "missing": {"definition": 2},
        "entryStatus": {"completed": 8, "partial": 2},
        "unresolved": [],
        "updatedAt": (updated_at or datetime.now(timezone.utc)).isoformat(),
        "qualityGateVersion": 2,
        "candidateDigest": CANDIDATE_DIGEST,
        "shardIndex": shard_index,
        "shardCount": 2,
        "datasetIdentity": {
            "device": dataset_stat.st_dev,
            "inode": dataset_stat.st_ino,
        },
        "candidateIdentity": {
            "device": candidate_stat.st_dev,
            "inode": candidate_stat.st_ino,
        },
    }


@unittest.skipIf(server is None, "FastAPI service dependencies are not installed")
class ServiceProgressTest(unittest.TestCase):
    def test_reads_latest_cached_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            build = state / "build"
            build.mkdir()
            dataset = build / "lexora-open-oxford-scope.sqlite"
            dataset.write_bytes(b"test-dataset-identity")
            candidate = build / "candidate.sqlite"
            build_candidate(candidate)
            snapshot = state / "progress-shard-1.json"
            snapshot.write_text(
                json.dumps(
                    {
                        "finished": 123,
                        "total": 456,
                        "entry_status": {"completed": 100, "partial": 23},
                        "provider_status": {"completed": 900},
                        "provider_attempts": 987,
                        "updatedAt": "2026-08-02T00:00:00+00:00",
                    }
                ),
                encoding="utf-8",
            )
            quality = quality_payload(dataset, candidate, shard_index=1)
            quality["unresolved"] = [
                {
                    "term": "missing",
                    "kind": "word",
                    "gaps": ["definition"],
                    "status": "partial",
                }
            ]
            (state / "top20k-quality-shard-1.json").write_text(
                json.dumps(quality),
                encoding="utf-8",
            )
            with (
                patch.object(server, "STATE", state),
                patch.object(server, "BUILD", build),
                patch.object(server, "TOP20K_CANDIDATE", candidate),
            ):
                value = server.collection_progress()
            self.assertEqual(value["shard"], 1)
            self.assertEqual(value["finished"], 123)
            self.assertEqual(value["total"], 456)
            self.assertEqual(value["remaining"], 333)
            self.assertEqual(value["entryStatus"]["partial"], 23)
            self.assertEqual(value["providerAttempts"], 987)
            self.assertEqual(value["top20k"]["complete"], 8)
            self.assertEqual(value["top20k"]["missing"]["definition"], 2)
            self.assertEqual(value["top20k"]["candidateDigest"], CANDIDATE_DIGEST)
            self.assertEqual(value["top20k"]["shardIndex"], 1)
            self.assertEqual(value["top20k"]["shardCount"], 2)

    def test_mismatched_quality_shard_identity_is_not_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            build = state / "build"
            build.mkdir()
            dataset = build / "lexora-open-oxford-scope.sqlite"
            dataset.write_bytes(b"test-dataset-identity")
            candidate = build / "candidate.sqlite"
            build_candidate(candidate)
            (state / "progress-shard-1.json").write_text(
                json.dumps({"finished": 4, "total": 10}),
                encoding="utf-8",
            )
            (state / "top20k-quality-shard-1.json").write_text(
                json.dumps(quality_payload(dataset, candidate, shard_index=0)),
                encoding="utf-8",
            )
            with (
                patch.object(server, "STATE", state),
                patch.object(server, "BUILD", build),
                patch.object(server, "TOP20K_CANDIDATE", candidate),
            ):
                value = server.collection_progress()
            self.assertIsNone(value["top20k"])

    def test_four_hour_old_quality_snapshot_is_not_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            build = state / "build"
            build.mkdir()
            dataset = build / "lexora-open-oxford-scope.sqlite"
            dataset.write_bytes(b"current")
            candidate = build / "candidate.sqlite"
            build_candidate(candidate)
            (state / "progress-shard-0.json").write_text(
                json.dumps({"finished": 4, "total": 10}),
                encoding="utf-8",
            )
            (state / "top20k-quality-shard-0.json").write_text(
                json.dumps(
                    quality_payload(
                        dataset,
                        candidate,
                        shard_index=0,
                        updated_at=datetime.now(timezone.utc) - timedelta(hours=4),
                    )
                ),
                encoding="utf-8",
            )
            with (
                patch.object(server, "STATE", state),
                patch.object(server, "BUILD", build),
                patch.object(server, "TOP20K_CANDIDATE", candidate),
            ):
                value = server.collection_progress()
            self.assertIsNone(value["top20k"])

    def test_wrong_dataset_identity_is_not_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            build = state / "build"
            build.mkdir()
            dataset = build / "lexora-open-oxford-scope.sqlite"
            dataset.write_bytes(b"current")
            candidate = build / "candidate.sqlite"
            build_candidate(candidate)
            (state / "progress-shard-0.json").write_text(
                json.dumps({"finished": 4, "total": 10}),
                encoding="utf-8",
            )
            quality = quality_payload(dataset, candidate, shard_index=0)
            quality["datasetIdentity"] = {"device": -1, "inode": -1}
            (state / "top20k-quality-shard-0.json").write_text(
                json.dumps(quality),
                encoding="utf-8",
            )
            with (
                patch.object(server, "STATE", state),
                patch.object(server, "BUILD", build),
                patch.object(server, "TOP20K_CANDIDATE", candidate),
            ):
                value = server.collection_progress()
            self.assertIsNone(value["top20k"])

    def test_candidate_digest_mismatch_is_not_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            build = state / "build"
            build.mkdir()
            dataset = build / "lexora-open-oxford-scope.sqlite"
            dataset.write_bytes(b"current")
            candidate = build / "candidate.sqlite"
            build_candidate(candidate)
            (state / "progress-shard-0.json").write_text(
                json.dumps({"finished": 4, "total": 10}),
                encoding="utf-8",
            )
            quality = quality_payload(dataset, candidate, shard_index=0)
            with closing(sqlite3.connect(candidate)) as database:
                database.execute(
                    "UPDATE fast20k_metadata SET candidate_digest=? WHERE id=1",
                    ("b" * 64,),
                )
                database.commit()
            (state / "top20k-quality-shard-0.json").write_text(
                json.dumps(quality),
                encoding="utf-8",
            )
            with (
                patch.object(server, "STATE", state),
                patch.object(server, "BUILD", build),
                patch.object(server, "TOP20K_CANDIDATE", candidate),
            ):
                value = server.collection_progress()
            self.assertIsNone(value["top20k"])

    def test_missing_quality_snapshot_keeps_full_progress_available(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state / "progress-shard-0.json").write_text(
                json.dumps({"finished": 4, "total": 10}),
                encoding="utf-8",
            )
            with patch.object(server, "STATE", state):
                value = server.collection_progress()
            self.assertIsNone(value["top20k"])

    def test_missing_snapshot_is_temporarily_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(server, "STATE", Path(directory)):
                with self.assertRaises(HTTPException) as raised:
                    server.collection_progress()
            self.assertEqual(raised.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
