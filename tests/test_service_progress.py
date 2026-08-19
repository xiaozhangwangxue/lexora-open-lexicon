from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

try:
    from fastapi import HTTPException
    from service import server
except ModuleNotFoundError:
    HTTPException = None
    server = None


@unittest.skipIf(server is None, "FastAPI service dependencies are not installed")
class ServiceProgressTest(unittest.TestCase):
    def test_reads_latest_cached_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            build = state / "build"
            build.mkdir()
            dataset = build / "lexora-open-oxford-scope.sqlite"
            dataset.write_bytes(b"test-dataset-identity")
            dataset_stat = dataset.stat()
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
            quality_updated_at = datetime.now(timezone.utc).isoformat()
            (state / "top20k-quality-shard-1.json").write_text(
                json.dumps(
                    {
                        "total": 10,
                        "complete": 8,
                        "incomplete": 2,
                        "terms": {"words": 7, "phrases": 3},
                        "missing": {"definition": 2},
                        "entryStatus": {"completed": 8, "partial": 2},
                        "unresolved": [
                            {
                                "term": "missing",
                                "kind": "word",
                                "gaps": ["definition"],
                                "status": "partial",
                            }
                        ],
                        "updatedAt": quality_updated_at,
                        "datasetIdentity": {
                            "device": dataset_stat.st_dev,
                            "inode": dataset_stat.st_ino,
                        },
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.object(server, "STATE", state),
                patch.object(server, "BUILD", build),
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

    def test_stale_or_wrong_dataset_quality_is_not_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            build = state / "build"
            build.mkdir()
            (build / "lexora-open-oxford-scope.sqlite").write_bytes(b"current")
            (state / "progress-shard-0.json").write_text(
                json.dumps({"finished": 4, "total": 10}),
                encoding="utf-8",
            )
            (state / "top20k-quality-shard-0.json").write_text(
                json.dumps(
                    {
                        "total": 10,
                        "complete": 10,
                        "incomplete": 0,
                        "updatedAt": "2026-08-02T00:01:00+00:00",
                        "datasetIdentity": {"device": -1, "inode": -1},
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.object(server, "STATE", state),
                patch.object(server, "BUILD", build),
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
