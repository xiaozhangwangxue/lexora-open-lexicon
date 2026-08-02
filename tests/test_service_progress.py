from __future__ import annotations

import json
import tempfile
import unittest
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
            snapshot = state / "progress-shard-1.json"
            snapshot.write_text(
                json.dumps(
                    {
                        "finished": 123,
                        "total": 456,
                        "updatedAt": "2026-08-02T00:00:00+00:00",
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(server, "STATE", state):
                value = server.collection_progress()
            self.assertEqual(value["shard"], 1)
            self.assertEqual(value["finished"], 123)
            self.assertEqual(value["total"], 456)

    def test_missing_snapshot_is_temporarily_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(server, "STATE", Path(directory)):
                with self.assertRaises(HTTPException) as raised:
                    server.collection_progress()
            self.assertEqual(raised.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
