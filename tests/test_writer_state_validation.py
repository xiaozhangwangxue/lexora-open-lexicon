from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

from tools.validate_collection_writer_state import validate_writer_states


class WriterStateValidationTest(unittest.TestCase):
    def test_each_single_writer_is_accepted_including_full_only(self) -> None:
        for writer in ("micro", "full", "repair"):
            with self.subTest(writer=writer):
                states = {"micro": "inactive", "full": "inactive", "repair": "inactive"}
                states[writer] = "activating" if writer == "repair" else "active"
                result = validate_writer_states(**states)
                self.assertEqual(result["writer"], writer)
                self.assertFalse(result["complete"])

    def test_dual_active_and_failed_states_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "multiple collection writers"):
            validate_writer_states(
                micro="active",
                full="active",
                repair="inactive",
            )
        with self.assertRaisesRegex(ValueError, "failed collection writer"):
            validate_writer_states(
                micro="inactive",
                full="failed",
                repair="inactive",
            )

    def test_all_inactive_requires_recent_complete_snapshot(self) -> None:
        now = dt.datetime(2026, 8, 19, 12, 0, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            snapshot = Path(directory) / "progress.json"
            with self.assertRaisesRegex(ValueError, "without completion proof"):
                validate_writer_states(
                    micro="inactive",
                    full="inactive",
                    repair="inactive",
                    progress_snapshot=snapshot,
                    now=now,
                )

            snapshot.write_text(
                json.dumps(
                    {
                        "finished": 9,
                        "total": 10,
                        "remaining": 1,
                        "updatedAt": now.isoformat(),
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "without completion proof"):
                validate_writer_states(
                    micro="inactive",
                    full="inactive",
                    repair="inactive",
                    progress_snapshot=snapshot,
                    now=now,
                )

            snapshot.write_text(
                json.dumps(
                    {
                        "finished": 10,
                        "total": 10,
                        "remaining": 0,
                        "updatedAt": now.isoformat(),
                    }
                ),
                encoding="utf-8",
            )
            result = validate_writer_states(
                micro="inactive",
                full="inactive",
                repair="inactive",
                progress_snapshot=snapshot,
                now=now,
            )
            self.assertTrue(result["complete"])
            self.assertIsNone(result["writer"])

    def test_stale_completion_snapshot_and_unknown_state_are_rejected(self) -> None:
        now = dt.datetime(2026, 8, 19, 12, 0, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            snapshot = Path(directory) / "progress.json"
            snapshot.write_text(
                json.dumps(
                    {
                        "finished": 10,
                        "total": 10,
                        "remaining": 0,
                        "updatedAt": (now - dt.timedelta(hours=1)).isoformat(),
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "without completion proof"):
                validate_writer_states(
                    micro="inactive",
                    full="inactive",
                    repair="inactive",
                    progress_snapshot=snapshot,
                    now=now,
                )
        with self.assertRaisesRegex(ValueError, "unknown collection writer state"):
            validate_writer_states(
                micro="deactivating",
                full="inactive",
                repair="inactive",
            )

    def test_boolean_snapshot_counts_are_not_completion_proof(self) -> None:
        now = dt.datetime(2026, 8, 19, 12, 0, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            snapshot = Path(directory) / "progress.json"
            snapshot.write_text(
                json.dumps(
                    {
                        "finished": True,
                        "total": 1,
                        "remaining": 0,
                        "updatedAt": now.isoformat(),
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "without completion proof"):
                validate_writer_states(
                    micro="inactive",
                    full="inactive",
                    repair="inactive",
                    progress_snapshot=snapshot,
                    now=now,
                )


if __name__ == "__main__":
    unittest.main()
