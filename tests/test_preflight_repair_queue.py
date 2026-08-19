from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "preflight_repair_queue", ROOT / "deploy" / "preflight_repair_queue.py"
)
assert SPEC is not None and SPEC.loader is not None
preflight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(preflight)
VALIDATOR_SPEC = importlib.util.spec_from_file_location(
    "validate_preflight_marker", ROOT / "deploy" / "validate_preflight_marker.py"
)
assert VALIDATOR_SPEC is not None and VALIDATOR_SPEC.loader is not None
validator = importlib.util.module_from_spec(VALIDATOR_SPEC)
VALIDATOR_SPEC.loader.exec_module(validator)


class PreflightRepairQueueTest(unittest.TestCase):
    def test_success_marker_atomically_replaces_only_after_complete_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "state" / "preflight.json"
            marker.parent.mkdir()
            marker.write_text('{"old":true}\n', encoding="utf-8")
            value = {
                "format": "lexora-top20k-preflight-v1",
                "releaseId": "release-1",
                "candidateDigest": "a" * 64,
                "shardIndex": 1,
                "shardCount": 2,
                "selectedOwnerRows": 10_000,
                "preflightRows": 123,
                "processId": 42,
                "completedAt": "2026-08-19T00:00:00+00:00",
            }

            preflight.write_success_marker(marker, value)

            self.assertEqual(json.loads(marker.read_text(encoding="utf-8")), value)
            self.assertEqual(list(marker.parent.glob(".preflight.json.*.tmp")), [])
            self.assertEqual(
                validator.validate_marker(
                    marker,
                    release_id="release-1",
                    candidate_digest="a" * 64,
                    shard_index=1,
                    shard_count=2,
                ),
                value,
            )

            with self.assertRaisesRegex(RuntimeError, "candidate mismatch"):
                validator.validate_marker(
                    marker,
                    release_id="release-1",
                    candidate_digest="b" * 64,
                    shard_index=1,
                    shard_count=2,
                )

    def test_runtime_marker_requires_its_own_format_and_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "runtime.json"
            value = {
                "format": "lexora-top20k-runtime-ready-v1",
                "releaseId": "release-2",
                "candidateDigest": "c" * 64,
                "shardIndex": 0,
                "shardCount": 2,
                "selectedOwnerRows": 10_000,
                "preflightRows": 0,
                "processId": 84,
                "readyAt": "2026-08-19T01:00:00+00:00",
            }
            marker.write_text(json.dumps(value), encoding="utf-8")

            self.assertEqual(
                validator.validate_marker(
                    marker,
                    release_id="release-2",
                    candidate_digest="c" * 64,
                    shard_index=0,
                    shard_count=2,
                    kind="runtime",
                ),
                value,
            )
            with self.assertRaisesRegex(RuntimeError, "format mismatch"):
                validator.validate_marker(
                    marker,
                    release_id="release-2",
                    candidate_digest="c" * 64,
                    shard_index=0,
                    shard_count=2,
                    kind="preflight",
                )

            validator.validate_service_state(
                value,
                active_state="activating",
                sub_state="start",
                main_pid=84,
                exec_started_monotonic=10,
                result="success",
                exec_code="exited",
                exec_status=0,
            )
            with self.assertRaisesRegex(RuntimeError, "does not match MainPID"):
                validator.validate_service_state(
                    value,
                    active_state="activating",
                    sub_state="start",
                    main_pid=85,
                    exec_started_monotonic=10,
                    result="success",
                    exec_code="exited",
                    exec_status=0,
                )
            validator.validate_service_state(
                value,
                active_state="inactive",
                sub_state="dead",
                main_pid=0,
                exec_started_monotonic=10,
                result="success",
                exec_code="exited",
                exec_status=0,
            )
            with self.assertRaisesRegex(RuntimeError, "neither running"):
                validator.validate_service_state(
                    value,
                    active_state="failed",
                    sub_state="failed",
                    main_pid=0,
                    exec_started_monotonic=10,
                    result="exit-code",
                    exec_code="exited",
                    exec_status=1,
                )


if __name__ == "__main__":
    unittest.main()
