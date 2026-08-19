from __future__ import annotations

import datetime as dt
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from service.progress_validation import validate_top20k_quality_snapshot


DIGEST = "a" * 64


def build_candidate(path: Path, digest: str = DIGEST) -> None:
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


def payload(
    dataset: Path,
    candidate: Path,
    *,
    updated_at: dt.datetime,
) -> dict[str, object]:
    dataset_stat = dataset.stat()
    candidate_stat = candidate.stat()
    return {
        "qualityGateVersion": 2,
        "candidateDigest": DIGEST,
        "shardIndex": 0,
        "shardCount": 2,
        "total": 10_000,
        "complete": 9_000,
        "incomplete": 1_000,
        "updatedAt": updated_at.isoformat(),
        "datasetIdentity": {
            "device": dataset_stat.st_dev,
            "inode": dataset_stat.st_ino,
        },
        "candidateIdentity": {
            "device": candidate_stat.st_dev,
            "inode": candidate_stat.st_ino,
        },
    }


class ProgressValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.dataset = self.root / "canonical.sqlite"
        self.dataset.write_bytes(b"canonical")
        self.candidate = self.root / "candidate.sqlite"
        build_candidate(self.candidate)
        self.snapshot = self.root / "quality.json"
        self.now = dt.datetime(2026, 8, 19, 12, 0, tzinfo=dt.timezone.utc)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_snapshot(self, value: dict[str, object]) -> None:
        self.snapshot.write_text(json.dumps(value), encoding="utf-8")

    def validate(self) -> dict[str, object]:
        return validate_top20k_quality_snapshot(
            self.snapshot,
            self.dataset,
            self.candidate,
            shard_index=0,
            now=self.now,
        )

    def test_valid_snapshot_is_bound_to_current_dataset_and_candidate(self) -> None:
        self.write_snapshot(payload(self.dataset, self.candidate, updated_at=self.now))
        self.assertEqual(self.validate()["candidateDigest"], DIGEST)

    def test_four_hour_old_snapshot_is_rejected(self) -> None:
        self.write_snapshot(
            payload(
                self.dataset,
                self.candidate,
                updated_at=self.now - dt.timedelta(hours=4),
            )
        )
        with self.assertRaisesRegex(ValueError, "stale"):
            self.validate()

    def test_dataset_identity_mismatch_is_rejected(self) -> None:
        value = payload(self.dataset, self.candidate, updated_at=self.now)
        value["datasetIdentity"] = {"device": -1, "inode": -1}
        self.write_snapshot(value)
        with self.assertRaisesRegex(ValueError, "different dataset"):
            self.validate()

    def test_candidate_identity_and_digest_mismatches_are_rejected(self) -> None:
        value = payload(self.dataset, self.candidate, updated_at=self.now)
        value["candidateIdentity"] = {"device": -1, "inode": -1}
        self.write_snapshot(value)
        with self.assertRaisesRegex(ValueError, "different candidate"):
            self.validate()

        value = payload(self.dataset, self.candidate, updated_at=self.now)
        value["candidateDigest"] = "matching-but-not-a-contract-digest"
        self.write_snapshot(value)
        with self.assertRaisesRegex(ValueError, "candidate digest is invalid"):
            self.validate()

        value = payload(self.dataset, self.candidate, updated_at=self.now)
        with closing(sqlite3.connect(self.candidate)) as database:
            database.execute(
                "UPDATE fast20k_metadata SET candidate_digest=? WHERE id=1",
                ("b" * 64,),
            )
            database.commit()
        self.write_snapshot(value)
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.validate()


if __name__ == "__main__":
    unittest.main()
