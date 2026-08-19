from __future__ import annotations

import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "sqlite_snapshot_manifest", ROOT / "tools" / "sqlite_snapshot_manifest.py"
)
assert SPEC is not None and SPEC.loader is not None
snapshot_manifest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(snapshot_manifest)


def make_canonical(
    path: Path, *, source: str = '["ecdict"]', definition: str = "d"
) -> None:
    database = sqlite3.connect(path)
    try:
        database.execute(
            "CREATE TABLE entries("
            "id INTEGER PRIMARY KEY,word TEXT,normalized_word TEXT,"
            "frequency_rank INTEGER,source_json TEXT,scope_json TEXT,"
            "definition TEXT)"
        )
        database.execute("CREATE INDEX idx_entries_word ON entries(normalized_word)")
        database.executemany(
            "INSERT INTO entries VALUES(?,?,?,?,?,?,?)",
            [
                (1, "word", "word", 1, source, "[]", definition),
                (
                    2,
                    "people-to-people",
                    "people-to-people",
                    2,
                    source,
                    "[]",
                    definition,
                ),
            ],
        )
        database.commit()
    finally:
        database.close()


class SqliteSnapshotManifestTest(unittest.TestCase):
    def test_online_backup_is_verified_and_manifested(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            snapshot = root / "snapshot.sqlite"
            manifest_path = root / "snapshot.json"
            make_canonical(source)

            manifest = snapshot_manifest.backup_with_manifest(
                source, snapshot, manifest_path, pages=1, sleep_seconds=0
            )

            self.assertTrue(snapshot.is_file())
            self.assertEqual(manifest["database"]["quickCheck"], "ok")
            self.assertEqual(manifest["canonical"]["rowCount"], 2)
            self.assertEqual(
                snapshot_manifest.verify_snapshot(snapshot, manifest_path)["verified"],
                True,
            )
            on_disk = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(
                on_disk["database"]["sha256"], manifest["database"]["sha256"]
            )

    def test_compare_ignores_mutable_enrichment_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifests: list[Path] = []
            mutable_values = ("first enrichment", "second enrichment")
            for index, definition in enumerate(mutable_values):
                source = root / f"source-{index}.sqlite"
                snapshot = root / f"snapshot-{index}.sqlite"
                manifest = root / f"snapshot-{index}.json"
                make_canonical(source, definition=definition)
                snapshot_manifest.backup_with_manifest(source, snapshot, manifest)
                manifests.append(manifest)

            result = snapshot_manifest.compare_manifests(manifests)
            self.assertTrue(result["compatible"])
            self.assertEqual(result["replicas"], 2)

    def test_compare_rejects_candidate_source_provenance_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifests: list[Path] = []
            for index, source_value in enumerate(('["ecdict"]', '["kaikki"]')):
                source = root / f"source-{index}.sqlite"
                snapshot = root / f"snapshot-{index}.sqlite"
                manifest = root / f"snapshot-{index}.json"
                make_canonical(source, source=source_value)
                snapshot_manifest.backup_with_manifest(source, snapshot, manifest)
                manifests.append(manifest)

            with self.assertRaisesRegex(RuntimeError, "canonical identity mismatch"):
                snapshot_manifest.compare_manifests(manifests)

    def test_compare_rejects_identity_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifests: list[Path] = []
            for index, word_value in enumerate(("word", "changed-word")):
                source = root / f"source-{index}.sqlite"
                snapshot = root / f"snapshot-{index}.sqlite"
                manifest = root / f"snapshot-{index}.json"
                make_canonical(source)
                if index:
                    database = sqlite3.connect(source)
                    try:
                        database.execute(
                            "UPDATE entries SET word=?,normalized_word=? WHERE id=1",
                            (word_value, word_value),
                        )
                        database.commit()
                    finally:
                        database.close()
                snapshot_manifest.backup_with_manifest(source, snapshot, manifest)
                manifests.append(manifest)

            with self.assertRaisesRegex(RuntimeError, "canonical identity mismatch"):
                snapshot_manifest.compare_manifests(manifests)

    def test_merge_owned_snapshots_uses_each_fixed_owner_mutable_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshots: list[Path] = []
            for shard in range(2):
                source = root / f"source-{shard}.sqlite"
                make_canonical(source, definition=f"default-{shard}")
                database = sqlite3.connect(source)
                try:
                    database.execute(
                        "UPDATE entries SET definition=? WHERE id=1",
                        (f"owner-one-from-{shard}",),
                    )
                    database.execute(
                        "UPDATE entries SET definition=? WHERE id=2",
                        (f"owner-zero-from-{shard}",),
                    )
                    database.commit()
                finally:
                    database.close()
                snapshots.append(source)

            output = root / "owned.sqlite"
            report = snapshot_manifest.merge_owned_snapshots(
                snapshots, output, shard_count=2, batch_size=1
            )

            database = sqlite3.connect(output)
            try:
                values = dict(database.execute("SELECT id,definition FROM entries"))
            finally:
                database.close()
            # shard_owner is canonical_id % 2.
            self.assertEqual(values[1], "owner-one-from-1")
            self.assertEqual(values[2], "owner-zero-from-0")
            self.assertEqual(report["shardCount"], 2)
            self.assertEqual(len(report["ownerContentSha256"]), 2)

    def test_merge_owned_snapshots_rejects_identity_drift_without_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.sqlite"
            second = root / "second.sqlite"
            output = root / "owned.sqlite"
            make_canonical(first)
            make_canonical(second, source='["kaikki"]')

            with self.assertRaisesRegex(RuntimeError, "canonical identity mismatch"):
                snapshot_manifest.merge_owned_snapshots(
                    [first, second], output, shard_count=2
                )

            self.assertFalse(output.exists())

    def test_backup_refuses_to_replace_existing_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            snapshot = root / "snapshot.sqlite"
            manifest = root / "snapshot.json"
            make_canonical(source)
            snapshot.write_bytes(b"keep-me")

            with self.assertRaises(FileExistsError):
                snapshot_manifest.backup_with_manifest(source, snapshot, manifest)

            self.assertEqual(snapshot.read_bytes(), b"keep-me")
            self.assertFalse(manifest.exists())


if __name__ == "__main__":
    unittest.main()
