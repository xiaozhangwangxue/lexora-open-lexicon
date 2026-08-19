from __future__ import annotations

import hashlib
import importlib.util
import json
import sqlite3
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "deployment_transaction", ROOT / "deploy" / "deployment_transaction.py"
)
assert SPEC is not None and SPEC.loader is not None
transaction = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(transaction)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_archive(root: Path, name: str, *, include_dependency: bool = True) -> Path:
    source = root / f"source-{name}"
    tools = source / "tools"
    deploy = source / "deploy"
    tools.mkdir(parents=True)
    deploy.mkdir()
    (tools / "entrypoint.py").write_text(
        "from dependency import VALUE\nRESULT = VALUE\n", encoding="utf-8"
    )
    if include_dependency:
        (tools / "dependency.py").write_text(f"VALUE = {name!r}\n", encoding="utf-8")
    (deploy / "unit.service").write_text("[Service]\nType=oneshot\n", encoding="utf-8")
    archive = root / f"{name}.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        bundle.add(tools, arcname="tools")
        bundle.add(deploy, arcname="deploy")
    return archive


def make_candidate(path: Path, rows: int = 20_000) -> None:
    database = sqlite3.connect(path)
    try:
        database.execute("CREATE TABLE entries(id INTEGER PRIMARY KEY,word TEXT)")
        database.execute(
            "CREATE TABLE entries_fts(rowid INTEGER PRIMARY KEY,word TEXT)"
        )
        database.execute(
            "CREATE TABLE fast20k_metadata("
            "id INTEGER PRIMARY KEY,expected_rows INTEGER,"
            "selection_version INTEGER,shard_count INTEGER,"
            "selection_digest TEXT,baseline_content_digest TEXT,"
            "repair_queue_digest TEXT,candidate_digest TEXT)"
        )
        database.execute(
            "CREATE TABLE fast20k_provenance(canonical_id INTEGER PRIMARY KEY)"
        )
        database.execute("CREATE TABLE repair_queue(canonical_id INTEGER PRIMARY KEY)")
        for identifier in range(1, rows + 1):
            database.execute(
                "INSERT INTO entries VALUES(?,?)", (identifier, f"w{identifier}")
            )
            database.execute(
                "INSERT INTO entries_fts VALUES(?,?)", (identifier, f"w{identifier}")
            )
            database.execute("INSERT INTO fast20k_provenance VALUES(?)", (identifier,))
        database.execute(
            "INSERT INTO fast20k_metadata VALUES(1,20000,3,2,?,?,?,?)",
            ("b" * 64, "c" * 64, "d" * 64, "e" * 64),
        )
        database.commit()
    finally:
        database.close()


def make_canonical_manifest(path: Path, identity: str = "a" * 64) -> None:
    path.write_text(
        json.dumps(
            {
                "format": "lexora-canonical-snapshot-v1",
                "database": {"quickCheck": "ok"},
                "canonical": {
                    "rowCount": 10,
                    "minId": 1,
                    "maxId": 10,
                    "identitySha256": identity,
                    "schemaSha256": "b" * 64,
                },
            }
        ),
        encoding="utf-8",
    )


class DeploymentTransactionTest(unittest.TestCase):
    def prepare_and_seal(self, root: Path, release_id: str) -> tuple[Path, str]:
        archive = make_archive(root, release_id)
        candidate = root / f"candidate-{release_id}.sqlite"
        canonical = root / f"canonical-{release_id}.json"
        make_candidate(candidate)
        make_canonical_manifest(canonical)
        transaction.prepare_code(
            root,
            "repair",
            release_id,
            archive,
            sha256(archive),
            ["entrypoint"],
        )
        transaction.seal_release(
            root,
            "repair",
            release_id,
            candidate,
            sha256(candidate),
            canonical,
        )
        candidate_environment = (
            root / "deployments" / "repair" / "releases" / release_id / "candidate.env"
        )
        self.assertEqual(
            candidate_environment.read_text(encoding="ascii"),
            "LEXORA_CANDIDATE_DIGEST=" + "e" * 64 + "\n",
        )
        return candidate, sha256(candidate)

    def test_missing_runtime_dependency_never_publishes_preparation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = make_archive(root, "missing", include_dependency=False)
            with self.assertRaises(Exception):
                transaction.prepare_code(
                    root,
                    "repair",
                    "missing",
                    archive,
                    sha256(archive),
                    ["entrypoint"],
                )
            preparation, release = transaction._release_paths(root, "repair", "missing")
            self.assertFalse(preparation.exists())
            self.assertFalse(release.exists())

    def test_candidate_sha_and_quick_check_are_verified_before_seal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = make_archive(root, "sha")
            candidate = root / "candidate.sqlite"
            canonical = root / "canonical.json"
            make_candidate(candidate)
            make_canonical_manifest(canonical)
            transaction.prepare_code(
                root, "repair", "sha", archive, sha256(archive), ["entrypoint"]
            )

            with self.assertRaisesRegex(RuntimeError, "SHA-256 mismatch"):
                transaction.seal_release(
                    root, "repair", "sha", candidate, "0" * 64, canonical
                )

            preparation, release = transaction._release_paths(root, "repair", "sha")
            self.assertTrue(preparation.is_dir())
            self.assertFalse(release.exists())

    def test_bad_canonical_receipt_does_not_attach_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = make_archive(root, "bad-canonical")
            candidate = root / "candidate.sqlite"
            canonical = root / "canonical.json"
            make_candidate(candidate)
            make_canonical_manifest(canonical)
            receipt = json.loads(canonical.read_text(encoding="utf-8"))
            receipt["database"]["quickCheck"] = "corrupt"
            canonical.write_text(json.dumps(receipt), encoding="utf-8")
            transaction.prepare_code(
                root,
                "repair",
                "bad-canonical",
                archive,
                sha256(archive),
                ["entrypoint"],
            )

            with self.assertRaisesRegex(RuntimeError, "quick_check"):
                transaction.seal_release(
                    root,
                    "repair",
                    "bad-canonical",
                    candidate,
                    sha256(candidate),
                    canonical,
                )

            preparation, release = transaction._release_paths(
                root, "repair", "bad-canonical"
            )
            self.assertTrue(preparation.is_dir())
            self.assertFalse((preparation / "build").exists())
            self.assertFalse(release.exists())

    def test_atomic_activation_and_rollback_restore_previous_release(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.prepare_and_seal(root, "release-one")
            self.prepare_and_seal(root, "release-two")

            first = transaction.activate_release(root, "repair", "release-one")
            self.assertIsNone(first["previousTarget"])
            second = transaction.activate_release(root, "repair", "release-two")
            self.assertEqual(second["previousTarget"], "releases/release-one")
            current = root / "deployments" / "repair" / "current"
            self.assertEqual(current.resolve().name, "release-two")

            transaction.rollback_release(root, "repair", "release-two")
            self.assertEqual(current.resolve().name, "release-one")
            state = json.loads(
                (root / "deployments" / "repair" / "activation-state.json").read_text()
            )
            self.assertEqual(state["activeRelease"], "release-one")

    def test_verify_detects_candidate_changed_after_seal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, candidate_sha = self.prepare_and_seal(root, "sealed")
            release_candidate = (
                root
                / "deployments"
                / "repair"
                / "releases"
                / "sealed"
                / "build"
                / "lexora-open-oxford-safe-20k.sqlite"
            )
            with release_candidate.open("ab") as stream:
                stream.write(b"tampered")

            with self.assertRaises(RuntimeError):
                transaction.verify_release(
                    root,
                    "repair",
                    "sealed",
                    candidate_sha,
                    "a" * 64,
                )

    def test_activation_metadata_failure_restores_link_and_previous_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.prepare_and_seal(root, "release-one")
            self.prepare_and_seal(root, "release-two")
            transaction.activate_release(root, "repair", "release-one")
            original_write = transaction._write_json_atomic

            def fail_active_state(path: Path, value: dict[str, object]) -> None:
                if (
                    value.get("phase") == "active"
                    and value.get("activeRelease") == "release-two"
                ):
                    raise OSError("injected activation journal failure")
                original_write(path, value)

            with patch.object(
                transaction, "_write_json_atomic", side_effect=fail_active_state
            ):
                with self.assertRaisesRegex(OSError, "injected"):
                    transaction.activate_release(root, "repair", "release-two")

            track = root / "deployments" / "repair"
            self.assertEqual((track / "current").resolve().name, "release-one")
            state = json.loads((track / "activation-state.json").read_text())
            self.assertEqual(state["activeRelease"], "release-one")
            self.assertEqual(state["phase"], "active")

    def test_interrupted_activation_journal_is_reconciled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.prepare_and_seal(root, "release-one")
            self.prepare_and_seal(root, "release-two")
            transaction.activate_release(root, "repair", "release-one")
            track = root / "deployments" / "repair"
            old_state = json.loads((track / "activation-state.json").read_text())
            target = "releases/release-two"
            prepared = {
                "format": transaction.STATE_FORMAT,
                "track": "repair",
                "activeRelease": "release-two",
                "activeTarget": target,
                "previousTarget": "releases/release-one",
                "previousState": old_state,
                "activatedAt": transaction._now(),
                "phase": "prepared",
            }
            transaction._write_json_atomic(track / "activation-state.json", prepared)
            replacement = track / ".injected-current"
            replacement.symlink_to(target)
            replacement.replace(track / "current")

            result = transaction.activate_release(root, "repair", "release-two")

            self.assertTrue(result["alreadyActive"])
            state = json.loads((track / "activation-state.json").read_text())
            self.assertEqual(state["phase"], "active")
            self.assertEqual((track / "current").resolve().name, "release-two")

    def test_interrupted_rollback_finishes_journal_restore(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.prepare_and_seal(root, "release-one")
            self.prepare_and_seal(root, "release-two")
            transaction.activate_release(root, "repair", "release-one")
            transaction.activate_release(root, "repair", "release-two")
            track = root / "deployments" / "repair"
            replacement = track / ".injected-rollback"
            replacement.symlink_to("releases/release-one")
            replacement.replace(track / "current")

            transaction.rollback_release(root, "repair", "release-two")

            self.assertEqual((track / "current").resolve().name, "release-one")
            state = json.loads((track / "activation-state.json").read_text())
            self.assertEqual(state["activeRelease"], "release-one")

    def test_refuses_archive_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "unsafe.tar"
            payload = root / "payload"
            payload.write_text("bad", encoding="utf-8")
            with tarfile.open(archive, "w") as bundle:
                bundle.add(payload, arcname="../escape")
            with self.assertRaisesRegex(RuntimeError, "unsafe archive member"):
                transaction.prepare_code(
                    root,
                    "repair",
                    "unsafe",
                    archive,
                    sha256(archive),
                    ["entrypoint"],
                )
            self.assertFalse((root / "escape").exists())


if __name__ == "__main__":
    unittest.main()
