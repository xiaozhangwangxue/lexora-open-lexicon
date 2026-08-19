#!/usr/bin/env python3
"""Stage, seal, verify, activate and roll back immutable Lexora releases.

Uploads are never written into active paths.  A release becomes eligible for
activation only after real imports, file hashes and (for repair releases) the
candidate SQLite checks pass.  Activation is one atomic symlink replacement;
the previous target is retained in durable state for coordinator rollback.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any


FORMAT = "lexora-deployment-release-v1"
STATE_FORMAT = "lexora-deployment-activation-v1"
RELEASE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
MODULE = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
REQUIRED_CANDIDATE_TABLES = {
    "entries",
    "entries_fts",
    "fast20k_metadata",
    "fast20k_provenance",
    "repair_queue",
}


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _validate_name(value: str, *, label: str) -> str:
    if not RELEASE_ID.fullmatch(value):
        raise ValueError(f"unsafe {label}: {value!r}")
    return value


def _validate_sha(value: str, *, label: str) -> str:
    normalized = value.strip().lower()
    if not SHA256.fullmatch(normalized):
        raise ValueError(f"invalid {label} SHA-256")
    return normalized


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object: {path}")
    return value


def _track_root(root: Path, track: str) -> Path:
    return root / "deployments" / _validate_name(track, label="track")


@contextlib.contextmanager
def _deployment_lock(root: Path, track: str):
    track_root = _track_root(root, track)
    track_root.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(track_root / ".deployment.lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _release_paths(root: Path, track: str, release_id: str) -> tuple[Path, Path]:
    base = _track_root(root, track)
    release_id = _validate_name(release_id, label="release id")
    return base / "preparations" / release_id, base / "releases" / release_id


def _safe_member_path(member: tarfile.TarInfo) -> PurePosixPath:
    path = PurePosixPath(member.name)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise RuntimeError(f"unsafe archive member: {member.name!r}")
    if not (member.isfile() or member.isdir()):
        raise RuntimeError(
            f"archive links and special files are forbidden: {member.name}"
        )
    return path


def _extract_archive(archive: Path, destination: Path) -> None:
    with tarfile.open(archive, "r:*") as bundle:
        members = bundle.getmembers()
        if not members:
            raise RuntimeError("code archive is empty")
        for member in members:
            relative = _safe_member_path(member)
            target = destination.joinpath(*relative.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            source = bundle.extractfile(member)
            if source is None:
                raise RuntimeError(f"cannot read archive member: {member.name}")
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            try:
                with os.fdopen(descriptor, "wb") as output:
                    while chunk := source.read(1024 * 1024):
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())
            finally:
                source.close()
            if member.mode & 0o111:
                target.chmod(0o755)


def _file_hashes(root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise RuntimeError(f"release must not contain symlinks: {path}")
        if path.is_file():
            result[path.relative_to(root).as_posix()] = _sha256_file(path)
    return result


def _parse_imports(value: str) -> list[str]:
    modules = [item.strip() for item in value.split(",") if item.strip()]
    if not modules:
        raise ValueError("at least one real import is required")
    invalid = [module for module in modules if not MODULE.fullmatch(module)]
    if invalid:
        raise ValueError(f"invalid import names: {invalid}")
    return modules


def _real_imports(release: Path, modules: list[str]) -> None:
    tools = release / "tools"
    if not tools.is_dir():
        raise RuntimeError("release is missing tools/")
    expression = ";".join(f"import {module}" for module in modules)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(tools)
    completed = subprocess.run(
        [sys.executable, "-c", expression],
        cwd=release,
        env=environment,
        check=False,
        timeout=60,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"staged import smoke test failed: {detail}")


def _verify_files(release: Path, expected: dict[str, Any]) -> None:
    actual = _file_hashes(release)
    # release-manifest.json is written only after the code hash list is made.
    actual.pop("release-manifest.json", None)
    expected_files = expected.get("files")
    if actual != expected_files:
        missing = sorted(set(expected_files or {}) - set(actual))
        extra = sorted(set(actual) - set(expected_files or {}))
        changed = sorted(
            key
            for key in set(actual) & set(expected_files or {})
            if actual[key] != expected_files[key]
        )
        raise RuntimeError(
            f"release file hashes differ: missing={missing} extra={extra} changed={changed}"
        )


def _candidate_report(path: Path) -> dict[str, Any]:
    digest = _sha256_file(path)
    database = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        check = [str(row[0]) for row in database.execute("PRAGMA quick_check")]
        if check != ["ok"]:
            raise RuntimeError(f"candidate quick_check failed: {check[:5]}")
        tables = {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
            )
        }
        missing = REQUIRED_CANDIDATE_TABLES - tables
        if missing:
            raise RuntimeError(
                "candidate is missing protocol tables: " + ", ".join(sorted(missing))
            )
        entries = int(database.execute("SELECT count(*) FROM entries").fetchone()[0])
        provenance = int(
            database.execute("SELECT count(*) FROM fast20k_provenance").fetchone()[0]
        )
        if entries < 1 or entries != provenance:
            raise RuntimeError(
                f"candidate entry/provenance counts differ: {entries}/{provenance}"
            )
        metadata_columns = {
            str(row[1])
            for row in database.execute("PRAGMA table_info(fast20k_metadata)")
        }
        contract_columns = {
            "expected_rows",
            "selection_version",
            "shard_count",
            "selection_digest",
            "baseline_content_digest",
            "repair_queue_digest",
            "candidate_digest",
        }
        if not contract_columns <= metadata_columns:
            raise RuntimeError("candidate is missing the v3 repair contract")
        contract = database.execute(
            "SELECT expected_rows,selection_version,shard_count,selection_digest,"
            "baseline_content_digest,repair_queue_digest,candidate_digest "
            "FROM fast20k_metadata WHERE id=1"
        ).fetchone()
        if (
            contract is None
            or int(contract[0]) != 20_000
            or entries != int(contract[0])
            or int(contract[1]) != 3
            or int(contract[2]) != 2
        ):
            raise RuntimeError("candidate has an unsupported repair contract")
        (
            selection_digest,
            baseline_digest,
            queue_digest,
            candidate_digest,
        ) = map(str, contract[3:])
        for label, value in (
            ("selection", selection_digest),
            ("baseline content", baseline_digest),
            ("repair queue", queue_digest),
            ("candidate", candidate_digest),
        ):
            _validate_sha(value, label=label)
    finally:
        database.close()
    return {
        "sha256": digest,
        "bytes": path.stat().st_size,
        "quickCheck": "ok",
        "entries": entries,
        "provenance": provenance,
        "expectedRows": int(contract[0]),
        "selectionVersion": int(contract[1]),
        "shardCount": int(contract[2]),
        "selectionDigest": selection_digest,
        "baselineContentDigest": baseline_digest,
        "repairQueueDigest": queue_digest,
        "candidateDigest": candidate_digest,
    }


def _canonical_identity(manifest_path: Path) -> dict[str, Any]:
    manifest = _load_json(manifest_path)
    if manifest.get("format") != "lexora-canonical-snapshot-v1":
        raise RuntimeError("unsupported canonical snapshot manifest")
    canonical = manifest.get("canonical")
    if not isinstance(canonical, dict):
        raise RuntimeError("canonical snapshot manifest has no identity")
    keys = ("rowCount", "minId", "maxId", "identitySha256", "schemaSha256")
    if any(key not in canonical for key in keys):
        raise RuntimeError("canonical snapshot identity is incomplete")
    try:
        row_count = int(canonical["rowCount"])
        minimum = int(canonical["minId"])
        maximum = int(canonical["maxId"])
    except (TypeError, ValueError) as error:
        raise RuntimeError("canonical snapshot counts are invalid") from error
    if row_count < 1 or minimum < 0 or maximum < minimum:
        raise RuntimeError("canonical snapshot range is invalid")
    identity_sha = _validate_sha(
        str(canonical["identitySha256"]), label="canonical identity"
    )
    schema_sha = _validate_sha(
        str(canonical["schemaSha256"]), label="canonical schema"
    )
    database = manifest.get("database")
    if not isinstance(database, dict) or database.get("quickCheck") != "ok":
        raise RuntimeError("canonical snapshot did not pass quick_check")
    return {
        "rowCount": row_count,
        "minId": minimum,
        "maxId": maximum,
        "identitySha256": identity_sha,
        "schemaSha256": schema_sha,
    }


def prepare_code(
    root: Path,
    track: str,
    release_id: str,
    archive: Path,
    archive_sha256: str,
    modules: list[str],
) -> dict[str, Any]:
    expected_archive_sha = _validate_sha(archive_sha256, label="code archive")
    actual_archive_sha = _sha256_file(archive)
    if actual_archive_sha != expected_archive_sha:
        raise RuntimeError("code archive SHA-256 mismatch")
    preparation, release = _release_paths(root, track, release_id)
    if preparation.exists() or release.exists():
        raise FileExistsError("release id already exists")
    preparation.parent.mkdir(parents=True, exist_ok=True)
    temporary = preparation.with_name(f".{preparation.name}.tmp-{os.getpid()}")
    if temporary.exists():
        raise FileExistsError(f"temporary preparation already exists: {temporary}")
    temporary.mkdir(mode=0o750)
    try:
        _extract_archive(archive, temporary)
        _real_imports(temporary, modules)
        manifest = {
            "format": FORMAT,
            "state": "preparing",
            "track": track,
            "releaseId": release_id,
            "preparedAt": _now(),
            "codeArchiveSha256": actual_archive_sha,
            "imports": modules,
            "files": _file_hashes(temporary),
        }
        _write_json_atomic(temporary / "release-manifest.json", manifest)
        os.replace(temporary, preparation)
        _fsync_directory(preparation.parent)
        return manifest
    finally:
        # Only this invocation's unpublished temporary tree is removed.  A
        # sealed/prepared release and every pre-existing deployment stay intact.
        if temporary.exists():
            shutil.rmtree(temporary)


def _load_release_manifest(release: Path, expected_state: str) -> dict[str, Any]:
    manifest = _load_json(release / "release-manifest.json")
    if manifest.get("format") != FORMAT or manifest.get("state") != expected_state:
        raise RuntimeError(f"release is not {expected_state}: {release}")
    return manifest


def seal_release(
    root: Path,
    track: str,
    release_id: str,
    candidate: Path | None,
    candidate_sha256: str | None,
    canonical_manifest: Path | None,
) -> dict[str, Any]:
    preparation, release = _release_paths(root, track, release_id)
    if release.exists():
        raise FileExistsError("sealed release already exists")
    manifest = _load_release_manifest(preparation, "preparing")
    _verify_files(preparation, manifest)
    modules = [str(item) for item in manifest["imports"]]
    _real_imports(preparation, modules)

    if track == "repair" and candidate is None:
        raise RuntimeError("repair releases require a candidate database")
    if candidate is not None:
        if candidate_sha256 is None or canonical_manifest is None:
            raise ValueError("candidate SHA and canonical manifest are required")
        # Validate the canonical receipt before modifying the reusable
        # preparation directory with candidate bytes.
        canonical = _canonical_identity(canonical_manifest)
        expected_candidate_sha = _validate_sha(candidate_sha256, label="candidate")
        source_report = _candidate_report(candidate)
        if source_report["sha256"] != expected_candidate_sha:
            raise RuntimeError("candidate SHA-256 mismatch before staging")
        build = preparation / "build"
        build.mkdir(exist_ok=True)
        destination = build / "lexora-open-oxford-safe-20k.sqlite"
        if destination.exists():
            raise FileExistsError("candidate is already attached")
        temporary = destination.with_name(f".{destination.name}.tmp-{os.getpid()}")
        with candidate.open("rb") as source, temporary.open("xb") as output:
            while chunk := source.read(1024 * 1024):
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
        destination_report = _candidate_report(destination)
        if destination_report != source_report:
            raise RuntimeError("staged candidate differs from uploaded candidate")
        manifest["candidate"] = destination_report
        manifest["canonical"] = canonical
        environment = preparation / "candidate.env"
        with environment.open("x", encoding="ascii") as stream:
            stream.write(
                "LEXORA_CANDIDATE_DIGEST="
                + destination_report["candidateDigest"]
                + "\n"
            )
            stream.flush()
            os.fsync(stream.fileno())

    manifest["state"] = "sealed"
    manifest["sealedAt"] = _now()
    manifest["files"] = _file_hashes(preparation)
    manifest["files"].pop("release-manifest.json", None)
    _write_json_atomic(preparation / "release-manifest.json", manifest)
    _verify_files(preparation, manifest)
    _real_imports(preparation, modules)
    release.parent.mkdir(parents=True, exist_ok=True)
    os.replace(preparation, release)
    _fsync_directory(release.parent)
    return manifest


def verify_release(
    root: Path,
    track: str,
    release_id: str,
    expected_candidate_sha256: str | None = None,
    expected_canonical_identity: str | None = None,
) -> dict[str, Any]:
    _, release = _release_paths(root, track, release_id)
    manifest = _load_release_manifest(release, "sealed")
    if manifest.get("track") != track or manifest.get("releaseId") != release_id:
        raise RuntimeError("release manifest identity mismatch")
    _verify_files(release, manifest)
    _real_imports(release, [str(item) for item in manifest["imports"]])
    candidate = manifest.get("candidate")
    if candidate is not None:
        actual = _candidate_report(
            release / "build" / "lexora-open-oxford-safe-20k.sqlite"
        )
        if actual != candidate:
            raise RuntimeError("sealed candidate no longer matches release manifest")
        if expected_candidate_sha256 is not None and actual["sha256"] != _validate_sha(
            expected_candidate_sha256, label="expected candidate"
        ):
            raise RuntimeError("sealed candidate does not match requested SHA-256")
    elif expected_candidate_sha256 is not None:
        raise RuntimeError("release has no candidate database")

    canonical = manifest.get("canonical")
    if expected_canonical_identity is not None:
        expected = _validate_sha(
            expected_canonical_identity, label="expected canonical identity"
        )
        if (
            not isinstance(canonical, dict)
            or canonical.get("identitySha256") != expected
        ):
            raise RuntimeError("release canonical identity does not match request")
    return {
        "verified": True,
        "track": track,
        "releaseId": release_id,
        "codeArchiveSha256": manifest["codeArchiveSha256"],
        "candidate": candidate,
        "canonical": canonical,
    }


def _relative_release_target(track_root: Path, release: Path) -> str:
    return release.relative_to(track_root).as_posix()


def _current_target(current: Path) -> str | None:
    if not current.exists() and not current.is_symlink():
        return None
    if not current.is_symlink():
        raise RuntimeError(f"refusing to replace non-symlink active path: {current}")
    return os.readlink(current)


def _restore_previous_journal(state_path: Path, previous_state: Any) -> None:
    if previous_state is None:
        state_path.unlink(missing_ok=True)
        _fsync_directory(state_path.parent)
    elif isinstance(previous_state, dict):
        if (
            previous_state.get("format") != STATE_FORMAT
            or previous_state.get("phase") != "active"
        ):
            raise RuntimeError("recorded previous activation state is invalid")
        _write_json_atomic(state_path, previous_state)
    else:
        raise RuntimeError("recorded previous activation state is invalid")


def _reconcile_prepared_activation(
    track_root: Path,
) -> tuple[str | None, dict[str, Any] | None]:
    """Finish or revert a journal left around an atomic symlink swap."""
    state_path = track_root / "activation-state.json"
    if not state_path.exists():
        return None, None
    state = _load_json(state_path)
    if state.get("phase") != "prepared":
        return None, None
    if (
        state.get("format") != STATE_FORMAT
        or state.get("track") != track_root.name
    ):
        raise RuntimeError("unsupported prepared activation journal")
    active = state.get("activeTarget")
    previous = state.get("previousTarget")
    if not isinstance(active, str) or not (
        previous is None or isinstance(previous, str)
    ):
        raise RuntimeError("prepared activation journal has unsafe targets")
    active_path = PurePosixPath(active)
    if (
        active_path.parts
        != ("releases", str(state.get("activeRelease") or ""))
        or not RELEASE_ID.fullmatch(active_path.name)
    ):
        raise RuntimeError("prepared activation target is invalid")
    if previous is not None:
        previous_path = PurePosixPath(previous)
        if (
            len(previous_path.parts) != 2
            or previous_path.parts[0] != "releases"
            or not RELEASE_ID.fullmatch(previous_path.name)
        ):
            raise RuntimeError("prepared previous target is invalid")
    current = _current_target(track_root / "current")
    if current == active:
        _write_json_atomic(state_path, {**state, "phase": "active"})
        return "activated", state
    if current == previous:
        _restore_previous_journal(state_path, state.get("previousState"))
        return "reverted", state
    raise RuntimeError("prepared activation journal and current symlink disagree")


def activate_release(root: Path, track: str, release_id: str) -> dict[str, Any]:
    verify_release(root, track, release_id)
    track_root = _track_root(root, track)
    _, release = _release_paths(root, track, release_id)
    current = track_root / "current"
    _reconcile_prepared_activation(track_root)
    previous = _current_target(current)
    target = _relative_release_target(track_root, release)
    temporary = track_root / f".current-{release_id}-{os.getpid()}"
    if temporary.exists() or temporary.is_symlink():
        raise FileExistsError(f"temporary activation path exists: {temporary}")
    state_path = track_root / "activation-state.json"
    previous_state = _load_json(state_path) if state_path.exists() else None
    if previous == target:
        if (
            previous_state is not None
            and previous_state.get("format") == STATE_FORMAT
            and previous_state.get("phase") == "active"
            and previous_state.get("activeRelease") == release_id
            and previous_state.get("activeTarget") == target
        ):
            return {**previous_state, "alreadyActive": True}
        raise RuntimeError("requested release is active without a matching journal")
    if previous is None:
        if previous_state is not None:
            raise RuntimeError("activation journal exists without an active release")
    elif (
        previous_state is None
        or previous_state.get("format") != STATE_FORMAT
        or previous_state.get("phase") != "active"
        or previous_state.get("activeTarget") != previous
    ):
        raise RuntimeError("active release and activation journal disagree")
    state = {
        "format": STATE_FORMAT,
        "track": track,
        "activeRelease": release_id,
        "activeTarget": target,
        "previousTarget": previous,
        "previousState": previous_state,
        "activatedAt": _now(),
    }
    # Persist the rollback target before changing the active symlink.  If the
    # post-swap state update fails, restore the old target immediately.
    prepared_state = {**state, "phase": "prepared"}
    os.symlink(target, temporary)
    try:
        _write_json_atomic(state_path, prepared_state)
        os.replace(temporary, current)
        _fsync_directory(track_root)
        _write_json_atomic(state_path, {**state, "phase": "active"})
    except BaseException:
        temporary.unlink(missing_ok=True)
        if previous is None:
            if current.is_symlink() and os.readlink(current) == target:
                current.unlink()
        else:
            recovery = track_root / f".current-recovery-{os.getpid()}"
            recovery.unlink(missing_ok=True)
            os.symlink(previous, recovery)
            os.replace(recovery, current)
        if previous_state is None:
            state_path.unlink(missing_ok=True)
        else:
            _write_json_atomic(state_path, previous_state)
        _fsync_directory(track_root)
        raise
    return state


def rollback_release(root: Path, track: str, release_id: str) -> dict[str, Any]:
    track_root = _track_root(root, track)
    current = track_root / "current"
    state_path = track_root / "activation-state.json"
    reconciled, prepared = _reconcile_prepared_activation(track_root)
    if reconciled == "reverted":
        if prepared is None or prepared.get("activeRelease") != release_id:
            raise RuntimeError("prepared rollback does not belong to requested release")
        result = {**prepared, "rolledBackAt": _now(), "rolledBack": True}
        _write_json_atomic(track_root / "last-rollback.json", result)
        return result
    state = _load_json(state_path)
    if (
        state.get("format") != STATE_FORMAT
        or state.get("activeRelease") != release_id
        or state.get("phase") != "active"
    ):
        raise RuntimeError("activation state does not belong to requested release")
    previous = state.get("previousTarget")
    current_target = _current_target(current)
    if current_target == state.get("activeTarget"):
        if previous is None:
            current.unlink()
        else:
            previous_path = track_root / str(previous)
            releases = (track_root / "releases").resolve()
            resolved = previous_path.resolve()
            if resolved.parent != releases or not resolved.is_dir():
                raise RuntimeError("recorded previous release is unavailable or unsafe")
            temporary = track_root / f".current-rollback-{os.getpid()}"
            os.symlink(str(previous), temporary)
            os.replace(temporary, current)
        _fsync_directory(track_root)
    elif current_target != previous:
        raise RuntimeError("active symlink changed after recorded activation")
    result = {
        **state,
        "rolledBackAt": _now(),
        "rolledBack": True,
    }
    _restore_previous_journal(state_path, state.get("previousState"))
    _write_json_atomic(track_root / "last-rollback.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/lexora"))
    parser.add_argument("--track", required=True)
    parser.add_argument("--release-id", required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare-code")
    prepare.add_argument("--archive", type=Path, required=True)
    prepare.add_argument("--archive-sha256", required=True)
    prepare.add_argument("--imports", required=True)

    seal = subparsers.add_parser("seal")
    seal.add_argument("--candidate", type=Path)
    seal.add_argument("--candidate-sha256")
    seal.add_argument("--canonical-manifest", type=Path)

    verify = subparsers.add_parser("verify-release")
    verify.add_argument("--candidate-sha256")
    verify.add_argument("--canonical-identity-sha256")

    subparsers.add_parser("activate")
    subparsers.add_parser("rollback")
    args = parser.parse_args()

    with _deployment_lock(args.root, args.track):
        if args.command == "prepare-code":
            result = prepare_code(
                args.root,
                args.track,
                args.release_id,
                args.archive,
                args.archive_sha256,
                _parse_imports(args.imports),
            )
        elif args.command == "seal":
            result = seal_release(
                args.root,
                args.track,
                args.release_id,
                args.candidate,
                args.candidate_sha256,
                args.canonical_manifest,
            )
        elif args.command == "verify-release":
            result = verify_release(
                args.root,
                args.track,
                args.release_id,
                args.candidate_sha256,
                args.canonical_identity_sha256,
            )
        elif args.command == "activate":
            result = activate_release(args.root, args.track, args.release_id)
        else:
            result = rollback_release(args.root, args.track, args.release_id)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
