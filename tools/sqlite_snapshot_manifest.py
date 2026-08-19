#!/usr/bin/env python3
"""Create and compare auditable online SQLite snapshots.

The file SHA identifies the exact exported snapshot.  The canonical identity
digest intentionally covers only immutable entry identity fields, allowing two
collector replicas to have different enrichment progress while still proving
that an exact-ID repair queue belongs to the same canonical corpus.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Iterable


FORMAT = "lexora-canonical-snapshot-v1"
IDENTITY_COLUMNS = (
    "id",
    "word",
    "normalized_word",
    "frequency_rank",
    # These fields determine lexical acceptance, phrase evidence and the
    # candidate identity contract.  They are not repair outputs and therefore
    # must agree between replicas before one shared candidate can be used.
    "source_json",
    "scope_json",
)


def _read_only_uri(path: Path) -> str:
    return f"{path.resolve().as_uri()}?mode=ro"


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _framed_digest(rows: Iterable[Iterable[Any]]) -> str:
    digest = hashlib.sha256(b"lexora-canonical-identity-v1\0")
    for row in rows:
        for value in row:
            encoded = json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
    return digest.hexdigest()


def _quick_check(database: sqlite3.Connection) -> None:
    rows = [str(row[0]) for row in database.execute("PRAGMA quick_check")]
    if rows != ["ok"]:
        raise RuntimeError(f"SQLite quick_check failed: {rows[:5]}")


def _schema_digest(database: sqlite3.Connection) -> str:
    rows = database.execute(
        "SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name,tbl_name"
    )
    return _framed_digest(rows)


def database_manifest(database_path: Path, *, include_file: bool) -> dict[str, Any]:
    """Describe one stable SQLite file without modifying it."""
    database = sqlite3.connect(_read_only_uri(database_path), uri=True)
    try:
        database.execute("BEGIN")
        _quick_check(database)
        columns = {
            str(row[1]) for row in database.execute("PRAGMA table_info(entries)")
        }
        missing = set(IDENTITY_COLUMNS) - columns
        if missing:
            raise RuntimeError(
                "entries is missing canonical identity columns: "
                + ", ".join(sorted(missing))
            )
        row_count, minimum, maximum = database.execute(
            "SELECT count(*),COALESCE(MIN(id),0),COALESCE(MAX(id),-1) " "FROM entries"
        ).fetchone()
        identity_rows = database.execute(
            "SELECT " + ",".join(IDENTITY_COLUMNS) + " FROM entries ORDER BY id"
        )
        identity_digest = _framed_digest(identity_rows)
        schema_digest = _schema_digest(database)
        database.commit()
    finally:
        database.close()

    result: dict[str, Any] = {
        "format": FORMAT,
        "createdAt": dt.datetime.now(dt.timezone.utc).isoformat(),
        "database": {"quickCheck": "ok"},
        "canonical": {
            "rowCount": int(row_count),
            "minId": int(minimum),
            "maxId": int(maximum),
            "identitySha256": identity_digest,
            "schemaSha256": schema_digest,
        },
    }
    if include_file:
        result["database"].update(
            {
                "bytes": database_path.stat().st_size,
                "sha256": _sha256_file(database_path),
            }
        )
    return result


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_new(temporary: Path, destination: Path) -> None:
    """Publish without ever replacing an existing snapshot or manifest."""
    os.link(temporary, destination)
    temporary.unlink()
    _fsync_directory(destination.parent)


def _write_json_temporary(path: Path, value: dict[str, Any]) -> Path:
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
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return temporary


def backup_with_manifest(
    source: Path,
    snapshot: Path,
    manifest_path: Path,
    *,
    pages: int = 2048,
    sleep_seconds: float = 0.01,
) -> dict[str, Any]:
    """Use SQLite's online backup API and publish a new snapshot + manifest."""
    if snapshot.exists() or manifest_path.exists():
        raise FileExistsError("refusing to replace an existing snapshot or manifest")
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix=f".{snapshot.name}.", suffix=".tmp", dir=snapshot.parent
    )
    os.close(descriptor)
    temporary_snapshot = Path(name)
    temporary_manifest: Path | None = None
    try:
        source_database = sqlite3.connect(_read_only_uri(source), uri=True)
        destination_database = sqlite3.connect(temporary_snapshot)
        try:
            source_database.backup(
                destination_database,
                pages=max(1, pages),
                sleep=max(0.0, sleep_seconds),
            )
            destination_database.execute("PRAGMA journal_mode=DELETE")
            destination_database.commit()
            _quick_check(destination_database)
        finally:
            destination_database.close()
            source_database.close()

        with temporary_snapshot.open("rb") as stream:
            os.fsync(stream.fileno())
        manifest = database_manifest(temporary_snapshot, include_file=True)
        manifest["snapshot"] = snapshot.name
        temporary_manifest = _write_json_temporary(manifest_path, manifest)
        _publish_new(temporary_snapshot, snapshot)
        try:
            _publish_new(temporary_manifest, manifest_path)
            temporary_manifest = None
        except BaseException:
            # The snapshot is deliberately retained if manifest publication
            # fails: never destroy a successfully exported database.
            raise
        return manifest
    finally:
        temporary_snapshot.unlink(missing_ok=True)
        if temporary_manifest is not None:
            temporary_manifest.unlink(missing_ok=True)


def load_manifest(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("format") != FORMAT:
        raise RuntimeError(f"unsupported snapshot manifest: {path}")
    if not isinstance(value.get("canonical"), dict):
        raise RuntimeError(f"missing canonical identity: {path}")
    return value


def canonical_identity(value: dict[str, Any]) -> dict[str, Any]:
    canonical = value["canonical"]
    return {
        key: canonical[key]
        for key in ("rowCount", "minId", "maxId", "identitySha256", "schemaSha256")
    }


def compare_manifests(paths: list[Path]) -> dict[str, Any]:
    if len(paths) < 2:
        raise ValueError("at least two manifests are required")
    values = [load_manifest(path) for path in paths]
    expected = canonical_identity(values[0])
    for path, value in zip(paths[1:], values[1:]):
        actual = canonical_identity(value)
        if actual != expected:
            raise RuntimeError(
                f"canonical identity mismatch for {path}: "
                f"expected={expected} actual={actual}"
            )
    return {"compatible": True, "replicas": len(paths), **expected}


def verify_snapshot(database: Path, manifest_path: Path) -> dict[str, Any]:
    expected = load_manifest(manifest_path)
    actual = database_manifest(database, include_file=True)
    if canonical_identity(actual) != canonical_identity(expected):
        raise RuntimeError("snapshot canonical identity does not match manifest")
    expected_database = expected.get("database", {})
    for key in ("bytes", "sha256"):
        if expected_database.get(key) != actual["database"].get(key):
            raise RuntimeError(f"snapshot {key} does not match manifest")
    return {"verified": True, **canonical_identity(actual)}


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    backup = subparsers.add_parser("backup")
    backup.add_argument("--source", type=Path, required=True)
    backup.add_argument("--snapshot", type=Path, required=True)
    backup.add_argument("--manifest", type=Path, required=True)
    backup.add_argument("--pages", type=int, default=2048)
    backup.add_argument("--sleep", type=float, default=0.01)

    verify = subparsers.add_parser("verify")
    verify.add_argument("--database", type=Path, required=True)
    verify.add_argument("--manifest", type=Path, required=True)

    compare = subparsers.add_parser("compare")
    compare.add_argument("--manifests", type=Path, nargs="+", required=True)

    identity = subparsers.add_parser("identity")
    identity.add_argument("--database", type=Path, required=True)

    args = parser.parse_args()
    if args.command == "backup":
        result = backup_with_manifest(
            args.source,
            args.snapshot,
            args.manifest,
            pages=args.pages,
            sleep_seconds=args.sleep,
        )
    elif args.command == "verify":
        result = verify_snapshot(args.database, args.manifest)
    elif args.command == "compare":
        result = compare_manifests(args.manifests)
    else:
        result = database_manifest(args.database, include_file=False)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
