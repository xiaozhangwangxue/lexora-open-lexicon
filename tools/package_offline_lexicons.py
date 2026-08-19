#!/usr/bin/env python3
"""Create deterministic full and fast offline Lexora packages."""
from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import errno
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

from build_oxford_scope import SCHEMA, rebuild_fts
from fast20k_pipeline import assert_candidate_ready
from top20k_quality import quality_report

COLUMNS = (
    "word",
    "normalized_word",
    "pos",
    "difficulty",
    "frequency",
    "frequency_rank",
    "us_phonetic",
    "uk_phonetic",
    "definition",
    "definition_zh",
    "synonyms_json",
    "antonyms_json",
    "examples_json",
    "phrases_json",
    "phrase_entries_json",
    "related_words_json",
    "related_entries_json",
    "senses_json",
    "source_json",
    "scope_json",
    "enrichment_json",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_full(source: Path, destination: Path) -> int:
    if destination.exists():
        raise FileExistsError(f"refusing to overwrite {destination}")
    source_db = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    destination_db = sqlite3.connect(destination)
    try:
        source_db.backup(destination_db)
        destination_db.execute("PRAGMA journal_mode=DELETE")
        destination_db.execute("PRAGMA optimize")
        destination_db.commit()
        count = destination_db.execute("SELECT count(*) FROM entries").fetchone()[0]
    finally:
        destination_db.close()
        source_db.close()
    return int(count)


def copy_fast(source: Path, destination: Path, limit: int) -> int:
    if destination.exists():
        raise FileExistsError(f"refusing to overwrite {destination}")
    source_db = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    source_db.row_factory = sqlite3.Row
    destination_db = sqlite3.connect(destination)
    try:
        destination_db.executescript(SCHEMA)
        column_sql = ",".join(COLUMNS)
        placeholders = ",".join("?" for _ in COLUMNS)
        destination_db.executemany(
            f"INSERT INTO entries({column_sql}) VALUES({placeholders})",
            (
                tuple(row[column] for column in COLUMNS)
                for row in source_db.execute(
                    f"SELECT {column_sql} FROM entries "
                    "WHERE frequency_rank IS NOT NULL "
                    "ORDER BY frequency_rank,id LIMIT ?",
                    (limit,),
                )
            ),
        )
        rebuild_fts(destination_db)
        destination_db.execute("PRAGMA journal_mode=DELETE")
        destination_db.execute("PRAGMA optimize")
        destination_db.commit()
        count = destination_db.execute("SELECT count(*) FROM entries").fetchone()[0]
    finally:
        destination_db.close()
        source_db.close()
    return int(count)


def compress(source: Path, destination: Path) -> None:
    if destination.exists():
        raise FileExistsError(f"refusing to overwrite {destination}")
    with source.open("rb") as input_stream, destination.open("wb") as raw_output:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw_output,
            compresslevel=9,
            mtime=0,
        ) as output_stream:
            shutil.copyfileobj(input_stream, output_stream, 1024 * 1024)


def publish_directory_no_replace(source: Path, destination: Path) -> None:
    """Atomically publish a directory without replacing even an empty target."""
    libc = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    result: int | None = None
    if hasattr(libc, "renameat2"):
        renameat2 = libc.renameat2
        renameat2.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameat2.restype = ctypes.c_int
        result = int(
            renameat2(-100, source_bytes, -100, destination_bytes, 1)
        )  # RENAME_NOREPLACE
    elif hasattr(libc, "renamex_np"):
        renamex_np = libc.renamex_np
        renamex_np.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        renamex_np.restype = ctypes.c_int
        result = int(
            renamex_np(source_bytes, destination_bytes, 0x00000004)
        )  # RENAME_EXCL
    if result is not None:
        if result == 0:
            return
        error = ctypes.get_errno()
        if error in {errno.EEXIST, errno.ENOTEMPTY}:
            raise FileExistsError(error, os.strerror(error), destination)
        raise OSError(error, os.strerror(error), destination)

    # Conservative fallback for platforms without an exclusive rename.  The
    # target is reserved with mkdir(O_EXCL semantics), files are hard-linked
    # without overwrite, and manifest.json is linked last as the readiness
    # marker.  This never removes or replaces a directory created by another
    # process.
    destination.mkdir()
    linked: list[Path] = []
    try:
        files = sorted(
            source.iterdir(), key=lambda path: (path.name == "manifest.json", path.name)
        )
        for path in files:
            if not path.is_file():
                raise ValueError(f"release staging contains non-file: {path}")
            published_file = destination / path.name
            os.link(path, published_file)
            linked.append(published_file)
        directory = os.open(destination, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except Exception:
        # Remove only links created by this invocation.  If another actor put
        # anything into the newly reserved directory, leave it untouched and
        # let rmdir fail rather than deleting data we do not own.
        for path in reversed(linked):
            path.unlink(missing_ok=True)
        try:
            destination.rmdir()
        except OSError:
            pass
        raise


def package_entry(
    edition: str,
    version: str,
    database: Path,
    archive: Path,
    rows: int,
    base_url: str,
) -> dict[str, object]:
    filename = archive.name
    return {
        "edition": edition,
        "version": version,
        "rows": rows,
        "archive": {
            "filename": filename,
            "compression": "gzip",
            "bytes": archive.stat().st_size,
            "sha256": sha256(archive),
            "urls": [
                f"{base_url.rstrip('/')}/v1/offline/download/{filename}",
                f"https://lexora.12323456.xyz/downloads/lexora-offline/{filename}",
            ],
        },
        "database": {
            "filename": database.name,
            "bytes": database.stat().st_size,
            "sha256": sha256(database),
        },
    }


def assert_fast_source_ready(source: Path, limit: int) -> dict[str, object]:
    """Fail closed unless the source's highest-ranked rows meet the gate."""
    if limit < 1:
        raise ValueError("fast limit must be positive")
    report = quality_report(
        source,
        max_frequency_rank=limit,
        shard_index=None,
        shard_count=1,
        unresolved_limit=20,
    )
    database = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        integrity = str(database.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        database.close()
    report["sqliteQuickCheck"] = integrity
    if (
        int(report["total"]) != limit
        or int(report["incomplete"]) != 0
        or integrity != "ok"
    ):
        raise ValueError(
            "fast lexicon quality gate failed: "
            + json.dumps(
                {
                    "expectedRows": limit,
                    "total": report["total"],
                    "incomplete": report["incomplete"],
                    "missing": report["missing"],
                    "sqliteQuickCheck": integrity,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument(
        "--fast-source",
        type=Path,
        help=(
            "atomic candidate produced by fast20k_pipeline.py; when omitted "
            "the canonical source is checked and will fail closed because it "
            "does not contain candidate provenance"
        ),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--fast-limit", type=int, default=20_000)
    parser.add_argument("--base-url", default="https://dict.12323456.xyz")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--fast-only",
        action="store_true",
        help="package only the quality-gated fast 20k edition (current default)",
    )
    mode.add_argument(
        "--include-full",
        action="store_true",
        help=(
            "reserved for a future full-dataset gate; currently rejected "
            "before any output is created"
        ),
    )
    args = parser.parse_args()
    if args.include_full:
        parser.error(
            "--include-full is disabled until full collection and quality "
            "gates are implemented"
        )

    fast_source = args.fast_source or args.source
    if args.output_dir.exists():
        raise FileExistsError(f"refusing to overwrite {args.output_dir}")
    args.output_dir.parent.mkdir(parents=True, exist_ok=True)
    # Both live inputs are first copied with SQLite's online-backup API.  The
    # exact staged snapshots are then gated, and the exact release copy is
    # gated again before compression.  Path replacement or concurrent WAL
    # writes therefore cannot swap in an unchecked database after the gate.
    with tempfile.TemporaryDirectory(
        prefix=f".{args.output_dir.name}.",
        dir=args.output_dir.parent,
    ) as temporary_directory:
        temporary = Path(temporary_directory)
        staged_canonical = temporary / "canonical.sqlite"
        staged_candidate = temporary / "candidate.sqlite"
        copy_full(args.source, staged_canonical)
        copy_full(fast_source, staged_candidate)
        assert_candidate_ready(
            staged_candidate,
            staged_canonical,
            expected_rows=args.fast_limit,
        )

        staged_release = temporary / "release"
        staged_release.mkdir()
        fast_db = staged_release / f"lexora-offline-fast20k-{args.version}.sqlite"
        fast_rows = copy_full(staged_candidate, fast_db)
        fast_quality = assert_candidate_ready(
            fast_db,
            staged_canonical,
            expected_rows=args.fast_limit,
        )
        fast_archive = fast_db.with_suffix(".sqlite.gz")
        compress(fast_db, fast_archive)
        packages: dict[str, dict[str, object]] = {
            "fast20k": package_entry(
                "fast20k",
                args.version,
                fast_db,
                fast_archive,
                fast_rows,
                args.base_url,
            )
        }
        manifest = {
            "schema_version": 2,
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "mode": "fast20k-only",
            "fast20kQuality": fast_quality,
            "packages": packages,
        }
        manifest_path = staged_release / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        for path in staged_release.iterdir():
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
        publish_directory_no_replace(staged_release, args.output_dir)
        directory = os.open(args.output_dir.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
