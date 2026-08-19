#!/usr/bin/env python3
"""Create deterministic full and fast offline Lexora packages."""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

from build_oxford_scope import SCHEMA, rebuild_fts
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
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--fast-limit", type=int, default=20_000)
    parser.add_argument("--base-url", default="https://dict.12323456.xyz")
    args = parser.parse_args()

    fast_quality = assert_fast_source_ready(args.source, args.fast_limit)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    full_db = args.output_dir / f"lexora-offline-full-{args.version}.sqlite"
    fast_db = args.output_dir / f"lexora-offline-fast20k-{args.version}.sqlite"
    full_rows = copy_full(args.source, full_db)
    fast_rows = copy_fast(args.source, fast_db, args.fast_limit)
    full_archive = full_db.with_suffix(".sqlite.gz")
    fast_archive = fast_db.with_suffix(".sqlite.gz")
    compress(full_db, full_archive)
    compress(fast_db, fast_archive)

    manifest = {
        "schema_version": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "fast20kQuality": fast_quality,
        "packages": {
            "fast20k": package_entry(
                "fast20k",
                args.version,
                fast_db,
                fast_archive,
                fast_rows,
                args.base_url,
            ),
            "full": package_entry(
                "full",
                args.version,
                full_db,
                full_archive,
                full_rows,
                args.base_url,
            ),
        },
    }
    manifest_path = args.output_dir / "manifest.json"
    if manifest_path.exists():
        raise FileExistsError(f"refusing to overwrite {manifest_path}")
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
