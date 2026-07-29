#!/usr/bin/env python3
"""Build a compact SQLite append-only Kaikki relation delta.

The default mode is a read-only dry-run.  Supplying ``--write-delta`` creates
one new SQLite file atomically; it never mutates or replaces the source
dataset.  The resulting file is intended to be copied to collector servers
instead of copying the multi-gigabyte Kaikki JSONL dump.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import sqlite3
from collections import Counter
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

from build_dataset import SOURCE, norm
from prefill_kaikki_sense_relations import (
    PROVENANCE,
    SOURCE_DUMP_DATE,
    SOURCE_EXTRACTED_AT,
    SOURCE_LICENSE,
    SOURCE_LICENSE_URL,
    SOURCE_PROVIDER,
    SOURCE_URL,
    aggregate_patches,
    extract_relation_senses,
    json_list,
    json_object,
    open_database,
    sense_matches,
    sense_patch_delta,
    validate_schema,
)


DELTA_SCHEMA_VERSION = 1
DELTA_SCHEMA = """
CREATE TABLE metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE relation_delta (
  entry_id INTEGER PRIMARY KEY,
  normalized_word TEXT NOT NULL,
  related_add_json TEXT NOT NULL,
  phrases_add_json TEXT NOT NULL,
  related_entries_add_json TEXT NOT NULL,
  phrase_entries_add_json TEXT NOT NULL,
  senses_patch_json TEXT NOT NULL,
  source_add_json TEXT NOT NULL
);
CREATE UNIQUE INDEX idx_relation_delta_term
  ON relation_delta(normalized_word);
"""


def validate_bounds(
    start_id: int | None,
    end_id: int | None,
) -> None:
    if start_id is not None and start_id < 1:
        raise ValueError("start_id must be positive")
    if end_id is not None and end_id < 1:
        raise ValueError("end_id must be positive")
    if (
        start_id is not None
        and end_id is not None
        and start_id > end_id
    ):
        raise ValueError("start_id cannot be greater than end_id")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def lookup_entry(
    database: sqlite3.Connection,
    key: str,
    start_id: int | None,
    end_id: int | None,
    *,
    has_named_columns: bool,
) -> tuple[Any, ...] | None:
    clauses = ["normalized_word=?"]
    arguments: list[Any] = [key]
    if start_id is not None:
        clauses.append("id>=?")
        arguments.append(start_id)
    if end_id is not None:
        clauses.append("id<=?")
        arguments.append(end_id)
    named_columns = (
        ",related_entries_json,phrase_entries_json"
        if has_named_columns
        else ",'[]' AS related_entries_json,'[]' AS phrase_entries_json"
    )
    return database.execute(
        """
        SELECT id,normalized_word,phrases_json,related_words_json,
               senses_json,source_json,scope_json
        FROM entries
        WHERE
        """
        .replace(
            "\n        FROM entries",
            f"{named_columns}\n        FROM entries",
        )
        + " AND ".join(clauses),
        arguments,
    ).fetchone()


def delta_for_entry(
    row: tuple[Any, ...],
    patches: list[dict[str, Any]],
    resolve_entry: Callable[[str], dict[str, str] | None],
    *,
    has_named_columns: bool,
) -> dict[str, Any]:
    entry_id = int(row[0])
    old_phrases = json_list(row[2], "phrases_json", entry_id)
    old_related = json_list(row[3], "related_words_json", entry_id)
    old_senses = json_list(row[4], "senses_json", entry_id)
    old_source = json_list(row[5], "source_json", entry_id)
    # Validate now even though the delta carries a compact global provenance
    # record.  A malformed target scope must never be hidden by the exporter.
    json_object(row[6], "scope_json", entry_id)
    old_related_entries = (
        json_list(row[7], "related_entries_json", entry_id)
        if has_named_columns
        else []
    )
    old_phrase_entries = (
        json_list(row[8], "phrase_entries_json", entry_id)
        if has_named_columns
        else []
    )

    _, related, _, _ = aggregate_patches(patches)
    resolved = [
        (value, resolve_entry(value))
        for value in related
    ]
    # Do not set the legacy word-only fields unless a detailed target entry can
    # be supplied at the same time.  Otherwise the network collector sees a
    # non-empty field and skips the richer Datamuse provider.
    resolved_entries = [
        entry for _, entry in resolved if entry is not None
    ]
    phrase_entries = [
        entry
        for value, entry in resolved
        if entry is not None
        and (
            " " in value
            or "-" in value
            or " " in str(entry.get("word") or "")
            or "-" in str(entry.get("word") or "")
        )
    ]
    related_add, related_entries_add = paired_field_delta(
        old_related,
        old_related_entries,
        resolved_entries,
    )
    phrases_add, phrase_entries_add = paired_field_delta(
        old_phrases,
        old_phrase_entries,
        phrase_entries,
    )
    senses_patch = compact_sense_patch_delta(old_senses, patches)
    source_add = [] if "kaikki" in old_source else ["kaikki"]
    return {
        "entry_id": entry_id,
        "normalized_word": str(row[1]),
        "related_add": related_add,
        "phrases_add": phrases_add,
        "related_entries_add": related_entries_add,
        "phrase_entries_add": phrase_entries_add,
        "senses_patch": senses_patch,
        # Keep an explicit per-row source even when the local source already
        # has it; remote datasets can independently append the attribution.
        "source_add": source_add or ["kaikki"],
        "has_changes": bool(
            related_add
            or phrases_add
            or related_entries_add
            or phrase_entries_add
            or senses_patch
            or source_add
        ),
    }


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def sense_fingerprint(sense: dict[str, Any]) -> str:
    identity = {
        "pos": str(sense.get("pos") or ""),
        "definitions": sense.get("definitions") or [],
        "sense_ids": sense.get("sense_ids") or [],
    }
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def compact_sense_patch_delta(
    existing: list[Any],
    patches: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Use a verified sense index when possible to keep the delta small."""
    result: list[dict[str, Any]] = []
    for patch in sense_patch_delta(existing, patches):
        match_index: int | None = None
        for index, item in enumerate(existing):
            if isinstance(item, dict) and sense_matches(item, patch):
                match_index = index
                break
        if match_index is None:
            result.append(patch)
            continue
        target = existing[match_index]
        assert isinstance(target, dict)
        result.append(
            {
                "sense_index": match_index,
                "sense_fingerprint": sense_fingerprint(target),
                "relations": patch["relations"],
            }
        )
    return result


def paired_field_delta(
    existing_words: list[Any],
    existing_entries: list[Any],
    candidates: list[dict[str, str]],
    limit: int = 40,
) -> tuple[list[str], list[dict[str, str]]]:
    """Return flat/rich additions that can be accepted as one pair.

    Every emitted flat word has the same normalized key as one emitted rich
    payload.  The payload is included even when the canonical row already has
    that rich entry, because a collector shard may have diverged.  Capacity is
    checked for both columns before accepting a new pair.
    """
    word_keys = {
        norm(clean_text(value))
        for value in existing_words
        if isinstance(value, str) and norm(clean_text(value))
    }
    all_entry_keys = {
        norm(clean_text(item.get("word")))
        for item in existing_entries
        if isinstance(item, dict)
        and norm(clean_text(item.get("word")))
    }
    valid_entry_keys = {
        norm(clean_text(item.get("word")))
        for item in existing_entries
        if isinstance(item, dict)
        and norm(clean_text(item.get("word")))
        and clean_text(item.get("definition"))
    }
    word_capacity = max(0, max(limit, len(existing_words)) - len(existing_words))
    entry_capacity = max(
        0,
        max(limit, len(existing_entries)) - len(existing_entries),
    )
    words_add: list[str] = []
    entries_add: list[dict[str, str]] = []
    accepted: set[str] = set()
    for raw in candidates:
        word = clean_text(raw.get("word"))
        definition = clean_text(raw.get("definition"))
        definition_zh = clean_text(raw.get("definition_zh"))
        key = norm(word)
        if not key or not definition or key in accepted or key in word_keys:
            continue
        accepted.add(key)
        needs_entry = key not in valid_entry_keys
        # An invalid pre-existing object cannot be replaced without violating
        # the append-only guarantee, so do not introduce its flat counterpart.
        if needs_entry and key in all_entry_keys:
            continue
        if len(words_add) >= word_capacity:
            continue
        if needs_entry and entry_capacity <= 0:
            continue

        entry = {"word": word, "definition": definition}
        if definition_zh:
            entry["definition_zh"] = definition_zh
        words_add.append(word)
        # Carry the companion on every delta row. The remote applier decides
        # whether it already exists or consumes one rich-entry slot.
        entries_add.append(entry)
        word_keys.add(key)
        if needs_entry:
            entry_capacity -= 1
            all_entry_keys.add(key)
            valid_entry_keys.add(key)
    return words_add, entries_add


def temporary_output_path(output: Path) -> Path:
    return output.with_name(f".{output.name}.tmp-{os.getpid()}")


def build_delta(
    dataset: Path,
    kaikki: Path,
    *,
    output: Path | None = None,
    start_id: int | None = None,
    end_id: int | None = None,
    commit_size: int = 5000,
) -> dict[str, Any]:
    """Scan Kaikki and optionally create an atomic compact delta."""
    validate_bounds(start_id, end_id)
    if commit_size <= 0:
        raise ValueError("commit_size must be positive")
    if not dataset.is_file():
        raise FileNotFoundError(dataset)
    if not kaikki.is_file():
        raise FileNotFoundError(kaikki)
    if output is not None and output.exists():
        raise FileExistsError(f"refusing to overwrite {output}")

    source_stat = kaikki.stat()
    source_identity = (
        source_stat.st_dev,
        source_stat.st_ino,
        source_stat.st_size,
        source_stat.st_mtime_ns,
    )
    source_sha256 = sha256(kaikki)
    source = open_database(dataset, apply=False)
    destination: sqlite3.Connection | None = None
    temporary: Path | None = None
    stats: dict[str, Any] = {
        "mode": "write-delta" if output is not None else "dry-run",
        "schemaVersion": DELTA_SCHEMA_VERSION,
        "startId": start_id,
        "endId": end_id,
        "source": {
            "provider": SOURCE_PROVIDER,
            "file": kaikki.name,
            "sha256": source_sha256,
            "url": SOURCE_URL,
            "license": SOURCE_LICENSE,
            "licenseUrl": SOURCE_LICENSE_URL,
            "dumpDate": SOURCE_DUMP_DATE,
            "extractedAt": SOURCE_EXTRACTED_AT,
            "modified": PROVENANCE["modified"],
            "modifications": PROVENANCE["modifications"],
        },
        "jsonLines": 0,
        "englishRecords": 0,
        "invalidJsonLines": 0,
        "relationTerms": 0,
        "matchedTerms": 0,
        "unmatchedTerms": 0,
        "deltaRows": 0,
        "relationValuesSeen": Counter(),
        "valuesIncluded": Counter(),
    }
    current_key: str | None = None
    current_patches: list[dict[str, Any]] = []

    try:
        validate_schema(source)
        source_columns = {
            str(row[1])
            for row in source.execute("PRAGMA table_info(entries)")
        }
        has_named_columns = {
            "related_entries_json",
            "phrase_entries_json",
        } <= source_columns

        @lru_cache(maxsize=100_000)
        def resolve_entry(value: str) -> dict[str, str] | None:
            target = source.execute(
                """
                SELECT word,definition,definition_zh
                FROM entries
                WHERE normalized_word=?
                """,
                (norm(value),),
            ).fetchone()
            if target is None:
                return None
            word = clean_text(target[0])
            definition = clean_text(target[1])
            definition_zh = clean_text(target[2])
            # A word-only link would suppress the richer network provider.
            # Require a displayable English definition before adding either.
            if not word or not definition:
                return None
            result = {"word": word, "definition": definition}
            if definition_zh:
                result["definition_zh"] = definition_zh
            return result

        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            temporary = temporary_output_path(output)
            if temporary.exists():
                raise FileExistsError(
                    f"temporary output already exists: {temporary}"
                )
            destination = sqlite3.connect(temporary)
            destination.executescript(DELTA_SCHEMA)

        def flush_current() -> None:
            nonlocal current_patches
            if not current_key or not current_patches:
                current_patches = []
                return
            stats["relationTerms"] += 1
            _, _, _, counts = aggregate_patches(current_patches)
            stats["relationValuesSeen"].update(counts)
            row = lookup_entry(
                source,
                current_key,
                start_id,
                end_id,
                has_named_columns=has_named_columns,
            )
            if row is None:
                stats["unmatchedTerms"] += 1
                current_patches = []
                return
            stats["matchedTerms"] += 1
            delta = delta_for_entry(
                row,
                current_patches,
                resolve_entry,
                has_named_columns=has_named_columns,
            )
            current_patches = []
            if not delta["has_changes"]:
                return
            stats["deltaRows"] += 1
            stats["valuesIncluded"]["related"] += len(
                delta["related_add"]
            )
            stats["valuesIncluded"]["phrases"] += len(
                delta["phrases_add"]
            )
            stats["valuesIncluded"]["senses"] += len(
                delta["senses_patch"]
            )
            stats["valuesIncluded"]["relatedEntries"] += len(
                delta["related_entries_add"]
            )
            stats["valuesIncluded"]["phraseEntries"] += len(
                delta["phrase_entries_add"]
            )
            if destination is not None:
                destination.execute(
                    """
                    INSERT INTO relation_delta(
                      entry_id,normalized_word,related_add_json,
                      phrases_add_json,related_entries_add_json,
                      phrase_entries_add_json,senses_patch_json,
                      source_add_json
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        delta["entry_id"],
                        delta["normalized_word"],
                        json.dumps(
                            delta["related_add"],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        json.dumps(
                            delta["phrases_add"],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        json.dumps(
                            delta["related_entries_add"],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        json.dumps(
                            delta["phrase_entries_add"],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        json.dumps(
                            delta["senses_patch"],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        json.dumps(
                            delta["source_add"],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    ),
                )
                if stats["deltaRows"] % commit_size == 0:
                    destination.commit()

        with gzip.open(kaikki, "rt", encoding="utf-8") as stream:
            for line in stream:
                stats["jsonLines"] += 1
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    stats["invalidJsonLines"] += 1
                    continue
                if data.get("lang_code") != "en":
                    continue
                stats["englishRecords"] += 1
                key = norm(str(data.get("word") or ""))
                if key != current_key:
                    flush_current()
                    current_key = key
                if key:
                    current_patches.extend(extract_relation_senses(data))
        flush_current()

        if destination is not None and output is not None:
            final_stat = kaikki.stat()
            final_identity = (
                final_stat.st_dev,
                final_stat.st_ino,
                final_stat.st_size,
                final_stat.st_mtime_ns,
            )
            if final_identity != source_identity:
                raise ValueError(
                    "Kaikki source changed while the delta was being built"
                )
            metadata = {
                "schema_version": str(DELTA_SCHEMA_VERSION),
                "created_at": dt.datetime.now(
                    dt.timezone.utc
                ).isoformat(),
                "source_provider": SOURCE_PROVIDER,
                "source_license": SOURCE_LICENSE,
                "source_license_url": SOURCE_LICENSE_URL,
                "source_file": kaikki.name,
                "source_bytes": str(source_stat.st_size),
                "source_sha256": source_sha256,
                "source_url": SOURCE_URL,
                "source_dump_date": SOURCE_DUMP_DATE,
                "source_extracted_at": SOURCE_EXTRACTED_AT,
                "modified": json.dumps(PROVENANCE["modified"]),
                "modifications": str(PROVENANCE["modifications"]),
                "dataset_file": dataset.name,
                "start_id": "" if start_id is None else str(start_id),
                "end_id": "" if end_id is None else str(end_id),
                "rows": str(stats["deltaRows"]),
            }
            destination.executemany(
                "INSERT INTO metadata(key,value) VALUES(?,?)",
                metadata.items(),
            )
            destination.execute("PRAGMA journal_mode=DELETE")
            destination.commit()
            destination.close()
            destination = None
            # Publish without replacing a path created during the long scan.
            # Both paths are in the same directory, so hard-link creation is
            # atomic and fails closed with FileExistsError.
            os.link(temporary, output)
            temporary.unlink()
            stats["output"] = str(output)
            stats["outputBytes"] = output.stat().st_size
            temporary = None
    finally:
        if destination is not None:
            destination.close()
        source.close()
        if temporary is not None and temporary.exists():
            temporary.unlink()

    for key in ("relationValuesSeen", "valuesIncluded"):
        stats[key] = dict(sorted(stats[key].items()))
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or atomically write a compact Kaikki relation delta."
        )
    )
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument(
        "--kaikki",
        type=Path,
        default=SOURCE / "enwiktionary-wiktextract.jsonl.gz",
    )
    parser.add_argument(
        "--write-delta",
        type=Path,
        help="explicitly create this new SQLite delta; never overwrites",
    )
    parser.add_argument("--start-id", type=int)
    parser.add_argument("--end-id", type=int)
    parser.add_argument("--commit-size", type=int, default=5000)
    args = parser.parse_args()
    report = build_delta(
        args.dataset,
        args.kaikki,
        output=args.write_delta,
        start_id=args.start_id,
        end_id=args.end_id,
        commit_size=args.commit_size,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
