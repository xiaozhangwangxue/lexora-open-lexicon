#!/usr/bin/env python3
"""Validate and append a compact Kaikki relation delta to a dataset.

Dry-run is the default.  ``--apply`` is required for writes.  Every selected
delta row is preflighted against both the immutable entry ID and normalized
term before any write begins; each write batch revalidates the pair and merges
only unique JSON values.  Definitions and enrichment state are never touched.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from build_dataset import norm
from build_kaikki_relation_delta import (
    DELTA_SCHEMA_VERSION,
    sense_fingerprint,
    validate_bounds,
)
from prefill_kaikki_sense_relations import (
    RELATION_FIELDS,
    SOURCE_LICENSE,
    SOURCE_LICENSE_URL,
    SOURCE_PROVIDER,
    SOURCE_URL,
    append_unique_strings,
    json_list,
    json_object,
    merge_relation_senses,
    merge_scope,
    open_database,
    validate_schema,
)


DELTA_COLUMNS = (
    "entry_id",
    "normalized_word",
    "related_add_json",
    "phrases_add_json",
    "related_entries_add_json",
    "phrase_entries_add_json",
    "senses_patch_json",
    "source_add_json",
)


def open_delta(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(path)
    uri = f"{path.resolve().as_uri()}?mode=ro"
    database = sqlite3.connect(uri, uri=True)
    database.execute("PRAGMA query_only=ON")
    return database


def validate_delta_schema(database: sqlite3.Connection) -> dict[str, str]:
    integrity = database.execute("PRAGMA quick_check").fetchone()
    if integrity != ("ok",):
        raise ValueError("delta failed SQLite quick_check")
    tables = {
        str(row[0])
        for row in database.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    if not {"metadata", "relation_delta"} <= tables:
        raise ValueError("not a Lexora Kaikki relation delta")
    columns = {
        str(row[1])
        for row in database.execute("PRAGMA table_info(relation_delta)")
    }
    missing = sorted(set(DELTA_COLUMNS) - columns)
    if missing:
        raise ValueError(
            "delta is missing required columns: " + ", ".join(missing)
        )
    metadata = dict(database.execute("SELECT key,value FROM metadata"))
    if metadata.get("schema_version") != str(DELTA_SCHEMA_VERSION):
        raise ValueError("unsupported delta schema version")
    if metadata.get("source_license") != SOURCE_LICENSE:
        raise ValueError("delta source license is missing or unexpected")
    if metadata.get("source_license_url") != SOURCE_LICENSE_URL:
        raise ValueError("delta source license URL is missing or unexpected")
    if metadata.get("source_provider") != SOURCE_PROVIDER:
        raise ValueError("delta source provider is missing or unexpected")
    if metadata.get("source_url") != SOURCE_URL:
        raise ValueError("delta source URL is missing or unexpected")
    if not str(metadata.get("source_file") or "").strip():
        raise ValueError("delta source filename is missing")
    if not re.fullmatch(
        r"[0-9a-f]{64}",
        str(metadata.get("source_sha256") or ""),
    ):
        raise ValueError("delta source SHA-256 is missing or malformed")
    if metadata.get("modified") != "true":
        raise ValueError("delta modification marker is missing")
    if not str(metadata.get("modifications") or "").strip():
        raise ValueError("delta modification description is missing")
    try:
        expected_rows = int(metadata["rows"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("delta row-count metadata is invalid") from error
    actual_rows = int(
        database.execute("SELECT COUNT(*) FROM relation_delta").fetchone()[0]
    )
    if actual_rows != expected_rows:
        raise ValueError(
            f"delta row-count mismatch: expected {expected_rows}, "
            f"found {actual_rows}"
        )
    return metadata


def provenance_from_metadata(metadata: dict[str, str]) -> dict[str, Any]:
    return {
        "source": "kaikki",
        "provider": metadata["source_provider"],
        "sourceUrl": metadata["source_url"],
        "sourceFile": metadata["source_file"],
        "sourceSha256": metadata["source_sha256"],
        "dumpDate": metadata.get("source_dump_date", ""),
        "extractedAt": metadata.get("source_extracted_at", ""),
        "license": metadata["source_license"],
        "licenseUrl": metadata["source_license_url"],
        "schema": DELTA_SCHEMA_VERSION,
        "modified": True,
        "modifications": metadata["modifications"],
    }


def selected_delta_rows(
    database: sqlite3.Connection,
    start_id: int | None,
    end_id: int | None,
) -> Iterator[tuple[Any, ...]]:
    clauses: list[str] = []
    arguments: list[int] = []
    if start_id is not None:
        clauses.append("entry_id>=?")
        arguments.append(start_id)
    if end_id is not None:
        clauses.append("entry_id<=?")
        arguments.append(end_id)
    where = "" if not clauses else " WHERE " + " AND ".join(clauses)
    columns = ",".join(DELTA_COLUMNS)
    return iter(
        database.execute(
            f"SELECT {columns} FROM relation_delta{where} "
            "ORDER BY entry_id",
            arguments,
        )
    )


def parse_patch_row(row: tuple[Any, ...]) -> dict[str, Any]:
    entry_id = int(row[0])
    normalized_word = str(row[1])
    related = json_list(row[2], "delta.related_add_json", entry_id)
    phrases = json_list(row[3], "delta.phrases_add_json", entry_id)
    related_entries = json_list(
        row[4],
        "delta.related_entries_add_json",
        entry_id,
    )
    phrase_entries = json_list(
        row[5],
        "delta.phrase_entries_add_json",
        entry_id,
    )
    senses = json_list(row[6], "delta.senses_patch_json", entry_id)
    source = json_list(row[7], "delta.source_add_json", entry_id)
    for column, values in (
        ("related_add_json", related),
        ("phrases_add_json", phrases),
        ("source_add_json", source),
    ):
        if not all(isinstance(value, str) for value in values):
            raise ValueError(
                f"entry {entry_id} has non-string values in {column}"
            )
    for column, entries in (
        ("related_entries_add_json", related_entries),
        ("phrase_entries_add_json", phrase_entries),
    ):
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError(
                    f"entry {entry_id} has a non-object in {column}"
                )
            if not str(entry.get("word") or "").strip():
                raise ValueError(
                    f"entry {entry_id} has a nameless item in {column}"
                )
            if not str(entry.get("definition") or "").strip():
                raise ValueError(
                    f"entry {entry_id} has an undefined item in {column}"
                )
    for label, words, entries in (
        ("related", related, related_entries),
        ("phrases", phrases, phrase_entries),
    ):
        word_keys = [norm(str(value)) for value in words]
        entry_keys = [
            norm(str(value.get("word") or ""))
            for value in entries
        ]
        if (
            any(not value for value in word_keys)
            or len(set(word_keys)) != len(word_keys)
            or word_keys != entry_keys
        ):
            raise ValueError(
                f"entry {entry_id} has unsynchronized {label} flat/rich "
                "delta values"
            )
    for sense in senses:
        if not isinstance(sense, dict):
            raise ValueError(
                f"entry {entry_id} has a non-object sense patch"
            )
        if not isinstance(sense.get("relations"), dict):
            raise ValueError(
                f"entry {entry_id} has an invalid sense relation patch"
            )
        relations = sense["relations"]
        unknown = sorted(set(relations) - set(RELATION_FIELDS))
        if unknown:
            raise ValueError(
                f"entry {entry_id} has unknown sense relations: "
                + ", ".join(unknown)
            )
        for field, values in relations.items():
            if not isinstance(values, list) or not all(
                isinstance(value, str) for value in values
            ):
                raise ValueError(
                    f"entry {entry_id} has invalid values for {field}"
                )
        if "sense_index" in sense:
            if not isinstance(sense.get("sense_index"), int):
                raise ValueError(
                    f"entry {entry_id} has an invalid sense index"
                )
            if not str(sense.get("sense_fingerprint") or ""):
                raise ValueError(
                    f"entry {entry_id} has no sense fingerprint"
                )
        else:
            if not isinstance(sense.get("definitions"), list):
                raise ValueError(
                    f"entry {entry_id} has no sense definitions"
                )
    return {
        "entry_id": entry_id,
        "normalized_word": normalized_word,
        "related": related,
        "phrases": phrases,
        "related_entries": related_entries,
        "phrase_entries": phrase_entries,
        "senses": senses,
        "source": source,
    }


def merge_paired_field(
    existing_words: list[Any],
    existing_entries: list[Any],
    additions: list[str],
    entry_payloads: list[dict[str, Any]],
    limit: int = 40,
) -> tuple[list[Any], list[Any]]:
    """Merge flat/rich values as an indivisible pair.

    Collector shards may have different list occupancy than the canonical
    database used to build the delta. A pair is therefore accepted only when
    every missing side has capacity on the target shard.
    """
    words = list(existing_words)
    entries = list(existing_entries)
    word_keys = {
        norm(str(value))
        for value in existing_words
        if isinstance(value, str) and norm(str(value))
    }
    all_entry_keys = {
        norm(str(item.get("word") or ""))
        for item in existing_entries
        if isinstance(item, dict)
        and norm(str(item.get("word") or ""))
    }
    valid_entry_keys = {
        norm(str(item.get("word") or ""))
        for item in existing_entries
        if isinstance(item, dict)
        and norm(str(item.get("word") or ""))
        and " ".join(str(item.get("definition") or "").split()).strip()
    }
    word_maximum = max(limit, len(words))
    entry_maximum = max(limit, len(entries))
    if len(additions) != len(entry_payloads):
        raise ValueError("flat/rich relation pair counts do not match")
    for addition, raw in zip(additions, entry_payloads):
        word = " ".join(str(raw.get("word") or "").split()).strip()
        definition = " ".join(
            str(raw.get("definition") or "").split()
        ).strip()
        definition_zh = " ".join(
            str(raw.get("definition_zh") or "").split()
        ).strip()
        key = norm(word)
        if not key or not definition:
            continue
        # parse_patch_row already proves the flat and rich keys match. Keep
        # that invariant explicit here as defense in depth for direct callers.
        if norm(addition) != key:
            raise ValueError("flat/rich relation pair changed after validation")
        needs_word = key not in word_keys
        needs_entry = key not in valid_entry_keys
        if not needs_word and not needs_entry:
            continue
        # Do not append a second object over an invalid existing one. More
        # importantly, do not add the flat side that would make the collector
        # skip network enrichment without a usable rich definition.
        if needs_entry and key in all_entry_keys:
            continue
        if needs_word and len(words) >= word_maximum:
            continue
        if needs_entry and len(entries) >= entry_maximum:
            continue

        item = {"word": word, "definition": definition}
        if definition_zh:
            item["definition_zh"] = definition_zh
        if needs_word:
            words.append(addition)
            word_keys.add(key)
        if needs_entry:
            entries.append(item)
            all_entry_keys.add(key)
            valid_entry_keys.add(key)
    return words, entries


def expand_sense_patches(
    existing: list[Any],
    patches: list[dict[str, Any]],
    entry_id: int,
) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for patch in patches:
        if "sense_index" not in patch:
            expanded.append(patch)
            continue
        index = int(patch["sense_index"])
        if index < 0 or index >= len(existing):
            raise ValueError(
                f"entry {entry_id} sense index {index} is out of range"
            )
        target = existing[index]
        if not isinstance(target, dict):
            raise ValueError(
                f"entry {entry_id} sense index {index} is not an object"
            )
        expected = str(patch["sense_fingerprint"])
        actual = sense_fingerprint(target)
        if actual != expected:
            raise ValueError(
                f"entry {entry_id} sense fingerprint mismatch at {index}"
            )
        item: dict[str, Any] = {
            "pos": str(target.get("pos") or ""),
            "definitions": list(target.get("definitions") or []),
            "relations": patch["relations"],
        }
        if target.get("sense_ids"):
            item["sense_ids"] = list(target["sense_ids"])
        expanded.append(item)
    return expanded


def target_row(
    database: sqlite3.Connection,
    entry_id: int,
) -> tuple[Any, ...] | None:
    return database.execute(
        """
        SELECT id,normalized_word,phrases_json,related_words_json,
               senses_json,source_json,scope_json,
               related_entries_json,phrase_entries_json
        FROM entries
        WHERE id=?
        """,
        (entry_id,),
    ).fetchone()


def validate_identity(
    database: sqlite3.Connection,
    patch: dict[str, Any],
) -> tuple[Any, ...]:
    row = target_row(database, patch["entry_id"])
    if row is None:
        raise ValueError(
            f"delta entry id {patch['entry_id']} is missing from dataset"
        )
    if str(row[1]) != patch["normalized_word"]:
        raise ValueError(
            "delta identity mismatch for id "
            f"{patch['entry_id']}: expected {patch['normalized_word']!r}, "
            f"found {row[1]!r}"
        )
    return row


def merge_patch(
    database: sqlite3.Connection,
    patch: dict[str, Any],
    *,
    apply: bool,
    has_fts: bool,
    provenance: dict[str, Any],
) -> dict[str, Any]:
    row = validate_identity(database, patch)
    entry_id = int(row[0])
    old_phrases = json_list(row[2], "phrases_json", entry_id)
    old_related = json_list(row[3], "related_words_json", entry_id)
    old_senses = json_list(row[4], "senses_json", entry_id)
    old_source = json_list(row[5], "source_json", entry_id)
    old_scope = json_object(row[6], "scope_json", entry_id)
    old_related_entries = json_list(
        row[7],
        "related_entries_json",
        entry_id,
    )
    old_phrase_entries = json_list(
        row[8],
        "phrase_entries_json",
        entry_id,
    )

    new_related, new_related_entries = merge_paired_field(
        old_related,
        old_related_entries,
        patch["related"],
        patch["related_entries"],
    )
    new_phrases, new_phrase_entries = merge_paired_field(
        old_phrases,
        old_phrase_entries,
        patch["phrases"],
        patch["phrase_entries"],
    )
    try:
        expanded_senses = expand_sense_patches(
            old_senses,
            patch["senses"],
            entry_id,
        )
        new_senses = merge_relation_senses(old_senses, expanded_senses)
        new_scope = merge_scope(old_scope, provenance)
    except ValueError as error:
        raise ValueError(f"entry {entry_id}: {error}") from error
    new_source = append_unique_strings(
        old_source,
        patch["source"],
        limit=max(40, len(old_source) + len(patch["source"])),
    )
    changes = {
        "phrases_json": new_phrases != old_phrases,
        "related_words_json": new_related != old_related,
        "related_entries_json": new_related_entries != old_related_entries,
        "phrase_entries_json": new_phrase_entries != old_phrase_entries,
        "senses_json": new_senses != old_senses,
        "source_json": new_source != old_source,
        "scope_json": new_scope != old_scope,
    }
    changed = any(changes.values())
    if apply and changed:
        database.execute(
            """
            UPDATE entries
            SET phrases_json=?,related_words_json=?,
                related_entries_json=?,phrase_entries_json=?,
                senses_json=?,source_json=?,scope_json=?
            WHERE id=? AND normalized_word=?
            """,
            (
                json.dumps(
                    new_phrases,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                json.dumps(
                    new_related,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                json.dumps(
                    new_related_entries,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                json.dumps(
                    new_phrase_entries,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                json.dumps(
                    new_senses,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                json.dumps(
                    new_source,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                json.dumps(
                    new_scope,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                entry_id,
                patch["normalized_word"],
            ),
        )
        if has_fts and changes["phrases_json"]:
            database.execute(
                """
                INSERT OR REPLACE INTO entries_fts(
                  rowid,word,definition,definition_zh,examples,phrases
                )
                SELECT id,word,definition,definition_zh,
                       examples_json,phrases_json
                FROM entries
                WHERE id=?
                """,
                (entry_id,),
            )
    return {
        "changed": changed,
        "columns": changes,
        "additions": {
            "phrases": len(new_phrases) - len(old_phrases),
            "related": len(new_related) - len(old_related),
            "relatedEntries": (
                len(new_related_entries) - len(old_related_entries)
            ),
            "phraseEntries": (
                len(new_phrase_entries) - len(old_phrase_entries)
            ),
            "senses": len(new_senses) - len(old_senses),
            "source": len(new_source) - len(old_source),
        },
    }


def apply_delta(
    dataset: Path,
    delta: Path,
    *,
    apply: bool = False,
    start_id: int | None = None,
    end_id: int | None = None,
    batch_size: int = 500,
) -> dict[str, Any]:
    validate_bounds(start_id, end_id)
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if not dataset.is_file():
        raise FileNotFoundError(dataset)

    target = open_database(dataset, apply=apply)
    patch_db = open_delta(delta)
    stats: dict[str, Any] = {
        "mode": "apply" if apply else "dry-run",
        "startId": start_id,
        "endId": end_id,
        "selectedRows": 0,
        "changedRows": 0,
        "unchangedRows": 0,
        "columnsChanged": Counter(),
        "valuesAdded": Counter(),
    }
    try:
        has_fts = validate_schema(target)
        target_columns = {
            str(row[1])
            for row in target.execute("PRAGMA table_info(entries)")
        }
        missing_target = sorted(
            {
                "related_entries_json",
                "phrase_entries_json",
            }
            - target_columns
        )
        if missing_target:
            raise ValueError(
                "target dataset is missing rich relation columns: "
                + ", ".join(missing_target)
            )
        metadata = validate_delta_schema(patch_db)
        stats["deltaMetadata"] = metadata
        provenance = provenance_from_metadata(metadata)

        # Full preflight before any write.  This rejects a delta built from a
        # different database (including sense-layout drift) instead of
        # partially applying it.
        for raw in selected_delta_rows(patch_db, start_id, end_id):
            patch = parse_patch_row(raw)
            if apply:
                merge_patch(
                    target,
                    patch,
                    apply=False,
                    has_fts=has_fts,
                    provenance=provenance,
                )
            else:
                validate_identity(target, patch)
            stats["selectedRows"] += 1

        cursor = selected_delta_rows(patch_db, start_id, end_id)
        while True:
            raw_batch = []
            for _ in range(batch_size):
                try:
                    raw_batch.append(next(cursor))
                except StopIteration:
                    break
            if not raw_batch:
                break
            if apply:
                target.execute("BEGIN IMMEDIATE")
            try:
                for raw in raw_batch:
                    patch = parse_patch_row(raw)
                    result = merge_patch(
                        target,
                        patch,
                        apply=apply,
                        has_fts=has_fts,
                        provenance=provenance,
                    )
                    if result["changed"]:
                        stats["changedRows"] += 1
                        for column, changed in result["columns"].items():
                            if changed:
                                stats["columnsChanged"][column] += 1
                        for name, count in result["additions"].items():
                            stats["valuesAdded"][name] += count
                    else:
                        stats["unchangedRows"] += 1
                if apply:
                    target.commit()
            except Exception:
                if apply:
                    target.rollback()
                raise
    finally:
        patch_db.close()
        target.close()

    for key in ("columnsChanged", "valuesAdded"):
        stats[key] = dict(sorted(stats[key].items()))
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Validate or idempotently append a Kaikki relation delta."
        )
    )
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--delta", type=Path, required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write append-only merges; default is read-only dry-run",
    )
    parser.add_argument("--start-id", type=int)
    parser.add_argument("--end-id", type=int)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    report = apply_delta(
        args.dataset,
        args.delta,
        apply=args.apply,
        start_id=args.start_id,
        end_id=args.end_id,
        batch_size=args.batch_size,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
