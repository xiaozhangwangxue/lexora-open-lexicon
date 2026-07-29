#!/usr/bin/env python3
"""Backfill missed sense-level relations from a Kaikki Wiktextract dump.

The original builders already ingest entry-level relations and the
``synonyms``, ``antonyms`` and ``related`` fields attached to a sense.  This
tool adds the sense-level relation fields that were not previously flattened:
alternative/form-of links, hypernyms, hyponyms, coordinate terms, meronyms,
holonyms and troponyms.

Safety properties:

* dry-run is the default and opens the dataset with SQLite ``mode=ro``;
* the standalone command rejects ``--apply`` and cannot edit its dataset;
* rerunning the same input is idempotent;
* existing JSON values and unknown sense keys are preserved;
* source/license provenance is recorded without changing definitions or
  network-enrichment state.

The standalone writer intentionally does not populate the legacy flat
``related_words_json`` or ``phrases_json`` fields.  Use
``build_kaikki_relation_delta.py`` and ``apply_kaikki_relation_delta.py`` for
that operation: those tools add a canonical target definition entry at the
same time, so a word-only value cannot accidentally suppress richer network
enrichment.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from build_dataset import SOURCE, clean_list, norm


RELATION_FIELDS = (
    "alt_of",
    "form_of",
    "hypernyms",
    "hyponyms",
    "coordinate_terms",
    "meronyms",
    "holonyms",
    "troponyms",
)
SOURCE_PROVIDER = "Kaikki/Wiktextract"
SOURCE_URL = "https://kaikki.org/dictionary/rawdata.html"
SOURCE_FILE = "enwiktionary-wiktextract.jsonl.gz"
SOURCE_DUMP_DATE = "2026-07-06"
SOURCE_EXTRACTED_AT = "2026-07-25"
SOURCE_LICENSE = "CC BY-SA 4.0"
SOURCE_LICENSE_URL = (
    "https://creativecommons.org/licenses/by-sa/4.0/"
)
TYPED_ONLY_FIELDS = ("alt_of", "form_of")
# These fields are semantically related terms.  Keep their order: the flat
# 40-item compatibility columns must prefer taxonomic relations before looser
# coordinate/part-whole links.  Form/alternative links remain typed only.
SEMANTIC_RELATION_FIELDS = (
    "hypernyms",
    "hyponyms",
    "coordinate_terms",
    "meronyms",
    "holonyms",
    "troponyms",
)
PROVENANCE_KEY = "kaikkiRelationPrefill"
PROVENANCE = {
    "source": "kaikki",
    "provider": SOURCE_PROVIDER,
    "sourceUrl": SOURCE_URL,
    "sourceFile": SOURCE_FILE,
    "dumpDate": SOURCE_DUMP_DATE,
    "extractedAt": SOURCE_EXTRACTED_AT,
    "license": SOURCE_LICENSE,
    "licenseUrl": SOURCE_LICENSE_URL,
    "schema": 1,
    "modified": True,
    "modifications": (
        "Selected, normalized, linked and reformatted sense relations "
        "for Lexora."
    ),
}
REQUIRED_COLUMNS = {
    "id",
    "normalized_word",
    "phrases_json",
    "related_words_json",
    "senses_json",
    "source_json",
    "scope_json",
}


def json_list(raw: str | None, column: str, entry_id: int) -> list[Any]:
    try:
        value = json.loads(raw or "[]")
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"entry {entry_id} has invalid JSON in {column}"
        ) from error
    if not isinstance(value, list):
        raise ValueError(f"entry {entry_id} has non-list JSON in {column}")
    return value


def json_object(raw: str | None, column: str, entry_id: int) -> dict[str, Any]:
    try:
        value = json.loads(raw or "{}")
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"entry {entry_id} has invalid JSON in {column}"
        ) from error
    if not isinstance(value, dict):
        raise ValueError(f"entry {entry_id} has non-object JSON in {column}")
    return value


def relation_words(items: Any) -> list[str]:
    """Return the link targets accepted by Wiktextract relation fields."""
    if not isinstance(items, list):
        return []
    values: list[str] = []
    for item in items:
        if isinstance(item, dict):
            value = item.get("word")
        elif isinstance(item, str):
            value = item
        else:
            value = None
        if value:
            values.append(str(value))
    return clean_list(values)


def append_unique_strings(
    existing: list[Any],
    additions: Iterable[str],
    limit: int = 40,
) -> list[Any]:
    """Append clean strings without normalizing or deleting existing values."""
    result = list(existing)
    seen = {
        str(value).strip()
        for value in existing
        if isinstance(value, str) and str(value).strip()
    }
    maximum = max(limit, len(result))
    for addition in additions:
        value = " ".join(str(addition).strip().split())
        if not value or value in seen:
            continue
        if len(result) >= maximum:
            break
        result.append(value)
        seen.add(value)
    return result


def extract_relation_senses(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract only relation fields omitted by the existing builder."""
    result: list[dict[str, Any]] = []
    pos = str(data.get("pos") or "")
    for sense in data.get("senses") or []:
        if not isinstance(sense, dict):
            continue
        relations = {
            field: relation_words(sense.get(field))
            for field in RELATION_FIELDS
        }
        relations = {
            field: values for field, values in relations.items() if values
        }
        if not relations:
            continue
        glosses = sense.get("glosses") or sense.get("raw_glosses") or []
        definitions = clean_list(
            (str(value) for value in glosses),
            12,
        )
        patch: dict[str, Any] = {
            "pos": pos,
            "definitions": definitions,
            "relations": relations,
        }
        sense_ids = clean_list(
            (str(value) for value in (sense.get("senseid") or [])),
            12,
        )
        if sense_ids:
            patch["sense_ids"] = sense_ids
        result.append(patch)
    return result


def sense_matches(existing: dict[str, Any], patch: dict[str, Any]) -> bool:
    if str(existing.get("pos") or "") != patch["pos"]:
        return False
    existing_definitions = existing.get("definitions") or []
    if not isinstance(existing_definitions, list):
        return False
    if clean_list(str(value) for value in existing_definitions) != patch[
        "definitions"
    ]:
        return False
    patch_ids = patch.get("sense_ids") or []
    existing_ids = existing.get("sense_ids") or []
    if patch_ids and existing_ids:
        if not isinstance(existing_ids, list):
            return False
        return clean_list(str(value) for value in existing_ids) == patch_ids
    return True


def merge_relation_senses(
    existing: list[Any],
    patches: Iterable[dict[str, Any]],
) -> list[Any]:
    """Merge structured relation data while preserving all existing keys."""
    merged = [
        dict(item) if isinstance(item, dict) else item
        for item in existing
    ]
    for patch in patches:
        target: dict[str, Any] | None = None
        for item in merged:
            if isinstance(item, dict) and sense_matches(item, patch):
                target = item
                break
        if target is None:
            target = {
                "pos": patch["pos"],
                "definitions": list(patch["definitions"]),
            }
            if patch.get("sense_ids"):
                target["sense_ids"] = list(patch["sense_ids"])
            merged.append(target)

        old_relations = target.get("relations") or {}
        if not isinstance(old_relations, dict):
            raise ValueError("sense has non-object relations JSON")
        new_relations = dict(old_relations)
        for field in RELATION_FIELDS:
            additions = patch["relations"].get(field) or []
            if not additions:
                continue
            current = new_relations.get(field) or []
            if not isinstance(current, list):
                raise ValueError(
                    f"sense relation {field} has non-list JSON"
                )
            new_relations[field] = append_unique_strings(
                current,
                additions,
            )
        target["relations"] = new_relations
        raw_sources = target.get("relation_sources") or {}
        if not isinstance(raw_sources, dict):
            raise ValueError("sense has non-object relation_sources JSON")
        relation_sources = dict(raw_sources)
        for field in patch["relations"]:
            current_sources = relation_sources.get(field) or []
            if isinstance(current_sources, str):
                current_sources = [current_sources]
            if not isinstance(current_sources, list):
                raise ValueError(
                    f"sense relation source {field} has non-list JSON"
                )
            relation_sources[field] = append_unique_strings(
                current_sources,
                ["kaikki"],
                limit=max(4, len(current_sources) + 1),
            )
        target["relation_sources"] = relation_sources
    return merged


def sense_patch_delta(
    existing: list[Any],
    patches: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return only relation values absent from the current structured senses."""
    delta: list[dict[str, Any]] = []
    for patch in patches:
        target: dict[str, Any] | None = None
        for item in existing:
            if isinstance(item, dict) and sense_matches(item, patch):
                target = item
                break
        current_relations: dict[str, Any] = {}
        if target is not None:
            raw_relations = target.get("relations") or {}
            if not isinstance(raw_relations, dict):
                raise ValueError("sense has non-object relations JSON")
            current_relations = raw_relations

        missing: dict[str, list[str]] = {}
        for field in RELATION_FIELDS:
            additions = patch["relations"].get(field) or []
            current = current_relations.get(field) or []
            if not isinstance(current, list):
                raise ValueError(
                    f"sense relation {field} has non-list JSON"
                )
            merged = append_unique_strings(current, additions)
            values = [
                str(value)
                for value in merged[len(current):]
                if isinstance(value, str)
            ]
            if values:
                missing[field] = values
        if not missing:
            continue
        item = {
            "pos": patch["pos"],
            "definitions": list(patch["definitions"]),
            "relations": missing,
        }
        if patch.get("sense_ids"):
            item["sense_ids"] = list(patch["sense_ids"])
        delta.append(item)
    return delta


def merge_source(existing: list[Any]) -> list[Any]:
    if "kaikki" in existing:
        return list(existing)
    return [*existing, "kaikki"]


def merge_scope(
    existing: dict[str, Any],
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged = dict(existing)
    expected = dict(provenance or PROVENANCE)
    old = merged.get(PROVENANCE_KEY)
    if old is None:
        merged[PROVENANCE_KEY] = expected
        return merged
    if not isinstance(old, dict):
        raise ValueError(f"{PROVENANCE_KEY} has non-object JSON")
    merged_provenance = dict(old)
    for key, value in expected.items():
        merged_provenance[key] = value
    merged[PROVENANCE_KEY] = merged_provenance
    return merged


def aggregate_patches(
    patches: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], list[str], Counter[str]]:
    patch_list = list(patches)
    flat: list[str] = []
    counts: Counter[str] = Counter()
    by_field: dict[str, list[str]] = {
        field: [] for field in SEMANTIC_RELATION_FIELDS
    }
    for patch in patch_list:
        for field, values in patch["relations"].items():
            counts[field] += len(values)
            if field in by_field:
                by_field[field].extend(values)
    for field in SEMANTIC_RELATION_FIELDS:
        values = by_field[field]
        flat.extend(clean_list(values, max(40, len(values))))
    related = clean_list(flat, max(40, len(flat)))
    phrase_values = [
        value for value in related if " " in value or "-" in value
    ]
    phrases = clean_list(
        phrase_values,
        max(40, len(phrase_values)),
    )
    return patch_list, related, phrases, counts


def validate_schema(database: sqlite3.Connection) -> bool:
    columns = {
        str(row[1])
        for row in database.execute("PRAGMA table_info(entries)")
    }
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        raise ValueError(
            "dataset is missing required entries columns: "
            + ", ".join(missing)
        )
    return (
        database.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='entries_fts'"
        ).fetchone()
        is not None
    )


def process_term(
    database: sqlite3.Connection,
    key: str,
    patches: list[dict[str, Any]],
) -> dict[str, Any]:
    row = database.execute(
        """
        SELECT id,phrases_json,related_words_json,senses_json,
               source_json,scope_json
        FROM entries
        WHERE normalized_word=?
        """,
        (key,),
    ).fetchone()
    if row is None:
        return {"matched": False, "changed": False, "additions": {}}

    entry_id = int(row[0])
    old_phrases = json_list(row[1], "phrases_json", entry_id)
    old_related = json_list(row[2], "related_words_json", entry_id)
    old_senses = json_list(row[3], "senses_json", entry_id)
    old_source = json_list(row[4], "source_json", entry_id)
    old_scope = json_object(row[5], "scope_json", entry_id)

    # Typed-only standalone mode.  Flat semantic fields require rich target
    # entries and therefore belong exclusively to the compact delta path.
    new_phrases = list(old_phrases)
    new_related = list(old_related)
    try:
        new_senses = merge_relation_senses(old_senses, patches)
        new_scope = merge_scope(old_scope)
    except ValueError as error:
        raise ValueError(f"entry {entry_id}: {error}") from error
    new_source = merge_source(old_source)

    changed_columns = {
        "phrases_json": new_phrases != old_phrases,
        "related_words_json": new_related != old_related,
        "senses_json": new_senses != old_senses,
        "source_json": new_source != old_source,
        "scope_json": new_scope != old_scope,
    }
    changed = any(changed_columns.values())
    return {
        "matched": True,
        "changed": changed,
        "columns": changed_columns,
        "additions": {
            "phrases": len(new_phrases) - len(old_phrases),
            "related": len(new_related) - len(old_related),
            "senses": len(new_senses) - len(old_senses),
        },
    }


def open_database(path: Path, apply: bool) -> sqlite3.Connection:
    if apply:
        database = sqlite3.connect(path)
        database.execute("PRAGMA busy_timeout=5000")
        return database
    uri = f"{path.resolve().as_uri()}?mode=ro"
    database = sqlite3.connect(uri, uri=True)
    database.execute("PRAGMA query_only=ON")
    return database


def prefill(
    dataset: Path,
    kaikki: Path,
    *,
    apply: bool = False,
    batch_size: int = 500,
) -> dict[str, Any]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if apply:
        raise ValueError(
            "standalone --apply is disabled; build and validate a compact "
            "delta, then use apply_kaikki_relation_delta.py on a backed-up "
            "collector shard"
        )
    if not dataset.is_file():
        raise FileNotFoundError(dataset)
    if not kaikki.is_file():
        raise FileNotFoundError(kaikki)

    database = open_database(dataset, apply=False)
    stats: dict[str, Any] = {
        "mode": "dry-run",
        "source": {
            "provider": SOURCE_PROVIDER,
            "url": SOURCE_URL,
            "file": kaikki.name,
            "dumpDate": SOURCE_DUMP_DATE,
            "extractedAt": SOURCE_EXTRACTED_AT,
            "license": SOURCE_LICENSE,
            "licenseUrl": SOURCE_LICENSE_URL,
        },
        "jsonLines": 0,
        "englishRecords": 0,
        "invalidJsonLines": 0,
        "relationTerms": 0,
        "matchedTerms": 0,
        "unmatchedTerms": 0,
        "changedTerms": 0,
        "unchangedTerms": 0,
        "relationValuesSeen": Counter(),
        "valuesAdded": Counter(),
        "columnsChanged": Counter(),
    }
    pending: list[tuple[str, list[dict[str, Any]]]] = []
    current_key: str | None = None
    current_patches: list[dict[str, Any]] = []

    try:
        validate_schema(database)

        def consume_batch() -> None:
            if not pending:
                return
            try:
                for key, patches in pending:
                    result = process_term(
                        database,
                        key,
                        patches,
                    )
                    if not result["matched"]:
                        stats["unmatchedTerms"] += 1
                        continue
                    stats["matchedTerms"] += 1
                    if result["changed"]:
                        stats["changedTerms"] += 1
                        for column, changed in result["columns"].items():
                            if changed:
                                stats["columnsChanged"][column] += 1
                        for name, count in result["additions"].items():
                            stats["valuesAdded"][name] += count
                    else:
                        stats["unchangedTerms"] += 1
            finally:
                pending.clear()

        def flush_current() -> None:
            nonlocal current_patches
            if not current_key or not current_patches:
                current_patches = []
                return
            stats["relationTerms"] += 1
            _, _, _, counts = aggregate_patches(current_patches)
            stats["relationValuesSeen"].update(counts)
            pending.append((current_key, current_patches))
            current_patches = []
            if len(pending) >= batch_size:
                consume_batch()

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
                if not key:
                    continue
                current_patches.extend(extract_relation_senses(data))
        flush_current()
        consume_batch()
    finally:
        database.close()

    for key in ("relationValuesSeen", "valuesAdded", "columnsChanged"):
        stats[key] = dict(sorted(stats[key].items()))
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or apply an idempotent Kaikki sense-relation prefill."
        )
    )
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument(
        "--kaikki",
        type=Path,
        default=SOURCE / "enwiktionary-wiktextract.jsonl.gz",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "disabled for safety; use the compact delta tools after a "
            "read-only dry-run"
        ),
    )
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    report = prefill(
        args.dataset,
        args.kaikki,
        apply=args.apply,
        batch_size=args.batch_size,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
