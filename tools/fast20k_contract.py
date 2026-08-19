"""Shared, dependency-free contract for fast-20k candidates and repair queues."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

SELECTION_VERSION = 3
POLICY_NAME = "bounded-phrase-v3-fixed-shards"
DEFAULT_REPAIR_SHARDS = 2

ENTRY_COLUMNS = (
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

JSON_COLUMNS = (
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

IMMUTABLE_IDENTITY_COLUMNS = (
    "word",
    "normalized_word",
    "frequency_rank",
    "source_json",
    "scope_json",
)


def _stable_digest(values: Sequence[Any]) -> str:
    encoded = json.dumps(
        list(values),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_content_digest(row: Mapping[str, Any]) -> str:
    return _stable_digest([row["id"], *(row[column] for column in ENTRY_COLUMNS)])


def canonical_identity_digest(row: Mapping[str, Any]) -> str:
    return _stable_digest(
        [row["id"], *(row[column] for column in IMMUTABLE_IDENTITY_COLUMNS)]
    )


def selection_row_digest(values: Sequence[Any]) -> str:
    """Digest one immutable selected-row contract without SQLite byte layout."""
    return _stable_digest(values)


def queue_row_digest(values: Sequence[Any]) -> str:
    """Digest one repair-queue row, including its permanently assigned shard."""
    return _stable_digest(values)


def candidate_contract_digest(
    selection_digest: str,
    baseline_content_digest: str,
    repair_queue_digest: str,
    *,
    shard_count: int,
) -> str:
    return _stable_digest(
        [
            SELECTION_VERSION,
            POLICY_NAME,
            shard_count,
            selection_digest,
            baseline_content_digest,
            repair_queue_digest,
        ]
    )
