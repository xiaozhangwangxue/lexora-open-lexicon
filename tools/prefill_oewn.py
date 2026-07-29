#!/usr/bin/env python3
"""Safely prefill Lexora entries from Open English WordNet 2025+.

The importer is deliberately conservative:

* dry-run is the default and opens the Lexora dataset read-only;
* the official OEWN release asset is pinned by SHA-256 and metadata;
* only existing ``normalized_word`` rows are considered;
* existing scalar values win, while lists are append-only and de-duplicated;
* ``enrichment_json``, row IDs, frequency data and Chinese text are untouched;
* ``--apply`` writes a new candidate database and never edits the input file.

WN-LMF is staged in a temporary SQLite database so the complete lexical graph
does not need to fit in memory.  The candidate is linked into place only after
SQLite integrity checks pass.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sqlite3
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Iterator

from build_dataset import clean_list, json_text, norm


EXPECTED_SHA256 = (
    "31f4af16c54b532fd5484d4cc33aee588a31bb5b70683ae8197842fde5b586bc"
)
EXPECTED_VERSION = "2025+"
EXPECTED_LEXICON_ID = "oewn"
EXPECTED_LABEL = "Open English Wordnet"
EXPECTED_LANGUAGE = "en"
EXPECTED_LICENSE = "https://creativecommons.org/licenses/by/4.0"
SOURCE_NAME = "oewn-2025+"
SOURCE_URL = (
    "https://github.com/globalwordnet/english-wordnet/releases/download/"
    "2025-edition/english-wordnet-2025-plus.xml.gz"
)
PRINCETON_LICENSE_URL = (
    "https://github.com/globalwordnet/english-wordnet/blob/"
    "2025-edition/WNDB_License.txt"
)
OEWN_LICENSE_URL = (
    "https://github.com/globalwordnet/english-wordnet/blob/"
    "2025-edition/LICENSE.md"
)

RICH_ENTRY_COLUMNS = {
    "phrase_entries_json",
    "related_entries_json",
}

REQUIRED_COLUMNS = {
    "id",
    "word",
    "normalized_word",
    "pos",
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
    "related_words_json",
    "senses_json",
    "source_json",
    "scope_json",
    "enrichment_json",
}

POS_MAP = {
    "n": "noun",
    "noun": "noun",
    "v": "verb",
    "verb": "verb",
    "a": "adj",
    "s": "adj",
    "adjective": "adj",
    "adjective_satellite": "adj",
    "r": "adv",
    "adverb": "adv",
}

# These direct lexical/semantic links are useful as dictionary "related"
# words.  Domain, usage and role relations are intentionally not flattened.
RELATED_RELATIONS = {
    "also",
    "attribute",
    "be_in_state",
    "causes",
    "derivation",
    "entails",
    "holo_location",
    "holo_member",
    "holo_part",
    "holo_portion",
    "holo_substance",
    "hypernym",
    "hyponym",
    "in_manner",
    "instance_hypernym",
    "instance_hyponym",
    "is_caused_by",
    "is_entailed_by",
    "is_subevent_of",
    "manner_of",
    "mero_location",
    "mero_member",
    "mero_part",
    "mero_portion",
    "mero_substance",
    "participle",
    "pertainym",
    "similar",
    "state_of",
    "subevent",
}
KEPT_RELATIONS = RELATED_RELATIONS | {"antonym"}

PROVENANCE_KEY = "openEnglishWordNet"
PROVENANCE = {
    "source": SOURCE_NAME,
    "edition": EXPECTED_VERSION,
    "releaseTag": "2025-edition",
    "url": SOURCE_URL,
    "sha256": EXPECTED_SHA256,
    "license": "CC BY 4.0",
    "licenseUrl": EXPECTED_LICENSE + "/",
    "upstreamLicense": OEWN_LICENSE_URL,
    "princetonWordNetLicense": PRINCETON_LICENSE_URL,
    "modified": True,
    "modifications": "Normalized, merged and reformatted for Lexora.",
}

STAGING_SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
PRAGMA temp_store=FILE;
CREATE TABLE lexical_entries (
  entry_id TEXT PRIMARY KEY,
  normalized TEXT NOT NULL,
  written TEXT NOT NULL,
  pos TEXT NOT NULL,
  us_phonetic TEXT NOT NULL DEFAULT '',
  uk_phonetic TEXT NOT NULL DEFAULT ''
);
CREATE TABLE senses (
  sense_id TEXT PRIMARY KEY,
  entry_id TEXT NOT NULL,
  synset_id TEXT NOT NULL,
  sense_order INTEGER NOT NULL,
  examples_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE sense_relations (
  source_sense TEXT NOT NULL,
  rel_type TEXT NOT NULL,
  target_sense TEXT NOT NULL
);
CREATE TABLE synsets (
  synset_id TEXT PRIMARY KEY,
  ili TEXT NOT NULL DEFAULT '',
  pos TEXT NOT NULL,
  definitions_json TEXT NOT NULL DEFAULT '[]',
  examples_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE synset_relations (
  source_synset TEXT NOT NULL,
  rel_type TEXT NOT NULL,
  target_synset TEXT NOT NULL
);
"""

TARGET_COLUMNS_PREFIX = """
id,pos,definition,definition_zh,us_phonetic,uk_phonetic,
synonyms_json,antonyms_json,examples_json,phrases_json,
"""

TARGET_COLUMNS_SUFFIX = """
,related_words_json,
senses_json,source_json,scope_json,enrichment_json
"""


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def element_text(element: ET.Element) -> str:
    return re.sub(r"\s+", " ", "".join(element.itertext()).strip())


def unique_text(values: Iterable[Any], limit: int = 40) -> list[str]:
    return clean_list((str(value) for value in values), limit)


def normalized_license(value: str) -> str:
    return value.strip().rstrip("/")


def mapped_pos(value: Any) -> str:
    return POS_MAP.get(str(value or "").strip().lower(), "")


def pronunciation_pair(lemma: ET.Element) -> tuple[str, str]:
    us = ""
    uk = ""
    for child in lemma:
        if local_name(child.tag) != "Pronunciation":
            continue
        value = element_text(child)
        variety = str(child.get("variety") or "").strip().lower()
        if not value or not variety or "fonxsamp" in variety:
            continue
        normalized = variety.replace("_", "-")
        tokens = {
            token
            for token in re.split(r"[\s,;/]+", normalized)
            if token
        }
        if not us and (
            tokens & {"us", "en-us", "en-us-fonipa", "general-american"}
            or normalized.startswith("en-us-")
        ):
            us = value
        if not uk and (
            tokens & {"gb", "uk", "en-gb", "en-gb-fonipa", "british"}
            or normalized.startswith("en-gb-")
        ):
            uk = value
    return us, uk


def sense_order(element: ET.Element, fallback: int) -> int:
    try:
        parsed = int(str(element.get("n") or ""))
    except ValueError:
        return fallback
    return parsed if parsed > 0 else fallback


def validate_release_metadata(metadata: dict[str, str]) -> None:
    expected = {
        "id": EXPECTED_LEXICON_ID,
        "label": EXPECTED_LABEL,
        "language": EXPECTED_LANGUAGE,
        "version": EXPECTED_VERSION,
    }
    for key, value in expected.items():
        if metadata.get(key) != value:
            raise ValueError(
                f"unexpected OEWN {key}: {metadata.get(key)!r}; "
                f"expected {value!r}"
            )
    if normalized_license(metadata.get("license", "")) != normalized_license(
        EXPECTED_LICENSE
    ):
        raise ValueError("unexpected OEWN license metadata")


def stage_oewn(
    source: Path,
    staging_path: Path,
    *,
    expected_sha256: str = EXPECTED_SHA256,
) -> tuple[sqlite3.Connection, dict[str, Any]]:
    if not source.is_file():
        raise FileNotFoundError(source)
    source_digest = sha256(source)
    if source_digest != expected_sha256:
        raise ValueError(
            "OEWN SHA-256 mismatch: "
            f"got {source_digest}, expected {expected_sha256}"
        )

    staging = sqlite3.connect(staging_path)
    staging.executescript(STAGING_SCHEMA)
    counters: Counter[str] = Counter()
    metadata: dict[str, str] | None = None
    entry_batch: list[tuple[Any, ...]] = []
    sense_batch: list[tuple[Any, ...]] = []
    sense_relation_batch: list[tuple[Any, ...]] = []
    synset_batch: list[tuple[Any, ...]] = []
    synset_relation_batch: list[tuple[Any, ...]] = []

    def flush() -> None:
        if entry_batch:
            staging.executemany(
                "INSERT INTO lexical_entries VALUES(?,?,?,?,?,?)",
                entry_batch,
            )
            entry_batch.clear()
        if sense_batch:
            staging.executemany(
                "INSERT INTO senses VALUES(?,?,?,?,?)",
                sense_batch,
            )
            sense_batch.clear()
        if sense_relation_batch:
            staging.executemany(
                "INSERT INTO sense_relations VALUES(?,?,?)",
                sense_relation_batch,
            )
            sense_relation_batch.clear()
        if synset_batch:
            staging.executemany(
                "INSERT INTO synsets VALUES(?,?,?,?,?)",
                synset_batch,
            )
            synset_batch.clear()
        if synset_relation_batch:
            staging.executemany(
                "INSERT INTO synset_relations VALUES(?,?,?)",
                synset_relation_batch,
            )
            synset_relation_batch.clear()
        staging.commit()

    try:
        with gzip.open(source, "rb") as stream:
            for event, element in ET.iterparse(
                stream,
                events=("start", "end"),
            ):
                tag = local_name(element.tag)
                if event == "start" and tag == "Lexicon":
                    if metadata is not None:
                        raise ValueError("multiple Lexicon elements are unsupported")
                    metadata = {
                        key: str(element.get(key) or "")
                        for key in (
                            "id",
                            "label",
                            "language",
                            "license",
                            "version",
                            "url",
                        )
                    }
                    validate_release_metadata(metadata)
                    continue
                if event != "end":
                    continue
                if tag == "LexicalEntry":
                    lemma = next(
                        (
                            child
                            for child in element
                            if local_name(child.tag) == "Lemma"
                        ),
                        None,
                    )
                    entry_id = str(element.get("id") or "")
                    if lemma is None or not entry_id:
                        raise ValueError("LexicalEntry is missing id or Lemma")
                    written = str(lemma.get("writtenForm") or "").strip()
                    index = str(element.get("index") or "").strip()
                    key = norm(index or written)
                    pos = mapped_pos(lemma.get("partOfSpeech"))
                    if not written or not key or not pos:
                        raise ValueError(
                            f"invalid LexicalEntry metadata for {entry_id!r}"
                        )
                    us, uk = pronunciation_pair(lemma)
                    entry_batch.append((entry_id, key, written, pos, us, uk))
                    counters["lexicalEntries"] += 1

                    ordinal = 0
                    for child in element:
                        if local_name(child.tag) != "Sense":
                            continue
                        ordinal += 1
                        sense_id = str(child.get("id") or "")
                        synset_id = str(child.get("synset") or "")
                        if not sense_id or not synset_id:
                            raise ValueError(
                                f"Sense in {entry_id!r} is missing id or synset"
                            )
                        examples = unique_text(
                            element_text(item)
                            for item in child
                            if local_name(item.tag) == "SenseExample"
                        )
                        sense_batch.append(
                            (
                                sense_id,
                                entry_id,
                                synset_id,
                                sense_order(child, ordinal),
                                json_text(examples),
                            )
                        )
                        counters["senses"] += 1
                        for relation in child:
                            if local_name(relation.tag) != "SenseRelation":
                                continue
                            rel_type = str(relation.get("relType") or "")
                            target = str(relation.get("target") or "")
                            if not rel_type or not target:
                                raise ValueError(
                                    f"invalid SenseRelation in {sense_id!r}"
                                )
                            sense_relation_batch.append(
                                (sense_id, rel_type, target)
                            )
                            counters["senseRelations"] += 1
                    element.clear()
                elif tag == "Synset":
                    synset_id = str(element.get("id") or "")
                    if not synset_id:
                        raise ValueError("Synset is missing id")
                    definitions: list[dict[str, str]] = []
                    examples: list[str] = []
                    for child in element:
                        child_tag = local_name(child.tag)
                        if child_tag == "Definition":
                            value = element_text(child)
                            if value:
                                definition = {"text": value}
                                source_sense = str(
                                    child.get("sourceSense") or ""
                                )
                                if source_sense:
                                    definition["sourceSense"] = source_sense
                                definitions.append(definition)
                        elif child_tag == "Example":
                            value = element_text(child)
                            if value:
                                examples.append(value)
                        elif child_tag == "SynsetRelation":
                            rel_type = str(child.get("relType") or "")
                            target = str(child.get("target") or "")
                            if not rel_type or not target:
                                raise ValueError(
                                    f"invalid SynsetRelation in {synset_id!r}"
                                )
                            synset_relation_batch.append(
                                (synset_id, rel_type, target)
                            )
                            counters["synsetRelations"] += 1
                    synset_batch.append(
                        (
                            synset_id,
                            str(element.get("ili") or ""),
                            mapped_pos(element.get("partOfSpeech")),
                            json_text(definitions),
                            json_text(unique_text(examples)),
                        )
                    )
                    counters["synsets"] += 1
                    element.clear()

                if (
                    len(entry_batch)
                    + len(sense_batch)
                    + len(synset_batch)
                    >= 5000
                ):
                    flush()
        flush()
        if metadata is None:
            raise ValueError("OEWN XML has no Lexicon metadata")
        if not counters["lexicalEntries"] or not counters["synsets"]:
            raise ValueError("OEWN XML contains no lexical data")
        staging.executescript(
            """
            CREATE INDEX idx_oewn_entries_normalized
              ON lexical_entries(normalized,entry_id);
            CREATE INDEX idx_oewn_senses_entry
              ON senses(entry_id,sense_order,synset_id);
            CREATE INDEX idx_oewn_senses_synset
              ON senses(synset_id,entry_id);
            CREATE INDEX idx_oewn_sense_rel_source
              ON sense_relations(source_sense,rel_type);
            CREATE INDEX idx_oewn_sense_rel_target
              ON sense_relations(target_sense);
            CREATE INDEX idx_oewn_synset_rel_source
              ON synset_relations(source_synset,rel_type);
            CREATE INDEX idx_oewn_synset_rel_target
              ON synset_relations(target_synset);
            """
        )
        staging.commit()
    except Exception:
        staging.close()
        raise

    return staging, {
        "sha256": source_digest,
        "metadata": metadata,
        **dict(sorted(counters.items())),
    }


def readonly_database(path: Path) -> sqlite3.Connection:
    uri = f"{path.resolve().as_uri()}?mode=ro"
    database = sqlite3.connect(uri, uri=True)
    database.execute("PRAGMA query_only=ON")
    return database


def target_select_columns(columns: set[str]) -> str:
    phrase_entries = (
        "phrase_entries_json"
        if "phrase_entries_json" in columns
        else "'[]' AS phrase_entries_json"
    )
    related_entries = (
        "related_entries_json"
        if "related_entries_json" in columns
        else "'[]' AS related_entries_json"
    )
    return (
        TARGET_COLUMNS_PREFIX
        + phrase_entries
        + TARGET_COLUMNS_SUFFIX.replace(
            ",related_words_json,",
            f",related_words_json,{related_entries},",
        )
    )


def ensure_rich_entry_columns(database: sqlite3.Connection) -> None:
    columns = {
        str(row[1])
        for row in database.execute("PRAGMA table_info(entries)")
    }
    for name in sorted(RICH_ENTRY_COLUMNS - columns):
        database.execute(
            f"ALTER TABLE entries ADD COLUMN {name} "
            "TEXT NOT NULL DEFAULT '[]'"
        )
    database.commit()


def validate_target_schema(database: sqlite3.Connection) -> set[str]:
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
    if (
        database.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='entries_fts'"
        ).fetchone()
        is None
    ):
        raise ValueError("dataset is missing entries_fts")
    return columns


def json_list(raw: Any, column: str, entry_id: int) -> list[Any]:
    try:
        value = json.loads(raw or "[]")
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"entry {entry_id} has invalid JSON in {column}"
        ) from error
    if not isinstance(value, list):
        raise ValueError(
            f"entry {entry_id} has non-list JSON in {column}"
        )
    return value


def json_object(raw: Any, column: str, entry_id: int) -> dict[str, Any]:
    try:
        value = json.loads(raw or "{}")
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"entry {entry_id} has invalid JSON in {column}"
        ) from error
    if not isinstance(value, dict):
        raise ValueError(
            f"entry {entry_id} has non-object JSON in {column}"
        )
    return value


def merge_words(
    existing: Iterable[Any],
    additions: Iterable[Any],
    *,
    forbidden: set[str] | None = None,
    limit: int = 40,
) -> list[str]:
    output = [str(value) for value in existing]
    seen = {norm(value) for value in output if norm(value)}
    blocked = forbidden or set()
    for raw in additions:
        value = re.sub(r"\s+", " ", str(raw).strip())
        key = norm(value)
        if not value or not key or key in seen or key in blocked:
            continue
        if len(output) >= limit:
            break
        seen.add(key)
        output.append(value)
    return output


def merge_text(
    existing: Iterable[Any],
    additions: Iterable[Any],
    *,
    limit: int = 40,
) -> list[str]:
    output = [str(value) for value in existing]
    seen = {
        re.sub(r"\s+", " ", value.strip())
        for value in output
        if value.strip()
    }
    for raw in additions:
        value = re.sub(r"\s+", " ", str(raw).strip())
        if not value or value in seen:
            continue
        if len(output) >= limit:
            break
        seen.add(value)
        output.append(value)
    return output


def stage_terms(
    staging: sqlite3.Connection,
    batch_size: int,
) -> Iterator[list[str]]:
    cursor = staging.execute(
        "SELECT normalized FROM lexical_entries "
        "GROUP BY normalized ORDER BY normalized"
    )
    try:
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                return
            yield [str(row[0]) for row in rows]
    finally:
        cursor.close()


def first_definition(raw: Any) -> str:
    """Return the first displayable OEWN synset definition."""
    try:
        values = json.loads(raw or "[]")
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("staging synset has invalid definitions JSON") from error
    if not isinstance(values, list):
        raise ValueError("staging synset definitions are not a list")
    for value in values:
        if not isinstance(value, dict):
            continue
        text = re.sub(
            r"\s+",
            " ",
            str(value.get("text") or "").strip(),
        )
        if text:
            return text
    return ""


def relation_rows(
    staging: sqlite3.Connection,
    key: str,
) -> tuple[
    dict[str, dict[str, list[dict[str, str]]]],
    dict[str, dict[str, list[dict[str, str]]]],
]:
    sense_map: dict[
        str,
        dict[str, list[dict[str, str]]],
    ] = defaultdict(
        lambda: defaultdict(list)
    )
    synset_map: dict[
        str,
        dict[str, list[dict[str, str]]],
    ] = defaultdict(
        lambda: defaultdict(list)
    )
    for source_id, rel_type, written, definitions_json in staging.execute(
        """
        SELECT relation.source_sense,relation.rel_type,target.written,
               target_synset.definitions_json
        FROM lexical_entries AS source_entry
        JOIN senses AS source
          ON source.entry_id=source_entry.entry_id
        JOIN sense_relations AS relation
          ON relation.source_sense=source.sense_id
        JOIN senses AS target_sense
          ON target_sense.sense_id=relation.target_sense
        JOIN lexical_entries AS target
          ON target.entry_id=target_sense.entry_id
        JOIN synsets AS target_synset
          ON target_synset.synset_id=target_sense.synset_id
        WHERE source_entry.normalized=?
        ORDER BY relation.rowid,target.entry_id
        """,
        (key,),
    ):
        if rel_type in KEPT_RELATIONS:
            sense_map[str(source_id)][str(rel_type)].append(
                {
                    "word": str(written),
                    "definition": first_definition(definitions_json),
                }
            )
    for source_id, rel_type, written, definitions_json in staging.execute(
        """
        SELECT relation.source_synset,relation.rel_type,target.written,
               target_synset.definitions_json
        FROM lexical_entries AS source_entry
        JOIN senses AS source
          ON source.entry_id=source_entry.entry_id
        JOIN synset_relations AS relation
          ON relation.source_synset=source.synset_id
        JOIN senses AS target_sense
          ON target_sense.synset_id=relation.target_synset
        JOIN lexical_entries AS target
          ON target.entry_id=target_sense.entry_id
        JOIN synsets AS target_synset
          ON target_synset.synset_id=target_sense.synset_id
        WHERE source_entry.normalized=?
        ORDER BY relation.rowid,target.entry_id
        """,
        (key,),
    ):
        if rel_type in KEPT_RELATIONS:
            synset_map[str(source_id)][str(rel_type)].append(
                {
                    "word": str(written),
                    "definition": first_definition(definitions_json),
                }
            )
    return sense_map, synset_map


def build_patch(
    staging: sqlite3.Connection,
    key: str,
) -> dict[str, Any]:
    entries = staging.execute(
        """
        SELECT entry_id,written,pos,us_phonetic,uk_phonetic
        FROM lexical_entries
        WHERE normalized=?
        ORDER BY entry_id
        """,
        (key,),
    ).fetchall()
    senses = staging.execute(
        """
        SELECT sense.sense_id,sense.entry_id,sense.synset_id,
               sense.sense_order,sense.examples_json,
               synset.ili,synset.pos,synset.definitions_json,
               synset.examples_json
        FROM lexical_entries AS entry
        JOIN senses AS sense ON sense.entry_id=entry.entry_id
        JOIN synsets AS synset ON synset.synset_id=sense.synset_id
        WHERE entry.normalized=?
        ORDER BY sense.sense_order,entry.entry_id,sense.sense_id
        """,
        (key,),
    ).fetchall()
    synonym_rows = staging.execute(
        """
        SELECT source.synset_id,target.written,target.normalized,
               synset.definitions_json
        FROM lexical_entries AS source_entry
        JOIN senses AS source ON source.entry_id=source_entry.entry_id
        JOIN synsets AS synset ON synset.synset_id=source.synset_id
        JOIN senses AS target_sense
          ON target_sense.synset_id=source.synset_id
        JOIN lexical_entries AS target
          ON target.entry_id=target_sense.entry_id
        WHERE source_entry.normalized=?
        ORDER BY source.sense_order,target_sense.sense_order,target.entry_id
        """,
        (key,),
    ).fetchall()
    synonyms_by_synset: dict[str, list[str]] = defaultdict(list)
    synonyms: list[str] = []
    synonym_candidates: list[dict[str, str]] = []
    for synset_id, written, normalized, definitions_json in synonym_rows:
        if str(normalized) == key:
            continue
        synonyms_by_synset[str(synset_id)].append(str(written))
        synonyms.append(str(written))
        synonym_candidates.append(
            {
                "word": str(written),
                "definition": first_definition(definitions_json),
            }
        )

    sense_relations, synset_relations = relation_rows(staging, key)
    definitions: list[str] = []
    examples: list[str] = []
    antonyms: list[str] = []
    antonym_candidates: list[dict[str, str]] = []
    related_candidates: list[dict[str, str]] = []
    structured_senses: list[dict[str, Any]] = []
    written_by_entry = {str(row[0]): str(row[1]) for row in entries}
    pos_by_entry = {str(row[0]): str(row[2]) for row in entries}

    for (
        sense_id,
        entry_id,
        synset_id,
        order,
        sense_examples_json,
        ili,
        synset_pos,
        definitions_json,
        synset_examples_json,
    ) in senses:
        sense_id = str(sense_id)
        synset_id = str(synset_id)
        raw_definitions = json.loads(definitions_json or "[]")
        sense_definitions = [
            str(item.get("text") or "")
            for item in raw_definitions
            if isinstance(item, dict)
            and (
                not item.get("sourceSense")
                or str(item.get("sourceSense")) == sense_id
            )
            and str(item.get("text") or "").strip()
        ]
        sense_examples = [
            *json.loads(sense_examples_json or "[]"),
            *json.loads(synset_examples_json or "[]"),
        ]
        definitions.extend(sense_definitions)
        examples.extend(sense_examples)

        relation_candidates: dict[str, list[dict[str, str]]] = {}
        for source in (
            sense_relations.get(sense_id, {}),
            synset_relations.get(synset_id, {}),
        ):
            for rel_type, values in source.items():
                relation_candidates.setdefault(rel_type, []).extend(values)
        relation_values = {
            rel_type: merge_words(
                [],
                (value["word"] for value in values),
                forbidden={key},
            )
            for rel_type, values in relation_candidates.items()
            if values
        }
        relation_candidates = {
            rel_type: [
                value
                for value in values
                if norm(value["word"]) != key
            ]
            for rel_type, values in relation_candidates.items()
            if values
        }
        antonyms.extend(relation_values.get("antonym", []))
        antonym_candidates.extend(
            relation_candidates.get("antonym", [])
        )
        for rel_type, values in relation_candidates.items():
            if rel_type in RELATED_RELATIONS:
                related_candidates.extend(values)

        structured: dict[str, Any] = {
            "source": SOURCE_NAME,
            "synset": synset_id,
            "writtenForm": written_by_entry.get(str(entry_id), key),
            "pos": pos_by_entry.get(str(entry_id))
            or str(synset_pos or ""),
            "order": int(order),
            "definitions": unique_text(sense_definitions),
        }
        if ili:
            structured["ili"] = str(ili)
        normalized_examples = unique_text(sense_examples)
        if normalized_examples:
            structured["examples"] = normalized_examples
        normalized_synonyms = merge_words(
            [],
            synonyms_by_synset.get(synset_id, []),
            forbidden={key},
        )
        if normalized_synonyms:
            structured["synonyms"] = normalized_synonyms
        if relation_values:
            structured["relations"] = relation_values
        structured_senses.append(structured)

    positions = unique_text(row[2] for row in entries if row[2])
    us = next((str(row[3]) for row in entries if row[3]), "")
    uk = next((str(row[4]) for row in entries if row[4]), "")
    synonyms = merge_words([], synonyms, forbidden={key})
    antonyms = merge_words([], antonyms, forbidden={key})
    phrase_candidates = [
        value
        for value in [
            *synonym_candidates,
            *antonym_candidates,
            *related_candidates,
        ]
        if " " in value["word"] or "-" in value["word"]
    ]
    return {
        "pos": ", ".join(positions),
        "definition": "\n".join(unique_text(definitions)),
        "us_phonetic": us,
        "uk_phonetic": uk,
        "synonyms": synonyms,
        "antonyms": antonyms,
        "examples": unique_text(examples),
        "phraseCandidates": phrase_candidates,
        "relatedCandidates": related_candidates,
        "senses": structured_senses,
        "writtenForms": unique_text(
            (row[1] for row in entries),
            1000,
        ),
    }


def merge_senses(
    existing: list[Any],
    additions: list[dict[str, Any]],
    *,
    per_source_limit: int = 40,
) -> list[Any]:
    output = [
        dict(value) if isinstance(value, dict) else value
        for value in existing
    ]
    existing_keys = {
        (
            str(value.get("source") or ""),
            str(value.get("synset") or ""),
            str(value.get("writtenForm") or ""),
            str(value.get("pos") or ""),
        )
        for value in output
        if isinstance(value, dict)
    }
    added = 0
    for value in additions:
        key = (
            str(value.get("source") or ""),
            str(value.get("synset") or ""),
            str(value.get("writtenForm") or ""),
            str(value.get("pos") or ""),
        )
        if key in existing_keys:
            continue
        if added >= per_source_limit:
            break
        output.append(dict(value))
        existing_keys.add(key)
        added += 1
    return output


def clean_display_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def resolve_rich_candidates(
    target: sqlite3.Connection,
    candidates: Iterable[dict[str, Any]],
    *,
    forbidden: set[str],
    limit: int = 40,
) -> list[dict[str, str]]:
    """Resolve a rich OEWN link, falling back to a canonical definition.

    Word-only links are intentionally dropped.  A non-empty flat field makes
    the collector skip Datamuse, so every newly flattened link must have the
    matching ``{word, definition}`` object available at the same time.
    """
    ordered: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in candidates:
        word = clean_display_text(raw.get("word"))
        key = norm(word)
        if not word or not key or key in forbidden or key in seen:
            continue
        seen.add(key)
        ordered.append(
            {
                "key": key,
                "word": word,
                "definition": clean_display_text(raw.get("definition")),
            }
        )

    missing = [
        value["key"]
        for value in ordered
        if not value["definition"]
    ]
    fallbacks: dict[str, tuple[str, str]] = {}
    for offset in range(0, len(missing), 500):
        batch = missing[offset:offset + 500]
        if not batch:
            continue
        placeholders = ",".join("?" for _ in batch)
        for normalized_word, word, definition in target.execute(
            """
            SELECT normalized_word,word,definition
            FROM entries
            WHERE normalized_word IN (
            """
            + placeholders
            + ")",
            batch,
        ):
            display_word = clean_display_text(word)
            display_definition = clean_display_text(definition)
            if display_word and display_definition:
                fallbacks[str(normalized_word)] = (
                    display_word,
                    display_definition,
                )

    result: list[dict[str, str]] = []
    for value in ordered:
        word = value["word"]
        definition = value["definition"]
        if not definition and value["key"] in fallbacks:
            word, definition = fallbacks[value["key"]]
        if not word or not definition:
            continue
        result.append({"word": word, "definition": definition})
        if len(result) >= limit:
            break
    return result


def merge_named_entries(
    existing: list[Any],
    additions: Iterable[dict[str, Any]],
    *,
    limit: int = 40,
) -> list[Any]:
    """Append valid rich entries without rewriting existing objects."""
    output = list(existing)
    seen = {
        norm(clean_display_text(value.get("word")))
        for value in existing
        if isinstance(value, dict)
        and norm(clean_display_text(value.get("word")))
    }
    maximum = max(limit, len(output))
    for raw in additions:
        word = clean_display_text(raw.get("word"))
        definition = clean_display_text(raw.get("definition"))
        key = norm(word)
        if not word or not definition or not key or key in seen:
            continue
        if len(output) >= maximum:
            break
        output.append({"word": word, "definition": definition})
        seen.add(key)
    return output


def valid_named_words(entries: Iterable[Any]) -> set[str]:
    return {
        norm(clean_display_text(value.get("word")))
        for value in entries
        if isinstance(value, dict)
        and clean_display_text(value.get("definition"))
        and norm(clean_display_text(value.get("word")))
    }


def merge_source(existing: list[Any]) -> list[Any]:
    if SOURCE_NAME in existing:
        return list(existing)
    return [*existing, SOURCE_NAME]


def merge_scope(existing: dict[str, Any]) -> dict[str, Any]:
    output = dict(existing)
    output[PROVENANCE_KEY] = dict(PROVENANCE)
    return output


def process_term(
    target: sqlite3.Connection,
    staging: sqlite3.Connection,
    key: str,
    *,
    apply: bool,
    select_columns: str,
) -> dict[str, Any]:
    row = target.execute(
        f"SELECT {select_columns} FROM entries "
        "WHERE normalized_word=?",
        (key,),
    ).fetchone()
    if row is None:
        return {"matched": False, "changed": False}
    (
        entry_id,
        old_pos,
        old_definition,
        _old_definition_zh,
        old_us,
        old_uk,
        synonyms_json,
        antonyms_json,
        examples_json,
        phrases_json,
        phrase_entries_json,
        related_json,
        related_entries_json,
        senses_json,
        source_json,
        scope_json,
        _enrichment_json,
    ) = row
    entry_id = int(entry_id)
    old_synonyms = json_list(synonyms_json, "synonyms_json", entry_id)
    old_antonyms = json_list(antonyms_json, "antonyms_json", entry_id)
    old_examples = json_list(examples_json, "examples_json", entry_id)
    old_phrases = json_list(phrases_json, "phrases_json", entry_id)
    old_phrase_entries = json_list(
        phrase_entries_json,
        "phrase_entries_json",
        entry_id,
    )
    old_related = json_list(related_json, "related_words_json", entry_id)
    old_related_entries = json_list(
        related_entries_json,
        "related_entries_json",
        entry_id,
    )
    old_senses = json_list(senses_json, "senses_json", entry_id)
    old_sources = json_list(source_json, "source_json", entry_id)
    old_scope = json_object(scope_json, "scope_json", entry_id)
    patch = build_patch(staging, key)

    scalar_conflicts: dict[str, bool] = {}

    def fill_scalar(old: Any, new: str, name: str) -> str:
        old_text = str(old or "")
        if old_text.strip():
            scalar_conflicts[name] = bool(
                new.strip() and old_text.strip() != new.strip()
            )
            return old_text
        scalar_conflicts[name] = False
        return new

    new_pos = fill_scalar(old_pos, patch["pos"], "pos")
    new_definition = fill_scalar(
        old_definition,
        patch["definition"],
        "definition",
    )
    new_us = fill_scalar(old_us, patch["us_phonetic"], "us_phonetic")
    new_uk = fill_scalar(old_uk, patch["uk_phonetic"], "uk_phonetic")
    new_synonyms = merge_words(
        old_synonyms,
        patch["synonyms"],
        forbidden={key},
    )
    new_antonyms = merge_words(
        old_antonyms,
        patch["antonyms"],
        forbidden={key},
    )
    new_examples = merge_text(old_examples, patch["examples"])
    resolved_phrase_entries = resolve_rich_candidates(
        target,
        patch["phraseCandidates"],
        forbidden={key},
    )
    new_phrase_entries = merge_named_entries(
        old_phrase_entries,
        resolved_phrase_entries,
    )
    usable_phrase_words = valid_named_words(new_phrase_entries)
    new_phrases = merge_words(
        old_phrases,
        (
            value["word"]
            for value in resolved_phrase_entries
            if norm(value["word"]) in usable_phrase_words
        ),
        forbidden={key},
    )
    resolved_related_entries = resolve_rich_candidates(
        target,
        patch["relatedCandidates"],
        forbidden={key},
    )
    new_related_entries = merge_named_entries(
        old_related_entries,
        resolved_related_entries,
    )
    usable_related_words = valid_named_words(new_related_entries)
    new_related = merge_words(
        old_related,
        (
            value["word"]
            for value in resolved_related_entries
            if norm(value["word"]) in usable_related_words
        ),
        forbidden={key},
    )
    new_senses = merge_senses(old_senses, patch["senses"])
    new_sources = merge_source(old_sources)
    new_scope = merge_scope(old_scope)

    values = {
        "pos": new_pos,
        "definition": new_definition,
        "us_phonetic": new_us,
        "uk_phonetic": new_uk,
        "synonyms_json": json_text(new_synonyms),
        "antonyms_json": json_text(new_antonyms),
        "examples_json": json_text(new_examples),
        "phrases_json": json_text(new_phrases),
        "phrase_entries_json": json_text(new_phrase_entries),
        "related_words_json": json_text(new_related),
        "related_entries_json": json_text(new_related_entries),
        "senses_json": json_text(new_senses),
        "source_json": json_text(new_sources),
        "scope_json": json_text(new_scope),
    }
    old_values = {
        "pos": str(old_pos or ""),
        "definition": str(old_definition or ""),
        "us_phonetic": str(old_us or ""),
        "uk_phonetic": str(old_uk or ""),
        "synonyms_json": str(synonyms_json or "[]"),
        "antonyms_json": str(antonyms_json or "[]"),
        "examples_json": str(examples_json or "[]"),
        "phrases_json": str(phrases_json or "[]"),
        "phrase_entries_json": str(phrase_entries_json or "[]"),
        "related_words_json": str(related_json or "[]"),
        "related_entries_json": str(related_entries_json or "[]"),
        "senses_json": str(senses_json or "[]"),
        "source_json": str(source_json or "[]"),
        "scope_json": str(scope_json or "{}"),
    }
    changed_columns = {
        name: value != old_values[name]
        for name, value in values.items()
    }
    changed = any(changed_columns.values())
    if apply and changed:
        target.execute(
            """
            UPDATE entries SET
              pos=?,definition=?,us_phonetic=?,uk_phonetic=?,
              synonyms_json=?,antonyms_json=?,examples_json=?,
              phrases_json=?,phrase_entries_json=?,
              related_words_json=?,related_entries_json=?,
              senses_json=?,source_json=?,scope_json=?
            WHERE id=?
            """,
            (
                values["pos"],
                values["definition"],
                values["us_phonetic"],
                values["uk_phonetic"],
                values["synonyms_json"],
                values["antonyms_json"],
                values["examples_json"],
                values["phrases_json"],
                values["phrase_entries_json"],
                values["related_words_json"],
                values["related_entries_json"],
                values["senses_json"],
                values["source_json"],
                values["scope_json"],
                entry_id,
            ),
        )
        if any(
            changed_columns[name]
            for name in ("definition", "examples_json", "phrases_json")
        ):
            target.execute(
                """
                INSERT OR REPLACE INTO entries_fts(
                  rowid,word,definition,definition_zh,examples,phrases
                )
                SELECT id,word,definition,definition_zh,
                       examples_json,phrases_json
                FROM entries WHERE id=?
                """,
                (entry_id,),
            )
    return {
        "matched": True,
        "changed": changed,
        "columns": changed_columns,
        "additions": {
            "synonyms": len(new_synonyms) - len(old_synonyms),
            "antonyms": len(new_antonyms) - len(old_antonyms),
            "examples": len(new_examples) - len(old_examples),
            "phrases": len(new_phrases) - len(old_phrases),
            "phraseEntries": (
                len(new_phrase_entries) - len(old_phrase_entries)
            ),
            "related": len(new_related) - len(old_related),
            "relatedEntries": (
                len(new_related_entries) - len(old_related_entries)
            ),
            "senses": len(new_senses) - len(old_senses),
        },
        "scalarConflictsPreserved": {
            name: conflict
            for name, conflict in scalar_conflicts.items()
            if conflict
        },
        "caseCollision": len(
            {
                value.casefold()
                for value in patch["writtenForms"]
            }
        )
        < len(set(patch["writtenForms"])),
    }


def install_metadata(database: sqlite3.Connection) -> bool:
    value = json_text(PROVENANCE)
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
        """
    )
    row = database.execute(
        "SELECT value FROM dataset_metadata WHERE key=?",
        (PROVENANCE_KEY,),
    ).fetchone()
    if row and str(row[0]) == value:
        return False
    database.execute(
        """
        INSERT INTO dataset_metadata(key,value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (PROVENANCE_KEY, value),
    )
    return True


def prefill_database(
    target: sqlite3.Connection,
    staging: sqlite3.Connection,
    *,
    apply: bool,
    batch_size: int,
) -> dict[str, Any]:
    if batch_size <= 0 or batch_size > 900:
        raise ValueError("batch_size must be within 1..900")
    if apply:
        # The deployed canonical predates rich relation columns.  Only the
        # newly-created candidate may be migrated; a dry-run must keep the
        # source database strictly read-only.
        ensure_rich_entry_columns(target)
    columns = validate_target_schema(target)
    select_columns = target_select_columns(columns)
    row_count = int(
        target.execute("SELECT count(*) FROM entries").fetchone()[0]
    )
    stats: dict[str, Any] = {
        "rowCountBefore": row_count,
        "matchedTerms": 0,
        "unmatchedTerms": 0,
        "changedTerms": 0,
        "unchangedTerms": 0,
        "caseCollisions": 0,
        "columnsChanged": Counter(),
        "valuesAdded": Counter(),
        "scalarConflictsPreserved": Counter(),
    }

    for terms in stage_terms(staging, batch_size):
        placeholders = ",".join("?" for _ in terms)
        matched = {
            str(row[0])
            for row in target.execute(
                "SELECT normalized_word FROM entries "
                f"WHERE normalized_word IN ({placeholders})",
                terms,
            )
        }
        stats["unmatchedTerms"] += len(terms) - len(matched)
        if apply and matched:
            target.execute("BEGIN IMMEDIATE")
        try:
            for key in terms:
                if key not in matched:
                    continue
                result = process_term(
                    target,
                    staging,
                    key,
                    apply=apply,
                    select_columns=select_columns,
                )
                stats["matchedTerms"] += 1
                if result["caseCollision"]:
                    stats["caseCollisions"] += 1
                if result["changed"]:
                    stats["changedTerms"] += 1
                    for column, changed in result["columns"].items():
                        if changed:
                            stats["columnsChanged"][column] += 1
                    for name, count in result["additions"].items():
                        stats["valuesAdded"][name] += count
                else:
                    stats["unchangedTerms"] += 1
                for name in result["scalarConflictsPreserved"]:
                    stats["scalarConflictsPreserved"][name] += 1
            if apply:
                target.commit()
        except Exception:
            if apply:
                target.rollback()
            raise

    metadata_changed = False
    if apply:
        target.execute("BEGIN IMMEDIATE")
        try:
            metadata_changed = install_metadata(target)
            target.commit()
        except Exception:
            target.rollback()
            raise
    stats["metadataChanged"] = metadata_changed
    stats["rowCountAfter"] = int(
        target.execute("SELECT count(*) FROM entries").fetchone()[0]
    )
    if stats["rowCountAfter"] != row_count:
        raise ValueError("existing-only invariant failed: row count changed")
    for key in (
        "columnsChanged",
        "valuesAdded",
        "scalarConflictsPreserved",
    ):
        stats[key] = dict(sorted(stats[key].items()))
    return stats


def validate_candidate(
    database: sqlite3.Connection,
    expected_rows: int,
) -> None:
    quick_check = database.execute("PRAGMA quick_check").fetchone()
    if not quick_check or quick_check[0] != "ok":
        raise ValueError(f"candidate quick_check failed: {quick_check!r}")
    foreign_keys = database.execute("PRAGMA foreign_key_check").fetchone()
    if foreign_keys is not None:
        raise ValueError(
            f"candidate foreign_key_check failed: {foreign_keys!r}"
        )
    row_count, distinct_ids, distinct_words = database.execute(
        """
        SELECT count(*),count(DISTINCT id),count(DISTINCT normalized_word)
        FROM entries
        """
    ).fetchone()
    if (row_count, distinct_ids, distinct_words) != (
        expected_rows,
        expected_rows,
        expected_rows,
    ):
        raise ValueError("candidate existing-only identity check failed")
    if (
        database.execute(
            "SELECT count(*) FROM entries_fts"
        ).fetchone()[0]
        != expected_rows
    ):
        raise ValueError("candidate FTS row count does not match entries")


def copy_database(source: Path, destination: Path) -> None:
    source_db = readonly_database(source)
    destination_db = sqlite3.connect(destination)
    try:
        source_db.backup(destination_db)
        destination_db.commit()
    finally:
        destination_db.close()
        source_db.close()


def prefill(
    dataset: Path,
    oewn: Path,
    *,
    apply: bool = False,
    output: Path | None = None,
    batch_size: int = 500,
    expected_sha256: str = EXPECTED_SHA256,
) -> dict[str, Any]:
    if not dataset.is_file():
        raise FileNotFoundError(dataset)
    if apply:
        if output is None:
            raise ValueError("--apply requires --output")
        if output.resolve() == dataset.resolve():
            raise ValueError("output must not be the input dataset")
        if output.exists():
            raise FileExistsError(f"refusing to overwrite {output}")
        if not output.parent.is_dir():
            raise FileNotFoundError(output.parent)
    elif output is not None:
        raise ValueError("--output is only valid with --apply")

    with tempfile.TemporaryDirectory(prefix="lexora-oewn-") as temp_dir:
        staging_path = Path(temp_dir) / "oewn-staging.sqlite"
        staging, source_report = stage_oewn(
            oewn,
            staging_path,
            expected_sha256=expected_sha256,
        )
        try:
            if not apply:
                target = readonly_database(dataset)
                try:
                    stats = prefill_database(
                        target,
                        staging,
                        apply=False,
                        batch_size=batch_size,
                    )
                finally:
                    target.close()
                return {
                    "mode": "dry-run",
                    "source": source_report,
                    **stats,
                }

            assert output is not None
            with tempfile.TemporaryDirectory(
                prefix=f".{output.name}.",
                dir=output.parent,
            ) as candidate_dir:
                candidate = Path(candidate_dir) / "candidate.sqlite"
                copy_database(dataset, candidate)
                target = sqlite3.connect(candidate)
                try:
                    target.execute("PRAGMA busy_timeout=5000")
                    stats = prefill_database(
                        target,
                        staging,
                        apply=True,
                        batch_size=batch_size,
                    )
                    target.execute("PRAGMA optimize")
                    target.commit()
                    target.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    target.execute("PRAGMA journal_mode=DELETE")
                    target.commit()
                    validate_candidate(target, stats["rowCountBefore"])
                finally:
                    target.close()
                # ``link`` is atomic and refuses to replace a path created
                # concurrently after the initial exists() check.
                os.link(candidate, output)
            return {
                "mode": "apply",
                "source": source_report,
                "output": str(output),
                **stats,
            }
        finally:
            staging.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or create a new OEWN-prefilled Lexora SQLite candidate."
        )
    )
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--oewn", type=Path, required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write a new candidate; default is read-only dry-run",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="new candidate path; required with --apply and must not exist",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    report = prefill(
        args.dataset,
        args.oewn,
        apply=args.apply,
        output=args.output,
        batch_size=args.batch_size,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
