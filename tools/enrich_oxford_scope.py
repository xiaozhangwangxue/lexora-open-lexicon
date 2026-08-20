#!/usr/bin/env python3
"""Rate-limited, resumable enrichment using Lexora's public providers.

The worker only fills empty fields.  It keeps provider state in a separate
SQLite file so a stopped run can resume without touching completed terms.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import html
import json
import math
import os
import random
import re
import sqlite3
import tempfile
import time
import unicodedata
from html.parser import HTMLParser
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from fast20k_contract import (
    ENTRY_COLUMNS,
    POLICY_NAME as FAST20K_POLICY_NAME,
    SELECTION_VERSION as FAST20K_SELECTION_VERSION,
    candidate_contract_digest,
    canonical_content_digest,
    canonical_identity_digest,
    queue_row_digest,
    selection_row_digest,
)

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build"
STATE = BUILD / "oxford-enrichment-state.sqlite"
EDGE_BASE = os.environ.get("LEXORA_EDGE_URL", "").rstrip("/")
EDGE_TOKEN = os.environ.get("LEXORA_ORIGIN_TOKEN", "")
ENTRY_SELECT_COLUMNS = (
    "id,word,normalized_word,definition,definition_zh,"
    "us_phonetic,uk_phonetic,synonyms_json,antonyms_json,"
    "examples_json,phrases_json,phrase_entries_json,"
    "related_words_json,related_entries_json,frequency,difficulty,"
    "enrichment_json,pos"
)
REPAIR_PAYLOAD_COLUMNS = (
    "pos",
    "definition",
    "definition_zh",
    "us_phonetic",
    "uk_phonetic",
    "synonyms_json",
    "antonyms_json",
    "examples_json",
    "phrases_json",
    "phrase_entries_json",
    "related_words_json",
    "related_entries_json",
    "frequency",
    "difficulty",
)


def repair_payload_digest(row: dict[str, Any]) -> str:
    encoded = json.dumps(
        [row.get(column) for column in REPAIR_PAYLOAD_COLUMNS],
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def write_runtime_ready_marker(path: Path, value: dict[str, Any]) -> None:
    """Atomically prove repair initialization finished before any HTTP work."""
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
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)

def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

def j(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

def unique(values: list[str], limit: int = 40) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        value = " ".join(str(value).split()).strip()
        if value and value not in seen:
            seen.add(value); result.append(value)
        if len(result) >= limit:
            break
    return result


def normalized_term(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("’", "'").split())


def candidate_term_key(value: Any) -> str:
    return normalized_term(unicodedata.normalize("NFKC", str(value or "")))


PHRASE_POS_NAMES = {
    "phrase",
    "prep_phrase",
    "prepositional phrase",
    "proverb",
    "idiom",
}

ARPABET_PHONETIC = re.compile(
    r"(?:[A-Z]+[0-2]?)(?:\s+[A-Z]+[0-2]?)+"
)


def is_phrase(term: Any, pos: Any = "") -> bool:
    """Recognize phrases without mistaking affixes such as ``pre-`` for one."""
    value = normalized_term(term)
    pos_names = {
        item.strip().lower()
        for item in re.split(r"[,;/]", str(pos or ""))
        if item.strip()
    }
    if pos_names & PHRASE_POS_NAMES or any(
        item.endswith("_phrase") for item in pos_names
    ):
        return True
    if " " in value:
        return True
    return (
        "-" in value
        and not value.startswith("-")
        and not value.endswith("-")
        and len([part for part in value.split("-") if part]) >= 2
    )


def entry_quality_gaps(
    term: Any,
    definition: Any,
    translation: Any,
    us_phonetic: Any,
    uk_phonetic: Any,
    pos: Any,
) -> list[str]:
    """Return required top-20k fields that are still incomplete."""
    gaps: list[str] = []
    if not str(definition or "").strip():
        gaps.append("definition")
    if needs_definition_translation(definition, translation):
        gaps.append("definition_zh")
    if not is_phrase(term, pos):
        if not str(pos or "").strip():
            gaps.append("pos")
        if needs_phonetic_repair(us_phonetic) and needs_phonetic_repair(
            uk_phonetic
        ):
            gaps.append("phonetic")
    return gaps


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.ignored_depth = 0

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        del attrs
        if tag.lower() in {"style", "script"}:
            self.ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"style", "script"} and self.ignored_depth:
            self.ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self.ignored_depth:
            self.parts.append(data)


def html_text(value: Any) -> str:
    parser = _TextExtractor()
    parser.feed(str(value or ""))
    return " ".join(html.unescape("".join(parser.parts)).split()).strip()


def wiktionary_definition_fields(data: Any) -> dict[str, str]:
    """Extract English definitions from Wiktionary's structured endpoint."""
    if not isinstance(data, dict):
        return {}
    entries = data.get("en")
    if not isinstance(entries, list):
        return {}
    parts_of_speech: list[str] = []
    definitions: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        pos = str(entry.get("partOfSpeech") or "").strip().lower()
        if pos:
            parts_of_speech.append(pos)
        for raw in entry.get("definitions", []) or []:
            if not isinstance(raw, dict):
                continue
            definition = html_text(raw.get("definition"))
            # The endpoint sometimes includes a parent definition containing
            # an entire nested ordered list followed by the same individual
            # senses.  Keep the bounded individual senses instead of storing
            # and translating that duplicated aggregate.
            if (
                definition
                and len(definition) <= 800
                and definition not in definitions
            ):
                definitions.append(definition)
            if len(definitions) >= 8:
                break
        if len(definitions) >= 8:
            break
    result: dict[str, str] = {}
    if definitions:
        result["definition"] = "\n".join(definitions)
    if parts_of_speech:
        result["pos"] = ", ".join(unique(parts_of_speech, 8))
    return result


def wiktionary_english_ipa(page_html: Any) -> str:
    """Return one reliable IPA value from the English section only."""
    source = str(page_html or "")
    start = re.search(r'<h2[^>]+id=["\']English["\'][^>]*>', source, re.I)
    if not start:
        return ""
    remainder = source[start.end():]
    end = re.search(r"<h2\b", remainder, re.I)
    section = remainder[:end.start()] if end else remainder
    for match in re.finditer(
        r'<span[^>]*class=["\'][^"\']*\bIPA\b[^"\']*["\'][^>]*>'
        r"(.*?)</span>",
        section,
        re.I | re.S,
    ):
        value = normalize_phonetic(html_text(match.group(1)))
        if value:
            return value
    return ""


def wiktionary_variants(term: Any) -> list[str]:
    """Try separator-equivalent spellings, never a fuzzy different term."""
    value = normalized_term(term)
    variants = [value] if value else []
    if value and not value.startswith("-") and not value.endswith("-"):
        alternate = (
            re.sub(r"-+", " ", value)
            if "-" in value
            else re.sub(r" +", "-", value)
        )
        if alternate != value and alternate not in variants:
            variants.append(alternate)
    return variants


def merge_named_entries(
    current: Any,
    incoming: Any,
    limit: int = 40,
) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    positions: dict[str, int] = {}
    values = [
        *(current if isinstance(current, list) else []),
        *(incoming if isinstance(incoming, list) else []),
    ]
    for raw in values:
        if not isinstance(raw, dict):
            continue
        word = " ".join(str(raw.get("word") or "").split()).strip()
        definition = " ".join(
            str(raw.get("definition") or raw.get("meaning") or "").split()
        ).strip()
        if not word or not definition:
            continue
        key = word.lower()
        if key in positions:
            index = positions[key]
            if len(definition) > len(result[index]["definition"]):
                result[index]["definition"] = definition
            continue
        positions[key] = len(result)
        result.append({"word": word, "definition": definition})
        if len(result) >= limit:
            break
    return result


def normalize_phonetic(value: Any) -> str:
    """Normalize common legacy ECDICT symbols to Unicode IPA."""
    text = str(value or "").strip().strip("/")
    return (
        text.replace("ә", "ə")
        .replace(":", "ː")
        .replace("'", "ˈ")
        .replace("ˈˈ", "ˈ")
    )


def needs_phonetic_repair(value: Any) -> bool:
    """Treat empty and known legacy transcription forms as incomplete."""
    text = str(value or "").strip().strip("/")
    return (
        not text
        or "ә" in text
        or ":" in text
        or "\ufffd" in text
        or bool(ARPABET_PHONETIC.fullmatch(text))
    )


def needs_definition_translation(definition: Any, translation: Any) -> bool:
    """Detect missing, truncated, or substantially untranslated Chinese text.

    A non-empty value is not sufficient: some interrupted batch translations
    contain a short Chinese prefix followed by a large untouched English
    block.  Keep the checks deliberately conservative so product names and
    short English labels inside an otherwise Chinese definition remain valid.
    """
    source = " ".join(str(definition or "").split()).strip()
    target = " ".join(str(translation or "").split()).strip()
    if not source:
        return False
    if not target:
        return True
    if not re.search(r"[\u3400-\u4dbf\u4e00-\u9fff]", target):
        return True
    if len(source) > 120 and len(target) < max(20, int(len(source) * 0.15)):
        return True

    if "\ufffd" in target:
        return True

    # Detect a copied English block by comparing it with the source.  A raw
    # Latin-character ratio incorrectly rejects valid Chinese translations
    # that preserve names such as "Cascading Style Sheets" or chemical names.
    # Eight consecutive source words is long enough to identify an untouched
    # provider chunk without penalizing those short, intentional labels.
    source_words = [
        item.lower()
        for item in re.findall(r"[A-Za-z]+(?:['’-][A-Za-z]+)*", source)
    ]
    target_words = [
        item.lower()
        for item in re.findall(r"[A-Za-z]+(?:['’-][A-Za-z]+)*", target)
    ]
    if len(source_words) >= 8 and len(target_words) >= 8:
        source_runs = {
            tuple(source_words[index:index + 8])
            for index in range(len(source_words) - 7)
        }
        if any(
            tuple(target_words[index:index + 8]) in source_runs
            for index in range(len(target_words) - 7)
        ):
            return True
    return False


def needs_frequency_repair(value: Any) -> bool:
    try:
        score = float(value or 0)
    except (TypeError, ValueError):
        return True
    # wordfreq and the exported manifest use the Zipf scale. Earlier
    # enrichment briefly stored Datamuse relevance scores, which can be in the
    # millions and must not survive into the public snapshots.
    return score > 10


def translation_chunks(text: str, limit: int = 480) -> list[str]:
    """Split a complete definition into ordered provider-safe chunks."""
    if limit < 1 or limit > 480:
        raise ValueError("translation chunk limit must be within 1..480")
    remaining = str(text or "").strip()
    if not remaining:
        return []
    chunks: list[str] = []
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        cut = max(
            remaining.rfind(" ", 0, limit + 1),
            remaining.rfind("\n", 0, limit + 1),
            remaining.rfind("\t", 0, limit + 1),
        )
        if cut <= 0:
            cut = limit
        chunks.append(remaining[:cut].strip())
        remaining = remaining[cut:].lstrip()
    return chunks


def init_state(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("""CREATE TABLE IF NOT EXISTS provider_state(
      term TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0, http_status INTEGER, last_error TEXT,
      updated_at TEXT NOT NULL, PRIMARY KEY(term,source))""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_provider_state_status ON provider_state(status,updated_at)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_provider_state_updated_at ON provider_state(updated_at)")
    db.execute(
        """CREATE TABLE IF NOT EXISTS run_contract(
          name TEXT PRIMARY KEY, value TEXT NOT NULL)"""
    )
    db.commit()
    return db


def bind_state_candidate(
    state: sqlite3.Connection,
    candidate_digest: str,
    *,
    shard_owner: int,
) -> None:
    """Refuse to reuse provider state for a different fixed candidate queue."""
    expected = {
        "candidate_digest": candidate_digest,
        "shard_owner": str(shard_owner),
    }
    state.execute("BEGIN IMMEDIATE")
    try:
        existing = {
            str(row[0]): str(row[1])
            for row in state.execute("SELECT name,value FROM run_contract")
        }
        if not existing:
            legacy_rows = int(
                state.execute("SELECT count(*) FROM provider_state").fetchone()[0]
            )
            if legacy_rows:
                raise ValueError(
                    "repair state has provider rows but no candidate digest: "
                    "use a fresh --state file"
                )
            state.executemany(
                "INSERT INTO run_contract(name,value) VALUES(?,?)",
                expected.items(),
            )
        elif existing != expected:
            raise ValueError(
                "repair state candidate digest or shard mismatch: "
                "use a fresh --state file"
            )
        rebound = {
            str(row[0]): str(row[1])
            for row in state.execute("SELECT name,value FROM run_contract")
        }
        if rebound != expected:
            raise ValueError("repair state binding verification failed")
        state.commit()
    except Exception:
        state.rollback()
        raise


def ensure_dataset_columns(database: sqlite3.Connection) -> None:
    columns = {
        str(row[1])
        for row in database.execute("PRAGMA table_info(entries)").fetchall()
    }
    for name in ("phrase_entries_json", "related_entries_json"):
        if name not in columns:
            database.execute(
                f"ALTER TABLE entries ADD COLUMN {name} "
                "TEXT NOT NULL DEFAULT '[]'"
            )
    database.commit()


def fetch_candidate_batch(
    database: sqlite3.Connection,
    *,
    start_id: int | None,
    end_id: int | None,
    frequency_first: bool,
    after_frequency_rank: int | None,
    after_id: int | None,
    batch_size: int,
    max_frequency_rank: int | None = None,
) -> tuple[list[tuple[Any, ...]], int | None, int | None]:
    """Read one stable keyset page and finalize its cursor before returning.

    The enrichment loop commits writes between pages.  A cursor that streams
    the complete shard would retain a read snapshot for the whole run and pin
    the WAL.  Fetching a bounded page, closing the cursor, and then performing
    writes lets SQLite checkpoints advance normally.

    ``frequency_rank`` is the stable, unique priority key in generated
    datasets.  ``id`` remains a tie-breaker so older/custom datasets with
    duplicate ranks still resume without skipping rows.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    where: list[str] = []
    params: list[int] = []
    if start_id is not None:
        where.append("id >= ?")
        params.append(start_id)
    if end_id is not None:
        where.append("id <= ?")
        params.append(end_id)
    if max_frequency_rank is not None:
        where.append("frequency_rank <= ?")
        params.append(max_frequency_rank)

    query = f"SELECT {ENTRY_SELECT_COLUMNS}"
    if frequency_first:
        query += ",frequency_rank FROM entries INDEXED BY idx_entries_freq"
        if after_frequency_rank is not None:
            # A row-value comparison keeps duplicate ranks safe while still
            # letting SQLite seek into idx_entries_freq.  The equivalent OR
            # expression makes SQLite rescan the index from its beginning on
            # every page, which becomes quadratic over a complete shard.
            where.append("(frequency_rank,id) > (?,?)")
            params.extend((after_frequency_rank, after_id or 0))
    else:
        query += " FROM entries"
        if after_id is not None:
            where.append("id > ?")
            params.append(after_id)

    if where:
        query += " WHERE " + " AND ".join(where)
    query += (
        " ORDER BY frequency_rank,id"
        if frequency_first
        else " ORDER BY id"
    )
    query += " LIMIT ?"
    params.append(batch_size)

    cursor = database.execute(query, params)
    try:
        raw_rows = cursor.fetchall()
    finally:
        # Explicit finalization is important: fetchall() exhausts the current
        # result, but close() makes the read-transaction boundary unambiguous
        # for both CPython's sqlite3 wrapper and alternative runtimes.
        cursor.close()

    if not raw_rows:
        return [], after_frequency_rank, after_id
    if frequency_first:
        next_rank = int(raw_rows[-1][-1])
        next_id = int(raw_rows[-1][0])
        return [tuple(row[:-1]) for row in raw_rows], next_rank, next_id
    next_id = int(raw_rows[-1][0])
    return [tuple(row) for row in raw_rows], None, next_id


def _contract_stream_digest(rows: Any, row_hasher: Any) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(row_hasher(tuple(row)).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def validate_repair_queue_internal(
    database: sqlite3.Connection,
    metadata: dict[str, Any],
) -> None:
    queue_rows = {
        int(row[0]): tuple(row)
        for row in database.execute(
            "SELECT canonical_id,selected_rank,canonical_frequency_rank,"
            "normalized_word,canonical_identity_sha256,gaps_json,shard_owner "
            "FROM repair_queue ORDER BY canonical_id"
        )
    }
    seen = 0
    columns = ",".join(f"e.{column}" for column in ENTRY_COLUMNS)
    cursor = database.execute(
        "SELECT e.id," + columns
        + ",p.selected_rank,p.canonical_frequency_rank,"
        "p.normalized_word AS provenance_word,p.term_key,"
        "p.canonical_content_sha256,p.canonical_identity_sha256 "
        "FROM entries e JOIN fast20k_provenance p ON p.canonical_id=e.id "
        "ORDER BY p.selected_rank"
    )
    for raw in cursor:
        seen += 1
        entry_id = int(raw[0])
        values = tuple(raw[1 : 1 + len(ENTRY_COLUMNS)])
        selected_rank = int(raw[1 + len(ENTRY_COLUMNS)])
        canonical_rank = int(raw[2 + len(ENTRY_COLUMNS)])
        provenance_word = str(raw[3 + len(ENTRY_COLUMNS)])
        provenance_key = str(raw[4 + len(ENTRY_COLUMNS)])
        expected_content = str(raw[5 + len(ENTRY_COLUMNS)])
        expected_identity = str(raw[6 + len(ENTRY_COLUMNS)])
        candidate_row = dict(zip(ENTRY_COLUMNS, values))
        if int(candidate_row["frequency_rank"]) != selected_rank:
            raise ValueError(
                f"repair queue internal rank mismatch: id={entry_id}"
            )
        baseline_row = {"id": entry_id, **candidate_row}
        baseline_row["frequency_rank"] = canonical_rank
        if (
            normalized_term(candidate_row["normalized_word"])
            != normalized_term(provenance_word)
            or candidate_term_key(candidate_row["normalized_word"])
            != provenance_key
            or canonical_content_digest(baseline_row) != expected_content
            or canonical_identity_digest(baseline_row) != expected_identity
        ):
            raise ValueError(
                f"repair queue internal provenance mismatch: id={entry_id}"
            )
        gaps = entry_quality_gaps(
            candidate_row["normalized_word"],
            candidate_row["definition"],
            candidate_row["definition_zh"],
            candidate_row["us_phonetic"],
            candidate_row["uk_phonetic"],
            candidate_row["pos"],
        )
        actual_queue = queue_rows.pop(entry_id, None)
        if not gaps:
            if actual_queue is not None:
                raise ValueError(
                    f"repair queue contains complete entry: id={entry_id}"
                )
            continue
        expected_queue = (
            entry_id,
            selected_rank,
            canonical_rank,
            str(candidate_row["normalized_word"]),
            expected_identity,
            json.dumps(gaps, ensure_ascii=False, separators=(",", ":")),
            entry_id % int(metadata["shard_count"]),
        )
        if actual_queue != expected_queue:
            raise ValueError(
                f"repair queue internal row mismatch: id={entry_id}"
            )
    cursor.close()
    expected_rows = int(metadata["expected_rows"])
    entry_count = int(database.execute("SELECT count(*) FROM entries").fetchone()[0])
    provenance_count = int(
        database.execute("SELECT count(*) FROM fast20k_provenance").fetchone()[0]
    )
    ranks = tuple(
        database.execute(
            "SELECT MIN(selected_rank),MAX(selected_rank),"
            "COUNT(DISTINCT selected_rank) FROM fast20k_provenance"
        ).fetchone()
    )
    kind_counts = {
        str(row[0]): int(row[1])
        for row in database.execute(
            "SELECT kind,count(*) FROM fast20k_provenance GROUP BY kind"
        )
    }
    if (
        entry_count != expected_rows
        or provenance_count != expected_rows
        or seen != expected_rows
        or ranks != (1, expected_rows, expected_rows)
        or kind_counts.get("word", 0) != int(metadata["word_count"])
        or kind_counts.get("phrase", 0) != int(metadata["phrase_count"])
        or queue_rows
    ):
        raise ValueError(
            "repair queue internal coverage mismatch: "
            f"expected={expected_rows} entries={entry_count} "
            f"provenance={provenance_count}/{seen} ranks={ranks} "
            f"kinds={kind_counts} extra_queue={len(queue_rows)}"
        )


def repair_queue_metadata(database: sqlite3.Connection) -> dict[str, Any]:
    row = database.execute(
        "SELECT selection_version,policy_name,expected_rows,word_count,phrase_count,"
        "shard_count,selection_digest,"
        "baseline_content_digest,repair_queue_digest,candidate_digest "
        "FROM fast20k_metadata WHERE id=1"
    ).fetchone()
    if row is None:
        raise ValueError("repair queue contract mismatch: metadata missing")
    metadata = {
        "selection_version": int(row[0]),
        "policy_name": str(row[1]),
        "expected_rows": int(row[2]),
        "word_count": int(row[3]),
        "phrase_count": int(row[4]),
        "shard_count": int(row[5]),
        "selection_digest": str(row[6]),
        "baseline_content_digest": str(row[7]),
        "repair_queue_digest": str(row[8]),
        "candidate_digest": str(row[9]),
    }
    if (
        metadata["selection_version"] != FAST20K_SELECTION_VERSION
        or metadata["policy_name"] != FAST20K_POLICY_NAME
        or metadata["shard_count"] < 1
    ):
        raise ValueError("repair queue contract mismatch: unsupported metadata")
    actual_selection = _contract_stream_digest(
        database.execute(
            "SELECT canonical_id,selected_rank,canonical_frequency_rank,"
            "normalized_word,term_key,kind,ranking_evidence,phrase_evidence,"
            "canonical_identity_sha256 FROM fast20k_provenance "
            "ORDER BY selected_rank"
        ),
        selection_row_digest,
    )
    actual_queue = _contract_stream_digest(
        database.execute(
            "SELECT canonical_id,selected_rank,canonical_frequency_rank,"
            "normalized_word,canonical_identity_sha256,gaps_json,shard_owner "
            "FROM repair_queue ORDER BY canonical_id"
        ),
        queue_row_digest,
    )
    actual_baseline = _contract_stream_digest(
        database.execute(
            "SELECT canonical_id,canonical_content_sha256 "
            "FROM fast20k_provenance ORDER BY selected_rank"
        ),
        selection_row_digest,
    )
    actual_candidate = candidate_contract_digest(
        actual_selection,
        actual_baseline,
        actual_queue,
        shard_count=metadata["shard_count"],
    )
    if (
        actual_selection != metadata["selection_digest"]
        or actual_baseline != metadata["baseline_content_digest"]
        or actual_queue != metadata["repair_queue_digest"]
        or actual_candidate != metadata["candidate_digest"]
    ):
        raise ValueError("repair queue contract mismatch: digest mismatch")
    validate_repair_queue_internal(database, metadata)
    return metadata


def open_repair_queue(path: Path) -> sqlite3.Connection:
    """Open and validate a candidate-embedded repair queue read-only."""
    database = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    try:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        # Pin every metadata, provenance and queue read to one immutable WAL
        # snapshot.  A candidate replacement during validation must never let
        # one process consume a mixture of two contracts.
        database.execute("BEGIN")
        integrity = [
            str(row[0]) for row in database.execute("PRAGMA quick_check").fetchmany(21)
        ]
        if integrity != ["ok"]:
            raise ValueError(
                "repair queue contract mismatch: SQLite quick_check failed"
            )
        tables = {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        required = {"fast20k_metadata", "fast20k_provenance", "repair_queue"}
        if not required <= tables:
            raise ValueError(
                "repair queue contract mismatch: missing "
                + ",".join(sorted(required - tables))
            )
        queue_columns = {
            str(row[1]) for row in database.execute("PRAGMA table_info(repair_queue)")
        }
        if "shard_owner" not in queue_columns:
            raise ValueError(
                "repair queue contract mismatch: shard_owner is missing"
            )
        repair_queue_metadata(database)
    except Exception:
        database.close()
        raise
    return database


def fetch_repair_queue_batch(
    dataset: sqlite3.Connection,
    queue: sqlite3.Connection,
    *,
    start_id: int | None,
    end_id: int | None,
    after_id: int | None,
    batch_size: int,
    shard_owner: int | None = None,
) -> tuple[list[tuple[Any, ...]], int | None]:
    """Fetch exact queued IDs without applying a frequency-rank ceiling."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    clauses: list[str] = []
    params: list[int] = []
    if shard_owner is not None:
        clauses.append("shard_owner=?")
        params.append(shard_owner)
    if start_id is not None:
        clauses.append("canonical_id>=?")
        params.append(start_id)
    if end_id is not None:
        clauses.append("canonical_id<=?")
        params.append(end_id)
    if after_id is not None:
        clauses.append("canonical_id>?")
        params.append(after_id)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    queue_rows = queue.execute(
        "SELECT canonical_id,normalized_word,canonical_frequency_rank,"
        "canonical_identity_sha256 FROM repair_queue"
        + where
        + " ORDER BY canonical_id LIMIT ?",
        (*params, batch_size),
    ).fetchall()
    if not queue_rows:
        return [], after_id
    ids = [int(row[0]) for row in queue_rows]
    placeholders = ",".join("?" for _ in ids)
    rows = dataset.execute(
        f"SELECT {ENTRY_SELECT_COLUMNS},frequency_rank,source_json,scope_json "
        f"FROM entries WHERE id IN ({placeholders}) ORDER BY id",
        ids,
    ).fetchall()
    by_id = {int(row[0]): row for row in rows}
    selected: list[tuple[Any, ...]] = []
    for queued in queue_rows:
        canonical_id = int(queued[0])
        row = by_id.get(canonical_id)
        if row is None:
            raise ValueError(
                "repair queue provenance mismatch: "
                f"canonical id {canonical_id} is missing"
            )
        identity = canonical_identity_digest(
            {
                "id": row[0],
                "word": row[1],
                "normalized_word": row[2],
                "frequency_rank": row[18],
                "source_json": row[19],
                "scope_json": row[20],
            }
        )
        if (
            normalized_term(row[2]) != normalized_term(queued[1])
            or int(row[18]) != int(queued[2])
            or identity != str(queued[3])
        ):
            raise ValueError(
                "repair queue provenance mismatch: "
                f"canonical id {canonical_id} changed"
            )
        selected.append(tuple(row[:18]))
    return selected, ids[-1]


def verify_selected_owner_page_content(
    dataset: sqlite3.Connection,
    queue: sqlite3.Connection,
    entry_ids: list[int],
    *,
    candidate_digest: str,
    shard_owner: int,
) -> None:
    if not entry_ids:
        return
    placeholders = ",".join("?" for _ in entry_ids)
    expected = {
        int(row[0]): (str(row[1]), bool(row[2]))
        for row in queue.execute(
            "SELECT p.canonical_id,p.canonical_content_sha256,"
            "CASE WHEN q.canonical_id IS NULL THEN 0 ELSE 1 END "
            "FROM fast20k_provenance p LEFT JOIN repair_queue q "
            "ON q.canonical_id=p.canonical_id "
            f"WHERE p.canonical_id IN ({placeholders})",
            entry_ids,
        )
    }
    columns = ("id", *ENTRY_COLUMNS)
    rows = dataset.execute(
        "SELECT " + ",".join(columns)
        + f" FROM entries WHERE id IN ({placeholders})",
        entry_ids,
    ).fetchall()
    current = {int(row[0]): dict(zip(columns, tuple(row))) for row in rows}
    for entry_id in entry_ids:
        row = current.get(entry_id)
        if row is None or entry_id not in expected:
            raise ValueError(
                f"repair queue content provenance mismatch: id={entry_id} missing"
            )
        expected_content, is_queued = expected[entry_id]
        if canonical_content_digest(row) == expected_content:
            continue
        try:
            marker = json.loads(str(row.get("enrichment_json") or "{}"))
        except json.JSONDecodeError:
            marker = {}
        if (
            not is_queued
            or not isinstance(marker, dict)
            or str(marker.get("repairCandidateDigest") or "") != candidate_digest
            or int(marker.get("repairShardOwner", -1)) != shard_owner
            or str(marker.get("repairPayloadDigest") or "")
            != repair_payload_digest(row)
        ):
            raise ValueError(
                "repair queue content provenance mismatch: "
                f"canonical id {entry_id} differs from candidate baseline"
            )


def verify_repair_page_content(
    dataset: sqlite3.Connection,
    queue: sqlite3.Connection,
    entry_ids: list[int],
    *,
    candidate_digest: str,
    shard_owner: int,
) -> None:
    """Compatibility wrapper for callers exporting only queued repair rows."""
    verify_selected_owner_page_content(
        dataset,
        queue,
        entry_ids,
        candidate_digest=candidate_digest,
        shard_owner=shard_owner,
    )


def preflight_repair_queue_shard(
    dataset: sqlite3.Connection,
    queue: sqlite3.Connection,
    *,
    shard_index: int,
    shard_count: int,
    batch_size: int = 512,
    validate_selected_owner: bool = True,
) -> tuple[list[tuple[Any, ...]], dict[str, Any]]:
    """Validate every selected owner row, then return its queued repair rows."""
    metadata = repair_queue_metadata(queue)
    if shard_count != int(metadata["shard_count"]):
        raise ValueError(
            "repair queue shard count mismatch: "
            f"candidate={metadata['shard_count']} runtime={shard_count}"
        )
    if not 0 <= shard_index < shard_count:
        raise ValueError("--shard-index must be within [0, --shard-count)")
    invalid_owner = queue.execute(
        "SELECT canonical_id FROM repair_queue "
        "WHERE shard_owner != (canonical_id % ?) LIMIT 1",
        (shard_count,),
    ).fetchone()
    if invalid_owner is not None:
        raise ValueError(
            "repair queue shard ownership mismatch: "
            f"canonical id {invalid_owner[0]}"
        )
    expected = int(
        queue.execute(
            "SELECT count(*) FROM repair_queue WHERE shard_owner=?",
            (shard_index,),
        ).fetchone()[0]
    )
    rows: list[tuple[Any, ...]] = []
    dataset.execute("BEGIN")
    try:
        # Validate the complete selected owner baseline, not just incomplete
        # queue rows.  Otherwise a term that is complete on the candidate-build
        # host but incomplete on its actual owner can silently escape repair.
        selected_expected = int(
            queue.execute(
                "SELECT count(*) FROM fast20k_provenance "
                "WHERE (canonical_id % ?) = ?",
                (shard_count, shard_index),
            ).fetchone()[0]
        )
        if validate_selected_owner:
            selected_seen = 0
            selected_after = -1
            while True:
                selected_ids = [
                    int(row[0])
                    for row in queue.execute(
                        "SELECT canonical_id FROM fast20k_provenance "
                        "WHERE (canonical_id % ?) = ? AND canonical_id > ? "
                        "ORDER BY canonical_id LIMIT ?",
                        (shard_count, shard_index, selected_after, batch_size),
                    ).fetchall()
                ]
                if not selected_ids:
                    break
                verify_selected_owner_page_content(
                    dataset,
                    queue,
                    selected_ids,
                    candidate_digest=str(metadata["candidate_digest"]),
                    shard_owner=shard_index,
                )
                selected_seen += len(selected_ids)
                selected_after = selected_ids[-1]
            if selected_seen != selected_expected:
                raise ValueError(
                    "repair selected-owner preflight count mismatch: "
                    f"expected={selected_expected} actual={selected_seen}"
                )

        after_id: int | None = None
        while True:
            page, next_id = fetch_repair_queue_batch(
                dataset,
                queue,
                start_id=None,
                end_id=None,
                after_id=after_id,
                batch_size=batch_size,
                shard_owner=shard_index,
            )
            if not page:
                break
            verify_repair_page_content(
                dataset,
                queue,
                [int(row[0]) for row in page],
                candidate_digest=str(metadata["candidate_digest"]),
                shard_owner=shard_index,
            )
            rows.extend(page)
            after_id = next_id
    finally:
        dataset.rollback()
    if len(rows) != expected:
        raise ValueError(
            f"repair queue preflight count mismatch: expected={expected} actual={len(rows)}"
        )
    metadata = dict(metadata)
    metadata["selected_owner_rows"] = selected_expected
    return rows, metadata


class HostGate:
    def __init__(self, interval: float, concurrency: int = 1):
        self.interval = interval
        self.lock = asyncio.Lock()
        self.last = 0.0
        self.semaphore = asyncio.Semaphore(max(1, concurrency))

    async def wait(self) -> None:
        async with self.lock:
            delta = time.monotonic() - self.last
            if delta < self.interval:
                await asyncio.sleep(self.interval - delta)
            self.last = time.monotonic()


def retry_after_seconds(value: Any, current_time: dt.datetime | None = None) -> float | None:
    """Parse an HTTP Retry-After delta or date into seconds."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        seconds = float(text)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(text)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=dt.timezone.utc)
        reference = current_time or dt.datetime.now(dt.timezone.utc)
        seconds = (retry_at - reference).total_seconds()
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


class EdgeBatchRetryExhausted(RuntimeError):
    """Stop a collection pass without marking a throttled batch as complete."""

    def __init__(
        self,
        *,
        status: int | None,
        attempts: int,
        retry_after: float | None,
        detail: str,
    ) -> None:
        self.status = status
        self.attempts = attempts
        self.retry_after = retry_after
        suffix = (
            f"; retry after {retry_after:.1f}s"
            if retry_after is not None
            else ""
        )
        super().__init__(
            f"edge batch unavailable after {attempts} attempt(s): "
            f"{detail}{suffix}"
        )


class EdgeDictionaryBatcher:
    """Coalesce concurrent term lookups into one bounded edge invocation."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        gate: HostGate,
        batch_size: int = 8,
        flush_delay: float = 0.04,
        profile: str = "core",
        max_attempts: int = 3,
        retry_base_delay: float = 1.0,
        retry_max_delay: float = 8.0,
        inline_retry_after_limit: float = 60.0,
        retry_jitter_ratio: float = 0.15,
    ):
        self.client = client
        self.gate = gate
        self.batch_size = max(1, min(8, batch_size))
        self.flush_delay = max(0.0, flush_delay)
        self.profile = "deep" if profile == "deep" else "core"
        self.max_attempts = max(1, min(6, max_attempts))
        self.retry_base_delay = max(0.0, retry_base_delay)
        self.retry_max_delay = max(
            self.retry_base_delay,
            retry_max_delay,
        )
        self.inline_retry_after_limit = max(
            0.0,
            inline_retry_after_limit,
        )
        self.retry_jitter_ratio = max(
            0.0,
            min(0.5, retry_jitter_ratio),
        )
        self.pending: list[
            tuple[
                str,
                dict[str, bool],
                asyncio.Future[
                    tuple[Any | None, int | None, str | None]
                ],
            ]
        ] = []
        self.lock = asyncio.Lock()
        self.flush_task: asyncio.Task[None] | None = None

    async def request(
        self,
        term: str,
        needs: dict[str, bool] | None = None,
    ) -> tuple[Any | None, int | None, str | None]:
        future: asyncio.Future[tuple[Any | None, int | None, str | None]] = (
            asyncio.get_running_loop().create_future()
        )
        legacy_needs = {
            "definition": True,
            "pos": True,
            "phonetic": True,
            "examples": True,
            "frequency": True,
            "deep": self.profile == "deep",
            "synonyms": self.profile == "deep",
            "antonyms": self.profile == "deep",
            "phrases": self.profile == "deep",
            "related": self.profile == "deep",
            "usPhonetic": True,
            "ukPhonetic": True,
        }
        requested_needs = legacy_needs if needs is None else needs
        normalized_needs = {
            name: bool(requested_needs.get(name))
            for name in (
                "definition",
                "pos",
                "phonetic",
                "examples",
                "frequency",
                "deep",
                "synonyms",
                "antonyms",
                "phrases",
                "related",
                "usPhonetic",
                "ukPhonetic",
            )
        }
        async with self.lock:
            self.pending.append((term, normalized_needs, future))
            if self.flush_task is None:
                self.flush_task = asyncio.create_task(self._flush())
        return await future

    async def _flush(self) -> None:
        if self.flush_delay:
            await asyncio.sleep(self.flush_delay)
        while True:
            async with self.lock:
                batch = self.pending[: self.batch_size]
                del self.pending[: self.batch_size]
                if not batch:
                    self.flush_task = None
                    return
            # A dataset normally contains each normalized term once, but
            # merging duplicate requests here keeps result mapping correct
            # when concurrent callers ask for different fields.
            terms: list[str] = []
            needs_by_term: dict[str, dict[str, bool]] = {}
            for term, needs, _ in batch:
                if term not in needs_by_term:
                    terms.append(term)
                    needs_by_term[term] = dict(needs)
                    continue
                for name, required in needs.items():
                    needs_by_term[term][name] = (
                        needs_by_term[term].get(name, False)
                        or required
                    )
            status: int | None = None
            error: str | None = None
            results: dict[str, Any] = {}
            fatal_error: EdgeBatchRetryExhausted | None = None
            for attempt in range(self.max_attempts):
                status = None
                error = None
                retryable = False
                retry_after: float | None = None
                await self.gate.semaphore.acquire()
                try:
                    await self.gate.wait()
                    response = await self.client.post(
                        f"{EDGE_BASE}/api/dictionary/batch",
                        json={
                            "terms": terms,
                            "profile": self.profile,
                            "needs": needs_by_term,
                        },
                        headers={
                            "Accept": "application/json",
                            "User-Agent": "LexoraOpenLexicon/1.0 (open-data enrichment)",
                            **(
                                {"X-Lexora-Origin-Token": EDGE_TOKEN}
                                if EDGE_TOKEN
                                else {}
                            ),
                        },
                    )
                    status = response.status_code
                    if status == 200:
                        payload = response.json()
                        if not isinstance(payload, dict):
                            raise ValueError(
                                "edge batch returned a non-object payload"
                            )
                        raw_results = payload.get("results", {})
                        if not isinstance(raw_results, dict):
                            raise ValueError(
                                "edge batch returned invalid results"
                            )
                        results = raw_results
                        missing_item_status: int | None = None
                        for requested_term in terms:
                            item = raw_results.get(requested_term)
                            if not isinstance(item, dict):
                                missing_item_status = 502
                                break
                        if missing_item_status is None:
                            error = None
                            break
                        status = missing_item_status
                        error = (
                            "edge batch omitted one or more requested terms"
                        )
                        retryable = True
                    error = f"HTTP {status}"
                    retryable = (
                        retryable
                        or status in (429, 500, 502, 503, 504)
                    )
                    if retryable and response.status_code != 200:
                        retry_after = retry_after_seconds(
                            response.headers.get("retry-after")
                        )
                except (httpx.HTTPError, ValueError, TypeError) as exc:
                    error = str(exc)
                    retryable = True
                finally:
                    self.gate.semaphore.release()

                if not retryable:
                    break

                attempts = attempt + 1
                too_long_to_wait = (
                    retry_after is not None
                    and retry_after > self.inline_retry_after_limit
                )
                if attempts >= self.max_attempts or too_long_to_wait:
                    fatal_error = EdgeBatchRetryExhausted(
                        status=status,
                        attempts=attempts,
                        retry_after=retry_after,
                        detail=error or "retryable edge failure",
                    )
                    break

                delay = (
                    retry_after
                    if retry_after is not None
                    else min(
                        self.retry_max_delay,
                        self.retry_base_delay * (2 ** attempt),
                    )
                )
                jitter = random.uniform(
                    0.0,
                    delay * self.retry_jitter_ratio,
                )
                await asyncio.sleep(delay + jitter)

            if fatal_error is not None:
                # A real quota response must stop the pass and preserve every
                # queued row for a later run. Transient 5xx failures only fail
                # the current bounded batch; the flusher then continues with
                # queued work instead of idling the whole server for 15 min.
                if fatal_error.status == 429:
                    async with self.lock:
                        queued = self.pending
                        self.pending = []
                        self.flush_task = None
                    owned = [*batch, *queued]
                else:
                    owned = batch
                for _, _, future in owned:
                    if not future.done():
                        future.set_exception(fatal_error)
                if fatal_error.status == 429:
                    return
                continue

            for term, _, future in batch:
                item = results.get(term)
                if not isinstance(item, dict):
                    # A successful outer batch response does not mean every
                    # requested term was accepted or processed.  Treat a
                    # missing entry as retryable instead of recording a false
                    # successful attempt with empty data.
                    item_status = (
                        502 if status in (None, 200) else status
                    )
                    data = None
                    item_error = (
                        "edge batch missing term result"
                        if status in (None, 200)
                        else error or "edge batch request failed"
                    )
                else:
                    item_status = (
                        int(item.get("status"))
                        if item.get("status") is not None
                        else status
                    )
                    data = item.get("data")
                    item_error = error
                if item_status != 200:
                    if isinstance(data, dict) and data.get("error"):
                        item_error = str(data["error"])
                    data = None
                if not future.done():
                    future.set_result((data, item_status, item_error))


class EdgeTranslationBatcher:
    """Batch translations so dictionary enrichment stays within free quotas."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        gate: HostGate,
        batch_size: int = 8,
        flush_delay: float = 0.04,
    ):
        self.client = client
        self.gate = gate
        self.batch_size = max(1, min(32, batch_size))
        self.flush_delay = max(0.0, flush_delay)
        self.pending: list[
            tuple[str, asyncio.Future[tuple[str, int | None, str | None]]]
        ] = []
        self.lock = asyncio.Lock()
        self.flush_task: asyncio.Task[None] | None = None

    async def request(self, text: str) -> tuple[str, int | None, str | None]:
        future: asyncio.Future[tuple[str, int | None, str | None]] = (
            asyncio.get_running_loop().create_future()
        )
        async with self.lock:
            self.pending.append((text, future))
            if self.flush_task is None:
                self.flush_task = asyncio.create_task(self._flush())
        return await future

    async def _flush(self) -> None:
        if self.flush_delay:
            await asyncio.sleep(self.flush_delay)
        while True:
            async with self.lock:
                batch = self.pending[: self.batch_size]
                del self.pending[: self.batch_size]
                if not batch:
                    self.flush_task = None
                    return
            chunk_owners: list[int] = []
            owner_chunk_counts = [0 for _ in batch]
            texts: list[str] = []
            for owner, (text, _) in enumerate(batch):
                for chunk in translation_chunks(text):
                    chunk_owners.append(owner)
                    owner_chunk_counts[owner] += 1
                    texts.append(chunk)
            translated_chunks = ["" for _ in texts]
            chunk_statuses: list[int | None] = [None for _ in texts]
            chunk_errors: list[str | None] = [None for _ in texts]
            # The Worker accepts at most 32 texts per request.  Split the
            # flattened chunk list again so a very long definition cannot be
            # silently truncated after its 32nd chunk.
            for start in range(0, len(texts), 32):
                request_texts = texts[start : start + 32]
                status: int | None = None
                error: str | None = None
                translations: list[str] = []
                await self.gate.semaphore.acquire()
                try:
                    await self.gate.wait()
                    response = await self.client.post(
                        f"{EDGE_BASE}/api/translate/batch",
                        json={"texts": request_texts},
                        headers={
                            "Accept": "application/json",
                            "User-Agent": "LexoraOpenLexicon/1.0 (open-data enrichment)",
                            **(
                                {"X-Lexora-Origin-Token": EDGE_TOKEN}
                                if EDGE_TOKEN
                                else {}
                            ),
                        },
                    )
                    status = response.status_code
                    if response.status_code == 200:
                        payload = response.json()
                        values = payload.get("translations", [])
                        if isinstance(values, list):
                            translations = [
                                str(value or "").strip() for value in values
                            ]
                    else:
                        error = f"HTTP {response.status_code}"
                except (httpx.HTTPError, ValueError, TypeError) as exc:
                    error = str(exc)
                finally:
                    self.gate.semaphore.release()
                for offset in range(len(request_texts)):
                    index = start + offset
                    chunk_statuses[index] = status
                    chunk_errors[index] = error
                    if offset < len(translations):
                        translated_chunks[index] = translations[offset]

            values_by_owner: list[list[str]] = [[] for _ in batch]
            statuses_by_owner: list[list[int | None]] = [[] for _ in batch]
            errors_by_owner: list[list[str]] = [[] for _ in batch]
            for index, owner in enumerate(chunk_owners):
                values_by_owner[owner].append(translated_chunks[index])
                statuses_by_owner[owner].append(chunk_statuses[index])
                if chunk_errors[index]:
                    errors_by_owner[owner].append(str(chunk_errors[index]))
            for index, (_, future) in enumerate(batch):
                translations = values_by_owner[index]
                complete = (
                    owner_chunk_counts[index] > 0
                    and len(translations) == owner_chunk_counts[index]
                    and all(translations)
                )
                value = "\n".join(translations).strip() if complete else ""
                owner_statuses = statuses_by_owner[index]
                failed_status = next(
                    (
                        item
                        for item in owner_statuses
                        if item is not None and item != 200
                    ),
                    None,
                )
                status = 200 if complete else failed_status
                if status is None and any(item == 200 for item in owner_statuses):
                    status = 200
                error = (
                    "; ".join(unique(errors_by_owner[index], 3))
                    or "translation missing"
                )
                if not future.done():
                    future.set_result(
                        (
                            value,
                            status,
                            None if complete else error,
                        )
                    )


async def request_json(client: httpx.AsyncClient, gate: HostGate, url: str, source: str, attempts: int = 3) -> tuple[Any | None, int | None, str | None]:
    for attempt in range(attempts):
        await gate.semaphore.acquire()
        try:
            await gate.wait()
            response = await client.get(url, headers={"User-Agent": "LexoraOpenLexicon/1.0 (open-data enrichment)"})
            if response.status_code == 200:
                return response.json(), 200, None
            if response.status_code in (429, 500, 502, 503, 504):
                retry = response.headers.get("retry-after")
                delay = float(retry) if retry and retry.isdigit() else min(60.0, 2 ** attempt)
                await asyncio.sleep(delay)
                continue
            return None, response.status_code, f"HTTP {response.status_code}"
        except (httpx.HTTPError, ValueError) as exc:
            if attempt + 1 < attempts:
                await asyncio.sleep(min(60.0, 2 ** attempt))
            else:
                return None, None, str(exc)
        finally:
            gate.semaphore.release()
    return None, None, f"{source} exhausted retries"


async def request_text(
    client: httpx.AsyncClient,
    gate: HostGate,
    url: str,
    source: str,
    attempts: int = 3,
) -> tuple[str, int | None, str | None]:
    for attempt in range(attempts):
        await gate.semaphore.acquire()
        try:
            await gate.wait()
            response = await client.get(
                url,
                headers={
                    "User-Agent": (
                        "LexoraOpenLexicon/1.0 "
                        "(https://lexora.12323456.xyz)"
                    )
                },
            )
            if response.status_code == 200:
                return response.text, 200, None
            if response.status_code in (429, 500, 502, 503, 504):
                retry = retry_after_seconds(response.headers.get("retry-after"))
                await asyncio.sleep(
                    retry if retry is not None else min(60.0, 2 ** attempt)
                )
                continue
            return "", response.status_code, f"HTTP {response.status_code}"
        except httpx.HTTPError as exc:
            if attempt + 1 < attempts:
                await asyncio.sleep(min(60.0, 2 ** attempt))
            else:
                return "", None, str(exc)
        finally:
            gate.semaphore.release()
    return "", None, f"{source} exhausted retries"


async def fetch_wiktionary_fields(
    client: httpx.AsyncClient,
    gate: HostGate,
    term: str,
    *,
    need_definition: bool,
    need_pos: bool,
    need_phonetic: bool,
) -> tuple[dict[str, str], int | None, str | None]:
    """Use only exact or separator-equivalent Wiktionary page titles."""
    last_status: int | None = None
    errors: list[str] = []
    for variant in wiktionary_variants(term):
        encoded = quote(variant, safe="")
        fields: dict[str, str] = {}
        if need_definition or need_pos:
            data, status, error = await request_json(
                client,
                gate,
                "https://en.wiktionary.org/api/rest_v1/page/definition/"
                + encoded,
                "wiktionary-definition",
                attempts=2,
            )
            last_status = status
            if error:
                errors.append(f"{variant}: {error}")
            parsed = wiktionary_definition_fields(data)
            if need_definition and parsed.get("definition"):
                fields["definition"] = parsed["definition"]
            if need_pos and parsed.get("pos"):
                fields["pos"] = parsed["pos"]
        if need_phonetic:
            page, status, error = await request_text(
                client,
                gate,
                "https://en.wiktionary.org/api/rest_v1/page/html/" + encoded,
                "wiktionary-pronunciation",
                attempts=2,
            )
            last_status = status
            if error:
                errors.append(f"{variant}: {error}")
            ipa = wiktionary_english_ipa(page)
            if ipa:
                fields["us"] = ipa
        if fields:
            fields["matched_variant"] = variant
            return fields, 200, None
    return {}, last_status, "; ".join(unique(errors, 4)) or "not found"

def dictionary_fields(data: Any) -> dict[str, Any]:
    if not isinstance(data, list): return {}
    definitions: list[str] = []; examples: list[str] = []; synonyms: list[str] = []; antonyms: list[str] = []
    parts_of_speech: list[str] = []
    us = ""; uk = ""; fallback = ""
    for entry in data:
        entry_fallback = normalize_phonetic(entry.get("phonetic"))
        if entry_fallback and not fallback:
            fallback = entry_fallback
        for phonetic in entry.get("phonetics", []) or []:
            text = normalize_phonetic(phonetic.get("text"))
            if not text: continue
            audio = str(phonetic.get("audio") or "").lower()
            if ("-us." in audio or "/us/" in audio) and not us:
                us = text
            elif ("-uk." in audio or "/uk/" in audio) and not uk:
                uk = text
            elif not fallback:
                fallback = text
        for meaning in entry.get("meanings", []) or []:
            if meaning.get("partOfSpeech"):
                parts_of_speech.append(str(meaning["partOfSpeech"]))
            for item in meaning.get("definitions", []) or []:
                if item.get("definition"): definitions.append(item["definition"])
                if item.get("example"): examples.append(item["example"])
                synonyms.extend(
                    (
                        x.get("word", "")
                        if isinstance(x, dict)
                        else str(x)
                    )
                    for x in item.get("synonyms", []) or []
                )
                antonyms.extend(
                    (
                        x.get("word", "")
                        if isinstance(x, dict)
                        else str(x)
                    )
                    for x in item.get("antonyms", []) or []
                )
            synonyms.extend((x.get("word", "") if isinstance(x, dict) else str(x)) for x in meaning.get("synonyms", []) or [])
            antonyms.extend((x.get("word", "") if isinstance(x, dict) else str(x)) for x in meaning.get("antonyms", []) or [])
    return {
        "definition": "\n".join(unique(definitions, 24)),
        "examples": unique(examples, 8),
        "synonyms": unique(synonyms),
        "antonyms": unique(antonyms),
        "pos": ", ".join(unique(parts_of_speech, 8)),
        # A generic transcription is useful provenance, but assigning it to
        # both dialect columns fabricates information.  Keep only explicitly
        # identified US/UK values in their respective fields.
        "generic_phonetic": fallback,
        "us": us,
        "uk": uk,
    }

def datamuse_fields(
    data: Any,
    *,
    exact_term: str | None = None,
) -> dict[str, Any]:
    if not isinstance(data, list): return {}
    items = [item for item in data if isinstance(item, dict)]
    if exact_term is not None:
        target = normalized_term(exact_term)
        items = [
            item
            for item in items
            if normalized_term(item.get("word")) == target
        ]
    words = unique([item.get("word", "") for item in items])
    definitions = unique([d for item in items for d in item.get("defs", [])])
    frequencies: list[float] = []
    phonetics: list[str] = []
    parts_of_speech: list[str] = []
    pos_names = {
        "n": "noun",
        "v": "verb",
        "adj": "adjective",
        "adv": "adverb",
    }
    for item in items:
        for tag in item.get("tags", []) or []:
            if not isinstance(tag, str):
                continue
            if tag in pos_names:
                parts_of_speech.append(pos_names[tag])
                continue
            if tag.startswith("ipa_pron:"):
                phonetic = normalize_phonetic(tag.split(":", 1)[1])
                if phonetic:
                    phonetics.append(phonetic)
                continue
            if tag.startswith("f:"):
                try:
                    raw = float(tag[2:])
                except ValueError:
                    continue
                if raw > 0:
                    frequencies.append(3 + math.log10(raw))
    entries: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in items:
        word = " ".join(str(item.get("word") or "").split()).strip()
        if not word or word in seen:
            continue
        cleaned = unique(
            [
                str(value).split("\t", 1)[-1].strip()
                for value in item.get("defs", []) or []
                if str(value).split("\t", 1)[-1].strip()
            ],
            3,
        )
        if not cleaned:
            continue
        seen.add(word)
        entries.append({"word": word, "definition": "\n".join(cleaned)})
        if len(entries) >= 40:
            break
    return {
        "words": words,
        "entries": entries,
        "definition": "\n".join(definitions[:24]),
        "frequency": max(frequencies) if frequencies else 0.0,
        "phonetic": unique(phonetics, 1)[0] if phonetics else "",
        "pos": ", ".join(unique(parts_of_speech, 8)),
    }


def edge_fields(data: Any, term: str) -> dict[str, Any]:
    """Normalize the aggregate response from Lexora's Cloudflare edge."""
    if not isinstance(data, dict):
        return {}
    result = dictionary_fields(data.get("dictionary"))
    exact = datamuse_fields(data.get("exact"), exact_term=term)
    if not result.get("definition"):
        result["definition"] = exact.get("definition", "")
    if not result.get("pos"):
        result["pos"] = exact.get("pos", "")
    # Datamuse's ``md=p&ipa=1`` result includes an ``ipa_pron:`` tag.  It is
    # Datamuse currently derives this value from its American pronunciation
    # metadata.  Use it only as a US fallback; copying a rhotic /ɝ/ value into
    # the UK field would incorrectly label it as British pronunciation.
    exact_phonetic = str(exact.get("phonetic") or "")
    if exact_phonetic:
        result["us"] = result.get("us") or exact_phonetic
    # Only the exact target's frequency describes this entry.  A semantically
    # related word may be much more common and must not change the target's
    # difficulty badge.
    result["frequency"] = float(exact.get("frequency", 0) or 0)

    # Core responses intentionally omit the three deep providers.  Absence is
    # different from an empty provider result: do not manufacture empty lists
    # that overwrite DictionaryAPI relationships or a caller's existing data.
    if "related" in data:
        related = datamuse_fields(data.get("related"))
        related_words = related.get("words", [])
        result["related"] = related_words
        result["phrases"] = [
            word for word in related_words if " " in word
        ]
        result["related_entries"] = [
            item
            for item in related.get("entries", [])
            if " " not in str(item.get("word") or "")
        ]
        result["phrase_entries"] = [
            item
            for item in related.get("entries", [])
            if " " in str(item.get("word") or "")
        ]
    if "synonyms" in data:
        synonyms = datamuse_fields(data.get("synonyms"))
        result["synonyms"] = unique(
            [
                *result.get("synonyms", []),
                *synonyms.get("words", []),
            ]
        )
    if "antonyms" in data:
        antonyms = datamuse_fields(data.get("antonyms"))
        result["antonyms"] = unique(
            [
                *result.get("antonyms", []),
                *antonyms.get("words", []),
            ]
        )
    return result

def difficulty(score: float) -> str:
    if score >= 6.0: return "A1–A2"
    if score >= 4.8: return "B1–B2"
    if score >= 3.4: return "C1–C2"
    return "C2+"


def marker_retry_due(raw: str | None, retry_after_hours: float) -> bool:
    marker = json.loads(raw or "{}")
    if not marker.get("status"):
        return True
    last_attempt = marker.get("lastAttempt")
    if not last_attempt:
        return True
    try:
        attempted_at = dt.datetime.fromisoformat(last_attempt)
        if attempted_at.tzinfo is None:
            attempted_at = attempted_at.replace(tzinfo=dt.timezone.utc)
        age = dt.datetime.now(dt.timezone.utc) - attempted_at
        return age.total_seconds() >= max(0.0, retry_after_hours) * 3600
    except (TypeError, ValueError):
        return True


def marker_profile(raw: str | None) -> str:
    """Return the completed collection stage, treating legacy markers as core."""
    try:
        marker = json.loads(raw or "{}")
    except (TypeError, json.JSONDecodeError):
        return "core"
    if not isinstance(marker, dict):
        return "core"
    return "deep" if marker.get("profile") == "deep" else "core"


def should_process_marker(raw: str | None, retry_after_hours: float) -> bool:
    """Resume efficiently without re-querying a recently attempted term."""
    marker = json.loads(raw or "{}")
    status = marker.get("status")
    if not status:
        return True
    if status == "completed":
        return False
    return marker_retry_due(raw, retry_after_hours)


def needs_deep_enrichment(
    synonyms: Any,
    antonyms: Any,
    phrases: Any,
    related: Any,
) -> bool:
    """Whether a row still lacks any field supplied by the deep providers."""
    return any(
        not value
        for value in (synonyms, antonyms, phrases, related)
    )


def deep_enrichment_needs(
    synonyms: Any,
    antonyms: Any,
    phrases: Any,
    related: Any,
) -> dict[str, bool]:
    """Return the exact relationship fields absent from the local row."""
    return {
        "synonyms": not bool(synonyms),
        "antonyms": not bool(antonyms),
        "phrases": not bool(phrases),
        "related": not bool(related),
    }


def needs_deep_profile_pass(
    raw: str | None,
    profile: str,
    has_deep_gaps: bool,
) -> bool:
    """Allow one immediate deep pass after a successful core-only pass."""
    if profile != "deep" or not has_deep_gaps:
        return False
    try:
        marker = json.loads(raw or "{}")
    except (TypeError, json.JSONDecodeError):
        return False
    if not isinstance(marker, dict):
        return False
    return (
        marker.get("status") == "completed"
        and marker_profile(raw) != "deep"
    )


def needs_entry_quality_repair(
    term: str,
    definition: Any,
    translation: Any,
    us_phonetic: Any,
    uk_phonetic: Any,
    frequency: Any,
) -> bool:
    # Phrases do not reliably have standalone dictionary IPA, so a missing
    # phrase pronunciation must not force an endless retry.  A normal word is
    # complete once either a US or UK transcription is reliable.
    phrase = is_phrase(term)
    return (
        (
            not phrase
            and (
                needs_phonetic_repair(us_phonetic)
                and needs_phonetic_repair(uk_phonetic)
            )
        )
        or needs_definition_translation(definition, translation)
        or needs_frequency_repair(frequency)
    )


def resolved_difficulty(
    current: Any,
    previous_frequency: Any,
    score: float,
) -> str:
    if not str(current or "").strip() or needs_frequency_repair(previous_frequency):
        return difficulty(score)
    return str(current)

async def enrich_term(
    client: httpx.AsyncClient,
    gates: dict[str, HostGate],
    term: str,
    state: sqlite3.Connection,
    existing: dict[str, Any],
    edge_batcher: EdgeDictionaryBatcher | None = None,
    profile: str = "core",
    quality_repair_only: bool = False,
) -> dict[str, Any]:
    profile = "deep" if profile == "deep" else "core"
    encoded = quote(term, safe="")
    urls = {
      "edge": f"{EDGE_BASE}/api/dictionary/full?term={encoded}" if EDGE_BASE else "",
      "dictionary": f"https://api.dictionaryapi.dev/api/v2/entries/en/{encoded}",
      "datamuse_related": f"https://api.datamuse.com/words?ml={encoded}&md=dfr&ipa=1&max=30",
      "datamuse_exact": f"https://api.datamuse.com/words?sp={encoded}&md=dfrp&ipa=1&max=8",
      "datamuse_synonyms": f"https://api.datamuse.com/words?rel_syn={encoded}&md=f&max=12",
      "datamuse_antonyms": f"https://api.datamuse.com/words?rel_ant={encoded}&max=12",
    }
    result: dict[str, Any] = {
        "_statuses": [],
        "_attempted": [],
        "_field_sources": {},
        "_provider_results": {},
    }
    sources: list[str] = []
    phrase = is_phrase(term, existing.get("pos"))
    needs_definition = not str(existing.get("definition") or "").strip()
    needs_pos = not phrase and not str(existing.get("pos") or "").strip()
    needs_phonetic = (
        not phrase
        and needs_phonetic_repair(existing.get("us"))
        and needs_phonetic_repair(existing.get("uk"))
    )
    needs_examples = not quality_repair_only and not existing.get("examples")
    needs_frequency = (
        not quality_repair_only
        and needs_frequency_repair(existing.get("frequency"))
    )
    deep_needs = (
        deep_enrichment_needs(
            existing.get("synonyms"),
            existing.get("antonyms"),
            existing.get("phrases"),
            existing.get("related"),
        )
        if profile == "deep" and not quality_repair_only
        else {
            "synonyms": False,
            "antonyms": False,
            "phrases": False,
            "related": False,
        }
    )
    needs_deep = any(deep_needs.values())
    needs_network = any(
        (
            needs_definition,
            needs_pos,
            needs_phonetic,
            needs_examples,
            needs_frequency,
            needs_deep,
        )
    )
    if EDGE_BASE and needs_network:
        sources.append("edge")
    elif needs_definition or needs_phonetic or needs_examples:
        sources.append("dictionary")
    if not EDGE_BASE:
        if deep_needs["related"] or deep_needs["phrases"]:
            sources.append("datamuse_related")
        if needs_definition or needs_pos or needs_frequency:
            sources.append("datamuse_exact")
        if deep_needs["synonyms"]:
            sources.append("datamuse_synonyms")
        if deep_needs["antonyms"]:
            sources.append("datamuse_antonyms")
    if quality_repair_only and any(
        (needs_definition, needs_pos, needs_phonetic)
    ):
        sources.append("wiktionary")

    def apply_fields(fields: dict[str, Any], source: str) -> None:
        for name, value in fields.items():
            if name == "matched_variant" or value in (None, "", [], {}):
                continue
            if not result.get(name):
                result[name] = value
                result["_field_sources"][name] = source

    async def fetch(source: str) -> tuple[str, Any | None, int | None, str | None]:
        if source == "edge" and edge_batcher is not None:
            data, status, error = await edge_batcher.request(
                term,
                {
                    "definition": needs_definition,
                    "pos": needs_pos,
                    "phonetic": needs_phonetic,
                    "examples": needs_examples,
                    "frequency": needs_frequency,
                    "deep": needs_deep,
                    "synonyms": deep_needs["synonyms"],
                    "antonyms": deep_needs["antonyms"],
                    "phrases": deep_needs["phrases"],
                    "related": deep_needs["related"],
                    # The quality gate needs one reliable pronunciation, not
                    # two dialect variants.  Prefer the US-capable exact
                    # fallback and still accept a UK value returned by the
                    # dictionary provider.
                    "usPhonetic": needs_phonetic,
                    "ukPhonetic": False,
                },
            )
            if status in (500, 502, 503, 504):
                # Keep a transient failure isolated to this term. Other terms
                # in the same successful outer batch can still be committed.
                raise EdgeBatchRetryExhausted(
                    status=status,
                    attempts=1,
                    retry_after=None,
                    detail=error or f"HTTP {status}",
                )
            return source, data, status, error
        if source == "wiktionary":
            data, status, error = await fetch_wiktionary_fields(
                client,
                gates["wiktionary"],
                term,
                need_definition=needs_definition,
                need_pos=needs_pos,
                need_phonetic=needs_phonetic,
            )
            return source, data, status, error
        gate = gates["datamuse"] if source.startswith("datamuse") else gates[source]
        data, status, error = await request_json(client, gate, urls[source], source)
        return source, data, status, error

    # Sources for one term are independent.  Fetching them together removes
    # the previous per-term serial bottleneck while HostGate still limits each
    # provider's request rate and in-flight concurrency.
    fetched = await asyncio.gather(*(fetch(source) for source in sources))
    for source, data, status, error in fetched:
        provider_status = "completed" if status == 200 else "failed"
        if source == "wiktionary" and not data and status in (200, 404):
            provider_status = "not_found"
        if source == "edge" and status == 200 and isinstance(data, dict):
            provider_details = data.get("_providers")
            if isinstance(provider_details, dict) and provider_details:
                provider_values = [
                    item
                    for item in provider_details.values()
                    if isinstance(item, dict)
                ]
                provider_ok = [
                    bool(item.get("ok"))
                    for item in provider_values
                ]
                if not bool(data.get("_found")):
                    provider_status = "not_found"
                elif data.get("_complete") is False:
                    provider_status = "partial"
                elif provider_ok and all(provider_ok):
                    provider_status = "completed"
                elif any(provider_ok):
                    provider_status = "partial"
                else:
                    provider_status = "failed"
        state.execute("""INSERT INTO provider_state(term,source,status,attempts,http_status,last_error,updated_at)
          VALUES(?,?,?,?,?,?,?) ON CONFLICT(term,source) DO UPDATE SET status=excluded.status,attempts=provider_state.attempts+1,http_status=excluded.http_status,last_error=excluded.last_error,updated_at=excluded.updated_at""",
          (term, source, provider_status, 1, status, error, now()))
        result["_statuses"].append(provider_status)
        result["_attempted"].append(source)
        result["_provider_results"][source] = {
            "status": provider_status,
            "httpStatus": status,
            "error": error or "",
        }
        if source == "dictionary":
            apply_fields(dictionary_fields(data), source)
        elif source.startswith("datamuse"):
            parsed = datamuse_fields(
                data,
                exact_term=term if source == "datamuse_exact" else None,
            )
            result[source] = parsed
            if source == "datamuse_exact" and parsed.get("definition") and not result.get("definition"):
                result["definition"] = parsed["definition"]
                result["_field_sources"]["definition"] = source
            if source == "datamuse_related":
                result["related"] = parsed.get("words", [])
                result["phrases"] = [
                    word for word in parsed.get("words", []) if " " in word
                ]
            if source == "datamuse_synonyms": result["synonyms"] = parsed.get("words", [])
            if source == "datamuse_antonyms": result["antonyms"] = parsed.get("words", [])
            if source == "datamuse_exact":
                result["frequency"] = max(
                    float(result.get("frequency", 0)),
                    float(parsed.get("frequency", 0)),
                )
        elif source == "edge":
            apply_fields(edge_fields(data, term), source)
        elif source == "wiktionary" and isinstance(data, dict):
            apply_fields(data, source)
    return result

async def translate(
    client: httpx.AsyncClient,
    gate: HostGate,
    text: str,
    translation_batcher: EdgeTranslationBatcher | None = None,
) -> tuple[str, int | None, str | None]:
    if not text: return "", None, None
    if EDGE_BASE and translation_batcher is not None:
        return await translation_batcher.request(text)
    if EDGE_BASE:
        return await EdgeTranslationBatcher(
            client,
            gate,
            batch_size=1,
            flush_delay=0,
        ).request(text)
    translations: list[str] = []
    statuses: list[int | None] = []
    for chunk in translation_chunks(text):
        url = (
            "https://api.mymemory.translated.net/get?q="
            + quote(chunk, safe="")
            + "&langpair=en|zh-CN"
        )
        data, status, error = await request_json(
            client,
            gate,
            url,
            "translation",
        )
        statuses.append(status)
        try:
            translated = str(data["responseData"]["translatedText"]).strip()
        except Exception:
            translated = ""
        if not translated:
            return "", status, error or "translation missing"
        translations.append(translated)
    return (
        "\n".join(translations),
        200 if statuses and all(item == 200 for item in statuses) else None,
        None,
    )

async def run(
    dataset: Path,
    state_path: Path,
    limit: int,
    delay: float,
    workers: int,
    translation_delay: float,
    retry_after_hours: float,
    start_id: int | None,
    end_id: int | None,
    shard_index: int | None,
    shard_count: int,
    profile: str = "core",
    max_frequency_rank: int | None = None,
    quality_repair_only: bool = False,
    repair_queue: Path | None = None,
    preflight_marker: Path | None = None,
    ready_marker: Path | None = None,
    release_id: str = "",
) -> None:
    if repair_queue is not None and not quality_repair_only:
        raise ValueError("--repair-queue requires --quality-repair-only")
    if repair_queue is not None and max_frequency_rank is not None:
        raise ValueError(
            "--repair-queue cannot be combined with --max-frequency-rank; "
            "replacement candidates may rank beyond 20,000"
        )
    if repair_queue is not None and shard_index is None:
        raise ValueError("--repair-queue requires --shard-index")
    if repair_queue is not None and (start_id is not None or end_id is not None):
        raise ValueError("--repair-queue uses fixed shard_owner, not ID bounds")
    if (preflight_marker is not None or ready_marker is not None) and (
        repair_queue is None or not release_id
    ):
        raise ValueError(
            "readiness markers require a repair queue and release id"
        )
    workers = max(1, workers)
    profile = "deep" if profile == "deep" else "core"
    state: sqlite3.Connection | None = None
    client: httpx.AsyncClient | None = None
    db: sqlite3.Connection | None = None
    repair_queue_db: sqlite3.Connection | None = None
    prevalidated_repair_rows: list[tuple[Any, ...]] | None = None
    queue_metadata: dict[str, Any] | None = None
    try:
        db = sqlite3.connect(dataset)
        ensure_dataset_columns(db)
        if repair_queue is not None:
            repair_queue_db = open_repair_queue(repair_queue)
            prevalidated_repair_rows, queue_metadata = preflight_repair_queue_shard(
                db,
                repair_queue_db,
                shard_index=int(shard_index),
                shard_count=shard_count,
            )
            state = init_state(state_path)
            bind_state_candidate(
                state,
                str(queue_metadata["candidate_digest"]),
                shard_owner=int(shard_index),
            )
        else:
            state = init_state(state_path)
        if shard_index is not None and repair_queue_db is None:
            if shard_count < 1 or not 0 <= shard_index < shard_count:
                raise ValueError("--shard-index must be within [0, --shard-count)")
            min_id, max_id = db.execute("SELECT COALESCE(MIN(id), 0), COALESCE(MAX(id), -1) FROM entries").fetchone()
            total = max(0, max_id - min_id + 1)
            start_id = min_id + (total * shard_index) // shard_count
            end_id = min_id + (total * (shard_index + 1)) // shard_count - 1

        # No HTTP client or batcher exists until the complete fixed shard has
        # passed provenance validation and its state file is bound to the
        # candidate contract digest.
        gates = {
            "edge": HostGate(delay, min(4, workers)),
            "dictionary": HostGate(delay, min(4, workers)),
            "datamuse": HostGate(delay, min(8, workers)),
            "translation": HostGate(
                max(delay, translation_delay), min(2, workers)
            ),
            "wiktionary": HostGate(max(2.0, min(delay, 4.0)), 1),
        }
        client_timeout = httpx.Timeout(20.0, connect=10.0)
        client = httpx.AsyncClient(timeout=client_timeout, follow_redirects=True)
        edge_batcher = (
            EdgeDictionaryBatcher(
                client,
                gates["edge"],
                batch_size=workers,
                profile=profile,
            )
            if EDGE_BASE
            else None
        )
        translation_batcher = (
            EdgeTranslationBatcher(
                client,
                gates["translation"],
                batch_size=workers,
            )
            if EDGE_BASE
            else None
        )
        if preflight_marker is not None or ready_marker is not None:
            assert queue_metadata is not None and shard_index is not None
            marker_base = {
                "candidateDigest": str(queue_metadata["candidate_digest"]),
                "releaseId": release_id,
                "selectedOwnerRows": int(queue_metadata["selected_owner_rows"]),
                "preflightRows": len(prevalidated_repair_rows or []),
                "shardCount": shard_count,
                "shardIndex": int(shard_index),
                "processId": os.getpid(),
            }
            if preflight_marker is not None:
                write_runtime_ready_marker(
                    preflight_marker,
                    {
                        "format": "lexora-top20k-preflight-v1",
                        **marker_base,
                        "completedAt": dt.datetime.now(
                            dt.timezone.utc
                        ).isoformat(),
                    },
                )
            if ready_marker is not None:
                write_runtime_ready_marker(
                    ready_marker,
                    {
                        "format": "lexora-top20k-runtime-ready-v1",
                        **marker_base,
                        "readyAt": dt.datetime.now(
                            dt.timezone.utc
                        ).isoformat(),
                    },
                )
        processed = 0
        async def process_row(row: tuple[Any, ...]) -> tuple[str, str]:
            entry_id, word, term, definition, definition_zh, us, uk, synonyms_json, antonyms_json, examples_json, phrases_json, phrase_entries_json, related_json, related_entries_json, freq, diff, enrich_json, pos = row
            marker = json.loads(enrich_json or "{}")
            existing = {
                "definition": definition or "",
                "pos": pos or "",
                "us": "" if needs_phonetic_repair(us) else normalize_phonetic(us),
                "uk": "" if needs_phonetic_repair(uk) else normalize_phonetic(uk),
                "examples": json.loads(examples_json or "[]"),
                "phrases": json.loads(phrases_json or "[]"),
                "phrase_entries": json.loads(phrase_entries_json or "[]"),
                "synonyms": json.loads(synonyms_json or "[]"),
                "antonyms": json.loads(antonyms_json or "[]"),
                "related": json.loads(related_json or "[]"),
                "related_entries": json.loads(related_entries_json or "[]"),
                "frequency": freq,
            }
            data = await enrich_term(
                client,
                gates,
                term,
                state,
                existing,
                edge_batcher=edge_batcher,
                profile=profile,
                quality_repair_only=quality_repair_only,
            )
            if not str(pos or "").strip():
                pos = data.get("pos", "")
            if not str(definition or "").strip():
                definition = data.get("definition", "")
            if needs_phonetic_repair(us):
                us = normalize_phonetic(
                    data.get("us", "") or data.get("us_phonetic", "")
                ) or normalize_phonetic(us)
            else:
                us = normalize_phonetic(us)
            if needs_phonetic_repair(uk):
                uk = normalize_phonetic(
                    data.get("uk", "") or data.get("uk_phonetic", "")
                ) or normalize_phonetic(uk)
            else:
                uk = normalize_phonetic(uk)
            synonyms = unique([*json.loads(synonyms_json or "[]"), *data.get("synonyms", [])])
            antonyms = unique([*json.loads(antonyms_json or "[]"), *data.get("antonyms", [])])
            examples = unique([*json.loads(examples_json or "[]"), *data.get("examples", [])])
            phrases = unique([*json.loads(phrases_json or "[]"), *data.get("phrases", [])])
            phrase_entries = merge_named_entries(
                json.loads(phrase_entries_json or "[]"),
                data.get("phrase_entries", []),
            )
            related = unique([*json.loads(related_json or "[]"), *data.get("related", [])])
            related_entries = merge_named_entries(
                json.loads(related_entries_json or "[]"),
                data.get("related_entries", []),
            )
            zh = definition_zh
            translated_zh = ""
            if needs_definition_translation(definition, zh):
                translated_zh, status, error = await translate(
                    client,
                    gates["translation"],
                    definition,
                    translation_batcher=translation_batcher,
                )
                if translated_zh:
                    zh = translated_zh
                state.execute("""INSERT INTO provider_state(term,source,status,attempts,http_status,last_error,updated_at) VALUES(?,?,?,?,?,?,?)
                  ON CONFLICT(term,source) DO UPDATE SET status=excluded.status,attempts=provider_state.attempts+1,http_status=excluded.http_status,last_error=excluded.last_error,updated_at=excluded.updated_at""",
                  (term, "translation", "completed" if translated_zh else "failed", 1, status, error, now()))
                data.setdefault("_attempted", []).append("translation")
                data.setdefault("_statuses", []).append("completed" if translated_zh else "failed")
                data.setdefault("_provider_results", {})["translation"] = {
                    "status": "completed" if translated_zh else "failed",
                    "httpStatus": status,
                    "error": error or "",
                }
            base_frequency = 0.0 if needs_frequency_repair(freq) else float(freq or 0)
            score = max(
                base_frequency,
                float(data.get("frequency", 0) or 0),
            )
            statuses = data.pop("_statuses", [])
            attempted = data.pop("_attempted", [])
            field_sources = data.pop("_field_sources", {})
            provider_results = data.pop("_provider_results", {})
            marker_status = (
                "completed"
                if not attempted
                or (
                    len(statuses) >= len(attempted)
                    and all(item == "completed" for item in statuses)
                )
                else (
                    "partial"
                    if any(
                        item in ("completed", "partial")
                        for item in statuses
                    )
                    else "not_found"
                )
            )
            quality_gaps = entry_quality_gaps(
                term,
                definition,
                zh,
                us,
                uk,
                pos,
            )
            if quality_repair_only:
                marker_status = "completed"
                if quality_gaps:
                    marker_status = (
                        "not_found"
                        if "definition" in quality_gaps
                        and statuses
                        and all(
                            item in ("not_found", "failed")
                            for item in statuses
                        )
                        else "partial"
                    )
            previous_field_sources = marker.get("fieldSources", {})
            if not isinstance(previous_field_sources, dict):
                previous_field_sources = {}
            marker = {
                "status": marker_status,
                "profile": profile,
                "lastAttempt": now(),
                "sources": sorted(set(["open-data", "network"])),
                "attemptedSources": attempted,
                "providerResults": provider_results,
                "fieldSources": {
                    **previous_field_sources,
                    **field_sources,
                    **({"definition_zh": "translation"} if translated_zh else {}),
                },
                "qualityGaps": quality_gaps,
            }
            payload = {
                "pos": pos,
                "definition": definition,
                "definition_zh": zh,
                "us_phonetic": us,
                "uk_phonetic": uk,
                "synonyms_json": j(synonyms),
                "antonyms_json": j(antonyms),
                "examples_json": j(examples),
                "phrases_json": j(phrases),
                "phrase_entries_json": j(phrase_entries),
                "related_words_json": j(related),
                "related_entries_json": j(related_entries),
                "frequency": score,
                "difficulty": resolved_difficulty(diff, freq, score),
            }
            if queue_metadata is not None:
                marker["repairCandidateDigest"] = queue_metadata[
                    "candidate_digest"
                ]
                marker["repairShardOwner"] = int(shard_index)
                marker["repairPayloadDigest"] = repair_payload_digest(payload)
            db.execute("""UPDATE entries SET pos=?,definition=?,definition_zh=?,us_phonetic=?,uk_phonetic=?,synonyms_json=?,antonyms_json=?,examples_json=?,phrases_json=?,phrase_entries_json=?,related_words_json=?,related_entries_json=?,frequency=?,difficulty=?,enrichment_json=? WHERE id=?""",
              (*(payload[column] for column in REPAIR_PAYLOAD_COLUMNS), j(marker), entry_id))
            db.execute("""INSERT OR REPLACE INTO entries_fts(rowid,word,definition,definition_zh,examples,phrases)
              SELECT id,word,definition,definition_zh,examples_json,phrases_json FROM entries WHERE id=?""", (entry_id,))
            db.commit(); state.commit()
            return term, marker_status
        pending: list[tuple[Any, ...]] = []

        async def flush_pending() -> None:
            nonlocal processed
            results = await asyncio.gather(
                *(process_row(item) for item in pending),
                return_exceptions=True,
            )
            fatal_error: EdgeBatchRetryExhausted | None = None
            for result in results:
                if isinstance(result, EdgeBatchRetryExhausted):
                    if result.status == 429:
                        fatal_error = fatal_error or result
                    else:
                        print(
                            f"enrichment-transient-error={result!r}",
                            flush=True,
                        )
                elif isinstance(result, Exception):
                    print(f"enrichment-error={result!r}", flush=True)
                else:
                    processed += 1
                    if processed % 25 == 0:
                        print(
                            f"enriched={processed} term={result[0]} "
                            f"status={result[1]}",
                            flush=True,
                        )
            pending.clear()
            if fatal_error is not None:
                # Abort this pass before advancing to another page.  The
                # affected rows received neither provider-state writes nor an
                # enrichment marker, so the next systemd-timer run resumes
                # them through the normal beginning-of-shard scan.
                raise fatal_error

        # Keep pages large enough to skip completed terms efficiently but
        # small enough for 1 GB collection instances.  Crucially, each page's
        # read cursor is closed by fetch_candidate_batch() before process_row()
        # can execute or commit any write.
        candidate_batch_size = max(64, min(512, workers * 16))
        after_frequency_rank: int | None = None
        after_id: int | None = None
        repair_offset = 0
        reached_limit = False
        while not reached_limit:
            if prevalidated_repair_rows is not None:
                rows = prevalidated_repair_rows[
                    repair_offset : repair_offset + candidate_batch_size
                ]
                repair_offset += len(rows)
                next_id = int(rows[-1][0]) if rows else after_id
                next_frequency_rank = None
            else:
                rows, next_frequency_rank, next_id = fetch_candidate_batch(
                    db,
                    start_id=start_id,
                    end_id=end_id,
                    frequency_first=shard_index is not None,
                    after_frequency_rank=after_frequency_rank,
                    after_id=after_id,
                    batch_size=candidate_batch_size,
                    max_frequency_rank=max_frequency_rank,
                )
            if not rows:
                break
            # Advance the keyset only after a complete, closed read page.  If
            # the process stops during writes, the next invocation starts from
            # the shard beginning and cheaply skips completed markers, exactly
            # preserving the existing resumability semantics.
            after_frequency_rank = next_frequency_rank
            after_id = next_id
            for row in rows:
                if limit and processed + len(pending) >= limit:
                    reached_limit = True
                    break
                needs_quality_repair = needs_entry_quality_repair(
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[14],
                )
                required_quality_gaps = entry_quality_gaps(
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[17],
                )
                has_deep_gaps = (
                    needs_deep_enrichment(
                        json.loads(row[7] or "[]"),
                        json.loads(row[8] or "[]"),
                        json.loads(row[10] or "[]"),
                        json.loads(row[12] or "[]"),
                    )
                    if profile == "deep"
                    else False
                )
                if quality_repair_only and not required_quality_gaps:
                    continue
                if not quality_repair_only and (
                    not should_process_marker(row[16], retry_after_hours)
                    and not (
                        needs_quality_repair
                        and marker_retry_due(row[16], retry_after_hours)
                    )
                    and not needs_deep_profile_pass(
                        row[16],
                        profile,
                        has_deep_gaps,
                    )
                ):
                    continue
                pending.append(row)
                if len(pending) >= workers:
                    await flush_pending()
        if pending:
            await flush_pending()
    finally:
        if repair_queue_db is not None:
            repair_queue_db.close()
        if db is not None:
            db.close()
        if client is not None:
            await client.aclose()
        if state is not None:
            state.close()

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", type=Path, default=BUILD / "lexora-open-oxford-scope.sqlite")
    ap.add_argument("--state", type=Path, default=STATE, help="provider state SQLite path")
    ap.add_argument("--limit", type=int, default=0, help="0 means all pending terms")
    ap.add_argument("--delay", type=float, default=0.5, help="minimum seconds between requests per provider host")
    ap.add_argument("--workers", type=int, default=16, help="number of terms processed concurrently")
    ap.add_argument("--translation-delay", type=float, default=1.0, help="minimum seconds between translation requests")
    ap.add_argument("--retry-after-hours", type=float, default=24.0, help="retry partial/not-found terms only after this many hours")
    ap.add_argument(
        "--profile",
        choices=("core", "deep", "auto"),
        default="core",
        help=(
            "core first pass, deep relationship-enrichment pass, or auto to "
            "run core followed by deep"
        ),
    )
    ap.add_argument("--start-id", type=int, default=None, help="inclusive entry ID start")
    ap.add_argument("--end-id", type=int, default=None, help="inclusive entry ID end")
    ap.add_argument("--shard-index", type=int, default=None, help="zero-based shard index")
    ap.add_argument("--shard-count", type=int, default=1, help="number of contiguous shards")
    ap.add_argument(
        "--max-frequency-rank",
        type=int,
        default=None,
        help="only inspect entries up to this frequency rank",
    )
    ap.add_argument(
        "--quality-repair-only",
        action="store_true",
        help=(
            "only repair required definition, Chinese translation, POS and "
            "single-word phonetic gaps"
        ),
    )
    ap.add_argument(
        "--repair-queue",
        type=Path,
        help=(
            "candidate SQLite produced by fast20k_pipeline.py; process only "
            "its exact repair_queue IDs (including replacements ranked above 20,000)"
        ),
    )
    ap.add_argument(
        "--preflight-marker",
        type=Path,
        help=(
            "atomic fixed-owner preflight marker written by the worker after "
            "its single complete local validation"
        ),
    )
    ap.add_argument(
        "--ready-marker",
        type=Path,
        help="atomic runtime-readiness marker for a fixed repair release",
    )
    ap.add_argument(
        "--release-id",
        default=os.environ.get("LEXORA_RELEASE_ID", ""),
        help="immutable deployment release id written into readiness evidence",
    )
    args = ap.parse_args()
    if args.shard_index is not None and (args.start_id is not None or args.end_id is not None):
        ap.error("use either --shard-index/--shard-count or --start-id/--end-id, not both")
    if args.repair_queue is not None and not args.quality_repair_only:
        ap.error("--repair-queue requires --quality-repair-only")
    if args.repair_queue is not None and args.max_frequency_rank is not None:
        ap.error("--repair-queue cannot be combined with --max-frequency-rank")
    profiles = ("core", "deep") if args.profile == "auto" else (args.profile,)
    for profile in profiles:
        asyncio.run(
            run(
                args.dataset,
                args.state,
                args.limit,
                args.delay,
                args.workers,
                args.translation_delay,
                args.retry_after_hours,
                args.start_id,
                args.end_id,
                args.shard_index,
                args.shard_count,
                profile=profile,
                max_frequency_rank=args.max_frequency_rank,
                quality_repair_only=args.quality_repair_only,
                repair_queue=args.repair_queue,
                preflight_marker=args.preflight_marker,
                ready_marker=args.ready_marker,
                release_id=args.release_id,
            )
        )

if __name__ == "__main__":
    main()
