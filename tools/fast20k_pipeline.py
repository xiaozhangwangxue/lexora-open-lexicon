#!/usr/bin/env python3
"""Build and validate a safe, auditable fast-20k Lexora candidate.

The canonical database is always opened read-only.  Selection is assembled in
a sibling temporary SQLite file, uses disk-backed staging and bounded batches,
and is published atomically only after structural validation.

Multi-token ``wordfreq`` scores are not comparable with single-word scores.
The default policy therefore reserves a bounded phrase share, accepts phrases
only with dictionary evidence, ranks words and phrases independently, and
interleaves the two streams deterministically.  Canonical rank is retained as
an auditable within-stream tie-breaker, never as proof that a phrase is more
frequent than a word.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import tempfile
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from build_oxford_scope import SCHEMA, rebuild_fts
from enrich_oxford_scope import entry_quality_gaps, is_phrase
from fast20k_contract import (
    DEFAULT_REPAIR_SHARDS,
    ENTRY_COLUMNS,
    JSON_COLUMNS,
    POLICY_NAME,
    SELECTION_VERSION,
    candidate_contract_digest,
    canonical_content_digest,
    canonical_identity_digest,
    queue_row_digest,
    selection_row_digest,
)

DEFAULT_LIMIT = 20_000
DEFAULT_PHRASE_TARGET = 4_000
MIN_FAST20K_WORDS = 15_000
DEFAULT_BATCH_SIZE = 512
EXAMPLE_LIMIT = 20

WORD_PART = r"(?:[a-z]+(?:'[a-z]+)?|[a-z]+s')"
PLAIN_TOKEN = re.compile(rf"{WORD_PART}(?:-{WORD_PART})*")
INITIALISM = re.compile(r"(?:[a-z]\.){2,}[a-z]?\.?")
SHORT_ABBREVIATION = re.compile(r"[a-z]{1,5}\.")
ARPABET = re.compile(r"(?:[A-Z]+[0-2]?)(?:\s+[A-Z]+[0-2]?)+")

AFFIX_POS = {
    "prefix",
    "suffix",
    "infix",
    "circumfix",
    "combining form",
    "combining_form",
}
ABBREVIATION_POS = {"abbr", "abbreviation", "acronym", "initialism"}
PHRASE_POS = {
    "phrase",
    "prep_phrase",
    "prepositional phrase",
    "proverb",
    "idiom",
}

REPAIRABLE_ISSUES = {
    "definition",
    "definition_zh",
    "pos",
    "phonetic",
    "phonetic_unreliable",
}

OPTIONAL_CANONICAL_DEFAULTS = {
    "phrase_entries_json": "[]",
    "related_entries_json": "[]",
}

ISSUE_MESSAGES = {
    "candidate_database_error": "候选数据库无法读取或已损坏",
    "canonical_database_error": "canonical 数据库无法读取或已损坏",
    "candidate_integrity": "候选 SQLite 完整性检查未通过",
    "canonical_integrity": "canonical SQLite 完整性检查未通过",
    "missing_table": "候选数据库缺少协议要求的数据表",
    "metadata": "候选选择元数据缺失或版本不匹配",
    "selection_counts": "候选中的单词和短语数量与选择元数据不一致",
    "minimum_word_count": "20,000 极速词库中的常用单词少于 15,000 个",
    "ranking_policy": "候选词条的排名证据与选择策略不一致",
    "stream_order": "单词或短语流没有保持 canonical 排名顺序",
    "row_count": "候选词条数量不是要求的数量",
    "provenance_count": "候选来源记录数量与词条数量不一致",
    "rank_sequence": "候选排名不是从 1 开始的连续序列",
    "fts_count": "全文索引与候选词条数量不一致",
    "fts_content_mismatch": "全文索引内容与候选词条不一致",
    "definition": "缺少英文释义",
    "definition_zh": "缺少或疑似截断的完整中文释义",
    "pos": "普通单词缺少词性",
    "phonetic": "普通单词缺少可靠音标",
    "phonetic_unreliable": "普通单词只有非 IPA 或损坏音标",
    "malformed_json": "候选词条包含无法解析的 JSON 字段",
    "malformed_source_json": "候选词条的来源记录不是有效 JSON 数组",
    "missing_source_provenance": "候选词条没有来源记录",
    "normalized_word_mismatch": "word 与 normalized_word 不一致",
    "lexical_policy": "候选词条不符合安全词形规则",
    "phrase_evidence": "短语缺少可靠词典来源证据",
    "term_key_mismatch": "保守去重键与来源记录不一致",
    "canonical_missing": "候选词条在 canonical 数据库中不存在",
    "canonical_identity_mismatch": "候选词条与 canonical 身份信息不一致",
    "canonical_content_mismatch": "候选内容与当前 canonical 数据不一致",
    "repair_queue_count": "修复队列与实际缺失词条不一致",
    "repair_queue_mismatch": "修复队列中的 ID、排名或缺失字段不一致",
    "selection_digest_mismatch": "固定选集摘要与候选来源记录不一致",
    "repair_queue_digest_mismatch": "修复队列摘要与实际队列不一致",
    "candidate_digest_mismatch": "候选契约摘要与选集或修复队列不一致",
    "shard_owner_mismatch": "修复队列的固定分片归属不正确",
    "comparison_error": "候选与 canonical 对照检查无法完成",
}

EXTRA_SCHEMA = """
CREATE TABLE fast20k_metadata(
  id INTEGER PRIMARY KEY CHECK(id=1),
  selection_version INTEGER NOT NULL,
  policy_name TEXT NOT NULL,
  expected_rows INTEGER NOT NULL,
  phrase_target INTEGER NOT NULL,
  phrase_count INTEGER NOT NULL,
  word_count INTEGER NOT NULL,
  canonical_path TEXT NOT NULL,
  canonical_bytes INTEGER NOT NULL,
  canonical_mtime_ns INTEGER NOT NULL,
  scanned_rows INTEGER NOT NULL,
  scan_last_frequency_rank INTEGER NOT NULL,
  rejected_json TEXT NOT NULL,
  shard_count INTEGER NOT NULL,
  selection_digest TEXT NOT NULL,
  baseline_content_digest TEXT NOT NULL,
  repair_queue_digest TEXT NOT NULL,
  candidate_digest TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE candidate_pool(
  canonical_id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL CHECK(kind IN ('word','phrase')),
  term_key TEXT NOT NULL UNIQUE,
  canonical_frequency_rank INTEGER NOT NULL,
  ranking_evidence TEXT NOT NULL,
  phrase_evidence TEXT NOT NULL
);
CREATE INDEX idx_candidate_pool_kind_rank
  ON candidate_pool(kind,canonical_frequency_rank,canonical_id);
CREATE TABLE fast20k_provenance(
  canonical_id INTEGER PRIMARY KEY,
  selected_rank INTEGER NOT NULL UNIQUE,
  canonical_frequency_rank INTEGER NOT NULL,
  normalized_word TEXT NOT NULL,
  term_key TEXT NOT NULL UNIQUE,
  kind TEXT NOT NULL CHECK(kind IN ('word','phrase')),
  ranking_evidence TEXT NOT NULL,
  phrase_evidence TEXT NOT NULL,
  canonical_content_sha256 TEXT NOT NULL,
  canonical_identity_sha256 TEXT NOT NULL
);
CREATE TABLE repair_queue(
  canonical_id INTEGER PRIMARY KEY,
  selected_rank INTEGER NOT NULL UNIQUE,
  canonical_frequency_rank INTEGER NOT NULL,
  normalized_word TEXT NOT NULL,
  canonical_identity_sha256 TEXT NOT NULL,
  gaps_json TEXT NOT NULL,
  shard_owner INTEGER NOT NULL CHECK(shard_owner>=0)
);
CREATE INDEX idx_repair_queue_owner_id
  ON repair_queue(shard_owner,canonical_id);
"""


def _stream_digest(rows: Any, row_hasher: Any) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(row_hasher(tuple(row)).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def selection_digest(database: sqlite3.Connection) -> str:
    return _stream_digest(
        database.execute(
            "SELECT canonical_id,selected_rank,canonical_frequency_rank,"
            "normalized_word,term_key,kind,ranking_evidence,phrase_evidence,"
            "canonical_identity_sha256 FROM fast20k_provenance "
            "ORDER BY selected_rank"
        ),
        selection_row_digest,
    )


def baseline_content_digest(database: sqlite3.Connection) -> str:
    return _stream_digest(
        database.execute(
            "SELECT canonical_id,canonical_content_sha256 "
            "FROM fast20k_provenance ORDER BY selected_rank"
        ),
        selection_row_digest,
    )


def repair_queue_digest(database: sqlite3.Connection) -> str:
    return _stream_digest(
        database.execute(
            "SELECT canonical_id,selected_rank,canonical_frequency_rank,"
            "normalized_word,canonical_identity_sha256,gaps_json,shard_owner "
            "FROM repair_queue ORDER BY canonical_id"
        ),
        queue_row_digest,
    )


def _pos_names(value: Any) -> set[str]:
    return {
        item.strip().lower().replace("_", " ")
        for item in re.split(r"[,;/]", str(value or ""))
        if item.strip()
    }


def term_key(value: Any) -> str:
    """Normalize case/spacing/apostrophe only; preserve dots and hyphens."""
    text = unicodedata.normalize("NFKC", str(value or ""))
    return " ".join(text.strip().lower().replace("’", "'").split())


def _json_list(value: Any) -> list[str] | None:
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, list):
        return None
    return [str(item).strip().lower() for item in parsed if str(item).strip()]


def _json_dict(value: Any) -> dict[str, Any] | None:
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _explicit_phrase_pos(pos: Any) -> bool:
    names = _pos_names(pos)
    return bool(names & {item.replace("_", " ") for item in PHRASE_POS}) or any(
        item.endswith(" phrase") for item in names
    )


def _source_marker(value: Any) -> bool:
    return value is True or str(value or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def phrase_evidence(
    term: Any,
    pos: Any,
    sources: list[str],
    scope: dict[str, Any],
) -> str | None:
    if not is_phrase(term, pos):
        return ""
    evidence: list[str] = []
    if _explicit_phrase_pos(pos):
        evidence.append("explicit-pos")
    if "kaikki" in sources or _source_marker(scope.get("kaikki")):
        evidence.append("kaikki-entry")
    if str(scope.get("ecdictOxfordFlag") or "") == "1":
        evidence.append("ecdict-oxford")
    return "+".join(evidence) or None


def lexical_rejection_reason(
    term: Any,
    pos: Any,
    sources: list[str],
    scope: dict[str, Any],
) -> str | None:
    value = term_key(term)
    if not value or len(value) > 120:
        return "invalid_length"
    names = _pos_names(pos)
    if names & {item.replace("_", " ") for item in AFFIX_POS}:
        return "affix"
    if "…" in value or ".." in value:
        return "ellipsis"
    if value.startswith("-") or value.endswith("-") or "--" in value:
        return "invalid_hyphen"
    tokens = value.split()
    if len(tokens) > 8:
        return "too_many_tokens"
    dot_evidence = bool(
        names & ABBREVIATION_POS
        or "kaikki" in sources
        or _source_marker(scope.get("kaikki"))
        or str(scope.get("ecdictOxfordFlag") or "") == "1"
    )
    for token in tokens:
        if PLAIN_TOKEN.fullmatch(token):
            continue
        if (
            "." in token
            and dot_evidence
            and (INITIALISM.fullmatch(token) or SHORT_ABBREVIATION.fullmatch(token))
        ):
            continue
        return "unsupported_punctuation"
    if len(tokens) == 1 and len(tokens[0]) == 1 and tokens[0] not in {"a", "i"}:
        return "single_letter_noise"
    return None


def _phonetic_unreliable(value: Any) -> bool:
    text = str(value or "").strip().strip("/")
    if not text:
        return True
    return (
        "\ufffd" in text or "ә" in text or ":" in text or bool(ARPABET.fullmatch(text))
    )


def strict_required_gaps(row: dict[str, Any]) -> list[str]:
    gaps = list(
        entry_quality_gaps(
            row.get("normalized_word"),
            row.get("definition"),
            row.get("definition_zh"),
            row.get("us_phonetic"),
            row.get("uk_phonetic"),
            row.get("pos"),
        )
    )
    if (
        not is_phrase(row.get("normalized_word"), row.get("pos"))
        and "phonetic" not in gaps
        and _phonetic_unreliable(row.get("us_phonetic"))
        and _phonetic_unreliable(row.get("uk_phonetic"))
    ):
        gaps.append("phonetic_unreliable")
    return list(dict.fromkeys(gaps))


def _source_query(database: sqlite3.Connection) -> str:
    indexes = {str(row[1]) for row in database.execute("PRAGMA index_list(entries)")}
    table = (
        "entries INDEXED BY idx_entries_freq"
        if "idx_entries_freq" in indexes
        else "entries"
    )
    return table


def _entry_projection(available: set[str]) -> str:
    expressions: list[str] = []
    for column in ENTRY_COLUMNS:
        if column in available:
            expressions.append(column)
        elif column in OPTIONAL_CANONICAL_DEFAULTS:
            value = OPTIONAL_CANONICAL_DEFAULTS[column].replace("'", "''")
            expressions.append(f"'{value}' AS {column}")
        else:
            raise ValueError(f"canonical schema missing column: {column}")
    return ",".join(expressions)


def _fetch_source_page(
    database: sqlite3.Connection,
    *,
    after_rank: int | None,
    after_id: int | None,
    batch_size: int,
) -> list[sqlite3.Row]:
    table = _source_query(database)
    where = "frequency_rank IS NOT NULL"
    params: list[int] = []
    if after_rank is not None and after_id is not None:
        # A row-value keyset comparison lets SQLite seek directly into the
        # composite index.  The logically equivalent OR form repeatedly
        # rescans the prefix when replacement candidates are far beyond 20k.
        where += " AND (frequency_rank,id)>(?,?)"
        params.extend((after_rank, after_id))
    params.append(batch_size)
    return database.execute(
        "SELECT id,word,normalized_word,pos,frequency,frequency_rank,"
        f"source_json,scope_json FROM {table} WHERE {where} "
        "ORDER BY frequency_rank,id LIMIT ?",
        params,
    ).fetchall()


def _atomic_temp_path(destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    os.close(descriptor)
    return Path(raw)


def _cleanup_sqlite(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass


def _fsync_file_and_parent(path: Path) -> None:
    with path.open("rb") as stream:
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _publish_candidate(partial: Path, destination: Path, *, replace: bool) -> None:
    """Publish atomically without a check-then-overwrite race."""
    if replace:
        os.replace(partial, destination)
    else:
        # Both paths are siblings, so a hard link is atomic and fails with
        # EEXIST instead of overwriting a file created after our initial check.
        os.link(partial, destination)
        partial.unlink()


def _choose_counts(
    *,
    limit: int,
    phrase_target: int,
    words: int,
    phrases: int,
) -> tuple[int, int]:
    phrase_count = min(phrase_target, phrases, limit)
    word_count = min(words, limit - phrase_count)
    if word_count + phrase_count < limit:
        phrase_count += min(phrases - phrase_count, limit - word_count - phrase_count)
    if word_count + phrase_count < limit:
        word_count += min(words - word_count, limit - word_count - phrase_count)
    if word_count + phrase_count != limit:
        raise ValueError(
            "insufficient eligible candidates: "
            f"required={limit} words={words} reliable_phrases={phrases}"
        )
    if limit == DEFAULT_LIMIT and word_count < MIN_FAST20K_WORDS:
        raise ValueError(
            "fast-20k requires at least "
            f"{MIN_FAST20K_WORDS} words: words={word_count} phrases={phrase_count}"
        )
    return word_count, phrase_count


def _insert_selection(
    database: sqlite3.Connection,
    *,
    limit: int,
    word_count: int,
    phrase_count: int,
) -> None:
    word_cursor = database.execute(
        "SELECT canonical_id,term_key,canonical_frequency_rank,"
        "ranking_evidence,phrase_evidence FROM candidate_pool "
        "WHERE kind='word' ORDER BY canonical_frequency_rank,canonical_id LIMIT ?",
        (word_count,),
    )
    phrase_cursor = database.execute(
        "SELECT canonical_id,term_key,canonical_frequency_rank,"
        "ranking_evidence,phrase_evidence FROM candidate_pool "
        "WHERE kind='phrase' ORDER BY canonical_frequency_rank,canonical_id LIMIT ?",
        (phrase_count,),
    )
    next_word = iter(word_cursor)
    next_phrase = iter(phrase_cursor)
    words_used = 0
    phrases_used = 0
    for selected_rank in range(1, limit + 1):
        phrase_due = (selected_rank * phrase_count) // limit > phrases_used
        kind = "phrase" if phrase_due and phrases_used < phrase_count else "word"
        if kind == "word" and words_used >= word_count:
            kind = "phrase"
        item = next(next_phrase if kind == "phrase" else next_word)
        canonical_id, key, canonical_rank, ranking, evidence = item
        database.execute(
            "INSERT INTO fast20k_provenance("
            "canonical_id,selected_rank,canonical_frequency_rank,normalized_word,"
            "term_key,kind,ranking_evidence,phrase_evidence,"
            "canonical_content_sha256,canonical_identity_sha256"
            ") VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                canonical_id,
                selected_rank,
                canonical_rank,
                "",
                key,
                kind,
                ranking,
                evidence,
                "",
                "",
            ),
        )
        if kind == "phrase":
            phrases_used += 1
        else:
            words_used += 1
    word_cursor.close()
    phrase_cursor.close()


def _copy_selected_rows(
    source: sqlite3.Connection,
    candidate: sqlite3.Connection,
    *,
    batch_size: int,
    preserve_identity: bool = False,
) -> None:
    available = {str(row[1]) for row in source.execute("PRAGMA table_info(entries)")}
    projection_sql = _entry_projection(available)
    insert_columns = "id," + ",".join(ENTRY_COLUMNS)
    placeholders = ",".join("?" for _ in range(len(ENTRY_COLUMNS) + 1))
    cursor = candidate.execute(
        "SELECT canonical_id,selected_rank,canonical_frequency_rank,"
        "canonical_identity_sha256,normalized_word,term_key "
        "FROM fast20k_provenance ORDER BY selected_rank"
    )
    while True:
        provenance_rows = cursor.fetchmany(batch_size)
        if not provenance_rows:
            break
        ids = [int(row[0]) for row in provenance_rows]
        marks = ",".join("?" for _ in ids)
        source_rows = source.execute(
            f"SELECT id,{projection_sql} FROM entries WHERE id IN ({marks})",
            ids,
        ).fetchall()
        by_id = {int(row["id"]): dict(row) for row in source_rows}
        for (
            canonical_id,
            selected_rank,
            original_rank,
            previous_identity,
            previous_word,
            previous_key,
        ) in provenance_rows:
            row = by_id.get(int(canonical_id))
            if row is None:
                raise ValueError(
                    f"canonical row disappeared during selection: id={canonical_id}"
                )
            if int(row["frequency_rank"]) != int(original_rank):
                raise ValueError(
                    "canonical rank changed during selection: "
                    f"id={canonical_id} expected={original_rank} "
                    f"actual={row['frequency_rank']}"
                )
            content_sha = canonical_content_digest(row)
            identity_sha = canonical_identity_digest(row)
            if preserve_identity and (
                identity_sha != str(previous_identity)
                or term_key(row["normalized_word"]) != term_key(previous_word)
                or term_key(row["normalized_word"]) != str(previous_key)
            ):
                raise ValueError(
                    "fixed selection identity changed during refresh: "
                    f"id={canonical_id}"
                )
            values = [row[column] for column in ENTRY_COLUMNS]
            values[ENTRY_COLUMNS.index("frequency_rank")] = int(selected_rank)
            candidate.execute(
                f"INSERT INTO entries({insert_columns}) VALUES({placeholders})",
                (canonical_id, *values),
            )
            candidate.execute(
                "UPDATE fast20k_provenance SET normalized_word=?,"
                "canonical_content_sha256=?,canonical_identity_sha256=? "
                "WHERE canonical_id=?",
                (
                    row["normalized_word"],
                    content_sha,
                    identity_sha,
                    canonical_id,
                ),
            )
    cursor.close()


def _populate_repair_queue(
    database: sqlite3.Connection,
    *,
    shard_count: int,
) -> int:
    if shard_count < 1:
        raise ValueError("repair shard count must be positive")
    columns = "id," + ",".join(ENTRY_COLUMNS)
    count = 0
    cursor = database.execute(
        f"SELECT {columns} FROM entries ORDER BY frequency_rank,id"
    )
    for raw in cursor:
        row = dict(raw)
        gaps = strict_required_gaps(row)
        if not gaps:
            continue
        provenance = database.execute(
            "SELECT selected_rank,canonical_frequency_rank,"
            "canonical_identity_sha256 FROM fast20k_provenance "
            "WHERE canonical_id=?",
            (row["id"],),
        ).fetchone()
        database.execute(
            "INSERT INTO repair_queue("
            "canonical_id,selected_rank,canonical_frequency_rank,normalized_word,"
            "canonical_identity_sha256,gaps_json,shard_owner"
            ") VALUES(?,?,?,?,?,?,?)",
            (
                row["id"],
                provenance[0],
                provenance[1],
                row["normalized_word"],
                provenance[2],
                json.dumps(gaps, ensure_ascii=False, separators=(",", ":")),
                int(row["id"]) % shard_count,
            ),
        )
        count += 1
    cursor.close()
    return count


def build_candidate(
    canonical: Path,
    destination: Path,
    *,
    limit: int = DEFAULT_LIMIT,
    phrase_target: int = DEFAULT_PHRASE_TARGET,
    replace: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    shard_count: int = DEFAULT_REPAIR_SHARDS,
) -> dict[str, Any]:
    if limit < 1:
        raise ValueError("limit must be positive")
    if not 0 <= phrase_target <= limit:
        raise ValueError("phrase_target must be within 0..limit")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if shard_count < 1:
        raise ValueError("repair shard count must be positive")
    if destination.exists() and not replace:
        raise FileExistsError(f"refusing to overwrite {destination}")
    if canonical.resolve() == destination.resolve():
        raise ValueError("candidate output cannot replace the canonical database")

    partial = _atomic_temp_path(destination)
    source: sqlite3.Connection | None = None
    output: sqlite3.Connection | None = None
    published = False
    rejected: Counter[str] = Counter()
    scanned = 0
    last_rank = 0
    word_pool = 0
    phrase_pool = 0
    repair_rows = 0
    try:
        source = sqlite3.connect(f"file:{canonical.resolve()}?mode=ro", uri=True)
        source.row_factory = sqlite3.Row
        source.execute("PRAGMA query_only=ON")
        # Hold one canonical read snapshot from selection through row copying.
        # Concurrent enrichment may continue in WAL mode, but cannot make this
        # candidate a mixture of multiple canonical revisions.
        source.execute("BEGIN")
        available = {
            str(row[1]) for row in source.execute("PRAGMA table_info(entries)")
        }
        required_columns = {
            "id",
            *(
                column
                for column in ENTRY_COLUMNS
                if column not in OPTIONAL_CANONICAL_DEFAULTS
            ),
        }
        missing = sorted(required_columns - available)
        if missing:
            raise ValueError("canonical schema missing columns: " + ",".join(missing))

        output = sqlite3.connect(partial)
        output.row_factory = sqlite3.Row
        output.executescript(SCHEMA)
        output.executescript(EXTRA_SCHEMA)
        output.execute("PRAGMA temp_store=FILE")

        after_rank: int | None = None
        after_id: int | None = None
        required_words = limit - phrase_target
        while word_pool < required_words or phrase_pool < phrase_target:
            page = _fetch_source_page(
                source,
                after_rank=after_rank,
                after_id=after_id,
                batch_size=batch_size,
            )
            if not page:
                break
            for raw in page:
                scanned += 1
                row = dict(raw)
                after_rank = int(row["frequency_rank"])
                after_id = int(row["id"])
                last_rank = after_rank
                sources = _json_list(row["source_json"])
                scope = _json_dict(row["scope_json"])
                if sources is None or not sources:
                    rejected["missing_source_provenance"] += 1
                    continue
                if scope is None:
                    rejected["malformed_scope_json"] += 1
                    continue
                key = term_key(row["normalized_word"])
                if key != term_key(row["word"]):
                    rejected["normalized_word_mismatch"] += 1
                    continue
                reason = lexical_rejection_reason(
                    key,
                    row["pos"],
                    sources,
                    scope,
                )
                if reason:
                    rejected[reason] += 1
                    continue
                kind = "phrase" if is_phrase(key, row["pos"]) else "word"
                evidence = phrase_evidence(key, row["pos"], sources, scope)
                if kind == "phrase" and not evidence:
                    rejected["phrase_evidence_insufficient"] += 1
                    continue
                if kind == "word" and word_pool >= limit:
                    continue
                if kind == "phrase" and phrase_pool >= limit:
                    continue
                try:
                    output.execute(
                        "INSERT INTO candidate_pool VALUES(?,?,?,?,?,?)",
                        (
                            row["id"],
                            kind,
                            key,
                            row["frequency_rank"],
                            (
                                "bounded-dictionary-evidence"
                                if kind == "phrase"
                                else "wordfreq-single-token"
                            ),
                            evidence or "",
                        ),
                    )
                except sqlite3.IntegrityError:
                    rejected["duplicate_term_key"] += 1
                    continue
                if kind == "phrase":
                    phrase_pool += 1
                else:
                    word_pool += 1
            output.commit()

        word_count, phrase_count = _choose_counts(
            limit=limit,
            phrase_target=phrase_target,
            words=word_pool,
            phrases=phrase_pool,
        )
        _insert_selection(
            output,
            limit=limit,
            word_count=word_count,
            phrase_count=phrase_count,
        )
        _copy_selected_rows(source, output, batch_size=batch_size)
        output.execute("DROP TABLE candidate_pool")
        rebuild_fts(output)
        repair_rows = _populate_repair_queue(output, shard_count=shard_count)
        selected_digest = selection_digest(output)
        baseline_digest = baseline_content_digest(output)
        queue_digest = repair_queue_digest(output)
        contract_digest = candidate_contract_digest(
            selected_digest,
            baseline_digest,
            queue_digest,
            shard_count=shard_count,
        )
        canonical_stat = canonical.stat()
        output.execute(
            "INSERT INTO fast20k_metadata("
            "id,selection_version,policy_name,expected_rows,phrase_target,"
            "phrase_count,word_count,canonical_path,canonical_bytes,"
            "canonical_mtime_ns,scanned_rows,scan_last_frequency_rank,"
            "rejected_json,shard_count,selection_digest,repair_queue_digest,"
            "baseline_content_digest,candidate_digest,created_at"
            ") VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                SELECTION_VERSION,
                POLICY_NAME,
                limit,
                phrase_target,
                phrase_count,
                word_count,
                str(canonical.resolve()),
                canonical_stat.st_size,
                canonical_stat.st_mtime_ns,
                scanned,
                last_rank,
                json.dumps(dict(sorted(rejected.items())), separators=(",", ":")),
                shard_count,
                selected_digest,
                queue_digest,
                baseline_digest,
                contract_digest,
                dt.datetime.now(dt.timezone.utc).isoformat(),
            ),
        )
        output.commit()
        output.execute("PRAGMA journal_mode=DELETE")
        output.execute("PRAGMA optimize")
        output.commit()
        output.close()
        output = None
        source.rollback()
        source.close()
        source = None

        report = candidate_quality_report(partial, canonical, expected_rows=limit)
        if not report["structuralReady"]:
            raise ValueError(
                "candidate structural gate failed: "
                + json.dumps(report, ensure_ascii=False, sort_keys=True)
            )
        _fsync_file_and_parent(partial)
        _publish_candidate(partial, destination, replace=replace)
        _fsync_file_and_parent(destination)
        published = True
        report["candidate"] = str(destination)
        return {
            "candidate": str(destination),
            "selection": {
                "version": SELECTION_VERSION,
                "policyName": POLICY_NAME,
                "policy": (
                    "words use single-token wordfreq order; reliable phrases use a "
                    "bounded dictionary-evidence quota and are not globally comparable "
                    "to word scores; canonical rank is only an auditable within-stream "
                    "tie-breaker"
                ),
                "rows": limit,
                "words": word_count,
                "phrases": phrase_count,
                "phraseTarget": phrase_target,
                "repairShards": shard_count,
                "selectionDigest": selected_digest,
                "baselineContentDigest": baseline_digest,
                "repairQueueDigest": queue_digest,
                "candidateDigest": contract_digest,
                "scanned": scanned,
                "lastCanonicalFrequencyRank": last_rank,
                "backfilledBeyondOriginalLimit": output_count_beyond_rank(
                    destination, limit
                ),
                "rejected": dict(sorted(rejected.items())),
            },
            "repairQueueRows": repair_rows,
            "quality": report,
        }
    finally:
        if output is not None:
            output.close()
        if source is not None:
            source.close()
        if not published:
            _cleanup_sqlite(partial)


def refresh_candidate(
    canonical: Path,
    template: Path,
    destination: Path,
    *,
    replace: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, Any]:
    """Refresh fields and repair gaps while preserving the exact selected IDs.

    This is the only safe post-repair operation.  It never re-runs ranking or
    lexical selection, and it rejects any change to immutable canonical
    identity before publishing a sibling SQLite file atomically.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if destination.exists() and not replace:
        raise FileExistsError(f"refusing to overwrite {destination}")
    if destination.resolve() in {canonical.resolve(), template.resolve()}:
        raise ValueError("refresh output must differ from canonical and template")

    partial = _atomic_temp_path(destination)
    source: sqlite3.Connection | None = None
    template_db: sqlite3.Connection | None = None
    output: sqlite3.Connection | None = None
    published = False
    try:
        template_db = sqlite3.connect(f"file:{template.resolve()}?mode=ro", uri=True)
        template_db.row_factory = sqlite3.Row
        template_db.execute("PRAGMA query_only=ON")
        template_db.execute("BEGIN")
        if str(template_db.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
            raise ValueError("template candidate failed SQLite quick_check")
        metadata_row = template_db.execute(
            "SELECT * FROM fast20k_metadata WHERE id=1"
        ).fetchone()
        if metadata_row is None:
            raise ValueError("template candidate metadata is missing")
        metadata = dict(metadata_row)
        if (
            int(metadata["selection_version"]) != SELECTION_VERSION
            or str(metadata["policy_name"]) != POLICY_NAME
        ):
            raise ValueError("template candidate contract version mismatch")
        template_selection_digest = selection_digest(template_db)
        template_baseline_digest = baseline_content_digest(template_db)
        template_queue_digest = repair_queue_digest(template_db)
        if (
            template_selection_digest != str(metadata["selection_digest"])
            or template_baseline_digest != str(metadata["baseline_content_digest"])
            or template_queue_digest != str(metadata["repair_queue_digest"])
            or candidate_contract_digest(
                template_selection_digest,
                template_baseline_digest,
                template_queue_digest,
                shard_count=int(metadata["shard_count"]),
            )
            != str(metadata["candidate_digest"])
        ):
            raise ValueError("template candidate digest mismatch")

        source = sqlite3.connect(f"file:{canonical.resolve()}?mode=ro", uri=True)
        source.row_factory = sqlite3.Row
        source.execute("PRAGMA query_only=ON")
        source.execute("BEGIN")
        available = {
            str(row[1]) for row in source.execute("PRAGMA table_info(entries)")
        }
        missing = sorted(
            {
                "id",
                *(
                    column
                    for column in ENTRY_COLUMNS
                    if column not in OPTIONAL_CANONICAL_DEFAULTS
                ),
            }
            - available
        )
        if missing:
            raise ValueError("canonical schema missing columns: " + ",".join(missing))

        output = sqlite3.connect(partial)
        output.row_factory = sqlite3.Row
        output.executescript(SCHEMA)
        output.executescript(EXTRA_SCHEMA)
        output.execute("PRAGMA temp_store=FILE")
        provenance_columns = (
            "canonical_id,selected_rank,canonical_frequency_rank,normalized_word,"
            "term_key,kind,ranking_evidence,phrase_evidence,"
            "canonical_content_sha256,canonical_identity_sha256"
        )
        cursor = template_db.execute(
            f"SELECT {provenance_columns} FROM fast20k_provenance "
            "ORDER BY selected_rank"
        )
        copied = 0
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            output.executemany(
                f"INSERT INTO fast20k_provenance({provenance_columns}) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (tuple(row) for row in rows),
            )
            copied += len(rows)
        cursor.close()
        if copied != int(metadata["expected_rows"]):
            raise ValueError(
                f"template selection rows changed: expected={metadata['expected_rows']} "
                f"actual={copied}"
            )

        _copy_selected_rows(
            source,
            output,
            batch_size=batch_size,
            preserve_identity=True,
        )
        output.execute("DROP TABLE candidate_pool")
        rebuild_fts(output)
        shard_count = int(metadata["shard_count"])
        repair_rows = _populate_repair_queue(output, shard_count=shard_count)
        selected_digest = selection_digest(output)
        if selected_digest != template_selection_digest:
            raise ValueError("fixed selection digest changed during refresh")
        baseline_digest = baseline_content_digest(output)
        queue_digest = repair_queue_digest(output)
        contract_digest = candidate_contract_digest(
            selected_digest,
            baseline_digest,
            queue_digest,
            shard_count=shard_count,
        )
        canonical_stat = canonical.stat()
        output.execute(
            "INSERT INTO fast20k_metadata("
            "id,selection_version,policy_name,expected_rows,phrase_target,"
            "phrase_count,word_count,canonical_path,canonical_bytes,"
            "canonical_mtime_ns,scanned_rows,scan_last_frequency_rank,"
            "rejected_json,shard_count,selection_digest,repair_queue_digest,"
            "baseline_content_digest,candidate_digest,created_at"
            ") VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                SELECTION_VERSION,
                POLICY_NAME,
                int(metadata["expected_rows"]),
                int(metadata["phrase_target"]),
                int(metadata["phrase_count"]),
                int(metadata["word_count"]),
                str(canonical.resolve()),
                canonical_stat.st_size,
                canonical_stat.st_mtime_ns,
                int(metadata["scanned_rows"]),
                int(metadata["scan_last_frequency_rank"]),
                str(metadata["rejected_json"]),
                shard_count,
                selected_digest,
                queue_digest,
                baseline_digest,
                contract_digest,
                dt.datetime.now(dt.timezone.utc).isoformat(),
            ),
        )
        output.commit()
        output.execute("PRAGMA journal_mode=DELETE")
        output.execute("PRAGMA optimize")
        output.commit()
        output.close()
        output = None
        source.rollback()
        source.close()
        source = None
        template_db.rollback()
        template_db.close()
        template_db = None

        report = candidate_quality_report(
            partial,
            canonical,
            expected_rows=int(metadata["expected_rows"]),
        )
        if not report["structuralReady"]:
            raise ValueError(
                "refreshed candidate structural gate failed: "
                + json.dumps(report, ensure_ascii=False, sort_keys=True)
            )
        _fsync_file_and_parent(partial)
        _publish_candidate(partial, destination, replace=replace)
        _fsync_file_and_parent(destination)
        published = True
        report["candidate"] = str(destination)
        return {
            "candidate": str(destination),
            "selectionDigest": selected_digest,
            "baselineContentDigest": baseline_digest,
            "repairQueueDigest": queue_digest,
            "candidateDigest": contract_digest,
            "repairQueueRows": repair_rows,
            "quality": report,
        }
    finally:
        for database in (output, source, template_db):
            if database is not None:
                if database.in_transaction:
                    database.rollback()
                database.close()
        if not published:
            _cleanup_sqlite(partial)


def refresh_candidate_owner(
    canonical: Path,
    template: Path,
    destination: Path,
    *,
    shard_index: int,
    shard_count: int,
    replace: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, Any]:
    """Refresh only one fixed owner's rows in an existing candidate.

    The selected IDs, selected ranks and word/phrase classification stay fixed.
    Mutable dictionary content is replaced only for ``id % shard_count`` rows
    owned by this canonical replica.  The candidate is rebuilt in a sibling
    temporary file and published atomically after its complete internal and
    owner-baseline checks pass.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("shard_index must be within [0, shard_count)")
    if destination.exists() and not replace:
        raise FileExistsError(f"refusing to overwrite {destination}")
    if destination.resolve() in {canonical.resolve(), template.resolve()}:
        raise ValueError("owner refresh output must differ from its inputs")

    partial = _atomic_temp_path(destination)
    source: sqlite3.Connection | None = None
    template_db: sqlite3.Connection | None = None
    output: sqlite3.Connection | None = None
    published = False
    try:
        template_db = sqlite3.connect(f"file:{template.resolve()}?mode=ro", uri=True)
        template_db.row_factory = sqlite3.Row
        template_db.execute("PRAGMA query_only=ON")
        template_db.execute("BEGIN")
        if str(template_db.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
            raise ValueError("template candidate failed SQLite quick_check")
        metadata_row = template_db.execute(
            "SELECT * FROM fast20k_metadata WHERE id=1"
        ).fetchone()
        if metadata_row is None:
            raise ValueError("template candidate metadata is missing")
        metadata = dict(metadata_row)
        if (
            int(metadata["selection_version"]) != SELECTION_VERSION
            or str(metadata["policy_name"]) != POLICY_NAME
            or int(metadata["shard_count"]) != shard_count
        ):
            raise ValueError("template candidate contract version mismatch")
        template_selection = selection_digest(template_db)
        template_baseline = baseline_content_digest(template_db)
        template_queue = repair_queue_digest(template_db)
        if (
            template_selection != str(metadata["selection_digest"])
            or template_baseline != str(metadata["baseline_content_digest"])
            or template_queue != str(metadata["repair_queue_digest"])
            or candidate_contract_digest(
                template_selection,
                template_baseline,
                template_queue,
                shard_count=shard_count,
            )
            != str(metadata["candidate_digest"])
        ):
            raise ValueError("template candidate digest mismatch")

        source = sqlite3.connect(f"file:{canonical.resolve()}?mode=ro", uri=True)
        source.row_factory = sqlite3.Row
        source.execute("PRAGMA query_only=ON")
        source.execute("BEGIN")
        available = {
            str(row[1]) for row in source.execute("PRAGMA table_info(entries)")
        }
        projection_sql = _entry_projection(available)

        output = sqlite3.connect(partial)
        output.row_factory = sqlite3.Row
        template_db.backup(output)
        output.execute("PRAGMA temp_store=FILE")
        owner_rows = output.execute(
            "SELECT count(*) FROM fast20k_provenance WHERE (canonical_id % ?) = ?",
            (shard_count, shard_index),
        ).fetchone()[0]
        cursor = output.execute(
            "SELECT canonical_id,selected_rank,canonical_frequency_rank,"
            "normalized_word,term_key,kind,ranking_evidence,phrase_evidence "
            "FROM fast20k_provenance WHERE (canonical_id % ?) = ? "
            "ORDER BY canonical_id",
            (shard_count, shard_index),
        )
        refreshed = 0
        update_columns = ",".join(f"{column}=?" for column in ENTRY_COLUMNS)
        while True:
            provenance_rows = cursor.fetchmany(batch_size)
            if not provenance_rows:
                break
            ids = [int(row[0]) for row in provenance_rows]
            marks = ",".join("?" for _ in ids)
            source_rows = source.execute(
                f"SELECT id,{projection_sql} FROM entries WHERE id IN ({marks})",
                ids,
            ).fetchall()
            by_id = {int(row["id"]): dict(row) for row in source_rows}
            for provenance in provenance_rows:
                entry_id = int(provenance["canonical_id"])
                row = by_id.get(entry_id)
                if row is None:
                    raise ValueError(f"owner canonical row is missing: id={entry_id}")
                key = term_key(row["normalized_word"])
                if (
                    int(row["frequency_rank"])
                    != int(provenance["canonical_frequency_rank"])
                    or key != term_key(provenance["normalized_word"])
                    or key != str(provenance["term_key"])
                    or key != term_key(row["word"])
                ):
                    raise ValueError(
                        f"fixed owner selection identity changed: id={entry_id}"
                    )
                sources = _json_list(row["source_json"])
                scope = _json_dict(row["scope_json"])
                if sources is None or not sources or scope is None:
                    raise ValueError(
                        f"owner source provenance is invalid: id={entry_id}"
                    )
                lexical = lexical_rejection_reason(key, row["pos"], sources, scope)
                if lexical:
                    raise ValueError(
                        f"fixed owner selection is no longer lexical: "
                        f"id={entry_id} reason={lexical}"
                    )
                kind = "phrase" if is_phrase(key, row["pos"]) else "word"
                if kind != str(provenance["kind"]):
                    raise ValueError(
                        f"fixed owner selection kind changed: id={entry_id}"
                    )
                evidence = phrase_evidence(key, row["pos"], sources, scope)
                if kind == "phrase" and not evidence:
                    raise ValueError(
                        f"fixed owner phrase lost dictionary evidence: id={entry_id}"
                    )
                values = [row[column] for column in ENTRY_COLUMNS]
                values[ENTRY_COLUMNS.index("frequency_rank")] = int(
                    provenance["selected_rank"]
                )
                output.execute(
                    f"UPDATE entries SET {update_columns} WHERE id=?",
                    (*values, entry_id),
                )
                output.execute(
                    "UPDATE fast20k_provenance SET normalized_word=?,"
                    "phrase_evidence=?,canonical_content_sha256=?,"
                    "canonical_identity_sha256=? WHERE canonical_id=?",
                    (
                        row["normalized_word"],
                        evidence or "",
                        canonical_content_digest(row),
                        canonical_identity_digest(row),
                        entry_id,
                    ),
                )
                refreshed += 1
        cursor.close()
        if refreshed != int(owner_rows):
            raise ValueError(
                f"owner refresh count mismatch: expected={owner_rows} "
                f"actual={refreshed}"
            )

        rebuild_fts(output)
        output.execute("DELETE FROM repair_queue")
        repair_rows = _populate_repair_queue(output, shard_count=shard_count)
        selected_digest = selection_digest(output)
        baseline_digest = baseline_content_digest(output)
        queue_digest = repair_queue_digest(output)
        contract_digest = candidate_contract_digest(
            selected_digest,
            baseline_digest,
            queue_digest,
            shard_count=shard_count,
        )
        canonical_stat = canonical.stat()
        output.execute(
            "UPDATE fast20k_metadata SET canonical_path=?,canonical_bytes=?,"
            "canonical_mtime_ns=?,selection_digest=?,baseline_content_digest=?,"
            "repair_queue_digest=?,candidate_digest=?,created_at=? WHERE id=1",
            (
                str(canonical.resolve()),
                canonical_stat.st_size,
                canonical_stat.st_mtime_ns,
                selected_digest,
                baseline_digest,
                queue_digest,
                contract_digest,
                dt.datetime.now(dt.timezone.utc).isoformat(),
            ),
        )
        output.commit()
        output.execute("PRAGMA journal_mode=DELETE")
        output.execute("PRAGMA optimize")
        output.commit()
        output.close()
        output = None
        source.rollback()
        source.close()
        source = None
        template_db.rollback()
        template_db.close()
        template_db = None

        report = candidate_quality_report(
            partial,
            canonical,
            expected_rows=int(metadata["expected_rows"]),
            canonical_shard_index=shard_index,
            canonical_shard_count=shard_count,
        )
        if not report["structuralReady"]:
            raise ValueError(
                "owner-refreshed candidate structural gate failed: "
                + json.dumps(report, ensure_ascii=False, sort_keys=True)
            )
        _fsync_file_and_parent(partial)
        _publish_candidate(partial, destination, replace=replace)
        _fsync_file_and_parent(destination)
        published = True
        report["candidate"] = str(destination)
        return {
            "candidate": str(destination),
            "owner": {"shardIndex": shard_index, "shardCount": shard_count},
            "ownerRowsRefreshed": refreshed,
            "selectionDigest": selected_digest,
            "baselineContentDigest": baseline_digest,
            "repairQueueDigest": queue_digest,
            "candidateDigest": contract_digest,
            "repairQueueRows": repair_rows,
            "quality": report,
        }
    finally:
        for database in (output, source, template_db):
            if database is not None:
                if database.in_transaction:
                    database.rollback()
                database.close()
        if not published:
            _cleanup_sqlite(partial)


def output_count_beyond_rank(candidate: Path, original_limit: int) -> int:
    database = sqlite3.connect(f"file:{candidate.resolve()}?mode=ro", uri=True)
    try:
        return int(
            database.execute(
                "SELECT count(*) FROM fast20k_provenance "
                "WHERE canonical_frequency_rank>?",
                (original_limit,),
            ).fetchone()[0]
        )
    finally:
        database.close()


class _Issues:
    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.examples: dict[str, list[str]] = defaultdict(list)

    def add(self, code: str, example: Any = "", count: int = 1) -> None:
        self.counts[code] += count
        text = str(example or "")
        if text and len(self.examples[code]) < EXAMPLE_LIMIT:
            self.examples[code].append(text)

    def report(
        self,
    ) -> tuple[dict[str, int], dict[str, list[str]], list[dict[str, Any]]]:
        counts = dict(sorted(self.counts.items()))
        examples = dict(sorted(self.examples.items()))
        diagnostics = [
            {
                "code": code,
                "count": count,
                "message": ISSUE_MESSAGES.get(code, "候选质量检查未通过"),
                "examples": examples.get(code, []),
            }
            for code, count in counts.items()
        ]
        return counts, examples, diagnostics


def _quick_check(
    database: sqlite3.Connection,
    issues: _Issues,
    issue_code: str,
) -> str:
    rows = [str(row[0]) for row in database.execute("PRAGMA quick_check").fetchmany(21)]
    if rows != ["ok"]:
        for item in rows[:EXAMPLE_LIMIT]:
            issues.add(issue_code, item)
        return "; ".join(rows)
    return "ok"


def _candidate_schema_ready(
    database: sqlite3.Connection,
    issues: _Issues,
) -> bool:
    required = {
        "entries",
        "entries_fts",
        "fast20k_metadata",
        "fast20k_provenance",
        "repair_queue",
    }
    present = {
        str(row[0])
        for row in database.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
        )
    }
    for table in sorted(required - present):
        issues.add("missing_table", table)
    return required <= present


def _canonical_rows(
    database: sqlite3.Connection,
    ids: list[int],
    available: set[str],
) -> dict[int, dict[str, Any]]:
    marks = ",".join("?" for _ in ids)
    rows = database.execute(
        f"SELECT id,{_entry_projection(available)} FROM entries WHERE id IN ({marks})",
        ids,
    ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def _validate_fts_content(
    database: sqlite3.Connection,
    issues: _Issues,
) -> None:
    """Compare the complete FTS payload without loading it into memory."""
    entries = database.execute(
        "SELECT id,word,definition,definition_zh,examples_json,phrases_json "
        "FROM entries ORDER BY id"
    )
    indexed = database.execute(
        "SELECT rowid,word,definition,definition_zh,examples,phrases "
        "FROM entries_fts ORDER BY rowid"
    )
    try:
        while True:
            entry = entries.fetchone()
            fts = indexed.fetchone()
            if entry is None and fts is None:
                break
            if entry is None:
                issues.add("fts_content_mismatch", f"extra:{fts[0]}")
                continue
            if fts is None:
                issues.add("fts_content_mismatch", f"missing:{entry[0]}")
                continue
            if tuple(entry) != tuple(fts):
                issues.add(
                    "fts_content_mismatch",
                    f"entry={entry[0]} fts={fts[0]}",
                )
    finally:
        entries.close()
        indexed.close()


def candidate_quality_report(
    candidate: Path,
    canonical: Path,
    *,
    expected_rows: int = DEFAULT_LIMIT,
    batch_size: int = DEFAULT_BATCH_SIZE,
    canonical_shard_index: int | None = None,
    canonical_shard_count: int | None = None,
) -> dict[str, Any]:
    if (canonical_shard_index is None) != (canonical_shard_count is None):
        raise ValueError(
            "canonical_shard_index and canonical_shard_count must be used together"
        )
    if canonical_shard_count is not None and (
        canonical_shard_count < 1
        or canonical_shard_index is None
        or not 0 <= canonical_shard_index < canonical_shard_count
    ):
        raise ValueError("canonical shard selection is invalid")
    issues = _Issues()
    candidate_db: sqlite3.Connection | None = None
    canonical_db: sqlite3.Connection | None = None
    total = 0
    incomplete = 0
    metadata: dict[str, Any] = {}
    candidate_integrity = "not-run"
    canonical_integrity = "not-run"
    actual_kinds: Counter[str] = Counter()
    phase = "candidate"
    try:
        candidate_db = sqlite3.connect(f"file:{candidate.resolve()}?mode=ro", uri=True)
        candidate_db.row_factory = sqlite3.Row
        candidate_db.execute("PRAGMA query_only=ON")
        candidate_integrity = _quick_check(candidate_db, issues, "candidate_integrity")
        if not _candidate_schema_ready(candidate_db, issues):
            counts, examples, diagnostics = issues.report()
            return {
                "candidate": str(candidate),
                "canonical": str(canonical),
                "expectedRows": expected_rows,
                "total": 0,
                "complete": 0,
                "incomplete": 0,
                "ready": False,
                "structuralReady": False,
                "candidateQuickCheck": candidate_integrity,
                "canonicalQuickCheck": canonical_integrity,
                "issues": counts,
                "examples": examples,
                "diagnostics": diagnostics,
            }
        metadata_rows = candidate_db.execute(
            "SELECT * FROM fast20k_metadata"
        ).fetchall()
        if len(metadata_rows) != 1:
            issues.add("metadata", f"rows={len(metadata_rows)}")
        else:
            metadata = dict(metadata_rows[0])
            if (
                int(metadata.get("selection_version", -1)) != SELECTION_VERSION
                or metadata.get("policy_name") != POLICY_NAME
                or int(metadata.get("expected_rows", -1)) != expected_rows
                or int(metadata.get("shard_count", 0)) < 1
            ):
                issues.add("metadata", json.dumps(metadata, default=str))

        total = int(candidate_db.execute("SELECT count(*) FROM entries").fetchone()[0])
        provenance_count = int(
            candidate_db.execute("SELECT count(*) FROM fast20k_provenance").fetchone()[
                0
            ]
        )
        fts_count = int(
            candidate_db.execute("SELECT count(*) FROM entries_fts").fetchone()[0]
        )
        if total != expected_rows:
            issues.add("row_count", f"expected={expected_rows} actual={total}")
        if provenance_count != total:
            issues.add(
                "provenance_count",
                f"entries={total} provenance={provenance_count}",
            )
        if fts_count != total:
            issues.add("fts_count", f"entries={total} fts={fts_count}")
        _validate_fts_content(candidate_db, issues)
        ranks = candidate_db.execute(
            "SELECT MIN(frequency_rank),MAX(frequency_rank),"
            "COUNT(DISTINCT frequency_rank) FROM entries"
        ).fetchone()
        if total and tuple(ranks) != (1, total, total):
            issues.add("rank_sequence", str(tuple(ranks)))

        phase = "canonical"
        canonical_db = sqlite3.connect(f"file:{canonical.resolve()}?mode=ro", uri=True)
        canonical_db.row_factory = sqlite3.Row
        canonical_db.execute("PRAGMA query_only=ON")
        canonical_db.execute("BEGIN")
        canonical_columns = {
            str(row[1]) for row in canonical_db.execute("PRAGMA table_info(entries)")
        }
        required_canonical_columns = {
            "id",
            *(
                column
                for column in ENTRY_COLUMNS
                if column not in OPTIONAL_CANONICAL_DEFAULTS
            ),
        }
        missing_canonical = sorted(required_canonical_columns - canonical_columns)
        if missing_canonical:
            raise ValueError(
                "canonical schema missing columns: " + ",".join(missing_canonical)
            )
        canonical_integrity = _quick_check(canonical_db, issues, "canonical_integrity")

        expected_queue: dict[int, tuple[int, int, str, str, list[str], int]] = {}
        last_kind_rank: dict[str, int] = {}
        phase = "comparison"
        cursor = candidate_db.execute(
            "SELECT e.id,"
            + ",".join(f"e.{column}" for column in ENTRY_COLUMNS)
            + ",p.selected_rank,p.canonical_frequency_rank,p.normalized_word AS p_word,"
            "p.term_key,p.kind,p.ranking_evidence,p.phrase_evidence,"
            "p.canonical_content_sha256,p.canonical_identity_sha256 "
            "FROM entries e LEFT JOIN fast20k_provenance p "
            "ON p.canonical_id=e.id ORDER BY e.frequency_rank,e.id"
        )
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            ids = [
                int(row["id"])
                for row in rows
                if canonical_shard_count is None
                or int(row["id"]) % canonical_shard_count == canonical_shard_index
            ]
            canonical_by_id = (
                _canonical_rows(
                    canonical_db,
                    ids,
                    canonical_columns,
                )
                if ids
                else {}
            )
            for raw in rows:
                row = dict(raw)
                entry_id = int(row["id"])
                term = term_key(row.get("normalized_word"))
                if row.get("selected_rank") is None:
                    issues.add("provenance_count", entry_id)
                    continue
                if int(row["frequency_rank"]) != int(row["selected_rank"]):
                    issues.add("rank_sequence", term)
                sources = _json_list(row.get("source_json"))
                scope = _json_dict(row.get("scope_json"))
                if sources is None:
                    issues.add("malformed_source_json", term)
                    sources = []
                elif not sources:
                    issues.add("missing_source_provenance", term)
                if scope is None:
                    issues.add("malformed_json", f"{term}:scope_json")
                    scope = {}
                for column in JSON_COLUMNS:
                    try:
                        json.loads(str(row.get(column) or ""))
                    except (TypeError, json.JSONDecodeError):
                        issues.add("malformed_json", f"{term}:{column}")
                if term_key(row.get("word")) != term:
                    issues.add("normalized_word_mismatch", term)
                lexical = lexical_rejection_reason(term, row.get("pos"), sources, scope)
                if lexical:
                    issues.add("lexical_policy", f"{term}:{lexical}")
                expected_kind = "phrase" if is_phrase(term, row.get("pos")) else "word"
                actual_kinds[expected_kind] += 1
                evidence = phrase_evidence(term, row.get("pos"), sources, scope)
                if expected_kind == "phrase" and not evidence:
                    issues.add("phrase_evidence", term)
                if row.get("kind") != expected_kind or (
                    expected_kind == "phrase" and row.get("phrase_evidence") != evidence
                ):
                    issues.add("phrase_evidence", term)
                if row.get("term_key") != term:
                    issues.add("term_key_mismatch", term)
                if term_key(row.get("p_word")) != term:
                    issues.add("term_key_mismatch", f"{term}:provenance")
                expected_ranking = (
                    "bounded-dictionary-evidence"
                    if expected_kind == "phrase"
                    else "wordfreq-single-token"
                )
                if row.get("ranking_evidence") != expected_ranking:
                    issues.add("ranking_policy", term)
                original_rank = int(row["canonical_frequency_rank"])
                if original_rank < last_kind_rank.get(expected_kind, 0):
                    issues.add("stream_order", term)
                last_kind_rank[expected_kind] = original_rank

                gaps = strict_required_gaps(row)
                if gaps:
                    incomplete += 1
                    for gap in gaps:
                        issues.add(gap, term)
                    expected_queue[entry_id] = (
                        int(row["selected_rank"]),
                        int(row["canonical_frequency_rank"]),
                        str(row["normalized_word"]),
                        str(row["canonical_identity_sha256"]),
                        gaps,
                        entry_id % max(1, int(metadata.get("shard_count", 0))),
                    )

                compare_canonical = (
                    canonical_shard_count is None
                    or entry_id % canonical_shard_count == canonical_shard_index
                )
                if not compare_canonical:
                    continue
                canonical_row = canonical_by_id.get(entry_id)
                if canonical_row is None:
                    issues.add("canonical_missing", entry_id)
                    continue
                canonical_identity = canonical_identity_digest(canonical_row)
                canonical_content = canonical_content_digest(canonical_row)
                if (
                    term_key(canonical_row["normalized_word"]) != term
                    or int(canonical_row["frequency_rank"])
                    != int(row["canonical_frequency_rank"])
                    or canonical_identity != row["canonical_identity_sha256"]
                ):
                    issues.add("canonical_identity_mismatch", term)
                candidate_matches = all(
                    row[column] == canonical_row[column]
                    for column in ENTRY_COLUMNS
                    if column != "frequency_rank"
                )
                if (
                    not candidate_matches
                    or canonical_content != row["canonical_content_sha256"]
                ):
                    issues.add("canonical_content_mismatch", term)
        cursor.close()

        if metadata and (
            int(metadata.get("word_count", -1)) != actual_kinds["word"]
            or int(metadata.get("phrase_count", -1)) != actual_kinds["phrase"]
        ):
            issues.add(
                "selection_counts",
                "metadata="
                f"{metadata.get('word_count')}/{metadata.get('phrase_count')} "
                f"actual={actual_kinds['word']}/{actual_kinds['phrase']}",
            )

        if expected_rows == DEFAULT_LIMIT and actual_kinds["word"] < MIN_FAST20K_WORDS:
            issues.add(
                "minimum_word_count",
                f"required={MIN_FAST20K_WORDS} actual={actual_kinds['word']}",
            )

        if metadata:
            actual_selection_digest = selection_digest(candidate_db)
            if actual_selection_digest != str(metadata.get("selection_digest", "")):
                issues.add(
                    "selection_digest_mismatch",
                    f"metadata={metadata.get('selection_digest')} actual={actual_selection_digest}",
                )
            actual_baseline_digest = baseline_content_digest(candidate_db)
            if actual_baseline_digest != str(
                metadata.get("baseline_content_digest", "")
            ):
                issues.add(
                    "selection_digest_mismatch",
                    "baseline metadata="
                    f"{metadata.get('baseline_content_digest')} "
                    f"actual={actual_baseline_digest}",
                )

        queue_rows = candidate_db.execute(
            "SELECT canonical_id,selected_rank,canonical_frequency_rank,"
            "normalized_word,canonical_identity_sha256,gaps_json,shard_owner "
            "FROM repair_queue ORDER BY canonical_id"
        ).fetchall()
        if len(queue_rows) != len(expected_queue):
            issues.add(
                "repair_queue_count",
                f"expected={len(expected_queue)} actual={len(queue_rows)}",
            )
        for queue_row in queue_rows:
            expected = expected_queue.get(int(queue_row[0]))
            try:
                queue_gaps = json.loads(str(queue_row[5]))
            except json.JSONDecodeError:
                queue_gaps = None
            if (
                expected is None
                or (
                    int(queue_row[1]),
                    int(queue_row[2]),
                    str(queue_row[3]),
                    str(queue_row[4]),
                    queue_gaps,
                    int(queue_row[6]),
                )
                != expected
            ):
                issues.add("repair_queue_mismatch", queue_row[0])
            if int(queue_row[6]) != int(queue_row[0]) % max(
                1, int(metadata.get("shard_count", 0))
            ):
                issues.add("shard_owner_mismatch", queue_row[0])
        if metadata:
            actual_queue_digest = repair_queue_digest(candidate_db)
            if actual_queue_digest != str(metadata.get("repair_queue_digest", "")):
                issues.add(
                    "repair_queue_digest_mismatch",
                    f"metadata={metadata.get('repair_queue_digest')} actual={actual_queue_digest}",
                )
            actual_candidate_digest = candidate_contract_digest(
                selection_digest(candidate_db),
                baseline_content_digest(candidate_db),
                actual_queue_digest,
                shard_count=int(metadata.get("shard_count", 1)),
            )
            if actual_candidate_digest != str(metadata.get("candidate_digest", "")):
                issues.add(
                    "candidate_digest_mismatch",
                    f"metadata={metadata.get('candidate_digest')} actual={actual_candidate_digest}",
                )
    except (OSError, sqlite3.Error, ValueError, TypeError) as error:
        code = {
            "candidate": "candidate_database_error",
            "canonical": "canonical_database_error",
            "comparison": "comparison_error",
        }[phase]
        issues.add(code, repr(error))
    finally:
        if canonical_db is not None:
            if canonical_db.in_transaction:
                canonical_db.rollback()
            canonical_db.close()
        if candidate_db is not None:
            candidate_db.close()

    counts, examples, diagnostics = issues.report()
    structural_codes = set(counts) - REPAIRABLE_ISSUES
    structural_ready = not structural_codes
    ready = structural_ready and incomplete == 0 and total == expected_rows
    return {
        "candidate": str(candidate),
        "canonical": str(canonical),
        "expectedRows": expected_rows,
        "total": total,
        "complete": max(0, total - incomplete),
        "incomplete": incomplete,
        "ready": ready,
        "structuralReady": structural_ready,
        "candidateQuickCheck": candidate_integrity,
        "canonicalQuickCheck": canonical_integrity,
        "terms": {
            "words": actual_kinds["word"],
            "phrases": actual_kinds["phrase"],
        },
        "issues": counts,
        "examples": examples,
        "diagnostics": diagnostics,
    }


def assert_candidate_ready(
    candidate: Path,
    canonical: Path,
    *,
    expected_rows: int = DEFAULT_LIMIT,
) -> dict[str, Any]:
    report = candidate_quality_report(
        candidate,
        canonical,
        expected_rows=expected_rows,
    )
    if not report["ready"]:
        raise ValueError(
            "fast candidate quality gate failed: "
            + json.dumps(report, ensure_ascii=False, sort_keys=True)
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    select = commands.add_parser("select")
    select.add_argument("--canonical", type=Path, required=True)
    select.add_argument("--output", type=Path, required=True)
    select.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    select.add_argument("--phrase-target", type=int, default=DEFAULT_PHRASE_TARGET)
    select.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    select.add_argument("--repair-shards", type=int, default=DEFAULT_REPAIR_SHARDS)
    select.add_argument("--replace", action="store_true")
    refresh = commands.add_parser("refresh")
    refresh.add_argument("--canonical", type=Path, required=True)
    refresh.add_argument("--template", type=Path, required=True)
    refresh.add_argument("--output", type=Path, required=True)
    refresh.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    refresh.add_argument("--replace", action="store_true")
    refresh_owner = commands.add_parser("refresh-owner")
    refresh_owner.add_argument("--canonical", type=Path, required=True)
    refresh_owner.add_argument("--template", type=Path, required=True)
    refresh_owner.add_argument("--output", type=Path, required=True)
    refresh_owner.add_argument("--shard-index", type=int, required=True)
    refresh_owner.add_argument("--shard-count", type=int, required=True)
    refresh_owner.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    refresh_owner.add_argument("--replace", action="store_true")
    validate = commands.add_parser("validate")
    validate.add_argument("--canonical", type=Path, required=True)
    validate.add_argument("--candidate", type=Path, required=True)
    validate.add_argument("--expected-rows", type=int, default=DEFAULT_LIMIT)
    validate.add_argument("--canonical-shard-index", type=int)
    validate.add_argument("--canonical-shard-count", type=int)
    args = parser.parse_args()
    if args.command == "select":
        report = build_candidate(
            args.canonical,
            args.output,
            limit=args.limit,
            phrase_target=args.phrase_target,
            replace=args.replace,
            batch_size=args.batch_size,
            shard_count=args.repair_shards,
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    if args.command == "refresh":
        report = refresh_candidate(
            args.canonical,
            args.template,
            args.output,
            replace=args.replace,
            batch_size=args.batch_size,
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    if args.command == "refresh-owner":
        report = refresh_candidate_owner(
            args.canonical,
            args.template,
            args.output,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
            replace=args.replace,
            batch_size=args.batch_size,
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    report = candidate_quality_report(
        args.candidate,
        args.canonical,
        expected_rows=args.expected_rows,
        canonical_shard_index=args.canonical_shard_index,
        canonical_shard_count=args.canonical_shard_count,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["ready"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
