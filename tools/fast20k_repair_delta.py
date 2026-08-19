#!/usr/bin/env python3
"""Export, validate and apply exact fixed-queue repair deltas.

Every delta is bound to one candidate contract digest and one persisted
``shard_owner``.  Union validation is fail-closed: all configured owners must
be present, every queued ID must appear exactly once, and no unqueued ID is
accepted.  Applying a union always creates a new canonical snapshot.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from enrich_oxford_scope import (
    entry_quality_gaps,
    open_repair_queue,
    preflight_repair_queue_shard,
    repair_queue_metadata,
)
from fast20k_contract import canonical_identity_digest
from merge_enrichment_shards import MUTABLE_COLUMNS


DELTA_SCHEMA = """
CREATE TABLE repair_delta_metadata(
  id INTEGER PRIMARY KEY CHECK(id=1),
  candidate_digest TEXT NOT NULL,
  selection_digest TEXT NOT NULL,
  baseline_content_digest TEXT NOT NULL,
  repair_queue_digest TEXT NOT NULL,
  shard_owner INTEGER NOT NULL,
  shard_count INTEGER NOT NULL,
  expected_rows INTEGER NOT NULL,
  delta_digest TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE entries(
  id INTEGER PRIMARY KEY,
  canonical_identity_sha256 TEXT NOT NULL,
  changed_columns_json TEXT NOT NULL,
  pos TEXT,
  definition TEXT,
  definition_zh TEXT,
  us_phonetic TEXT,
  uk_phonetic TEXT,
  synonyms_json TEXT,
  antonyms_json TEXT,
  examples_json TEXT,
  phrases_json TEXT,
  phrase_entries_json TEXT,
  related_words_json TEXT,
  related_entries_json TEXT,
  frequency REAL,
  difficulty TEXT,
  enrichment_json TEXT
);
"""

GAP_MUTABLE_COLUMNS = {
    # A missing English definition prevents the baseline quality check from
    # determining whether its Chinese translation is truncated or absent.
    # The repair therefore owns both halves of that dependent pair.
    "definition": ("definition", "definition_zh"),
    "definition_zh": ("definition_zh",),
    "pos": ("pos",),
    "phonetic": ("us_phonetic", "uk_phonetic"),
    "phonetic_unreliable": ("us_phonetic", "uk_phonetic"),
}


def allowed_columns_for_gaps(gaps_json: str) -> tuple[str, ...]:
    try:
        gaps = json.loads(gaps_json)
    except json.JSONDecodeError as error:
        raise ValueError("repair queue gaps JSON is invalid") from error
    if not isinstance(gaps, list):
        raise ValueError("repair queue gaps must be a list")
    allowed = {
        column
        for gap in gaps
        for column in GAP_MUTABLE_COLUMNS.get(str(gap), ())
    }
    return tuple(column for column in MUTABLE_COLUMNS if column in allowed)


def readonly(path: Path) -> sqlite3.Connection:
    database = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    database.row_factory = sqlite3.Row
    database.execute("PRAGMA query_only=ON")
    return database


def quick_check(database: sqlite3.Connection, label: str) -> None:
    rows = [str(row[0]) for row in database.execute("PRAGMA quick_check")]
    if rows != ["ok"]:
        raise ValueError(f"{label} SQLite quick_check failed: {'; '.join(rows[:20])}")


def sqlite_snapshot(source: Path, destination: Path) -> None:
    source_db = readonly(source)
    destination_db = sqlite3.connect(destination)
    try:
        source_db.backup(destination_db)
        destination_db.execute("PRAGMA journal_mode=DELETE")
        destination_db.commit()
        quick_check(destination_db, f"snapshot of {source}")
    finally:
        destination_db.close()
        source_db.close()


def _delta_digest(database: sqlite3.Connection) -> str:
    digest = hashlib.sha256()
    columns = (
        "id,canonical_identity_sha256,changed_columns_json,"
        + ",".join(MUTABLE_COLUMNS)
    )
    for row in database.execute(f"SELECT {columns} FROM entries ORDER BY id"):
        encoded = json.dumps(
            list(row),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        digest.update(encoded)
        digest.update(b"\n")
    return digest.hexdigest()


def _publish_no_replace(partial: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.link(partial, destination)
    partial.unlink()
    directory = os.open(destination.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def export_repair_delta(
    dataset: Path,
    candidate: Path,
    output: Path,
    *,
    shard_index: int,
    shard_count: int,
) -> dict[str, Any]:
    if output.exists():
        raise FileExistsError(f"refusing to overwrite {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw_partial = tempfile.mkstemp(
        prefix=f".{output.name}.", suffix=".tmp", dir=output.parent
    )
    os.close(descriptor)
    partial = Path(raw_partial)
    source: sqlite3.Connection | None = None
    queue: sqlite3.Connection | None = None
    destination: sqlite3.Connection | None = None
    published = False
    try:
        source = sqlite3.connect(f"file:{dataset.resolve()}?mode=ro", uri=True)
        source.row_factory = sqlite3.Row
        source.execute("PRAGMA query_only=ON")
        queue = open_repair_queue(candidate)
        rows, metadata = preflight_repair_queue_shard(
            source,
            queue,
            shard_index=shard_index,
            shard_count=shard_count,
        )
        unresolved = [
            (row[0], row[2])
            for row in rows
            if entry_quality_gaps(
                row[2], row[3], row[4], row[5], row[6], row[17]
            )
        ]
        if unresolved:
            raise ValueError(
                "repair delta still contains incomplete rows: "
                + ",".join(f"{entry_id}:{term}" for entry_id, term in unresolved[:20])
            )
        queue_contracts = {
            int(row[0]): (str(row[1]), str(row[2]))
            for row in queue.execute(
                "SELECT canonical_id,canonical_identity_sha256,gaps_json "
                "FROM repair_queue "
                "WHERE shard_owner=? ORDER BY canonical_id",
                (shard_index,),
            )
        }
        baseline_rows = {
            int(row[0]): tuple(row[1:])
            for row in queue.execute(
                "SELECT id," + ",".join(MUTABLE_COLUMNS)
                + " FROM entries WHERE id IN ("
                "SELECT canonical_id FROM repair_queue WHERE shard_owner=?"
                ") ORDER BY id",
                (shard_index,),
            )
        }
        destination = sqlite3.connect(partial)
        destination.executescript(DELTA_SCHEMA)
        columns = (
            "id,canonical_identity_sha256,changed_columns_json,"
            + ",".join(MUTABLE_COLUMNS)
        )
        placeholders = ",".join("?" for _ in range(len(MUTABLE_COLUMNS) + 3))
        values = []
        for row in rows:
            mutable_values = (
                row[17],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                row[11],
                row[12],
                row[13],
                row[14],
                row[15],
                row[16],
            )
            baseline = baseline_rows.get(int(row[0]))
            if baseline is None:
                raise ValueError(f"candidate baseline row is missing: id={row[0]}")
            queue_contract = queue_contracts.get(int(row[0]))
            if queue_contract is None:
                raise ValueError(f"repair queue row is missing: id={row[0]}")
            allowed_columns = allowed_columns_for_gaps(queue_contract[1])
            changed_columns = [
                column
                for column, before, after in zip(
                    MUTABLE_COLUMNS, baseline, mutable_values
                )
                if column in allowed_columns and before != after
            ]
            values.append(
                (
                    int(row[0]),
                    queue_contract[0],
                    json.dumps(changed_columns, separators=(",", ":")),
                    *mutable_values,
                )
            )
        destination.executemany(
            f"INSERT INTO entries({columns}) VALUES({placeholders})", values
        )
        digest = _delta_digest(destination)
        destination.execute(
            "INSERT INTO repair_delta_metadata VALUES(1,?,?,?,?,?,?,?,?,?)",
            (
                metadata["candidate_digest"],
                metadata["selection_digest"],
                metadata["baseline_content_digest"],
                metadata["repair_queue_digest"],
                shard_index,
                shard_count,
                len(rows),
                digest,
                dt.datetime.now(dt.timezone.utc).isoformat(),
            ),
        )
        destination.commit()
        destination.execute("PRAGMA journal_mode=DELETE")
        destination.execute("PRAGMA optimize")
        destination.commit()
        quick_check(destination, "repair delta")
        destination.close()
        destination = None
        with partial.open("rb") as stream:
            os.fsync(stream.fileno())
        _publish_no_replace(partial, output)
        published = True
        return {
            "output": str(output),
            "candidateDigest": metadata["candidate_digest"],
            "shardOwner": shard_index,
            "shardCount": shard_count,
            "rows": len(rows),
            "deltaDigest": digest,
        }
    finally:
        for database in (destination, queue, source):
            if database is not None:
                database.close()
        if not published:
            for suffix in ("", "-wal", "-shm"):
                try:
                    Path(str(partial) + suffix).unlink()
                except FileNotFoundError:
                    pass


def validate_delta_union(
    candidate: Path,
    deltas: list[Path],
) -> dict[str, Any]:
    if not deltas:
        raise ValueError("at least one repair delta is required")
    queue = open_repair_queue(candidate)
    opened: list[sqlite3.Connection] = []
    try:
        metadata = repair_queue_metadata(queue)
        expected = {
            int(row[0]): (
                int(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
            )
            for row in queue.execute(
                "SELECT canonical_id,shard_owner,canonical_identity_sha256,"
                "normalized_word,gaps_json "
                "FROM repair_queue ORDER BY canonical_id"
            )
        }
        actual: dict[int, tuple[int, str, str]] = {}
        owners: set[int] = set()
        delta_reports: list[dict[str, Any]] = []
        for path in deltas:
            database = readonly(path)
            opened.append(database)
            quick_check(database, f"repair delta {path}")
            row = database.execute(
                "SELECT * FROM repair_delta_metadata WHERE id=1"
            ).fetchone()
            if row is None:
                raise ValueError(f"repair delta metadata missing: {path}")
            values = dict(row)
            owner = int(values["shard_owner"])
            if owner in owners:
                raise ValueError(f"duplicate repair delta shard owner: {owner}")
            owners.add(owner)
            if (
                str(values["candidate_digest"]) != metadata["candidate_digest"]
                or int(values["shard_count"]) != metadata["shard_count"]
                or str(values["selection_digest"]) != metadata["selection_digest"]
                or str(values["baseline_content_digest"])
                != metadata["baseline_content_digest"]
                or str(values["repair_queue_digest"])
                != metadata["repair_queue_digest"]
            ):
                raise ValueError(f"repair delta candidate contract mismatch: {path}")
            digest = _delta_digest(database)
            if digest != str(values["delta_digest"]):
                raise ValueError(f"repair delta content digest mismatch: {path}")
            rows = database.execute(
                "SELECT id,canonical_identity_sha256,changed_columns_json,"
                + ",".join(MUTABLE_COLUMNS)
                + " FROM entries ORDER BY id"
            ).fetchall()
            if len(rows) != int(values["expected_rows"]):
                raise ValueError(f"repair delta row count mismatch: {path}")
            for entry in rows:
                entry_id = int(entry[0])
                if entry_id in actual:
                    raise ValueError(f"overlapping repair delta id: {entry_id}")
                contract = expected.get(entry_id)
                if contract is None:
                    raise ValueError(f"unqueued repair delta id: {entry_id}")
                try:
                    changed_columns = json.loads(str(entry[2]))
                except json.JSONDecodeError as error:
                    raise ValueError(
                        f"repair delta changed-columns JSON is invalid: id={entry_id}"
                    ) from error
                if (
                    not isinstance(changed_columns, list)
                    or len(changed_columns) != len(set(changed_columns))
                    or any(column not in MUTABLE_COLUMNS for column in changed_columns)
                ):
                    raise ValueError(
                        f"repair delta changed-columns contract is invalid: id={entry_id}"
                    )
                baseline_row = queue.execute(
                    "SELECT " + ",".join(MUTABLE_COLUMNS)
                    + " FROM entries WHERE id=?",
                    (entry_id,),
                ).fetchone()
                if baseline_row is None:
                    raise ValueError(f"candidate baseline row is missing: id={entry_id}")
                delta_mutable = tuple(entry[3:])
                allowed_columns = allowed_columns_for_gaps(contract[3])
                expected_changed = [
                    column
                    for column, before, after in zip(
                        MUTABLE_COLUMNS, tuple(baseline_row), delta_mutable
                    )
                    if column in allowed_columns and before != after
                ]
                if changed_columns != expected_changed:
                    raise ValueError(
                        f"repair delta changed-columns do not match baseline: id={entry_id}"
                    )
                gaps = entry_quality_gaps(
                    contract[2],
                    entry[4],
                    entry[5],
                    entry[6],
                    entry[7],
                    entry[3],
                )
                if gaps:
                    raise ValueError(
                        f"repair delta row is incomplete: id={entry_id} "
                        f"gaps={','.join(gaps)}"
                    )
                actual[entry_id] = (owner, str(entry[1]), contract[2])
            delta_reports.append(
                {"path": str(path), "shardOwner": owner, "rows": len(rows)}
            )
        expected_owners = set(range(int(metadata["shard_count"])))
        if owners != expected_owners:
            raise ValueError(
                "repair delta owners incomplete: "
                f"expected={sorted(expected_owners)} actual={sorted(owners)}"
            )
        if set(actual) != set(expected):
            missing = sorted(set(expected) - set(actual))
            extra = sorted(set(actual) - set(expected))
            raise ValueError(
                "repair delta union is not exact: "
                f"missing={missing[:20]} extra={extra[:20]}"
            )
        mismatched = [
            entry_id
            for entry_id, contract in actual.items()
            if contract != expected[entry_id][:3]
        ]
        if mismatched:
            raise ValueError(
                "repair delta owner or identity mismatch: "
                + ",".join(map(str, mismatched[:20]))
            )
        return {
            "candidateDigest": metadata["candidate_digest"],
            "expectedRows": len(expected),
            "actualRows": len(actual),
            "shards": sorted(delta_reports, key=lambda item: item["shardOwner"]),
            "ready": True,
        }
    finally:
        for database in opened:
            database.close()
        queue.close()


def apply_delta_union(
    canonical: Path,
    candidate: Path,
    deltas: list[Path],
    output: Path,
) -> dict[str, Any]:
    if output.exists():
        raise FileExistsError(f"refusing to overwrite {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.inputs.", dir=output.parent)
    )
    staged_canonical = staging / "canonical.sqlite"
    staged_candidate = staging / "candidate.sqlite"
    staged_deltas = [staging / f"delta-{index}.sqlite" for index in range(len(deltas))]
    try:
        sqlite_snapshot(canonical, staged_canonical)
        sqlite_snapshot(candidate, staged_candidate)
        for source_delta, staged_delta in zip(deltas, staged_deltas):
            sqlite_snapshot(source_delta, staged_delta)
        report = validate_delta_union(staged_candidate, staged_deltas)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    descriptor, raw_partial = tempfile.mkstemp(
        prefix=f".{output.name}.", suffix=".tmp", dir=output.parent
    )
    os.close(descriptor)
    partial = Path(raw_partial)
    source: sqlite3.Connection | None = None
    destination: sqlite3.Connection | None = None
    queue: sqlite3.Connection | None = None
    delta_databases: list[sqlite3.Connection] = []
    published = False
    try:
        source = readonly(staged_canonical)
        destination = sqlite3.connect(partial)
        source.backup(destination)
        source.close()
        source = None
        queue = open_repair_queue(staged_candidate)
        expected = {
            int(row[0]): str(row[1])
            for row in queue.execute(
                "SELECT canonical_id,canonical_identity_sha256 FROM repair_queue"
            )
        }
        destination.row_factory = sqlite3.Row
        destination.execute("BEGIN IMMEDIATE")
        applied = 0
        for path in staged_deltas:
            delta = readonly(path)
            delta_databases.append(delta)
            columns = "id,changed_columns_json," + ",".join(MUTABLE_COLUMNS)
            for row in delta.execute(f"SELECT {columns} FROM entries ORDER BY id"):
                entry_id = int(row[0])
                current = destination.execute(
                    "SELECT id,word,normalized_word,frequency_rank,source_json,scope_json "
                    "FROM entries WHERE id=?",
                    (entry_id,),
                ).fetchone()
                if current is None or canonical_identity_digest(dict(current)) != expected.get(
                    entry_id
                ):
                    raise ValueError(
                        f"canonical identity changed before delta apply: id={entry_id}"
                    )
                changed_columns = json.loads(str(row[1]))
                delta_values = dict(zip(MUTABLE_COLUMNS, tuple(row[2:])))
                baseline_row = queue.execute(
                    "SELECT " + ",".join(MUTABLE_COLUMNS)
                    + " FROM entries WHERE id=?",
                    (entry_id,),
                ).fetchone()
                if baseline_row is None:
                    raise ValueError(f"candidate baseline row is missing: id={entry_id}")
                baseline_values = dict(zip(MUTABLE_COLUMNS, tuple(baseline_row)))
                central_row = destination.execute(
                    "SELECT " + ",".join(MUTABLE_COLUMNS)
                    + " FROM entries WHERE id=?",
                    (entry_id,),
                ).fetchone()
                central_values = dict(zip(MUTABLE_COLUMNS, tuple(central_row)))
                conflicts = [
                    column
                    for column in changed_columns
                    if central_values[column] not in {
                        baseline_values[column],
                        delta_values[column],
                    }
                ]
                if conflicts:
                    raise ValueError(
                        f"repair delta three-way merge conflict: id={entry_id} "
                        f"columns={','.join(conflicts)}"
                    )
                if changed_columns:
                    update_sql = (
                        "UPDATE entries SET "
                        + ",".join(f"{column}=?" for column in changed_columns)
                        + " WHERE id=?"
                    )
                    cursor = destination.execute(
                        update_sql,
                        (*(delta_values[column] for column in changed_columns), entry_id),
                    )
                    if cursor.rowcount != 1:
                        raise ValueError(
                            f"canonical row missing during delta apply: {entry_id}"
                        )
                destination.execute(
                    "INSERT OR REPLACE INTO entries_fts("
                    "rowid,word,definition,definition_zh,examples,phrases"
                    ") SELECT id,word,definition,definition_zh,examples_json,"
                    "phrases_json FROM entries WHERE id=?",
                    (entry_id,),
                )
                applied += 1
        if applied != int(report["expectedRows"]):
            raise ValueError(
                f"repair delta apply count mismatch: expected={report['expectedRows']} "
                f"actual={applied}"
            )
        queued_terms = queue.execute(
            "SELECT canonical_id,normalized_word FROM repair_queue "
            "ORDER BY canonical_id"
        ).fetchall()
        remaining_gaps: list[str] = []
        for offset in range(0, len(queued_terms), 900):
            batch = queued_terms[offset : offset + 900]
            ids = [int(row[0]) for row in batch]
            terms = {int(row[0]): str(row[1]) for row in batch}
            placeholders = ",".join("?" for _ in ids)
            repaired = destination.execute(
                "SELECT id,pos,definition,definition_zh,us_phonetic,uk_phonetic "
                f"FROM entries WHERE id IN ({placeholders})",
                ids,
            ).fetchall()
            found = {int(row[0]) for row in repaired}
            for row in repaired:
                gaps = entry_quality_gaps(
                    terms[int(row[0])], row[2], row[3], row[4], row[5], row[1]
                )
                if gaps:
                    remaining_gaps.append(
                        f"{int(row[0])}:{','.join(gaps)}"
                    )
            remaining_gaps.extend(
                f"{entry_id}:missing" for entry_id in ids if entry_id not in found
            )
            if len(remaining_gaps) >= 20:
                break
        if remaining_gaps:
            raise ValueError(
                "merged canonical still contains repair gaps: "
                + ";".join(remaining_gaps[:20])
            )
        destination.commit()
        destination.execute("PRAGMA journal_mode=DELETE")
        destination.execute("PRAGMA optimize")
        destination.commit()
        quick_check(destination, "merged canonical")
        destination.close()
        destination = None
        with partial.open("rb") as stream:
            os.fsync(stream.fileno())
        _publish_no_replace(partial, output)
        published = True
        return {**report, "output": str(output), "appliedRows": applied}
    finally:
        for database in delta_databases:
            database.close()
        for database in (queue, destination, source):
            if database is not None:
                database.close()
        if not published:
            for suffix in ("", "-wal", "-shm"):
                try:
                    Path(str(partial) + suffix).unlink()
                except FileNotFoundError:
                    pass
        shutil.rmtree(staging, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export")
    export.add_argument("--dataset", type=Path, required=True)
    export.add_argument("--candidate", type=Path, required=True)
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--shard-index", type=int, required=True)
    export.add_argument("--shard-count", type=int, required=True)
    validate = commands.add_parser("validate-union")
    validate.add_argument("--candidate", type=Path, required=True)
    validate.add_argument("deltas", type=Path, nargs="+")
    apply = commands.add_parser("apply-union")
    apply.add_argument("--canonical", type=Path, required=True)
    apply.add_argument("--candidate", type=Path, required=True)
    apply.add_argument("--output", type=Path, required=True)
    apply.add_argument("deltas", type=Path, nargs="+")
    args = parser.parse_args()
    if args.command == "export":
        report = export_repair_delta(
            args.dataset,
            args.candidate,
            args.output,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
        )
    elif args.command == "validate-union":
        report = validate_delta_union(args.candidate, args.deltas)
    else:
        report = apply_delta_union(
            args.canonical,
            args.candidate,
            args.deltas,
            args.output,
        )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
