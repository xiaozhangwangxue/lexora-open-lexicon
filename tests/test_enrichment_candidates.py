from __future__ import annotations

import asyncio
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import enrich_oxford_scope as enrichment  # noqa: E402
from enrich_oxford_scope import fetch_candidate_batch  # noqa: E402


class _TrackedReadCursor:
    def __init__(
        self,
        cursor: sqlite3.Cursor,
        owner: "_CommitGuardConnection",
    ) -> None:
        self._cursor = cursor
        self._owner = owner
        self._closed = False
        owner.active_candidate_cursors += 1

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._cursor.fetchall()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._cursor.close()
        self._owner.active_candidate_cursors -= 1


class _CommitGuardConnection:
    """Fail a test if a candidate reader survives into a write commit."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self.active_candidate_cursors = 0
        self.commit_count = 0

    def execute(
        self,
        sql: str,
        parameters: tuple[Any, ...] | list[Any] = (),
    ) -> sqlite3.Cursor | _TrackedReadCursor:
        cursor = self._connection.execute(sql, parameters)
        if sql.startswith("SELECT id,word,normalized_word"):
            return _TrackedReadCursor(cursor, self)
        return cursor

    def commit(self) -> None:
        if self.active_candidate_cursors:
            raise AssertionError(
                "candidate read cursor crossed a write commit"
            )
        self._connection.commit()
        self.commit_count += 1

    def close(self) -> None:
        self._connection.close()


def create_database(path: str | Path = ":memory:") -> sqlite3.Connection:
    database = sqlite3.connect(path)
    database.executescript(
        """
        CREATE TABLE entries(
          id INTEGER PRIMARY KEY,
          word TEXT NOT NULL,
          normalized_word TEXT NOT NULL,
          definition TEXT NOT NULL DEFAULT '',
          definition_zh TEXT NOT NULL DEFAULT '',
          us_phonetic TEXT NOT NULL DEFAULT '',
          uk_phonetic TEXT NOT NULL DEFAULT '',
          synonyms_json TEXT NOT NULL DEFAULT '[]',
          antonyms_json TEXT NOT NULL DEFAULT '[]',
          examples_json TEXT NOT NULL DEFAULT '[]',
          phrases_json TEXT NOT NULL DEFAULT '[]',
          phrase_entries_json TEXT NOT NULL DEFAULT '[]',
          related_words_json TEXT NOT NULL DEFAULT '[]',
          related_entries_json TEXT NOT NULL DEFAULT '[]',
          frequency REAL NOT NULL DEFAULT 0,
          frequency_rank INTEGER NOT NULL,
          difficulty TEXT NOT NULL DEFAULT '',
          enrichment_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX idx_entries_freq
          ON entries(frequency_rank, frequency);
        CREATE TABLE entries_fts(
          word TEXT,
          definition TEXT,
          definition_zh TEXT,
          examples TEXT,
          phrases TEXT
        );
        """
    )
    # Deliberately make frequency priority differ from ID order, and include a
    # duplicate rank to exercise the ID tie-breaker used by the keyset.
    ranks = (6, 2, 1, 5, 2, 4)
    database.executemany(
        """
        INSERT INTO entries(
          id,word,normalized_word,frequency_rank,enrichment_json
        ) VALUES(?,?,?,?,?)
        """,
        [
            (entry_id, f"word-{entry_id}", f"word-{entry_id}", rank, "{}")
            for entry_id, rank in enumerate(ranks, 1)
        ],
    )
    database.commit()
    return database


def build_database() -> _CommitGuardConnection:
    database = create_database()
    return _CommitGuardConnection(database)


class CandidateBatchTest(unittest.TestCase):
    def test_frequency_keyset_closes_reader_before_each_write_commit(
        self,
    ) -> None:
        database = build_database()
        try:
            first, rank, entry_id = fetch_candidate_batch(
                database,  # type: ignore[arg-type]
                start_id=2,
                end_id=6,
                frequency_first=True,
                after_frequency_rank=None,
                after_id=None,
                batch_size=2,
            )
            self.assertEqual([row[0] for row in first], [3, 2])
            self.assertEqual((rank, entry_id), (2, 2))
            self.assertEqual(database.active_candidate_cursors, 0)

            database.execute(
                "UPDATE entries SET enrichment_json=? WHERE id=?",
                ('{"status":"completed"}', 3),
            )
            database.commit()

            second, rank, entry_id = fetch_candidate_batch(
                database,  # type: ignore[arg-type]
                start_id=2,
                end_id=6,
                frequency_first=True,
                after_frequency_rank=rank,
                after_id=entry_id,
                batch_size=2,
            )
            self.assertEqual([row[0] for row in second], [5, 6])
            self.assertEqual((rank, entry_id), (4, 6))
            self.assertEqual(database.active_candidate_cursors, 0)

            database.execute(
                "UPDATE entries SET enrichment_json=? WHERE id=?",
                ('{"status":"completed"}', 5),
            )
            database.commit()

            third, rank, entry_id = fetch_candidate_batch(
                database,  # type: ignore[arg-type]
                start_id=2,
                end_id=6,
                frequency_first=True,
                after_frequency_rank=rank,
                after_id=entry_id,
                batch_size=2,
            )
            self.assertEqual([row[0] for row in third], [4])
            self.assertEqual((rank, entry_id), (5, 4))
            self.assertEqual(database.active_candidate_cursors, 0)
            self.assertEqual(database.commit_count, 2)
        finally:
            database.close()

    def test_id_keyset_preserves_bounds_across_batches(self) -> None:
        database = build_database()
        try:
            first, rank, entry_id = fetch_candidate_batch(
                database,  # type: ignore[arg-type]
                start_id=2,
                end_id=5,
                frequency_first=False,
                after_frequency_rank=None,
                after_id=None,
                batch_size=2,
            )
            self.assertEqual([row[0] for row in first], [2, 3])
            self.assertIsNone(rank)
            self.assertEqual(entry_id, 3)
            self.assertEqual(database.active_candidate_cursors, 0)

            database.execute(
                "UPDATE entries SET enrichment_json=? WHERE id=?",
                ('{"status":"completed"}', 2),
            )
            database.commit()

            second, rank, entry_id = fetch_candidate_batch(
                database,  # type: ignore[arg-type]
                start_id=2,
                end_id=5,
                frequency_first=False,
                after_frequency_rank=rank,
                after_id=entry_id,
                batch_size=2,
            )
            self.assertEqual([row[0] for row in second], [4, 5])
            self.assertIsNone(rank)
            self.assertEqual(entry_id, 5)
            self.assertEqual(database.active_candidate_cursors, 0)
        finally:
            database.close()

    def test_closed_pages_do_not_block_real_wal_checkpoints(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            dataset = Path(directory) / "wal.sqlite"
            database = create_database(dataset)
            self.assertEqual(
                database.execute("PRAGMA journal_mode=WAL").fetchone()[0],
                "wal",
            )
            checkpoint = sqlite3.connect(dataset)
            rank: int | None = None
            entry_id: int | None = None
            try:
                for _ in range(2):
                    rows, rank, entry_id = fetch_candidate_batch(
                        database,
                        start_id=1,
                        end_id=6,
                        frequency_first=True,
                        after_frequency_rank=rank,
                        after_id=entry_id,
                        batch_size=2,
                    )
                    self.assertEqual(len(rows), 2)
                    database.execute(
                        "UPDATE entries SET enrichment_json=? WHERE id=?",
                        ('{"status":"completed"}', rows[0][0]),
                    )
                    database.commit()
                    busy, _, _ = checkpoint.execute(
                        "PRAGMA wal_checkpoint(TRUNCATE)"
                    ).fetchone()
                    self.assertEqual(
                        busy,
                        0,
                        "a completed candidate page pinned the WAL",
                    )
            finally:
                checkpoint.close()
                database.close()

    def test_rejects_non_positive_batch_size(self) -> None:
        database = build_database()
        try:
            with self.assertRaisesRegex(ValueError, "batch_size"):
                fetch_candidate_batch(
                    database,  # type: ignore[arg-type]
                    start_id=None,
                    end_id=None,
                    frequency_first=False,
                    after_frequency_rank=None,
                    after_id=None,
                    batch_size=0,
                )
        finally:
            database.close()

    def test_run_keeps_frequency_priority_and_restart_markers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            state = root / "state.sqlite"
            database = create_database(dataset)
            database.execute(
                """
                UPDATE entries SET
                  definition='complete definition',
                  definition_zh='完整释义',
                  us_phonetic='wɜːd',
                  uk_phonetic='wɜːd',
                  frequency=5.0
                """
            )
            database.commit()
            database.close()

            attempted: list[str] = []

            async def fake_enrich_term(
                *args: Any,
                **kwargs: Any,
            ) -> dict[str, Any]:
                attempted.append(str(args[2]))
                return {"_statuses": [], "_attempted": []}

            with patch.object(
                enrichment,
                "enrich_term",
                new=fake_enrich_term,
            ):
                asyncio.run(
                    enrichment.run(
                        dataset,
                        state,
                        2,
                        0,
                        2,
                        0,
                        24,
                        None,
                        None,
                        0,
                        1,
                    )
                )
                self.assertEqual(attempted, ["word-3", "word-2"])

                # A fresh invocation starts its keyset from the shard
                # beginning, skips durable completion markers, and continues
                # in frequency order without losing the duplicate rank.
                asyncio.run(
                    enrichment.run(
                        dataset,
                        state,
                        0,
                        0,
                        2,
                        0,
                        24,
                        None,
                        None,
                        0,
                        1,
                    )
                )

            self.assertEqual(
                attempted,
                [
                    "word-3",
                    "word-2",
                    "word-5",
                    "word-6",
                    "word-4",
                    "word-1",
                ],
            )
            database = sqlite3.connect(dataset)
            statuses = database.execute(
                """
                SELECT json_extract(enrichment_json, '$.status')
                FROM entries ORDER BY id
                """
            ).fetchall()
            database.close()
            self.assertEqual(statuses, [("completed",)] * 6)

    def test_run_keeps_calculated_shard_bounds(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            state = root / "state.sqlite"
            database = create_database(dataset)
            database.execute(
                """
                UPDATE entries SET
                  definition='complete definition',
                  definition_zh='完整释义',
                  us_phonetic='wɜːd',
                  uk_phonetic='wɜːd',
                  frequency=5.0
                """
            )
            database.commit()
            database.close()
            attempted: list[str] = []

            async def fake_enrich_term(
                *args: Any,
                **kwargs: Any,
            ) -> dict[str, Any]:
                attempted.append(str(args[2]))
                return {"_statuses": [], "_attempted": []}

            with patch.object(
                enrichment,
                "enrich_term",
                new=fake_enrich_term,
            ):
                asyncio.run(
                    enrichment.run(
                        dataset,
                        state,
                        0,
                        0,
                        2,
                        0,
                        24,
                        None,
                        None,
                        1,
                        2,
                    )
                )

            self.assertEqual(attempted, ["word-5", "word-6", "word-4"])
            database = sqlite3.connect(dataset)
            statuses = database.execute(
                """
                SELECT id,json_extract(enrichment_json, '$.status')
                FROM entries ORDER BY id
                """
            ).fetchall()
            database.close()
            self.assertEqual(
                statuses,
                [
                    (1, None),
                    (2, None),
                    (3, None),
                    (4, "completed"),
                    (5, "completed"),
                    (6, "completed"),
                ],
            )


if __name__ == "__main__":
    unittest.main()
