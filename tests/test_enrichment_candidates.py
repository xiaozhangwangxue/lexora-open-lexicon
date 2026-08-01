from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import enrich_oxford_scope as enrichment  # noqa: E402
from enrich_oxford_scope import fetch_candidate_batch  # noqa: E402


def edge_payload(
    terms: list[str],
    profile: str = "core",
    *,
    needs: dict[str, dict[str, bool]] | None = None,
) -> dict[str, Any]:
    if needs is None:
        needs = {
            term: {
                "definition": True,
                "pos": True,
                "phonetic": True,
                "examples": True,
                "frequency": True,
                "deep": profile == "deep",
                "synonyms": profile == "deep",
                "antonyms": profile == "deep",
                "phrases": profile == "deep",
                "related": profile == "deep",
                "usPhonetic": True,
                "ukPhonetic": True,
            }
            for term in terms
        }
    return {"terms": terms, "profile": profile, "needs": needs}


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
          pos TEXT NOT NULL DEFAULT '',
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
    def test_core_enrichment_skips_network_for_optional_empty_lists(
        self,
    ) -> None:
        class UnexpectedBatcher:
            async def request(self, term: str) -> tuple[Any, int, None]:
                raise AssertionError(f"unexpected network request for {term}")

        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as temp_dir:
                state = enrichment.init_state(
                    Path(temp_dir) / "state.sqlite"
                )
                try:
                    async with httpx.AsyncClient() as client:
                        with patch.object(
                            enrichment,
                            "EDGE_BASE",
                            "https://edge.example",
                        ):
                            result = await enrichment.enrich_term(
                                client,
                                {
                                    "edge": enrichment.HostGate(0),
                                    "dictionary": enrichment.HostGate(0),
                                    "datamuse": enrichment.HostGate(0),
                                },
                                "word",
                                state,
                                {
                                    "definition": "A unit of language.",
                                    "pos": "noun",
                                    "us": "wɝːd",
                                    "uk": "wɜːd",
                                    "examples": ["This is a word."],
                                    "frequency": 5.0,
                                    "phrases": [],
                                    "synonyms": [],
                                    "antonyms": [],
                                    "related": [],
                                },
                                edge_batcher=UnexpectedBatcher(),
                                profile="core",
                            )
                    self.assertEqual(result["_attempted"], [])
                finally:
                    state.close()

        asyncio.run(scenario())

    def test_deep_enrichment_requests_missing_relationships(self) -> None:
        class RecordingBatcher:
            def __init__(self) -> None:
                self.terms: list[str] = []
                self.needs: list[dict[str, bool]] = []

            async def request(
                self,
                term: str,
                needs: dict[str, bool],
            ) -> tuple[Any, int, None]:
                self.terms.append(term)
                self.needs.append(needs)
                return (
                    {
                        "dictionary": None,
                        "exact": [],
                        "related": [
                            {
                                "word": "written word",
                                "defs": ["n\tA written or printed term."],
                            }
                        ],
                        "synonyms": [{"word": "term"}],
                        "antonyms": [{"word": "silence"}],
                        "_providers": {
                            name: {
                                "ok": True,
                                "status": 200,
                                "found": name != "dictionary",
                            }
                            for name in (
                                "dictionary",
                                "exact",
                                "related",
                                "synonyms",
                                "antonyms",
                            )
                        },
                        "_found": True,
                        "_profile": "deep",
                    },
                    200,
                    None,
                )

        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as temp_dir:
                state = enrichment.init_state(
                    Path(temp_dir) / "state.sqlite"
                )
                batcher = RecordingBatcher()
                try:
                    async with httpx.AsyncClient() as client:
                        with patch.object(
                            enrichment,
                            "EDGE_BASE",
                            "https://edge.example",
                        ):
                            result = await enrichment.enrich_term(
                                client,
                                {
                                    "edge": enrichment.HostGate(0),
                                    "dictionary": enrichment.HostGate(0),
                                    "datamuse": enrichment.HostGate(0),
                                },
                                "word",
                                state,
                                {
                                    "definition": "A unit of language.",
                                    "pos": "noun",
                                    "us": "wɝːd",
                                    "uk": "wɜːd",
                                    "examples": ["This is a word."],
                                    "frequency": 5.0,
                                    "phrases": [],
                                    "synonyms": [],
                                    "antonyms": [],
                                    "related": [],
                                },
                                edge_batcher=batcher,
                                profile="deep",
                            )
                    self.assertEqual(batcher.terms, ["word"])
                    self.assertEqual(
                        batcher.needs,
                        [
                            {
                                "definition": False,
                                "pos": False,
                                "phonetic": False,
                                "examples": False,
                                "frequency": False,
                                "deep": True,
                                "synonyms": True,
                                "antonyms": True,
                                "phrases": True,
                                "related": True,
                                "usPhonetic": False,
                                "ukPhonetic": False,
                            }
                        ],
                    )
                    self.assertEqual(result["_attempted"], ["edge"])
                    self.assertEqual(result["synonyms"], ["term"])
                    self.assertEqual(result["antonyms"], ["silence"])
                    self.assertEqual(result["phrases"], ["written word"])
                finally:
                    state.close()

        asyncio.run(scenario())

    def test_deep_enrichment_sends_only_each_rows_missing_relationships(
        self,
    ) -> None:
        class RecordingBatcher:
            def __init__(self) -> None:
                self.needs: list[dict[str, bool]] = []

            async def request(
                self,
                term: str,
                needs: dict[str, bool],
            ) -> tuple[Any, int, None]:
                self.needs.append(needs)
                return (
                    {
                        "related": [{"word": "spoken word"}],
                        "antonyms": [{"word": "silence"}],
                        "_providers": {
                            "related": {
                                "ok": True,
                                "status": 200,
                                "found": True,
                            },
                            "antonyms": {
                                "ok": True,
                                "status": 200,
                                "found": True,
                            },
                        },
                        "_found": True,
                        "_complete": True,
                        "_profile": "deep",
                    },
                    200,
                    None,
                )

        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as temp_dir:
                state = enrichment.init_state(
                    Path(temp_dir) / "state.sqlite"
                )
                batcher = RecordingBatcher()
                try:
                    async with httpx.AsyncClient() as client:
                        with patch.object(
                            enrichment,
                            "EDGE_BASE",
                            "https://edge.example",
                        ):
                            result = await enrichment.enrich_term(
                                client,
                                {
                                    "edge": enrichment.HostGate(0),
                                    "dictionary": enrichment.HostGate(0),
                                    "datamuse": enrichment.HostGate(0),
                                },
                                "word",
                                state,
                                {
                                    "definition": "A unit of language.",
                                    "pos": "noun",
                                    "us": "wɝːd",
                                    "uk": "wɜːd",
                                    "examples": ["This is a word."],
                                    "frequency": 5.0,
                                    "phrases": ["word for word"],
                                    "synonyms": ["term"],
                                    "antonyms": [],
                                    "related": [],
                                },
                                edge_batcher=batcher,
                                profile="deep",
                            )
                    self.assertEqual(
                        batcher.needs,
                        [
                            {
                                "definition": False,
                                "pos": False,
                                "phonetic": False,
                                "examples": False,
                                "frequency": False,
                                "deep": True,
                                "synonyms": False,
                                "antonyms": True,
                                "phrases": False,
                                "related": True,
                                "usPhonetic": False,
                                "ukPhonetic": False,
                            }
                        ],
                    )
                    self.assertNotIn("synonyms", result)
                    self.assertEqual(result["antonyms"], ["silence"])
                    self.assertEqual(result["related"], ["spoken word"])
                finally:
                    state.close()

        asyncio.run(scenario())

    def test_cli_forwards_core_and_deep_profiles(self) -> None:
        captured: list[str] = []

        async def fake_run(*args: Any, **kwargs: Any) -> None:
            del args
            captured.append(str(kwargs["profile"]))

        with patch.object(enrichment, "run", new=fake_run):
            with patch.object(
                sys,
                "argv",
                ["enrich_oxford_scope.py"],
            ):
                enrichment.main()
            with patch.object(
                sys,
                "argv",
                ["enrich_oxford_scope.py", "--profile", "deep"],
            ):
                enrichment.main()
            with patch.object(
                sys,
                "argv",
                ["enrich_oxford_scope.py", "--profile", "auto"],
            ):
                enrichment.main()

        self.assertEqual(captured, ["core", "deep", "core", "deep"])

    def test_edge_provider_metadata_marks_partial_success(self) -> None:
        class PartialBatcher:
            async def request(
                self,
                term: str,
                needs: dict[str, bool],
            ) -> tuple[Any, int, None]:
                self.assert_needs = needs
                return (
                    {
                        "dictionary": [
                            {
                                "word": term,
                                "phonetics": [],
                                "meanings": [
                                    {
                                        "partOfSpeech": "noun",
                                        "definitions": [
                                            {
                                                "definition":
                                                    "A unit of language."
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                        "exact": None,
                        "_providers": {
                            "dictionary": {
                                "ok": True,
                                "status": 200,
                                "found": True,
                            },
                            "exact": {
                                "ok": False,
                                "status": 503,
                                "found": False,
                            },
                        },
                        "_found": True,
                    },
                    200,
                    None,
                )

        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as temp_dir:
                state = enrichment.init_state(
                    Path(temp_dir) / "state.sqlite"
                )
                try:
                    async with httpx.AsyncClient() as client:
                        with patch.object(
                            enrichment,
                            "EDGE_BASE",
                            "https://edge.example",
                        ):
                            result = await enrichment.enrich_term(
                                client,
                                {
                                    "edge": enrichment.HostGate(0),
                                    "dictionary": enrichment.HostGate(0),
                                    "datamuse": enrichment.HostGate(0),
                                },
                                "word",
                                state,
                                {
                                    "definition": "",
                                    "us": "",
                                    "uk": "",
                                    "examples": [],
                                    "frequency": 0,
                                    "phrases": [],
                                    "synonyms": [],
                                    "antonyms": [],
                                    "related": [],
                                },
                                edge_batcher=PartialBatcher(),
                            )
                    self.assertEqual(result["_statuses"], ["partial"])
                    self.assertEqual(
                        state.execute(
                            """
                            SELECT status FROM provider_state
                            WHERE term='word' AND source='edge'
                            """
                        ).fetchone(),
                        ("partial",),
                    )
                finally:
                    state.close()

        asyncio.run(scenario())

    def test_translation_batcher_keeps_all_ordered_chunks(self) -> None:
        async def scenario() -> None:
            source = " ".join(
                f"segment-{index:04d}" for index in range(1_500)
            )
            expected_chunks = enrichment.translation_chunks(source)
            self.assertGreater(len(expected_chunks), 32)
            posted_texts: list[str] = []

            def handler(request: httpx.Request) -> httpx.Response:
                payload = json.loads(request.content)
                texts = payload["texts"]
                self.assertLessEqual(len(texts), 32)
                self.assertTrue(all(0 < len(text) <= 480 for text in texts))
                posted_texts.extend(texts)
                return httpx.Response(
                    200,
                    json={
                        "translations": [
                            f"译文-{len(posted_texts) - len(texts) + index}"
                            for index in range(len(texts))
                        ]
                    },
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                batcher = enrichment.EdgeTranslationBatcher(
                    client,
                    enrichment.HostGate(0),
                    batch_size=1,
                    flush_delay=0,
                )
                with patch.object(
                    enrichment,
                    "EDGE_BASE",
                    "https://edge.example",
                ):
                    translated, status, error = await batcher.request(source)

            self.assertEqual(posted_texts, expected_chunks)
            self.assertEqual(
                " ".join(posted_texts),
                " ".join(source.split()),
            )
            self.assertEqual(len(translated.splitlines()), len(expected_chunks))
            self.assertEqual(status, 200)
            self.assertIsNone(error)

        asyncio.run(scenario())

    def test_edge_batch_missing_term_is_retryable_failure(self) -> None:
        async def scenario() -> None:
            request_count = 0

            def handler(request: httpx.Request) -> httpx.Response:
                nonlocal request_count
                request_count += 1
                self.assertEqual(
                    request.url,
                    httpx.URL(
                        "https://edge.example/api/dictionary/batch"
                    ),
                )
                self.assertEqual(
                    json.loads(request.content),
                    edge_payload(["word", "e.g."]),
                )
                results = {
                    "word": {
                        "status": 200,
                        "data": {"dictionary": []},
                    }
                }
                if request_count > 1:
                    results["e.g."] = {
                        "status": 200,
                        "data": {"dictionary": []},
                    }
                return httpx.Response(
                    200,
                    json={"results": results},
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                batcher = enrichment.EdgeDictionaryBatcher(
                    client,
                    enrichment.HostGate(0),
                    batch_size=8,
                    flush_delay=0,
                )
                with patch.object(
                    enrichment,
                    "EDGE_BASE",
                    "https://edge.example",
                ):
                    found, missing = await asyncio.gather(
                        batcher.request("word"),
                        batcher.request("e.g."),
                    )

            self.assertEqual(found, ({"dictionary": []}, 200, None))
            self.assertEqual(
                missing,
                ({"dictionary": []}, 200, None),
            )
            self.assertEqual(request_count, 2)

        asyncio.run(scenario())

    def test_edge_batch_sends_deep_profile(self) -> None:
        async def scenario() -> None:
            def handler(request: httpx.Request) -> httpx.Response:
                self.assertEqual(
                    json.loads(request.content),
                    edge_payload(["word"], "deep"),
                )
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "word": {
                                "status": 200,
                                "data": {"_profile": "deep"},
                            }
                        }
                    },
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                batcher = enrichment.EdgeDictionaryBatcher(
                    client,
                    enrichment.HostGate(0),
                    batch_size=1,
                    flush_delay=0,
                    profile="deep",
                )
                with patch.object(
                    enrichment,
                    "EDGE_BASE",
                    "https://edge.example",
                ):
                    result = await batcher.request("word")

            self.assertEqual(result, ({"_profile": "deep"}, 200, None))

        asyncio.run(scenario())

    def test_edge_batch_keeps_successes_when_one_item_is_transient(self) -> None:
        async def scenario() -> None:
            def handler(request: httpx.Request) -> httpx.Response:
                self.assertEqual(
                    json.loads(request.content),
                    edge_payload(["word", "phrase"]),
                )
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "word": {
                                "status": 200,
                                "data": {"dictionary": []},
                            },
                            "phrase": {
                                "status": 504,
                                "data": {
                                    "error": "providers temporarily unavailable"
                                },
                            },
                        }
                    },
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                batcher = enrichment.EdgeDictionaryBatcher(
                    client,
                    enrichment.HostGate(0),
                    batch_size=8,
                    flush_delay=0,
                )
                with patch.object(
                    enrichment,
                    "EDGE_BASE",
                    "https://edge.example",
                ):
                    found, transient = await asyncio.gather(
                        batcher.request("word"),
                        batcher.request("phrase"),
                    )

            self.assertEqual(found, ({"dictionary": []}, 200, None))
            self.assertEqual(
                transient,
                (None, 504, "providers temporarily unavailable"),
            )

        asyncio.run(scenario())

    def test_edge_batch_sends_each_terms_missing_field_needs(self) -> None:
        async def scenario() -> None:
            posted: list[dict[str, Any]] = []

            def handler(request: httpx.Request) -> httpx.Response:
                posted.append(json.loads(request.content))
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            term: {
                                "status": 200,
                                "data": {"dictionary": []},
                            }
                            for term in ("definition-gap", "frequency-gap")
                        }
                    },
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                batcher = enrichment.EdgeDictionaryBatcher(
                    client,
                    enrichment.HostGate(0),
                    batch_size=8,
                    flush_delay=0,
                )
                with patch.object(
                    enrichment,
                    "EDGE_BASE",
                    "https://edge.example",
                ):
                    await asyncio.gather(
                        batcher.request(
                            "definition-gap",
                            {
                                "definition": True,
                                "phonetic": True,
                                "usPhonetic": True,
                            },
                        ),
                        batcher.request(
                            "frequency-gap",
                            {"frequency": True},
                        ),
                    )

            self.assertEqual(
                posted,
                [
                    edge_payload(
                        ["definition-gap", "frequency-gap"],
                        needs={
                            "definition-gap": {
                                "definition": True,
                                "pos": False,
                                "phonetic": True,
                                "examples": False,
                                "frequency": False,
                                "deep": False,
                                "synonyms": False,
                                "antonyms": False,
                                "phrases": False,
                                "related": False,
                                "usPhonetic": True,
                                "ukPhonetic": False,
                            },
                            "frequency-gap": {
                                "definition": False,
                                "pos": False,
                                "phonetic": False,
                                "examples": False,
                                "frequency": True,
                                "deep": False,
                                "synonyms": False,
                                "antonyms": False,
                                "phrases": False,
                                "related": False,
                                "usPhonetic": False,
                                "ukPhonetic": False,
                            },
                        },
                    )
                ],
            )

        asyncio.run(scenario())

    def test_edge_batch_retries_same_terms_and_honors_retry_after(
        self,
    ) -> None:
        async def scenario() -> None:
            requests: list[dict[str, Any]] = []
            sleeps: list[float] = []

            def handler(request: httpx.Request) -> httpx.Response:
                requests.append(json.loads(request.content))
                if len(requests) == 1:
                    return httpx.Response(
                        429,
                        headers={"Retry-After": "2"},
                    )
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "word": {
                                "status": 200,
                                "data": {"dictionary": []},
                            }
                        }
                    },
                )

            async def fake_sleep(delay: float) -> None:
                sleeps.append(delay)

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                batcher = enrichment.EdgeDictionaryBatcher(
                    client,
                    enrichment.HostGate(0),
                    batch_size=1,
                    flush_delay=0,
                )
                with (
                    patch.object(
                        enrichment,
                        "EDGE_BASE",
                        "https://edge.example",
                    ),
                    patch.object(
                        enrichment.asyncio,
                        "sleep",
                        new=fake_sleep,
                    ),
                    patch.object(
                        enrichment.random,
                        "uniform",
                        return_value=0.25,
                    ),
                ):
                    result = await batcher.request("word")

            self.assertEqual(
                requests,
                [
                    edge_payload(["word"]),
                    edge_payload(["word"]),
                ],
            )
            self.assertEqual(sleeps, [2.25])
            self.assertEqual(result, ({"dictionary": []}, 200, None))

        asyncio.run(scenario())

    def test_edge_batch_exhausts_finite_5xx_retries(self) -> None:
        async def scenario() -> None:
            requests: list[dict[str, Any]] = []
            sleeps: list[float] = []

            def handler(request: httpx.Request) -> httpx.Response:
                requests.append(json.loads(request.content))
                return httpx.Response(503)

            async def fake_sleep(delay: float) -> None:
                sleeps.append(delay)

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                batcher = enrichment.EdgeDictionaryBatcher(
                    client,
                    enrichment.HostGate(0),
                    batch_size=1,
                    flush_delay=0,
                    max_attempts=3,
                )
                with (
                    patch.object(
                        enrichment,
                        "EDGE_BASE",
                        "https://edge.example",
                    ),
                    patch.object(
                        enrichment.asyncio,
                        "sleep",
                        new=fake_sleep,
                    ),
                    patch.object(
                        enrichment.random,
                        "uniform",
                        return_value=0.0,
                    ),
                ):
                    outcomes = await asyncio.gather(
                        batcher.request("word"),
                        return_exceptions=True,
                    )

            self.assertEqual(
                requests,
                [edge_payload(["word"])] * 3,
            )
            self.assertEqual(sleeps, [1.0, 2.0])
            failure = outcomes[0]
            self.assertIsInstance(
                failure,
                enrichment.EdgeBatchRetryExhausted,
            )
            assert isinstance(
                failure,
                enrichment.EdgeBatchRetryExhausted,
            )
            self.assertEqual(failure.status, 503)
            self.assertEqual(failure.attempts, 3)

        asyncio.run(scenario())

    def test_edge_batch_continues_queued_work_after_transient_5xx(self) -> None:
        async def scenario() -> None:
            requests: list[dict[str, Any]] = []

            def handler(request: httpx.Request) -> httpx.Response:
                payload = json.loads(request.content)
                requests.append(payload)
                if payload["terms"] == ["word"]:
                    return httpx.Response(504)
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "phrase": {
                                "status": 200,
                                "data": {"dictionary": []},
                            }
                        }
                    },
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                batcher = enrichment.EdgeDictionaryBatcher(
                    client,
                    enrichment.HostGate(0),
                    batch_size=1,
                    flush_delay=0,
                    max_attempts=1,
                )
                with patch.object(
                    enrichment,
                    "EDGE_BASE",
                    "https://edge.example",
                ):
                    outcomes = await asyncio.gather(
                        batcher.request("word"),
                        batcher.request("phrase"),
                        return_exceptions=True,
                    )

            self.assertEqual(
                requests,
                [edge_payload(["word"]), edge_payload(["phrase"])],
            )
            self.assertIsInstance(
                outcomes[0],
                enrichment.EdgeBatchRetryExhausted,
            )
            self.assertEqual(
                outcomes[1],
                ({"dictionary": []}, 200, None),
            )

        asyncio.run(scenario())

    def test_edge_long_retry_after_aborts_all_queued_terms(self) -> None:
        async def scenario() -> None:
            requests: list[dict[str, Any]] = []

            def handler(request: httpx.Request) -> httpx.Response:
                requests.append(json.loads(request.content))
                return httpx.Response(
                    429,
                    headers={"Retry-After": "3600"},
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                batcher = enrichment.EdgeDictionaryBatcher(
                    client,
                    enrichment.HostGate(0),
                    batch_size=1,
                    flush_delay=0,
                )
                with patch.object(
                    enrichment,
                    "EDGE_BASE",
                    "https://edge.example",
                ):
                    outcomes = await asyncio.gather(
                        batcher.request("word"),
                        batcher.request("phrase"),
                        return_exceptions=True,
                    )

            self.assertEqual(
                requests,
                [edge_payload(["word"])],
            )
            self.assertTrue(
                all(
                    isinstance(
                        outcome,
                        enrichment.EdgeBatchRetryExhausted,
                    )
                    for outcome in outcomes
                )
            )
            self.assertEqual(
                [
                    outcome.retry_after
                    for outcome in outcomes
                    if isinstance(
                        outcome,
                        enrichment.EdgeBatchRetryExhausted,
                    )
                ],
                [3600.0, 3600.0],
            )

        asyncio.run(scenario())

    def test_retry_after_http_date_is_supported(self) -> None:
        reference = enrichment.dt.datetime(
            2026,
            7,
            29,
            12,
            0,
            tzinfo=enrichment.dt.timezone.utc,
        )
        self.assertEqual(
            enrichment.retry_after_seconds(
                "Wed, 29 Jul 2026 12:00:30 GMT",
                current_time=reference,
            ),
            30.0,
        )

    def test_rate_limited_batch_does_not_mark_or_advance_entry(self) -> None:
        class TrackingConnection(sqlite3.Connection):
            closed_by_run = False

            def close(self) -> None:
                self.closed_by_run = True
                super().close()

        async def fail_request(
            batcher: enrichment.EdgeDictionaryBatcher,
            term: str,
            needs: dict[str, bool],
        ) -> tuple[Any | None, int | None, str | None]:
            del batcher, term, needs
            raise enrichment.EdgeBatchRetryExhausted(
                status=429,
                attempts=1,
                retry_after=3600.0,
                detail="HTTP 429",
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            state_path = root / "state.sqlite"
            database = create_database(dataset)
            database.close()
            tracked_connections: list[TrackingConnection] = []
            original_connect = sqlite3.connect

            def tracking_connect(
                *args: Any,
                **kwargs: Any,
            ) -> TrackingConnection:
                kwargs["factory"] = TrackingConnection
                connection = original_connect(*args, **kwargs)
                tracked_connections.append(connection)
                return connection

            with (
                patch.object(
                    enrichment,
                    "EDGE_BASE",
                    "https://edge.example",
                ),
                patch.object(
                    enrichment.EdgeDictionaryBatcher,
                    "request",
                    new=fail_request,
                ),
                patch.object(
                    enrichment.sqlite3,
                    "connect",
                    side_effect=tracking_connect,
                ),
            ):
                with self.assertRaises(
                    enrichment.EdgeBatchRetryExhausted
                ):
                    asyncio.run(
                        enrichment.run(
                            dataset,
                            state_path,
                            1,
                            0,
                            1,
                            0,
                            24,
                            1,
                            1,
                            None,
                            1,
                        )
                    )
            self.assertEqual(len(tracked_connections), 2)
            self.assertTrue(
                all(
                    connection.closed_by_run
                    for connection in tracked_connections
                )
            )

            database = sqlite3.connect(dataset)
            marker = database.execute(
                """
                SELECT enrichment_json FROM entries WHERE id=1
                """
            ).fetchone()
            database.close()
            self.assertEqual(marker, ("{}",))

            state = sqlite3.connect(state_path)
            provider_rows = state.execute(
                "SELECT COUNT(*) FROM provider_state"
            ).fetchone()
            state.close()
            self.assertEqual(provider_rows, (0,))

    def test_datamuse_ipa_is_used_when_dictionary_has_no_phonetic(
        self,
    ) -> None:
        fields = enrichment.edge_fields(
            {
                "dictionary": None,
                "exact": [
                    {
                        "word": "word",
                        "tags": ["n", "ipa_pron:wˈɝd", "f:147.674682"],
                        "defs": ["n\tA unit of language."],
                    }
                ],
                "related": [],
                "synonyms": [],
                "antonyms": [],
            },
            "word",
        )
        self.assertEqual(fields["us"], "wˈɝd")
        self.assertEqual(fields.get("uk", ""), "")
        self.assertEqual(fields["definition"], "n A unit of language.")
        self.assertEqual(fields["pos"], "noun")

    def test_core_edge_preserves_dictionary_relationships(self) -> None:
        fields = enrichment.edge_fields(
            {
                "dictionary": [
                    {
                        "word": "word",
                        "meanings": [
                            {
                                "partOfSpeech": "noun",
                                "synonyms": ["term", "expression"],
                                "antonyms": ["silence"],
                                "definitions": [
                                    {
                                        "definition":
                                            "A unit of language."
                                    }
                                ],
                            }
                        ],
                    }
                ],
                "exact": [],
                "_profile": "core",
            },
            "word",
        )

        self.assertEqual(fields["synonyms"], ["term", "expression"])
        self.assertEqual(fields["antonyms"], ["silence"])
        self.assertNotIn("related", fields)
        self.assertNotIn("phrases", fields)
        self.assertNotIn("related_entries", fields)
        self.assertNotIn("phrase_entries", fields)

    def test_deep_edge_merges_dictionary_and_datamuse_relationships(
        self,
    ) -> None:
        fields = enrichment.edge_fields(
            {
                "dictionary": [
                    {
                        "word": "word",
                        "meanings": [
                            {
                                "partOfSpeech": "noun",
                                "synonyms": ["term"],
                                "antonyms": ["silence"],
                                "definitions": [],
                            }
                        ],
                    }
                ],
                "exact": [],
                "related": [],
                "synonyms": [
                    {"word": "term"},
                    {"word": "expression"},
                ],
                "antonyms": [
                    {"word": "silence"},
                    {"word": "gesture"},
                ],
                "_profile": "deep",
            },
            "word",
        )

        self.assertEqual(fields["synonyms"], ["term", "expression"])
        self.assertEqual(fields["antonyms"], ["silence", "gesture"])

    def test_dictionary_dialect_phonetics_override_datamuse_fallback(
        self,
    ) -> None:
        fields = enrichment.edge_fields(
            {
                "dictionary": [
                    {
                        "word": "word",
                        "phonetics": [
                            {
                                "text": "/wɝːd/",
                                "audio": "https://example.test/word-us.mp3",
                            },
                            {
                                "text": "/wɜːd/",
                                "audio": "https://example.test/word-uk.mp3",
                            },
                        ],
                        "meanings": [],
                    }
                ],
                "exact": [
                    {
                        "word": "word",
                        "tags": ["ipa_pron:wˈɝd"],
                    }
                ],
                "related": [],
                "synonyms": [],
                "antonyms": [],
            },
            "word",
        )
        self.assertEqual(fields["us"], "wɝːd")
        self.assertEqual(fields["uk"], "wɜːd")

    def test_dictionary_generic_phonetic_is_not_faked_as_two_dialects(
        self,
    ) -> None:
        generic = enrichment.dictionary_fields(
            [
                {
                    "word": "either",
                    "phonetic": "/ˈaɪðə/",
                    "phonetics": [
                        {
                            "text": "/ˈaɪðə/",
                            "audio": "",
                        }
                    ],
                    "meanings": [],
                }
            ]
        )
        self.assertEqual(generic["generic_phonetic"], "ˈaɪðə")
        self.assertEqual(generic["us"], "")
        self.assertEqual(generic["uk"], "")

        us_only = enrichment.dictionary_fields(
            [
                {
                    "word": "word",
                    "phonetics": [
                        {
                            "text": "/wɝːd/",
                            "audio": "https://example.test/word-us.mp3",
                        }
                    ],
                    "meanings": [],
                }
            ]
        )
        self.assertEqual(us_only["us"], "wɝːd")
        self.assertEqual(us_only["uk"], "")

    def test_datamuse_exact_rejects_fuzzy_word_data(self) -> None:
        fields = enrichment.edge_fields(
            {
                "dictionary": None,
                "exact": [
                    {
                        "word": "regrowing",
                        "tags": [
                            "v",
                            "ipa_pron:rigrˈoʊɪŋ",
                            "f:0.25",
                        ],
                        "defs": ["v\tTo grow again."],
                    }
                ],
                "related": [
                    {
                        "word": "growing",
                        "tags": ["f:5000"],
                    }
                ],
                "synonyms": [],
                "antonyms": [],
            },
            "peagrowing",
        )
        self.assertEqual(fields.get("us", ""), "")
        self.assertEqual(fields.get("definition", ""), "")
        self.assertEqual(fields.get("pos", ""), "")
        self.assertEqual(fields["frequency"], 0.0)

    def test_top20k_phrase_classification_and_quality_gate(self) -> None:
        self.assertTrue(enrichment.is_phrase("look after"))
        self.assertTrue(enrichment.is_phrase("people-to-people"))
        self.assertTrue(enrichment.is_phrase("entry", "prep_phrase"))
        self.assertFalse(enrichment.is_phrase("pre-", "prefix"))
        self.assertFalse(enrichment.is_phrase("word", "noun"))

        self.assertEqual(
            enrichment.entry_quality_gaps(
                "look after",
                "To take care of.",
                "照顾。",
                "",
                "",
                "",
            ),
            [],
        )
        self.assertEqual(
            enrichment.entry_quality_gaps(
                "word",
                "A unit of language.",
                "语言单位。",
                "wɝːd",
                "",
                "noun",
            ),
            [],
        )
        self.assertEqual(
            enrichment.entry_quality_gaps(
                "word",
                "A unit of language.",
                "语言单位。",
                "",
                "",
                "",
            ),
            ["pos", "phonetic"],
        )

    def test_wiktionary_fallback_parses_only_exact_english_data(self) -> None:
        fields = enrichment.wiktionary_definition_fields(
            {
                "en": [
                    {
                        "partOfSpeech": "Noun",
                        "definitions": [
                            {
                                "definition": (
                                    "A <a href='/wiki/unit'>unit</a> of "
                                    "language."
                                )
                            }
                        ],
                    }
                ],
                "fr": [
                    {
                        "partOfSpeech": "Nom",
                        "definitions": [{"definition": "mot"}],
                    }
                ],
            }
        )
        self.assertEqual(fields["definition"], "A unit of language.")
        self.assertEqual(fields["pos"], "noun")
        page = (
            '<h2 id="English">English</h2>'
            '<span class="IPA">/wɜːd/</span>'
            '<h2 id="French">French</h2>'
            '<span class="IPA">/mɔ/</span>'
        )
        self.assertEqual(enrichment.wiktionary_english_ipa(page), "wɜːd")
        self.assertEqual(
            enrichment.wiktionary_variants("people-to-people"),
            ["people-to-people", "people to people"],
        )
        self.assertEqual(enrichment.wiktionary_variants("pre-"), ["pre-"])

    def test_quality_retry_skips_phrase_ipa_and_respects_age(self) -> None:
        recent = enrichment.j(
            {
                "status": "completed",
                "lastAttempt": enrichment.now(),
            }
        )
        self.assertFalse(
            enrichment.needs_entry_quality_repair(
                "look after",
                "To take care of.",
                "照顾。",
                "",
                "",
                5.0,
            )
        )
        self.assertTrue(
            enrichment.needs_entry_quality_repair(
                "word",
                "A unit of language.",
                "单词。",
                "",
                "",
                5.0,
            )
        )
        self.assertFalse(enrichment.marker_retry_due(recent, 24))
        self.assertTrue(enrichment.marker_retry_due(recent, 0))

    def test_quality_repair_mode_only_processes_incomplete_top_rank(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            dataset = Path(directory) / "dataset.sqlite"
            state = Path(directory) / "state.sqlite"
            database = create_database(dataset)
            database.execute(
                """
                UPDATE entries SET pos='noun',definition='Complete.',
                  definition_zh='完整。',us_phonetic='wɜːd'
                """
            )
            database.execute(
                """
                UPDATE entries SET pos='',definition='',definition_zh='',
                  us_phonetic='',uk_phonetic=''
                WHERE frequency_rank=1
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
                return {
                    "definition": "A repaired definition.",
                    "pos": "noun",
                    "us": "wɜːd",
                    "_statuses": ["completed"],
                    "_attempted": ["test"],
                    "_field_sources": {
                        "definition": "test",
                        "pos": "test",
                        "us": "test",
                    },
                    "_provider_results": {
                        "test": {"status": "completed"}
                    },
                }

            async def fake_translate(
                *args: Any,
                **kwargs: Any,
            ) -> tuple[str, int, None]:
                return "修复后的释义。", 200, None

            with (
                patch.object(enrichment, "enrich_term", new=fake_enrich_term),
                patch.object(enrichment, "translate", new=fake_translate),
            ):
                asyncio.run(
                    enrichment.run(
                        dataset,
                        state,
                        0,
                        0,
                        1,
                        0,
                        24,
                        None,
                        None,
                        0,
                        1,
                        max_frequency_rank=1,
                        quality_repair_only=True,
                    )
                )

            self.assertEqual(attempted, ["word-3"])
            database = sqlite3.connect(dataset)
            row = database.execute(
                "SELECT definition_zh,enrichment_json FROM entries WHERE id=3"
            ).fetchone()
            database.close()
            self.assertEqual(row[0], "修复后的释义。")
            marker = json.loads(row[1])
            self.assertEqual(marker["status"], "completed")
            self.assertEqual(marker["qualityGaps"], [])
            self.assertEqual(marker["fieldSources"]["definition"], "test")

    def test_deep_pass_runs_once_after_completed_core_marker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            state = root / "state.sqlite"
            database = create_database(dataset)
            database.execute(
                """
                UPDATE entries SET
                  pos='noun',
                  definition='A unit of language.',
                  definition_zh='语言单位。',
                  us_phonetic='wɝːd',
                  uk_phonetic='wɜːd',
                  frequency=5.0,
                  enrichment_json=?
                WHERE id=1
                """,
                (
                    enrichment.j(
                        {
                            "status": "completed",
                            "lastAttempt": enrichment.now(),
                        }
                    ),
                ),
            )
            database.commit()
            database.close()
            attempted: list[tuple[str, str]] = []

            async def fake_enrich_term(
                *args: Any,
                **kwargs: Any,
            ) -> dict[str, Any]:
                attempted.append((str(args[2]), str(kwargs["profile"])))
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
                        1,
                        0,
                        1,
                        0,
                        24,
                        1,
                        1,
                        None,
                        1,
                        profile="deep",
                    )
                )
                # The deep marker is terminal for legitimately empty
                # relationship results, so a second invocation does not loop.
                asyncio.run(
                    enrichment.run(
                        dataset,
                        state,
                        0,
                        0,
                        1,
                        0,
                        24,
                        1,
                        1,
                        None,
                        1,
                        profile="deep",
                    )
                )

            self.assertEqual(attempted, [("word-1", "deep")])
            database = sqlite3.connect(dataset)
            marker = database.execute(
                "SELECT enrichment_json FROM entries WHERE id=1"
            ).fetchone()
            database.close()
            self.assertIsNotNone(marker)
            parsed = json.loads(marker[0])
            self.assertEqual(parsed["status"], "completed")
            self.assertEqual(parsed["profile"], "deep")

    def test_invalid_legacy_frequency_recalculates_difficulty(self) -> None:
        self.assertEqual(
            enrichment.resolved_difficulty("A1–A2", 500_000, 3.7),
            "C1–C2",
        )
        self.assertEqual(
            enrichment.resolved_difficulty("B1–B2", 5.1, 5.2),
            "B1–B2",
        )

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

    def test_run_translates_the_complete_long_definition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            state = root / "state.sqlite"
            definition = (
                " ".join(f"definition-{index:04d}" for index in range(240))
                + " final-sentinel"
            )
            self.assertGreater(len(definition), 1_800)
            database = create_database(dataset)
            database.execute(
                """
                UPDATE entries SET
                  definition=?,
                  definition_zh='',
                  us_phonetic='wɜːd',
                  uk_phonetic='wɜːd',
                  frequency=5.0
                """,
                (definition,),
            )
            database.commit()
            database.close()
            translated_sources: list[str] = []

            async def fake_enrich_term(
                *args: Any,
                **kwargs: Any,
            ) -> dict[str, Any]:
                return {"_statuses": [], "_attempted": []}

            async def fake_translate(
                client: httpx.AsyncClient,
                gate: enrichment.HostGate,
                text: str,
                translation_batcher: (
                    enrichment.EdgeTranslationBatcher | None
                ) = None,
            ) -> tuple[str, int | None, str | None]:
                del client, gate, translation_batcher
                translated_sources.append(text)
                return "完整翻译", 200, None

            with (
                patch.object(
                    enrichment,
                    "enrich_term",
                    new=fake_enrich_term,
                ),
                patch.object(
                    enrichment,
                    "translate",
                    new=fake_translate,
                ),
            ):
                asyncio.run(
                    enrichment.run(
                        dataset,
                        state,
                        1,
                        0,
                        1,
                        0,
                        24,
                        None,
                        None,
                        0,
                        1,
                    )
                )

            self.assertEqual(translated_sources, [definition])
            database = sqlite3.connect(dataset)
            translated, status = database.execute(
                """
                SELECT definition_zh,
                       json_extract(enrichment_json, '$.status')
                FROM entries WHERE id=3
                """
            ).fetchone()
            database.close()
            self.assertEqual(translated, "完整翻译")
            self.assertEqual(status, "completed")

    def test_translation_only_failure_never_completes_marker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            state_path = root / "state.sqlite"
            database = create_database(dataset)
            database.execute(
                """
                UPDATE entries SET
                  definition='A definition requiring translation.',
                  definition_zh='',
                  us_phonetic='wɜːd',
                  uk_phonetic='wɜːd',
                  frequency=5.0
                """
            )
            database.commit()
            database.close()

            async def fake_enrich_term(
                *args: Any,
                **kwargs: Any,
            ) -> dict[str, Any]:
                return {"_statuses": [], "_attempted": []}

            async def failed_translate(
                client: httpx.AsyncClient,
                gate: enrichment.HostGate,
                text: str,
                translation_batcher: (
                    enrichment.EdgeTranslationBatcher | None
                ) = None,
            ) -> tuple[str, int | None, str | None]:
                del client, gate, text, translation_batcher
                return "", 200, "translation missing"

            with (
                patch.object(
                    enrichment,
                    "enrich_term",
                    new=fake_enrich_term,
                ),
                patch.object(
                    enrichment,
                    "translate",
                    new=failed_translate,
                ),
            ):
                asyncio.run(
                    enrichment.run(
                        dataset,
                        state_path,
                        1,
                        0,
                        1,
                        0,
                        24,
                        None,
                        None,
                        0,
                        1,
                    )
                )

            database = sqlite3.connect(dataset)
            definition_zh, marker_status = database.execute(
                """
                SELECT definition_zh,
                       json_extract(enrichment_json, '$.status')
                FROM entries WHERE id=3
                """
            ).fetchone()
            database.close()
            self.assertEqual(definition_zh, "")
            self.assertEqual(marker_status, "not_found")

            state = sqlite3.connect(state_path)
            provider_status = state.execute(
                """
                SELECT status FROM provider_state
                WHERE term='word-3' AND source='translation'
                """
            ).fetchone()
            state.close()
            self.assertEqual(provider_status, ("failed",))

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
