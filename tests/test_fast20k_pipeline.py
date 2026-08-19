from __future__ import annotations

import asyncio
import contextlib
import io
import json
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
import package_offline_lexicons as packaging  # noqa: E402
from build_oxford_scope import SCHEMA  # noqa: E402
from fast20k_pipeline import (  # noqa: E402
    assert_candidate_ready,
    build_candidate,
    candidate_quality_report,
    lexical_rejection_reason,
    refresh_candidate,
    term_key,
)
from fast20k_repair_delta import (  # noqa: E402
    apply_delta_union,
    export_repair_delta,
    validate_delta_union,
)
from package_offline_lexicons import main as package_main  # noqa: E402
from sqlite_snapshot_manifest import merge_owned_snapshots  # noqa: E402
from top20k_quality import quality_report  # noqa: E402
from write_top20k_quality_snapshot import write_quality_snapshot  # noqa: E402


COLUMNS = (
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


def create_source(path: Path) -> sqlite3.Connection:
    database = sqlite3.connect(path)
    database.executescript(SCHEMA)
    return database


def add_entry(
    database: sqlite3.Connection,
    word: str,
    rank: int,
    *,
    pos: str = "noun",
    definition: str | None = None,
    translation: str | None = None,
    us: str = "wɜːd",
    uk: str = "",
    sources: tuple[str, ...] = ("ecdict", "kaikki"),
    oxford: bool = False,
) -> int:
    definition = definition if definition is not None else f"Definition of {word}."
    translation = translation if translation is not None else f"{word} 的中文释义。"
    values = (
        word,
        word.lower(),
        pos,
        "A1-A2",
        max(0.1, 8.0 - rank / 100_000),
        rank,
        us,
        uk,
        definition,
        translation,
        "[]",
        "[]",
        "[]",
        "[]",
        "[]",
        "[]",
        "[]",
        "[]",
        json.dumps(list(sources)),
        json.dumps(
            {
                "scope": "test",
                "ecdictOxfordFlag": "1" if oxford else "",
            }
        ),
        '{"status":"completed"}',
    )
    cursor = database.execute(
        f"INSERT INTO entries({','.join(COLUMNS)}) "
        f"VALUES({','.join('?' for _ in COLUMNS)})",
        values,
    )
    return int(cursor.lastrowid)


def mark_rows_as_candidate_repaired(
    dataset: Path,
    candidate: Path,
    entry_ids: list[int],
    *,
    shard_count: int,
) -> None:
    candidate_db = sqlite3.connect(candidate)
    try:
        candidate_digest = str(
            candidate_db.execute(
                "SELECT candidate_digest FROM fast20k_metadata WHERE id=1"
            ).fetchone()[0]
        )
    finally:
        candidate_db.close()
    database = sqlite3.connect(dataset)
    database.row_factory = sqlite3.Row
    try:
        for entry_id in entry_ids:
            row = database.execute(
                "SELECT " + ",".join(enrichment.REPAIR_PAYLOAD_COLUMNS)
                + ",enrichment_json FROM entries WHERE id=?",
                (entry_id,),
            ).fetchone()
            marker = json.loads(str(row["enrichment_json"] or "{}"))
            marker.update(
                {
                    "repairCandidateDigest": candidate_digest,
                    "repairShardOwner": entry_id % shard_count,
                    "repairPayloadDigest": enrichment.repair_payload_digest(
                        dict(row)
                    ),
                }
            )
            database.execute(
                "UPDATE entries SET enrichment_json=? WHERE id=?",
                (json.dumps(marker, separators=(",", ":")), entry_id),
            )
        database.commit()
    finally:
        database.close()


class Fast20kPipelineTest(unittest.TestCase):
    def test_runtime_ready_marker_is_atomic_and_complete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "state" / "runtime.json"
            value = {
                "format": "lexora-top20k-runtime-ready-v1",
                "releaseId": "release-1",
                "candidateDigest": "a" * 64,
                "selectedOwnerRows": 10_000,
                "preflightRows": 12,
                "shardCount": 2,
                "shardIndex": 0,
                "processId": 42,
                "readyAt": "2026-08-19T00:00:00+00:00",
            }

            enrichment.write_runtime_ready_marker(marker, value)

            self.assertEqual(json.loads(marker.read_text(encoding="utf-8")), value)
            self.assertEqual(list(marker.parent.glob(".runtime.json.*.tmp")), [])

    def test_owner_merged_candidate_uses_the_responsible_servers_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshots: list[Path] = []
            for shard in range(2):
                source = root / f"source-{shard}.sqlite"
                database = create_source(source)
                alpha = add_entry(database, "alpha", 1)
                bravo = add_entry(database, "bravo", 2)
                self.assertEqual((alpha, bravo), (1, 2))
                if shard == 1:
                    database.execute(
                        "UPDATE entries SET pos='',definition='',definition_zh='',"
                        "us_phonetic='',uk_phonetic='' WHERE id=?",
                        (alpha,),
                    )
                    database.execute(
                        "UPDATE entries SET definition='Non-owner drift.' WHERE id=?",
                        (bravo,),
                    )
                database.commit()
                database.close()
                snapshots.append(source)

            merged = root / "owned.sqlite"
            candidate = root / "candidate.sqlite"
            merge_owned_snapshots(snapshots, merged, shard_count=2)
            build_candidate(
                merged,
                candidate,
                limit=2,
                phrase_target=0,
                shard_count=2,
            )

            database = sqlite3.connect(candidate)
            try:
                queue = database.execute(
                    "SELECT canonical_id,shard_owner,gaps_json "
                    "FROM repair_queue ORDER BY canonical_id"
                ).fetchall()
                definitions = dict(
                    database.execute("SELECT id,definition FROM entries ORDER BY id")
                )
            finally:
                database.close()
            self.assertEqual(queue[0][0:2], (alpha, 1))
            self.assertIn("definition", json.loads(queue[0][2]))
            self.assertEqual(definitions[bravo], "Definition of bravo.")

    def test_lexical_policy_preserves_real_dots_hyphens_and_apostrophes(self) -> None:
        self.assertIsNone(
            lexical_rejection_reason(
                "u.s.",
                "abbreviation",
                ["kaikki"],
                {"kaikki": True},
            )
        )
        self.assertIsNone(
            lexical_rejection_reason(
                "people-to-people",
                "phrase",
                ["kaikki"],
                {"kaikki": True},
            )
        )
        self.assertEqual(
            lexical_rejection_reason("as of...", "phrase", ["kaikki"], {}),
            "ellipsis",
        )
        self.assertEqual(
            lexical_rejection_reason("pre-", "prefix", ["kaikki"], {}),
            "affix",
        )
        self.assertEqual(
            lexical_rejection_reason("c--", "noun", ["kaikki"], {}),
            "invalid_hyphen",
        )
        self.assertEqual(
            lexical_rejection_reason("c'", "noun", ["kaikki"], {}),
            "unsupported_punctuation",
        )
        self.assertIsNone(lexical_rejection_reason("students'", "noun", ["kaikki"], {}))
        self.assertNotEqual(term_key("u.s."), term_key("us"))
        self.assertNotEqual(term_key("people-to-people"), term_key("people to people"))
        self.assertNotEqual(term_key("were"), term_key("we're"))

    def test_global_selection_caps_phrase_share_and_backfills_words(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            database = create_source(source)
            for rank, suffix in enumerate(
                ("one", "two", "three", "four", "five", "six", "seven", "eight"),
                1,
            ):
                add_entry(
                    database,
                    f"phrase {suffix}",
                    rank,
                    pos="phrase",
                    sources=("kaikki",),
                )
            for offset, word in enumerate(("alpha", "bravo", "charlie", "delta"), 100):
                add_entry(database, word, offset)
            database.commit()
            database.close()

            result = build_candidate(
                source,
                candidate,
                limit=5,
                phrase_target=1,
            )

            database = sqlite3.connect(candidate)
            selected = database.execute(
                "SELECT e.normalized_word,p.kind,p.canonical_frequency_rank,"
                "p.ranking_evidence FROM entries e JOIN fast20k_provenance p "
                "ON p.canonical_id=e.id ORDER BY p.selected_rank"
            ).fetchall()
            database.close()
            self.assertEqual(sum(row[1] == "phrase" for row in selected), 1)
            self.assertEqual(sum(row[1] == "word" for row in selected), 4)
            self.assertTrue(any(row[2] > 5 for row in selected))
            self.assertTrue(
                all(
                    row[3] == "bounded-dictionary-evidence"
                    for row in selected
                    if row[1] == "phrase"
                )
            )
            self.assertEqual(result["selection"]["phraseTarget"], 1)
            self.assertIn("not globally comparable", result["selection"]["policy"])

    def test_fast20k_rejects_fewer_than_fifteen_thousand_words(self) -> None:
        from fast20k_pipeline import _choose_counts

        with self.assertRaisesRegex(ValueError, "at least 15000 words"):
            _choose_counts(
                limit=20_000,
                phrase_target=6_000,
                words=20_000,
                phrases=6_000,
            )

        self.assertEqual(
            _choose_counts(
                limit=20_000,
                phrase_target=5_000,
                words=20_000,
                phrases=5_000,
            ),
            (15_000, 5_000),
        )

    def test_failed_replace_keeps_previous_candidate_byte_for_byte(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            good_source = root / "good.sqlite"
            bad_source = root / "bad.sqlite"
            candidate = root / "candidate.sqlite"
            database = create_source(good_source)
            add_entry(database, "alpha", 1)
            add_entry(database, "bravo", 2)
            database.commit()
            database.close()
            build_candidate(good_source, candidate, limit=2, phrase_target=0)
            previous = candidate.read_bytes()

            database = create_source(bad_source)
            add_entry(database, "pre-", 1, pos="prefix")
            database.commit()
            database.close()

            with self.assertRaisesRegex(ValueError, "insufficient eligible candidates"):
                build_candidate(
                    bad_source,
                    candidate,
                    limit=2,
                    phrase_target=0,
                    replace=True,
                )

            self.assertEqual(candidate.read_bytes(), previous)
            self.assertEqual(list(root.glob(".candidate.sqlite.*.tmp")), [])

    def test_legacy_optional_relation_columns_receive_safe_empty_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "legacy.sqlite"
            candidate = root / "candidate.sqlite"
            database = create_source(source)
            add_entry(database, "alpha", 1)
            database.execute("ALTER TABLE entries DROP COLUMN phrase_entries_json")
            database.execute("ALTER TABLE entries DROP COLUMN related_entries_json")
            database.commit()
            database.close()

            build_candidate(source, candidate, limit=1, phrase_target=0)

            database = sqlite3.connect(candidate)
            values = database.execute(
                "SELECT phrase_entries_json,related_entries_json FROM entries"
            ).fetchone()
            database.close()
            self.assertEqual(values, ("[]", "[]"))

    def test_gate_reports_field_json_and_canonical_provenance_failures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            database = create_source(source)
            entry_id = add_entry(database, "word", 1)
            database.commit()
            database.close()
            build_candidate(source, candidate, limit=1, phrase_target=0)
            self.assertTrue(
                assert_candidate_ready(candidate, source, expected_rows=1)["ready"]
            )

            database = sqlite3.connect(candidate)
            database.execute(
                "UPDATE entries SET source_json='not-json' WHERE id=?",
                (entry_id,),
            )
            database.commit()
            database.close()
            database = sqlite3.connect(source)
            database.execute(
                "UPDATE entries SET definition='Changed after selection.' WHERE id=?",
                (entry_id,),
            )
            database.commit()
            database.close()

            report = candidate_quality_report(candidate, source, expected_rows=1)

            self.assertFalse(report["ready"])
            self.assertIn("malformed_source_json", report["issues"])
            self.assertIn("canonical_content_mismatch", report["issues"])
            self.assertTrue(report["diagnostics"])
            with self.assertRaisesRegex(ValueError, "canonical_content_mismatch"):
                assert_candidate_ready(candidate, source, expected_rows=1)

    def test_gate_rejects_reassigned_repair_shard_and_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            database = create_source(source)
            add_entry(
                database,
                "word",
                1,
                pos="",
                definition="",
                translation="",
                us="",
            )
            database.commit()
            database.close()
            build_candidate(source, candidate, limit=1, phrase_target=0)
            database = sqlite3.connect(candidate)
            database.execute(
                "UPDATE repair_queue SET shard_owner=1-shard_owner"
            )
            database.commit()
            database.close()
            report = candidate_quality_report(candidate, source, expected_rows=1)
            self.assertFalse(report["structuralReady"])
            self.assertIn("repair_queue_digest_mismatch", report["issues"])
            self.assertIn("shard_owner_mismatch", report["issues"])

    def test_gate_rejects_same_count_but_stale_fts_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            database = create_source(source)
            add_entry(database, "word", 1)
            database.commit()
            database.close()
            build_candidate(source, candidate, limit=1, phrase_target=0)

            database = sqlite3.connect(candidate)
            database.execute("UPDATE entries_fts SET word='wrong' WHERE rowid=1")
            database.commit()
            database.close()

            report = candidate_quality_report(candidate, source, expected_rows=1)
            self.assertFalse(report["structuralReady"])
            self.assertIn("fts_content_mismatch", report["issues"])

    def test_repair_queue_contains_and_consumes_exact_rank_over_20000(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            state = root / "state.sqlite"
            database = create_source(source)
            add_entry(database, "complete", 1)
            ignored_id = add_entry(
                database,
                "unreliable phrase",
                2,
                pos="",
                definition="",
                translation="",
                us="",
                sources=("ecdict",),
            )
            repair_id = add_entry(
                database,
                "backfill",
                30_001,
                pos="  \n",
                definition="  ",
                translation="",
                us="",
            )
            database.commit()
            database.close()
            build_candidate(
                source,
                candidate,
                limit=2,
                phrase_target=0,
                shard_count=1,
            )
            database = sqlite3.connect(candidate)
            queue = database.execute(
                "SELECT canonical_id,canonical_frequency_rank,gaps_json "
                "FROM repair_queue ORDER BY canonical_id"
            ).fetchall()
            database.close()
            self.assertEqual([row[0] for row in queue], [repair_id])
            self.assertEqual(queue[0][1], 30_001)

            attempted: list[str] = []

            async def fake_enrich_term(*args: Any, **kwargs: Any) -> dict[str, Any]:
                attempted.append(str(args[2]))
                return {
                    "definition": "A repaired definition.",
                    "pos": "noun",
                    "us": "bækfɪl",
                    "_statuses": ["completed"],
                    "_attempted": ["test"],
                    "_field_sources": {
                        "definition": "test",
                        "pos": "test",
                        "us": "test",
                    },
                    "_provider_results": {"test": {"status": "completed"}},
                }

            async def fake_translate(
                *args: Any, **kwargs: Any
            ) -> tuple[str, int, None]:
                return "补位词。", 200, None

            with (
                patch.object(enrichment, "enrich_term", new=fake_enrich_term),
                patch.object(enrichment, "translate", new=fake_translate),
            ):
                asyncio.run(
                    enrichment.run(
                        source,
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
                        quality_repair_only=True,
                        repair_queue=candidate,
                    )
                )

            self.assertEqual(attempted, ["backfill"])
            database = sqlite3.connect(source)
            repaired = database.execute(
                "SELECT definition,definition_zh FROM entries WHERE id=?",
                (repair_id,),
            ).fetchone()
            ignored = database.execute(
                "SELECT definition FROM entries WHERE id=?",
                (ignored_id,),
            ).fetchone()
            database.close()
            self.assertEqual(repaired, ("A repaired definition.", "补位词。"))
            self.assertEqual(ignored, ("",))

    def test_candidate_quality_progress_uses_fixed_ids_and_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            database = create_source(source)
            add_entry(database, "complete", 1)
            add_entry(
                database,
                "unreliable phrase",
                2,
                pos="",
                definition="",
                translation="",
                us="",
                sources=("ecdict",),
            )
            backfill_id = add_entry(
                database,
                "backfill",
                30_001,
                pos="",
                definition="",
                translation="",
                us="",
            )
            database.commit()
            database.close()
            build_candidate(
                source,
                candidate,
                limit=2,
                phrase_target=0,
                shard_count=2,
            )

            reports = [
                quality_report(
                    source,
                    candidate=candidate,
                    shard_index=owner,
                    shard_count=2,
                )
                for owner in range(2)
            ]
            self.assertEqual(sum(report["total"] for report in reports), 2)
            self.assertEqual(sum(report["incomplete"] for report in reports), 1)
            self.assertEqual(len({report["candidateDigest"] for report in reports}), 1)
            self.assertTrue(all(report["maxFrequencyRank"] is None for report in reports))
            self.assertEqual(reports[backfill_id % 2]["unresolved"][0]["term"], "backfill")
            snapshot = write_quality_snapshot(
                source,
                root / "quality.json",
                0,
                2,
                candidate=candidate,
            )
            self.assertEqual(snapshot["qualityGateVersion"], 2)
            self.assertEqual(snapshot["candidateDigest"], reports[0]["candidateDigest"])

    def test_repair_queue_identity_mismatch_fails_before_network(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            state = root / "state.sqlite"
            database = create_source(source)
            entry_id = add_entry(
                database,
                "backfill",
                30_001,
                pos="",
                definition="",
                translation="",
                us="",
            )
            database.commit()
            database.close()
            build_candidate(
                source,
                candidate,
                limit=1,
                phrase_target=0,
                shard_count=1,
            )
            database = sqlite3.connect(source)
            database.execute(
                "UPDATE entries SET normalized_word='different' WHERE id=?",
                (entry_id,),
            )
            database.commit()
            database.close()

            with self.assertRaisesRegex(
                ValueError, "repair queue (?:content )?provenance mismatch"
            ):
                asyncio.run(
                    enrichment.run(
                        source,
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
                        quality_repair_only=True,
                        repair_queue=candidate,
                    )
                )

    def test_preflight_checks_complete_selected_rows_for_the_fixed_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            database = create_source(source)
            owner_one = add_entry(database, "alpha", 1)
            owner_zero = add_entry(database, "bravo", 2)
            self.assertEqual(owner_one % 2, 1)
            self.assertEqual(owner_zero % 2, 0)
            database.commit()
            database.close()
            build_candidate(
                source,
                candidate,
                limit=2,
                phrase_target=0,
                shard_count=2,
            )

            database = sqlite3.connect(source)
            database.execute(
                "UPDATE entries SET definition='Owner one drifted.' WHERE id=?",
                (owner_one,),
            )
            database.commit()
            database.close()

            dataset = sqlite3.connect(source)
            queue = enrichment.open_repair_queue(candidate)
            try:
                rows, metadata = enrichment.preflight_repair_queue_shard(
                    dataset,
                    queue,
                    shard_index=0,
                    shard_count=2,
                )
                self.assertEqual(rows, [])
                self.assertEqual(metadata["selected_owner_rows"], 1)
                with self.assertRaisesRegex(
                    ValueError, "differs from candidate baseline"
                ):
                    enrichment.preflight_repair_queue_shard(
                        dataset,
                        queue,
                        shard_index=1,
                        shard_count=2,
                    )
            finally:
                queue.close()
                dataset.close()

    def test_repair_queue_pages_are_disjoint_contiguous_id_shards(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            database = create_source(source)
            ids = [
                add_entry(
                    database,
                    word,
                    rank,
                    pos="",
                    definition="",
                    translation="",
                    us="",
                )
                for rank, word in enumerate(
                    ("alpha", "bravo", "charlie", "delta"),
                    1,
                )
            ]
            database.commit()
            database.close()
            build_candidate(source, candidate, limit=4, phrase_target=0)

            dataset = sqlite3.connect(source)
            queue = enrichment.open_repair_queue(candidate)
            try:
                first, first_after = enrichment.fetch_repair_queue_batch(
                    dataset,
                    queue,
                    start_id=ids[0],
                    end_id=ids[1],
                    after_id=None,
                    batch_size=10,
                )
                second, second_after = enrichment.fetch_repair_queue_batch(
                    dataset,
                    queue,
                    start_id=ids[2],
                    end_id=ids[3],
                    after_id=None,
                    batch_size=10,
                )
            finally:
                queue.close()
                dataset.close()

            self.assertEqual([row[0] for row in first], ids[:2])
            self.assertEqual([row[0] for row in second], ids[2:])
            self.assertEqual(first_after, ids[1])
            self.assertEqual(second_after, ids[3])

    def test_packager_fails_before_output_directory_for_stale_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            release = root / "release"
            database = create_source(source)
            entry_id = add_entry(database, "word", 1)
            database.commit()
            database.close()
            build_candidate(source, candidate, limit=1, phrase_target=0)
            database = sqlite3.connect(source)
            database.execute(
                "UPDATE entries SET definition='Changed after selection.' WHERE id=?",
                (entry_id,),
            )
            database.commit()
            database.close()

            with patch.object(
                sys,
                "argv",
                [
                    "package_offline_lexicons.py",
                    "--source",
                    str(source),
                    "--fast-source",
                    str(candidate),
                    "--output-dir",
                    str(release),
                    "--version",
                    "test",
                    "--fast-limit",
                    "1",
                ],
            ):
                with self.assertRaisesRegex(ValueError, "canonical_content_mismatch"):
                    package_main()

            self.assertFalse(release.exists())

    def test_packager_copies_the_exact_gated_candidate_with_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            release = root / "release"
            database = create_source(source)
            add_entry(database, "alpha", 1)
            add_entry(database, "bravo", 2)
            database.commit()
            database.close()
            build_candidate(source, candidate, limit=2, phrase_target=0)

            with patch.object(
                sys,
                "argv",
                [
                    "package_offline_lexicons.py",
                    "--source",
                    str(source),
                    "--fast-source",
                    str(candidate),
                    "--output-dir",
                    str(release),
                    "--version",
                    "test",
                    "--fast-limit",
                    "2",
                ],
            ):
                with contextlib.redirect_stdout(io.StringIO()):
                    package_main()

            fast = release / "lexora-offline-fast20k-test.sqlite"
            database = sqlite3.connect(fast)
            try:
                self.assertEqual(
                    database.execute("SELECT count(*) FROM entries").fetchone()[0],
                    2,
                )
                self.assertEqual(
                    database.execute(
                        "SELECT count(*) FROM fast20k_provenance"
                    ).fetchone()[0],
                    2,
                )
            finally:
                database.close()

            manifest = json.loads((release / "manifest.json").read_text("utf-8"))
            self.assertEqual(manifest["mode"], "fast20k-only")
            self.assertEqual(set(manifest["packages"]), {"fast20k"})

    def test_packager_gates_the_actual_release_copy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            release = root / "release"
            database = create_source(source)
            entry_id = add_entry(database, "word", 1)
            database.commit()
            database.close()
            build_candidate(source, candidate, limit=1, phrase_target=0)
            original_copy = packaging.copy_full
            calls = 0

            def tampering_copy(source_path: Path, destination: Path) -> int:
                nonlocal calls
                calls += 1
                rows = original_copy(source_path, destination)
                if calls == 3:
                    copied = sqlite3.connect(destination)
                    copied.execute(
                        "UPDATE entries SET definition='unchecked' WHERE id=?",
                        (entry_id,),
                    )
                    copied.commit()
                    copied.close()
                return rows

            with (
                patch.object(packaging, "copy_full", new=tampering_copy),
                patch.object(
                    sys,
                    "argv",
                    [
                        "package_offline_lexicons.py",
                        "--source",
                        str(source),
                        "--fast-source",
                        str(candidate),
                        "--output-dir",
                        str(release),
                        "--version",
                        "test",
                        "--fast-limit",
                        "1",
                        "--fast-only",
                    ],
                ),
            ):
                with self.assertRaisesRegex(
                    ValueError, "canonical_content_mismatch"
                ):
                    package_main()
            self.assertFalse(release.exists())

    def test_release_publish_never_replaces_even_an_empty_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            staged = root / "staged"
            destination = root / "release"
            staged.mkdir()
            destination.mkdir()
            marker = destination / "belongs-to-other-process"
            marker.write_text("keep", encoding="utf-8")
            (staged / "manifest.json").write_text("{}", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                packaging.publish_directory_no_replace(staged, destination)
            self.assertEqual(marker.read_text(encoding="utf-8"), "keep")
            self.assertTrue(staged.exists())

    def test_fixed_refresh_preserves_selection_and_rebuilds_queue(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            refreshed = root / "refreshed.sqlite"
            database = create_source(source)
            first = add_entry(
                database,
                "alpha",
                1,
                pos="",
                definition="",
                translation="",
                us="",
            )
            second = add_entry(database, "bravo", 2)
            database.commit()
            database.close()
            build_candidate(source, candidate, limit=2, phrase_target=0)
            candidate_db = sqlite3.connect(candidate)
            try:
                before = candidate_db.execute(
                    "SELECT canonical_id,selected_rank FROM fast20k_provenance "
                    "ORDER BY selected_rank"
                ).fetchall()
            finally:
                candidate_db.close()

            database = sqlite3.connect(source)
            database.execute(
                "UPDATE entries SET pos='noun',definition='First letter.',"
                "definition_zh='第一个字母。',us_phonetic='ælfə' WHERE id=?",
                (first,),
            )
            database.commit()
            database.close()
            report = refresh_candidate(source, candidate, refreshed)
            refreshed_db = sqlite3.connect(refreshed)
            try:
                after = refreshed_db.execute(
                    "SELECT canonical_id,selected_rank FROM fast20k_provenance "
                    "ORDER BY selected_rank"
                ).fetchall()
                queue_count = refreshed_db.execute(
                    "SELECT count(*) FROM repair_queue"
                ).fetchone()[0]
            finally:
                refreshed_db.close()
            self.assertEqual(before, after)
            self.assertEqual({row[0] for row in before}, {first, second})
            self.assertEqual(queue_count, 0)
            self.assertTrue(report["quality"]["ready"])

    def test_fixed_refresh_rejects_immutable_identity_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            refreshed = root / "refreshed.sqlite"
            database = create_source(source)
            entry_id = add_entry(database, "alpha", 1)
            database.commit()
            database.close()
            build_candidate(source, candidate, limit=1, phrase_target=0)
            database = sqlite3.connect(source)
            database.execute(
                "UPDATE entries SET source_json='[\"different-source\"]' WHERE id=?",
                (entry_id,),
            )
            database.commit()
            database.close()
            with self.assertRaisesRegex(ValueError, "fixed selection identity changed"):
                refresh_candidate(source, candidate, refreshed)
            self.assertFalse(refreshed.exists())

    def test_repair_delta_union_is_exact_and_applies_to_new_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            candidate = root / "candidate.sqlite"
            merged = root / "merged.sqlite"
            database = create_source(source)
            ids = [
                add_entry(
                    database,
                    word,
                    rank,
                    pos="",
                    definition="",
                    translation="",
                    us="",
                )
                for rank, word in enumerate(("alpha", "bravo", "charlie", "delta"), 1)
            ]
            database.commit()
            database.close()
            build_candidate(
                source,
                candidate,
                limit=4,
                phrase_target=0,
                shard_count=2,
            )
            database = sqlite3.connect(source)
            for entry_id in ids:
                database.execute(
                    "UPDATE entries SET pos='noun',definition='Definition.',"
                    "definition_zh='完整中文释义。',us_phonetic='wɜːd' WHERE id=?",
                    (entry_id,),
                )
            database.commit()
            database.close()
            mark_rows_as_candidate_repaired(
                source,
                candidate,
                ids,
                shard_count=2,
            )

            deltas = []
            for owner in range(2):
                delta = root / f"delta-{owner}.sqlite"
                export_repair_delta(
                    source,
                    candidate,
                    delta,
                    shard_index=owner,
                    shard_count=2,
                )
                deltas.append(delta)
            with self.assertRaisesRegex(ValueError, "owners incomplete"):
                validate_delta_union(candidate, deltas[:1])
            union = validate_delta_union(candidate, deltas)
            self.assertTrue(union["ready"])
            self.assertEqual(union["actualRows"], 4)

            # Apply to an untouched canonical copy; the source used for export
            # already contains the repaired values, so create the old snapshot
            # by clearing only mutable required fields.
            old = root / "old.sqlite"
            source_db = sqlite3.connect(source)
            old_db = sqlite3.connect(old)
            source_db.backup(old_db)
            old_db.execute(
                "UPDATE entries SET pos='',definition='',definition_zh='',"
                "us_phonetic='',uk_phonetic=''"
            )
            old_db.commit()
            old_db.close()
            source_db.close()
            applied = apply_delta_union(old, candidate, deltas, merged)
            self.assertEqual(applied["appliedRows"], 4)
            merged_db = sqlite3.connect(merged)
            try:
                complete = merged_db.execute(
                    "SELECT count(*) FROM entries WHERE definition='Definition.' "
                    "AND definition_zh='完整中文释义。'"
                ).fetchone()[0]
            finally:
                merged_db.close()
            self.assertEqual(complete, 4)

    def test_delta_apply_preserves_newer_central_fields_and_rejects_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.sqlite"
            candidate = root / "candidate.sqlite"
            server = root / "server.sqlite"
            central = root / "central.sqlite"
            output = root / "merged.sqlite"
            database = create_source(baseline)
            entry_id = add_entry(
                database,
                "word",
                1,
                definition="Baseline definition.",
                translation="",
            )
            database.commit()
            database.close()
            build_candidate(
                baseline,
                candidate,
                limit=1,
                phrase_target=0,
                shard_count=1,
            )
            for destination in (server, central):
                source_db = sqlite3.connect(baseline)
                destination_db = sqlite3.connect(destination)
                source_db.backup(destination_db)
                destination_db.close()
                source_db.close()
            server_db = sqlite3.connect(server)
            server_db.execute(
                "UPDATE entries SET definition_zh='服务器翻译。' WHERE id=?",
                (entry_id,),
            )
            server_db.commit()
            server_db.close()
            mark_rows_as_candidate_repaired(
                server,
                candidate,
                [entry_id],
                shard_count=1,
            )
            central_db = sqlite3.connect(central)
            central_db.execute(
                "UPDATE entries SET definition='Newer central definition.' WHERE id=?",
                (entry_id,),
            )
            central_db.commit()
            central_db.close()
            delta = root / "delta.sqlite"
            export_repair_delta(
                server,
                candidate,
                delta,
                shard_index=0,
                shard_count=1,
            )
            apply_delta_union(central, candidate, [delta], output)
            merged_db = sqlite3.connect(output)
            try:
                values = merged_db.execute(
                    "SELECT definition,definition_zh FROM entries WHERE id=?",
                    (entry_id,),
                ).fetchone()
            finally:
                merged_db.close()
            self.assertEqual(
                values,
                ("Newer central definition.", "服务器翻译。"),
            )

            conflict_server = root / "conflict-server.sqlite"
            source_db = sqlite3.connect(baseline)
            conflict_db = sqlite3.connect(conflict_server)
            source_db.backup(conflict_db)
            conflict_db.execute(
                "UPDATE entries SET definition_zh='服务器另一翻译。' WHERE id=?",
                (entry_id,),
            )
            conflict_db.commit()
            conflict_db.close()
            source_db.close()
            mark_rows_as_candidate_repaired(
                conflict_server,
                candidate,
                [entry_id],
                shard_count=1,
            )
            conflict_delta = root / "conflict-delta.sqlite"
            export_repair_delta(
                conflict_server,
                candidate,
                conflict_delta,
                shard_index=0,
                shard_count=1,
            )
            central_db = sqlite3.connect(central)
            central_db.execute(
                "UPDATE entries SET definition_zh='中央另一翻译。' WHERE id=?",
                (entry_id,),
            )
            central_db.commit()
            central_db.close()
            conflict_output = root / "conflict-output.sqlite"
            with self.assertRaisesRegex(ValueError, "three-way merge conflict"):
                apply_delta_union(
                    central,
                    candidate,
                    [conflict_delta],
                    conflict_output,
                )
            self.assertFalse(conflict_output.exists())

    def test_state_file_is_bound_to_one_candidate_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "state.sqlite"
            state = enrichment.init_state(state_path)
            try:
                enrichment.bind_state_candidate(
                    state, "candidate-a", shard_owner=0
                )
                enrichment.bind_state_candidate(
                    state, "candidate-a", shard_owner=0
                )
                with self.assertRaisesRegex(ValueError, "candidate digest or shard"):
                    enrichment.bind_state_candidate(
                        state, "candidate-b", shard_owner=0
                    )
            finally:
                state.close()

    def test_legacy_nonempty_state_requires_fresh_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = enrichment.init_state(Path(directory) / "state.sqlite")
            try:
                state.execute(
                    "INSERT INTO provider_state(term,source,status,attempts,updated_at) "
                    "VALUES('word','test','failed',1,'2026-01-01T00:00:00Z')"
                )
                state.commit()
                with self.assertRaisesRegex(ValueError, "fresh --state"):
                    enrichment.bind_state_candidate(
                        state, "candidate-a", shard_owner=0
                    )
            finally:
                state.close()


if __name__ == "__main__":
    unittest.main()
