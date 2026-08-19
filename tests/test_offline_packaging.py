from __future__ import annotations

import gzip
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from build_oxford_scope import SCHEMA  # noqa: E402
from auto_export_ready_shard import export_if_ready  # noqa: E402
from export_enrichment_shard import export_delta  # noqa: E402
from merge_enrichment_shards import merge  # noqa: E402
from package_offline_lexicons import (  # noqa: E402
    assert_fast_source_ready,
    compress,
    copy_fast,
    copy_full,
)


def build_source(path: Path) -> None:
    database = sqlite3.connect(path)
    database.executescript(SCHEMA)
    columns = (
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
    values = []
    for index, word in enumerate(("alpha", "bravo", "charlie", "delta"), 1):
        values.append(
            (
                word,
                word,
                "noun",
                "A1-A2",
                8.0 - index,
                index,
                "wɜːd",
                "",
                f"{word} definition",
                f"{word} 中文",
                "[]",
                "[]",
                "[]",
                "[]",
                "[]",
                "[]",
                "[]",
                "[]",
                "{}",
                "{}",
                '{"status":"completed"}',
            )
        )
    database.executemany(
        f"INSERT INTO entries({','.join(columns)}) "
        f"VALUES({','.join('?' for _ in columns)})",
        values,
    )
    database.commit()
    database.close()


class OfflinePackagingTest(unittest.TestCase):
    def test_include_full_is_rejected_before_creating_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "release"
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "tools" / "package_offline_lexicons.py"),
                    "--source",
                    str(root / "missing.sqlite"),
                    "--output-dir",
                    str(output),
                    "--version",
                    "test",
                    "--include-full",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("--include-full is disabled", result.stderr)
            self.assertFalse(output.exists())

    def test_fast_package_gate_rejects_missing_required_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.sqlite"
            build_source(source)
            self.assertEqual(assert_fast_source_ready(source, 4)["complete"], 4)

            database = sqlite3.connect(source)
            database.execute(
                "UPDATE entries SET definition='' WHERE frequency_rank=1"
            )
            database.commit()
            database.close()

            with self.assertRaisesRegex(
                ValueError,
                "fast lexicon quality gate failed",
            ):
                assert_fast_source_ready(source, 4)

    def test_auto_export_waits_and_then_creates_an_atomic_shard(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            output = root / "exports" / "fast.sqlite"
            build_source(source)
            database = sqlite3.connect(source)
            database.execute(
                "UPDATE entries SET enrichment_json='{}' WHERE id=2"
            )
            database.commit()
            database.close()

            self.assertEqual(
                export_if_ready(source, output, 0, 1, 2),
                "waiting finished=1 total=2",
            )
            self.assertFalse(output.exists())

            database = sqlite3.connect(source)
            database.execute(
                "UPDATE entries SET enrichment_json='{\"status\":\"partial\"}' "
                "WHERE id=2"
            )
            database.commit()
            database.close()
            self.assertTrue(
                export_if_ready(source, output, 0, 1, 2).startswith(
                    "created rows=2"
                )
            )
            self.assertTrue(
                export_if_ready(source, output, 0, 1, 2).startswith(
                    "ready rows=2"
                )
            )

    def test_exports_only_the_requested_shard(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            delta = root / "delta.sqlite"
            build_source(source)

            count = export_delta(source, delta, 1, 2, None)

            self.assertEqual(count, 2)
            database = sqlite3.connect(delta)
            self.assertEqual(
                database.execute("SELECT id FROM entries ORDER BY id").fetchall(),
                [(3,), (4,)],
            )
            self.assertEqual(
                database.execute(
                    "SELECT DISTINCT pos FROM entries"
                ).fetchall(),
                [("noun",)],
            )
            database.close()

    def test_builds_queryable_full_and_fast_gzip_packages(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            full = root / "full.sqlite"
            fast = root / "fast.sqlite"
            archive = root / "fast.sqlite.gz"
            build_source(source)

            self.assertEqual(copy_full(source, full), 4)
            self.assertEqual(copy_fast(source, fast, 2), 2)
            compress(fast, archive)

            with gzip.open(archive, "rb") as compressed:
                self.assertEqual(compressed.read(), fast.read_bytes())
            database = sqlite3.connect(fast)
            self.assertEqual(
                database.execute(
                    "SELECT normalized_word FROM entries ORDER BY frequency_rank"
                ).fetchall(),
                [("alpha",), ("bravo",)],
            )
            database.close()

    def test_fast_copy_never_includes_an_unranked_row(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            fast = root / "fast.sqlite"
            build_source(source)
            database = sqlite3.connect(source)
            database.execute(
                """
                INSERT INTO entries(
                  word,normalized_word,pos,frequency_rank,definition,
                  definition_zh,enrichment_json
                ) VALUES('unranked','unranked','',NULL,'','','{}')
                """
            )
            database.commit()
            database.close()

            self.assertEqual(assert_fast_source_ready(source, 4)["complete"], 4)
            self.assertEqual(copy_fast(source, fast, 4), 4)

            database = sqlite3.connect(fast)
            try:
                self.assertEqual(
                    database.execute(
                        "SELECT normalized_word FROM entries "
                        "ORDER BY frequency_rank,id"
                    ).fetchall(),
                    [("alpha",), ("bravo",), ("charlie",), ("delta",)],
                )
            finally:
                database.close()

    def test_merge_streams_shards_in_small_batches_and_refreshes_fts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical = root / "canonical.sqlite"
            first = root / "first.sqlite"
            second = root / "second.sqlite"
            build_source(canonical)
            export_delta(canonical, first, 0, 2, None)
            export_delta(canonical, second, 1, 2, None)
            for shard, suffix in ((first, " first"), (second, " second")):
                database = sqlite3.connect(shard)
                database.execute(
                    "UPDATE entries SET definition=definition || ?",
                    (suffix,),
                )
                database.commit()
                database.close()

            self.assertEqual(
                merge(canonical, [first, second], batch_size=1),
                4,
            )

            database = sqlite3.connect(canonical)
            try:
                self.assertEqual(
                    database.execute(
                        "SELECT definition FROM entries ORDER BY id"
                    ).fetchall(),
                    [
                        ("alpha definition first",),
                        ("bravo definition first",),
                        ("charlie definition second",),
                        ("delta definition second",),
                    ],
                )
                self.assertEqual(
                    database.execute(
                        "SELECT definition FROM entries_fts "
                        "WHERE rowid=3"
                    ).fetchone(),
                    ("charlie definition second",),
                )
            finally:
                database.close()

    def test_merge_rejects_ids_missing_from_canonical_and_rolls_back(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical = root / "canonical.sqlite"
            shard = root / "missing.sqlite"
            build_source(canonical)
            export_delta(canonical, shard, 0, 2, None)
            database = sqlite3.connect(shard)
            database.execute(
                "UPDATE entries SET definition='must roll back'"
            )
            database.execute("UPDATE entries SET id=999 WHERE id=1")
            database.commit()
            database.close()

            with self.assertRaisesRegex(
                ValueError,
                "did not match canonical rows: 999",
            ):
                merge(canonical, [shard], batch_size=2)

            database = sqlite3.connect(canonical)
            try:
                self.assertEqual(
                    database.execute(
                        "SELECT definition FROM entries WHERE id=2"
                    ).fetchone(),
                    ("bravo definition",),
                )
            finally:
                database.close()

    def test_merge_rejects_overlapping_shards_transactionally(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical = root / "canonical.sqlite"
            first = root / "first.sqlite"
            duplicate = root / "duplicate.sqlite"
            build_source(canonical)
            export_delta(canonical, first, 0, 2, None)
            export_delta(canonical, duplicate, 0, 2, None)
            database = sqlite3.connect(first)
            database.execute(
                "UPDATE entries SET definition='must roll back'"
            )
            database.commit()
            database.close()

            with self.assertRaisesRegex(ValueError, "overlapping entry id"):
                merge(canonical, [first, duplicate], batch_size=1)

            database = sqlite3.connect(canonical)
            try:
                self.assertEqual(
                    database.execute(
                        "SELECT definition FROM entries WHERE id=1"
                    ).fetchone(),
                    ("alpha definition",),
                )
            finally:
                database.close()


if __name__ == "__main__":
    unittest.main()
