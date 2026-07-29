from __future__ import annotations

import gzip
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from build_oxford_scope import SCHEMA  # noqa: E402
from export_enrichment_shard import export_delta  # noqa: E402
from package_offline_lexicons import compress, copy_fast, copy_full  # noqa: E402


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
                "",
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


if __name__ == "__main__":
    unittest.main()
