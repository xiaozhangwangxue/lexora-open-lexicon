from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from top20k_quality import quality_report  # noqa: E402


class Top20kQualityTest(unittest.TestCase):
    def test_report_uses_phrase_aware_required_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            dataset = Path(directory) / "quality.sqlite"
            database = sqlite3.connect(dataset)
            database.executescript(
                """
                CREATE TABLE entries(
                  id INTEGER PRIMARY KEY,
                  normalized_word TEXT,
                  definition TEXT,
                  definition_zh TEXT,
                  us_phonetic TEXT,
                  uk_phonetic TEXT,
                  pos TEXT,
                  frequency_rank INTEGER,
                  enrichment_json TEXT
                );
                CREATE INDEX idx_entries_freq ON entries(frequency_rank);
                """
            )
            database.executemany(
                "INSERT INTO entries VALUES(?,?,?,?,?,?,?,?,?)",
                [
                    (
                        1,
                        "word",
                        "A unit of language.",
                        "语言单位。",
                        "wɜːd",
                        "",
                        "noun",
                        1,
                        '{"status":"completed"}',
                    ),
                    (
                        2,
                        "look after",
                        "To take care of.",
                        "照顾。",
                        "",
                        "",
                        "",
                        2,
                        '{"status":"completed"}',
                    ),
                    (
                        3,
                        "missing",
                        "Not complete.",
                        "",
                        "",
                        "",
                        "",
                        3,
                        '{"status":"partial"}',
                    ),
                ],
            )
            database.commit()
            database.close()

            report = quality_report(dataset, unresolved_limit=5)

        self.assertEqual(report["total"], 3)
        self.assertEqual(report["complete"], 2)
        self.assertEqual(report["incomplete"], 1)
        self.assertEqual(report["terms"], {"words": 2, "phrases": 1})
        self.assertEqual(
            report["missing"],
            {"definition_zh": 1, "phonetic": 1, "pos": 1},
        )


if __name__ == "__main__":
    unittest.main()
