from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tools.write_top20k_quality_snapshot import write_quality_snapshot


class Top20kQualitySnapshotTest(unittest.TestCase):
    def test_writes_detailed_quality_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            output = root / "quality.json"
            database = sqlite3.connect(dataset)
            database.execute(
                """
                CREATE TABLE entries (
                  id INTEGER PRIMARY KEY,
                  normalized_word TEXT,
                  definition TEXT,
                  definition_zh TEXT,
                  us_phonetic TEXT,
                  uk_phonetic TEXT,
                  pos TEXT,
                  frequency_rank INTEGER,
                  enrichment_json TEXT
                )
                """
            )
            database.execute(
                "CREATE INDEX idx_entries_freq ON entries(frequency_rank)"
            )
            database.executemany(
                "INSERT INTO entries VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (
                        1,
                        "word",
                        "A unit of language.",
                        "语言单位。",
                        "/wɜːd/",
                        "",
                        "noun",
                        1,
                        '{"status":"completed"}',
                    ),
                    (
                        2,
                        "missing",
                        "",
                        "",
                        "",
                        "",
                        "noun",
                        2,
                        '{"status":"not_found"}',
                    ),
                ],
            )
            database.commit()
            database.close()

            value = write_quality_snapshot(dataset, output, 0, 1)
            self.assertEqual(value["complete"], 1)
            self.assertEqual(value["incomplete"], 1)
            self.assertEqual(value["missing"]["definition"], 1)
            self.assertIn("updatedAt", value)
            self.assertEqual(value["qualityGateVersion"], 1)
            self.assertEqual(value["datasetIdentity"]["inode"], dataset.stat().st_ino)
            saved = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(saved["total"], 2)
            self.assertFalse(any(root.glob(".quality.json.tmp-*")))


if __name__ == "__main__":
    unittest.main()
