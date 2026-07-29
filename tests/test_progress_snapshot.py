from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tools.write_progress_snapshot import write_snapshot


class ProgressSnapshotTest(unittest.TestCase):
    def test_snapshot_is_exact_and_replaced_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            state = root / "state.sqlite"
            output = root / "progress.json"

            database = sqlite3.connect(dataset)
            database.execute(
                """
                CREATE TABLE entries (
                  id INTEGER PRIMARY KEY,
                  enrichment_json TEXT
                )
                """
            )
            database.executemany(
                "INSERT INTO entries(id,enrichment_json) VALUES (?,?)",
                [
                    (1, '{"status":"completed"}'),
                    (2, '{"status":"partial"}'),
                    (3, '{"status":"not_found"}'),
                    (4, None),
                ],
            )
            database.commit()
            database.close()

            database = sqlite3.connect(state)
            database.execute(
                """
                CREATE TABLE provider_state (
                  term TEXT,
                  source TEXT,
                  status TEXT,
                  attempts INTEGER
                )
                """
            )
            database.execute(
                "INSERT INTO provider_state VALUES ('word','edge','completed',2)"
            )
            database.commit()
            database.close()

            first = write_snapshot(dataset, state, output, 0, 1)
            self.assertEqual(first["finished"], 3)
            self.assertEqual(first["total"], 4)
            self.assertEqual(first["percent"], 75.0)
            self.assertIn("updatedAt", first)
            self.assertEqual(json.loads(output.read_text())["finished"], 3)

            database = sqlite3.connect(dataset)
            database.execute(
                "UPDATE entries SET enrichment_json=? WHERE id=4",
                ('{"status":"completed"}',),
            )
            database.commit()
            database.close()
            second = write_snapshot(dataset, state, output, 0, 1)
            self.assertEqual(second["finished"], 4)
            self.assertEqual(second["percent"], 100.0)
            self.assertFalse(any(root.glob(".progress.json.tmp-*")))


if __name__ == "__main__":
    unittest.main()
