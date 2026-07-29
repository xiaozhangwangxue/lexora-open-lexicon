from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from build_oxford_scope import SCHEMA  # noqa: E402
from prefill_kaikki_sense_relations import (  # noqa: E402
    extract_relation_senses,
    prefill,
)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def create_database(path: Path) -> None:
    database = sqlite3.connect(path)
    database.executescript(SCHEMA)
    database.execute(
        """
        INSERT INTO entries(
          word,normalized_word,pos,definition,definition_zh,
          synonyms_json,related_words_json,phrases_json,senses_json,
          source_json,scope_json,enrichment_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "colour",
            "colour",
            "noun",
            "The characteristic of visible light.",
            "颜色",
            '["hue"]',
            '["shade"]',
            "[]",
            json.dumps(
                [
                    {
                        "pos": "noun",
                        "definitions": [
                            "The spectral composition of visible light."
                        ],
                        "custom": "preserve-me",
                    }
                ]
            ),
            '["ecdict"]',
            '{"scope":"fixture","kaikki":true}',
            '{"status":"completed"}',
        ),
    )
    database.execute(
        """
        INSERT INTO entries_fts(
          rowid,word,definition,definition_zh,examples,phrases
        )
        SELECT id,word,definition,definition_zh,examples_json,phrases_json
        FROM entries
        """
    )
    database.commit()
    database.close()


def write_dump(path: Path) -> None:
    records = [
        {
            "word": "colour",
            "lang_code": "en",
            "pos": "noun",
            # Entry-level data is intentionally ignored: the existing builder
            # already handles it.
            "hypernyms": [{"word": "entry-level-existing"}],
            "senses": [
                {
                    "senseid": ["en:colour-1"],
                    "glosses": [
                        "The spectral composition of visible light."
                    ],
                    "synonyms": [{"word": "already-handled-synonym"}],
                    "form_of": [{"word": "color"}],
                    "hypernyms": [{"word": "visual property"}],
                    "coordinate_terms": [{"word": "brightness"}],
                    "meronyms": [{"word": "hue"}],
                }
            ],
        },
        {
            "word": "colour",
            "lang_code": "en",
            "pos": "verb",
            "senses": [
                {
                    "glosses": ["To give something colour."],
                    "alt_of": [{"word": "color"}],
                    "hyponyms": [{"word": "colour in"}],
                    "holonyms": [{"word": "graphic design"}],
                    "troponyms": [{"word": "hand-colour"}],
                }
            ],
        },
        {
            "word": "missing",
            "lang_code": "en",
            "pos": "adj",
            "senses": [
                {
                    "glosses": ["Not present."],
                    "alt_of": [{"word": "missin'"}],
                }
            ],
        },
        {
            "word": "couleur",
            "lang_code": "fr",
            "pos": "noun",
            "senses": [
                {
                    "glosses": ["French colour."],
                    "alt_of": [{"word": "color"}],
                }
            ],
        },
        "not-json",
    ]
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        for record in records:
            if isinstance(record, str):
                stream.write(record + "\n")
            else:
                stream.write(json.dumps(record) + "\n")


class KaikkiSenseRelationPrefillTest(unittest.TestCase):
    def test_extracts_only_previously_missed_sense_relations(self) -> None:
        patches = extract_relation_senses(
            {
                "pos": "noun",
                "senses": [
                    {
                        "glosses": ["A thing."],
                        "synonyms": [{"word": "handled"}],
                        "related": [{"word": "also handled"}],
                        "form_of": [{"word": "thing"}],
                        "hypernyms": [{"word": "object"}],
                        "coordinate_terms": [{"word": "idea"}],
                    }
                ],
            }
        )

        self.assertEqual(
            patches,
            [
                {
                    "pos": "noun",
                    "definitions": ["A thing."],
                    "relations": {
                        "form_of": ["thing"],
                        "hypernyms": ["object"],
                        "coordinate_terms": ["idea"],
                    },
                }
            ],
        )

    def test_default_dry_run_is_read_only_and_reports_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            kaikki = root / "kaikki.jsonl.gz"
            create_database(dataset)
            write_dump(kaikki)
            before = digest(dataset)

            report = prefill(dataset, kaikki, batch_size=1)

            self.assertEqual(report["mode"], "dry-run")
            self.assertEqual(report["englishRecords"], 3)
            self.assertEqual(report["invalidJsonLines"], 1)
            self.assertEqual(report["relationTerms"], 2)
            self.assertEqual(report["matchedTerms"], 1)
            self.assertEqual(report["unmatchedTerms"], 1)
            self.assertEqual(report["changedTerms"], 1)
            # The standalone path is typed-senses-only.  Only the compact
            # rich-entry delta may populate flat related/phrase columns.
            self.assertEqual(report["valuesAdded"]["related"], 0)
            self.assertEqual(report["valuesAdded"]["phrases"], 0)
            self.assertEqual(before, digest(dataset))

    def test_standalone_apply_is_rejected_without_touching_dataset(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            kaikki = root / "kaikki.jsonl.gz"
            create_database(dataset)
            write_dump(kaikki)
            before = digest(dataset)

            with self.assertRaisesRegex(
                ValueError,
                "standalone --apply is disabled",
            ):
                prefill(
                    dataset,
                    kaikki,
                    apply=True,
                    batch_size=1,
                )

            self.assertEqual(before, digest(dataset))


if __name__ == "__main__":
    unittest.main()
