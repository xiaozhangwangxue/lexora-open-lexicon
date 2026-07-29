from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from build_oxford_scope import SCHEMA  # noqa: E402
from prefill_oewn import (  # noqa: E402
    EXPECTED_LICENSE,
    PROVENANCE,
    PROVENANCE_KEY,
    prefill,
)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(64 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def create_database(path: Path) -> None:
    database = sqlite3.connect(path)
    database.executescript(SCHEMA)
    rows = [
        (
            "alpha",
            "alpha",
            "",
            "A1",
            5.1,
            1,
            "",
            "",
            "",
            "",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            '["ecdict"]',
            '{"scope":"fixture"}',
            '{"status":"pending"}',
        ),
        (
            "word",
            "word",
            "custom-pos",
            "B2",
            5.0,
            2,
            "/old-us/",
            "/old-uk/",
            "Existing definition.",
            "现有释义",
            '["term"]',
            '["silence"]',
            '["Existing example."]',
            '["existing phrase"]',
            '[{"word":"existing phrase","definition":"Existing rich phrase.",'
            '"custom":"keep-phrase"}]',
            '["language"]',
            '[{"word":"language","definition":"Existing rich relation.",'
            '"custom":"keep-related"}]',
            '[{"source":"custom","custom":"preserve-me"}]',
            '["ecdict"]',
            '{"scope":"fixture","custom":true}',
            '{"status":"completed","attempts":7}',
        ),
        (
            "god",
            "god",
            "",
            "B1",
            4.9,
            3,
            "",
            "",
            "",
            "神",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            '["ecdict"]',
            '{"scope":"fixture"}',
            '{"status":"pending"}',
        ),
        (
            "untouched",
            "untouched",
            "adj",
            "A2",
            4.8,
            4,
            "/u/",
            "/u/",
            "Not changed.",
            "未改变",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            '["ecdict"]',
            '{"scope":"fixture"}',
            '{"status":"completed"}',
        ),
    ]
    database.executemany(
        """
        INSERT INTO entries(
          word,normalized_word,pos,difficulty,frequency,frequency_rank,
          us_phonetic,uk_phonetic,definition,definition_zh,
          synonyms_json,antonyms_json,examples_json,phrases_json,
          phrase_entries_json,related_words_json,related_entries_json,
          senses_json,source_json,scope_json,enrichment_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
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


def remove_rich_entry_columns(path: Path) -> None:
    database = sqlite3.connect(path)
    try:
        database.execute(
            "ALTER TABLE entries DROP COLUMN phrase_entries_json"
        )
        database.execute(
            "ALTER TABLE entries DROP COLUMN related_entries_json"
        )
        database.commit()
    finally:
        database.close()


def fixture_xml(
    *,
    version: str = "2025+",
    license_url: str = EXPECTED_LICENSE,
) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE LexicalResource SYSTEM "http://globalwordnet.github.io/schemas/WN-LMF-1.3.dtd">
<LexicalResource>
  <Lexicon id="oewn" label="Open English Wordnet" language="en"
           license="{license_url}" version="{version}"
           url="https://github.com/globalwordnet/english-wordnet">
    <LexicalEntry id="oewn-alpha-n">
      <Lemma writtenForm="alpha" partOfSpeech="n">
        <Pronunciation variety="en-US-fonipa">/ˈælfə/</Pronunciation>
        <Pronunciation variety="en-GB-fonipa">/ˈalfə/</Pronunciation>
        <Pronunciation variety="en-fonxsamp">ignored</Pronunciation>
        <Pronunciation>also ignored</Pronunciation>
      </Lemma>
      <Sense id="oewn-alpha-1" synset="oewn-ss-alpha" n="1">
        <SenseExample>Alpha sense example.</SenseExample>
        <SenseRelation relType="antonym" target="oewn-beta-1"/>
      </Sense>
    </LexicalEntry>
    <LexicalEntry id="oewn-beginning-n">
      <Lemma writtenForm="beginning" partOfSpeech="n"/>
      <Sense id="oewn-beginning-1" synset="oewn-ss-alpha"/>
    </LexicalEntry>
    <LexicalEntry id="oewn-beta-n">
      <Lemma writtenForm="beta" partOfSpeech="n"/>
      <Sense id="oewn-beta-1" synset="oewn-ss-beta"/>
    </LexicalEntry>
    <LexicalEntry id="oewn-letter-n">
      <Lemma writtenForm="letter" partOfSpeech="n"/>
      <Sense id="oewn-letter-1" synset="oewn-ss-letter"/>
    </LexicalEntry>
    <LexicalEntry id="oewn-alpha-wave-n">
      <Lemma writtenForm="alpha wave" partOfSpeech="n"/>
      <Sense id="oewn-alpha-wave-1" synset="oewn-ss-wave"/>
    </LexicalEntry>
    <LexicalEntry id="oewn-word-n">
      <Lemma writtenForm="word" partOfSpeech="n">
        <Pronunciation variety="US">/new-us/</Pronunciation>
        <Pronunciation variety="GB">/new-uk/</Pronunciation>
      </Lemma>
      <Sense id="oewn-word-1" synset="oewn-ss-word"/>
    </LexicalEntry>
    <LexicalEntry id="oewn-lexeme-n">
      <Lemma writtenForm="lexeme" partOfSpeech="n"/>
      <Sense id="oewn-lexeme-1" synset="oewn-ss-word"/>
    </LexicalEntry>
    <LexicalEntry id="oewn-God-n">
      <Lemma writtenForm="God" partOfSpeech="n"/>
      <Sense id="oewn-God-1" synset="oewn-ss-God"/>
    </LexicalEntry>
    <LexicalEntry id="oewn-god-n">
      <Lemma writtenForm="god" partOfSpeech="n"/>
      <Sense id="oewn-god-1" synset="oewn-ss-god"/>
    </LexicalEntry>
    <LexicalEntry id="oewn-unmatched-a">
      <Lemma writtenForm="unmatched" partOfSpeech="a"/>
      <Sense id="oewn-unmatched-1" synset="oewn-ss-unmatched"/>
    </LexicalEntry>
    <LexicalEntry id="oewn-untouched-a">
      <Lemma writtenForm="untouched" partOfSpeech="a"/>
      <Sense id="oewn-untouched-1" synset="oewn-ss-untouched"/>
    </LexicalEntry>
    <LexicalEntry id="oewn-word-only-relation-n">
      <Lemma writtenForm="word only relation" partOfSpeech="n"/>
      <Sense id="oewn-word-only-relation-1"
             synset="oewn-ss-word-only-relation"/>
    </LexicalEntry>
    <Synset id="oewn-ss-alpha" ili="i-alpha" partOfSpeech="n">
      <Definition sourceSense="oewn-alpha-1">The first letter.</Definition>
      <Example>Alpha comes first.</Example>
      <SynsetRelation relType="hypernym" target="oewn-ss-letter"/>
      <SynsetRelation relType="derivation" target="oewn-ss-wave"/>
      <SynsetRelation relType="similar" target="oewn-ss-untouched"/>
      <SynsetRelation relType="also"
                      target="oewn-ss-word-only-relation"/>
    </Synset>
    <Synset id="oewn-ss-beta" ili="i-beta" partOfSpeech="n">
      <Definition>The second letter.</Definition>
    </Synset>
    <Synset id="oewn-ss-letter" ili="i-letter" partOfSpeech="n">
      <Definition>A written symbol.</Definition>
    </Synset>
    <Synset id="oewn-ss-wave" ili="i-wave" partOfSpeech="n">
      <Definition>A wave form.</Definition>
    </Synset>
    <Synset id="oewn-ss-word" ili="i-word" partOfSpeech="n">
      <Definition>A unit of language.</Definition>
      <Example>A word can be spoken.</Example>
    </Synset>
    <Synset id="oewn-ss-God" ili="i-God" partOfSpeech="n">
      <Definition>A proper-name sense.</Definition>
    </Synset>
    <Synset id="oewn-ss-god" ili="i-god" partOfSpeech="n">
      <Definition>A common-noun sense.</Definition>
    </Synset>
    <Synset id="oewn-ss-unmatched" ili="i-unmatched" partOfSpeech="a">
      <Definition>Not matched.</Definition>
    </Synset>
    <Synset id="oewn-ss-untouched" ili="i-untouched" partOfSpeech="a"/>
    <Synset id="oewn-ss-word-only-relation"
            ili="i-word-only-relation" partOfSpeech="n"/>
  </Lexicon>
</LexicalResource>
"""


def write_fixture(
    path: Path,
    *,
    version: str = "2025+",
    license_url: str = EXPECTED_LICENSE,
) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        stream.write(
            fixture_xml(
                version=version,
                license_url=license_url,
            )
        )


class OewnPrefillTest(unittest.TestCase):
    def test_legacy_schema_dry_run_is_read_only_and_apply_migrates_candidate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "legacy.sqlite"
            candidate = root / "candidate.sqlite"
            oewn = root / "oewn.xml.gz"
            create_database(dataset)
            remove_rich_entry_columns(dataset)
            write_fixture(oewn)
            before = digest(dataset)
            expected_digest = digest(oewn)

            dry_run = prefill(
                dataset,
                oewn,
                expected_sha256=expected_digest,
            )

            self.assertEqual(dry_run["mode"], "dry-run")
            self.assertEqual(dry_run["changedTerms"], 4)
            self.assertEqual(before, digest(dataset))
            source = sqlite3.connect(dataset)
            try:
                source_columns = {
                    str(row[1])
                    for row in source.execute(
                        "PRAGMA table_info(entries)"
                    )
                }
                self.assertNotIn(
                    "phrase_entries_json",
                    source_columns,
                )
                self.assertNotIn(
                    "related_entries_json",
                    source_columns,
                )
            finally:
                source.close()

            applied = prefill(
                dataset,
                oewn,
                apply=True,
                output=candidate,
                expected_sha256=expected_digest,
            )

            self.assertEqual(applied["mode"], "apply")
            self.assertEqual(before, digest(dataset))
            migrated = sqlite3.connect(candidate)
            try:
                candidate_columns = {
                    str(row[1])
                    for row in migrated.execute(
                        "PRAGMA table_info(entries)"
                    )
                }
                self.assertIn(
                    "phrase_entries_json",
                    candidate_columns,
                )
                self.assertIn(
                    "related_entries_json",
                    candidate_columns,
                )
                alpha = migrated.execute(
                    """
                    SELECT phrases_json,phrase_entries_json,
                           related_words_json,related_entries_json
                    FROM entries WHERE normalized_word='alpha'
                    """
                ).fetchone()
                self.assertEqual(
                    len(json.loads(alpha[0])),
                    len(json.loads(alpha[1])),
                )
                self.assertEqual(
                    len(json.loads(alpha[2])),
                    len(json.loads(alpha[3])),
                )
            finally:
                migrated.close()

    def test_default_dry_run_is_read_only_and_reports_exact_plan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            oewn = root / "oewn.xml.gz"
            create_database(dataset)
            write_fixture(oewn)
            before = digest(dataset)

            report = prefill(
                dataset,
                oewn,
                batch_size=2,
                expected_sha256=digest(oewn),
            )

            self.assertEqual(report["mode"], "dry-run")
            self.assertEqual(report["source"]["metadata"]["version"], "2025+")
            self.assertEqual(report["source"]["lexicalEntries"], 12)
            self.assertEqual(report["matchedTerms"], 4)
            self.assertEqual(report["unmatchedTerms"], 7)
            self.assertEqual(report["changedTerms"], 4)
            self.assertEqual(report["rowCountBefore"], 4)
            self.assertEqual(report["rowCountAfter"], 4)
            self.assertEqual(report["caseCollisions"], 1)
            self.assertEqual(before, digest(dataset))
            database = sqlite3.connect(dataset)
            try:
                metadata = database.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='dataset_metadata'"
                ).fetchone()
                self.assertIsNone(metadata)
            finally:
                database.close()

    def test_apply_writes_new_candidate_and_preserves_existing_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            oewn = root / "oewn.xml.gz"
            candidate = root / "candidate.sqlite"
            create_database(dataset)
            write_fixture(oewn)
            before = digest(dataset)

            report = prefill(
                dataset,
                oewn,
                apply=True,
                output=candidate,
                batch_size=2,
                expected_sha256=digest(oewn),
            )

            self.assertEqual(report["mode"], "apply")
            self.assertEqual(report["changedTerms"], 4)
            self.assertEqual(report["caseCollisions"], 1)
            self.assertTrue(candidate.is_file())
            self.assertEqual(before, digest(dataset))

            database = sqlite3.connect(candidate)
            try:
                self.assertEqual(
                    database.execute(
                        "SELECT count(*) FROM entries"
                    ).fetchone()[0],
                    4,
                )
                self.assertIsNone(
                    database.execute(
                        "SELECT 1 FROM entries "
                        "WHERE normalized_word='unmatched'"
                    ).fetchone()
                )
                alpha = database.execute(
                    """
                    SELECT pos,difficulty,frequency,frequency_rank,
                           us_phonetic,uk_phonetic,definition,definition_zh,
                           synonyms_json,antonyms_json,examples_json,
                           phrases_json,phrase_entries_json,
                           related_words_json,related_entries_json,
                           senses_json,source_json,scope_json,enrichment_json
                    FROM entries WHERE normalized_word='alpha'
                    """
                ).fetchone()
                self.assertEqual(alpha[:8], (
                    "noun",
                    "A1",
                    5.1,
                    1,
                    "/ˈælfə/",
                    "/ˈalfə/",
                    "The first letter.",
                    "",
                ))
                self.assertEqual(json.loads(alpha[8]), ["beginning"])
                self.assertEqual(json.loads(alpha[9]), ["beta"])
                self.assertEqual(
                    json.loads(alpha[10]),
                    ["Alpha sense example.", "Alpha comes first."],
                )
                self.assertEqual(json.loads(alpha[11]), ["alpha wave"])
                self.assertEqual(
                    json.loads(alpha[12]),
                    [
                        {
                            "word": "alpha wave",
                            "definition": "A wave form.",
                        }
                    ],
                )
                self.assertEqual(
                    json.loads(alpha[13]),
                    ["letter", "alpha wave", "untouched"],
                )
                self.assertEqual(
                    json.loads(alpha[14]),
                    [
                        {
                            "word": "letter",
                            "definition": "A written symbol.",
                        },
                        {
                            "word": "alpha wave",
                            "definition": "A wave form.",
                        },
                        {
                            "word": "untouched",
                            "definition": "Not changed.",
                        },
                    ],
                )
                alpha_senses = json.loads(alpha[15])
                self.assertEqual(alpha_senses[0]["source"], "oewn-2025+")
                self.assertEqual(alpha_senses[0]["ili"], "i-alpha")
                self.assertEqual(
                    alpha_senses[0]["relations"],
                    {
                        "antonym": ["beta"],
                        "hypernym": ["letter"],
                        "derivation": ["alpha wave"],
                        "similar": ["untouched"],
                        "also": ["word only relation"],
                    },
                )
                self.assertEqual(
                    json.loads(alpha[16]),
                    ["ecdict", "oewn-2025+"],
                )
                alpha_scope = json.loads(alpha[17])
                self.assertEqual(alpha_scope["scope"], "fixture")
                self.assertEqual(
                    alpha_scope[PROVENANCE_KEY],
                    PROVENANCE,
                )
                self.assertEqual(
                    json.loads(alpha[18]),
                    {"status": "pending"},
                )

                word = database.execute(
                    """
                    SELECT pos,difficulty,frequency,frequency_rank,
                           us_phonetic,uk_phonetic,definition,definition_zh,
                           synonyms_json,antonyms_json,examples_json,
                           phrases_json,phrase_entries_json,
                           related_words_json,related_entries_json,
                           senses_json,source_json,scope_json,enrichment_json
                    FROM entries WHERE normalized_word='word'
                    """
                ).fetchone()
                self.assertEqual(word[:8], (
                    "custom-pos",
                    "B2",
                    5.0,
                    2,
                    "/old-us/",
                    "/old-uk/",
                    "Existing definition.",
                    "现有释义",
                ))
                self.assertEqual(json.loads(word[8]), ["term", "lexeme"])
                self.assertEqual(json.loads(word[9]), ["silence"])
                self.assertEqual(
                    json.loads(word[10]),
                    ["Existing example.", "A word can be spoken."],
                )
                self.assertEqual(
                    json.loads(word[11]),
                    ["existing phrase"],
                )
                self.assertEqual(
                    json.loads(word[12]),
                    [
                        {
                            "word": "existing phrase",
                            "definition": "Existing rich phrase.",
                            "custom": "keep-phrase",
                        }
                    ],
                )
                self.assertEqual(json.loads(word[13]), ["language"])
                self.assertEqual(
                    json.loads(word[14]),
                    [
                        {
                            "word": "language",
                            "definition": "Existing rich relation.",
                            "custom": "keep-related",
                        }
                    ],
                )
                self.assertEqual(
                    json.loads(word[15])[0],
                    {"source": "custom", "custom": "preserve-me"},
                )
                self.assertEqual(
                    json.loads(word[18]),
                    {"status": "completed", "attempts": 7},
                )
                self.assertGreaterEqual(
                    sum(report["scalarConflictsPreserved"].values()),
                    4,
                )
                metadata = database.execute(
                    "SELECT value FROM dataset_metadata WHERE key=?",
                    (PROVENANCE_KEY,),
                ).fetchone()
                self.assertEqual(json.loads(metadata[0]), PROVENANCE)
                fts_alpha = database.execute(
                    """
                    SELECT definition,examples,phrases
                    FROM entries_fts
                    WHERE rowid=(
                      SELECT id FROM entries
                      WHERE normalized_word='alpha'
                    )
                    """
                ).fetchone()
                self.assertEqual(
                    fts_alpha,
                    (
                        "The first letter.",
                        json.dumps(
                            [
                                "Alpha sense example.",
                                "Alpha comes first.",
                            ],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        '["alpha wave"]',
                    ),
                )
            finally:
                database.close()

    def test_second_candidate_is_logically_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            first_candidate = root / "first.sqlite"
            second_candidate = root / "second.sqlite"
            oewn = root / "oewn.xml.gz"
            create_database(dataset)
            write_fixture(oewn)
            expected_digest = digest(oewn)

            prefill(
                dataset,
                oewn,
                apply=True,
                output=first_candidate,
                expected_sha256=expected_digest,
            )
            second = prefill(
                first_candidate,
                oewn,
                apply=True,
                output=second_candidate,
                expected_sha256=expected_digest,
            )

            self.assertEqual(second["changedTerms"], 0)
            self.assertEqual(second["unchangedTerms"], 4)
            self.assertFalse(second["metadataChanged"])
            first = sqlite3.connect(first_candidate)
            next_database = sqlite3.connect(second_candidate)
            try:
                self.assertEqual(
                    first.execute(
                        "SELECT * FROM entries ORDER BY id"
                    ).fetchall(),
                    next_database.execute(
                        "SELECT * FROM entries ORDER BY id"
                    ).fetchall(),
                )
                self.assertEqual(
                    first.execute(
                        "SELECT * FROM entries_fts ORDER BY rowid"
                    ).fetchall(),
                    next_database.execute(
                        "SELECT * FROM entries_fts ORDER BY rowid"
                    ).fetchall(),
                )
            finally:
                first.close()
                next_database.close()

    def test_fails_closed_for_digest_metadata_and_output_misuse(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.sqlite"
            oewn = root / "oewn.xml.gz"
            wrong_version = root / "wrong-version.xml.gz"
            candidate = root / "candidate.sqlite"
            create_database(dataset)
            write_fixture(oewn)
            write_fixture(wrong_version, version="2024")

            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                prefill(
                    dataset,
                    oewn,
                    apply=True,
                    output=candidate,
                    expected_sha256="0" * 64,
                )
            self.assertFalse(candidate.exists())

            with self.assertRaisesRegex(ValueError, "unexpected OEWN version"):
                prefill(
                    dataset,
                    wrong_version,
                    apply=True,
                    output=candidate,
                    expected_sha256=digest(wrong_version),
                )
            self.assertFalse(candidate.exists())

            with self.assertRaisesRegex(ValueError, "requires --output"):
                prefill(
                    dataset,
                    oewn,
                    apply=True,
                    expected_sha256=digest(oewn),
                )
            with self.assertRaisesRegex(
                ValueError,
                "output must not be the input",
            ):
                prefill(
                    dataset,
                    oewn,
                    apply=True,
                    output=dataset,
                    expected_sha256=digest(oewn),
                )
            candidate.write_text("do not overwrite", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                prefill(
                    dataset,
                    oewn,
                    apply=True,
                    output=candidate,
                    expected_sha256=digest(oewn),
                )
            self.assertEqual(
                candidate.read_text(encoding="utf-8"),
                "do not overwrite",
            )

            malformed = root / "malformed.xml.gz"
            malformed_output = root / "malformed-output.sqlite"
            with gzip.open(malformed, "wt", encoding="utf-8") as stream:
                stream.write(
                    '<LexicalResource><Lexicon id="oewn" '
                    'label="Open English Wordnet" language="en" '
                    f'license="{EXPECTED_LICENSE}" version="2025+">'
                    "<LexicalEntry"
                )
            with self.assertRaises(ET.ParseError):
                prefill(
                    dataset,
                    malformed,
                    apply=True,
                    output=malformed_output,
                    expected_sha256=digest(malformed),
                )
            self.assertFalse(malformed_output.exists())


if __name__ == "__main__":
    unittest.main()
