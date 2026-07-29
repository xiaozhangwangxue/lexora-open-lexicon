from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from apply_kaikki_relation_delta import apply_delta  # noqa: E402
from build_kaikki_relation_delta import build_delta  # noqa: E402
from build_oxford_scope import SCHEMA  # noqa: E402


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def insert_entry(
    database: sqlite3.Connection,
    entry_id: int,
    word: str,
    *,
    related: list[str] | None = None,
    phrases: list[str] | None = None,
    senses: list[dict[str, object]] | None = None,
    source: list[str] | None = None,
    scope: dict[str, object] | None = None,
) -> None:
    database.execute(
        """
        INSERT INTO entries(
          id,word,normalized_word,pos,definition,definition_zh,
          phrases_json,related_words_json,senses_json,source_json,
          scope_json,enrichment_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            entry_id,
            word,
            word,
            "noun",
            f"{word} definition",
            f"{word} 中文",
            json.dumps(phrases or []),
            json.dumps(related or []),
            json.dumps(senses or []),
            json.dumps(source or []),
            json.dumps(scope or {}),
            '{"status":"completed","remote":"preserve"}',
        ),
    )


def create_dataset(path: Path, *, remote: bool = False) -> None:
    database = sqlite3.connect(path)
    database.executescript(SCHEMA)
    colour_sense = {
        "pos": "noun",
        "definitions": ["A property of visible light."],
        "custom": "preserve",
    }
    insert_entry(
        database,
        1,
        "colour",
        related=["server-only"] if remote else ["shade"],
        phrases=["server phrase"] if remote else [],
        senses=[colour_sense],
        source=["datamuse"] if remote else ["kaikki"],
        scope=(
            {
                "remote": True,
                "kaikkiRelationPrefill": {
                    "source": "kaikki",
                    "license": "stale",
                    "custom": "preserve",
                },
            }
            if remote
            else {"local": True}
        ),
    )
    insert_entry(
        database,
        2,
        "beta",
        related=["remote-beta"] if remote else [],
        source=["dictionary"] if remote else ["kaikki"],
        scope={"remote": True} if remote else {"local": True},
    )
    # Canonical target definitions let the delta add a flat word and its rich
    # entry together.  Form/alternative targets are deliberately absent from
    # this lookup path because those relations remain typed-only.
    insert_entry(
        database,
        3,
        "visual property",
        source=["kaikki"],
    )
    insert_entry(
        database,
        4,
        "software release",
        source=["kaikki"],
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


def create_dump(path: Path) -> None:
    rows = [
        {
            "word": "colour",
            "lang_code": "en",
            "pos": "noun",
            "senses": [
                {
                    "glosses": ["A property of visible light."],
                    "form_of": [{"word": "color"}],
                    "coordinate_terms": [{"word": "visual property"}],
                }
            ],
        },
        {
            "word": "colour",
            "lang_code": "en",
            "pos": "verb",
            "senses": [
                {
                    "glosses": ["To add colour."],
                    "alt_of": [{"word": "color in"}],
                }
            ],
        },
        {
            "word": "beta",
            "lang_code": "en",
            "pos": "noun",
            "senses": [
                {
                    "glosses": ["A test release."],
                    "hypernyms": [{"word": "software release"}],
                }
            ],
        },
    ]
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row) + "\n")


def create_noncontiguous_dump(path: Path) -> None:
    """Put one normalized term in two source regions."""
    rows = [
        {
            "word": "colour",
            "lang_code": "en",
            "pos": "noun",
            "senses": [
                {
                    "glosses": ["A property of visible light."],
                    "form_of": [{"word": "color"}],
                    "coordinate_terms": [{"word": "visual property"}],
                }
            ],
        },
        {
            "word": "beta",
            "lang_code": "en",
            "pos": "noun",
            "senses": [
                {
                    "glosses": ["A test release."],
                    "hypernyms": [{"word": "software release"}],
                }
            ],
        },
        {
            "word": "colour",
            "lang_code": "en",
            "pos": "noun",
            "senses": [
                {
                    "glosses": ["A property of visible light."],
                    "hypernyms": [{"word": "software release"}],
                }
            ],
        },
        {
            "word": "colour",
            "lang_code": "en",
            "pos": "verb",
            "senses": [
                {
                    "glosses": ["To add colour."],
                    "alt_of": [{"word": "color in"}],
                }
            ],
        },
    ]
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row) + "\n")


class KaikkiRelationDeltaTest(unittest.TestCase):
    def test_delta_builder_defaults_to_read_only_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "local.sqlite"
            dump = root / "kaikki.jsonl.gz"
            create_dataset(dataset)
            create_dump(dump)
            before = digest(dataset)

            report = build_delta(
                dataset,
                dump,
                start_id=2,
                end_id=2,
            )

            self.assertEqual(report["mode"], "dry-run")
            self.assertEqual(report["deltaRows"], 1)
            self.assertEqual(report["matchedTerms"], 1)
            self.assertEqual(before, digest(dataset))
            self.assertEqual(list(root.glob("*.tmp-*")), [])

    def test_builds_compact_addition_only_delta_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "local.sqlite"
            dump = root / "kaikki.jsonl.gz"
            delta = root / "relations.sqlite"
            create_dataset(dataset)
            create_dump(dump)

            report = build_delta(dataset, dump, output=delta)

            self.assertEqual(report["mode"], "write-delta")
            self.assertEqual(report["deltaRows"], 2)
            self.assertTrue(delta.is_file())
            database = sqlite3.connect(delta)
            try:
                metadata = dict(
                    database.execute("SELECT key,value FROM metadata")
                )
                self.assertEqual(metadata["source_license"], "CC BY-SA 4.0")
                self.assertEqual(metadata["source_sha256"], digest(dump))
                self.assertEqual(
                    metadata["source_url"],
                    "https://kaikki.org/dictionary/rawdata.html",
                )
                self.assertEqual(metadata["source_provider"], "Kaikki/Wiktextract")
                self.assertEqual(metadata["modified"], "true")
                row = database.execute(
                    """
                    SELECT entry_id,normalized_word,related_add_json,
                           phrases_add_json,senses_patch_json,source_add_json
                    FROM relation_delta
                    WHERE entry_id=1
                    """
                ).fetchone()
                self.assertEqual(row[:2], (1, "colour"))
                self.assertEqual(
                    json.loads(row[2]),
                    ["visual property"],
                )
                self.assertEqual(
                    json.loads(row[3]),
                    ["visual property"],
                )
                senses = json.loads(row[4])
                self.assertEqual(
                    senses[0]["relations"],
                    {
                        "form_of": ["color"],
                        "coordinate_terms": ["visual property"],
                    },
                )
                self.assertEqual(json.loads(row[5]), ["kaikki"])
                phrase_entries = database.execute(
                    """
                    SELECT related_entries_add_json,
                           phrase_entries_add_json
                    FROM relation_delta WHERE entry_id=1
                    """
                ).fetchone()
                self.assertEqual(
                    json.loads(phrase_entries[0])[0]["word"],
                    "visual property",
                )
                self.assertEqual(
                    json.loads(phrase_entries[1])[0]["word"],
                    "visual property",
                )
            finally:
                database.close()
            with self.assertRaises(FileExistsError):
                build_delta(dataset, dump, output=delta)

    def test_noncontiguous_term_chunks_merge_into_one_safe_delta(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local = root / "local.sqlite"
            remote = root / "remote.sqlite"
            dump = root / "kaikki.jsonl.gz"
            delta = root / "relations.sqlite"
            create_dataset(local)
            create_dataset(remote, remote=True)
            create_noncontiguous_dump(dump)

            dry_run = build_delta(local, dump, commit_size=1)
            report = build_delta(
                local,
                dump,
                output=delta,
                commit_size=1,
            )

            for result in (dry_run, report):
                self.assertEqual(result["relationTerms"], 2)
                self.assertEqual(result["matchedTerms"], 2)
                self.assertEqual(result["unmatchedTerms"], 0)
                self.assertEqual(result["deltaRows"], 2)
                self.assertEqual(result["valuesIncluded"]["related"], 3)
                self.assertEqual(
                    result["valuesIncluded"]["relatedEntries"],
                    3,
                )

            database = sqlite3.connect(delta)
            try:
                rows = database.execute(
                    "SELECT COUNT(*) FROM relation_delta"
                ).fetchone()[0]
                self.assertEqual(rows, 2)
                row = database.execute(
                    """
                    SELECT related_add_json,related_entries_add_json,
                           phrases_add_json,phrase_entries_add_json,
                           senses_patch_json
                    FROM relation_delta WHERE entry_id=1
                    """
                ).fetchone()
            finally:
                database.close()

            related = json.loads(row[0])
            related_entries = json.loads(row[1])
            phrases = json.loads(row[2])
            phrase_entries = json.loads(row[3])
            self.assertEqual(
                related,
                ["visual property", "software release"],
            )
            self.assertEqual(
                [value["word"] for value in related_entries],
                related,
            )
            self.assertEqual(phrases, related)
            self.assertEqual(
                [value["word"] for value in phrase_entries],
                phrases,
            )

            senses = json.loads(row[4])
            indexed = [
                value for value in senses if "sense_index" in value
            ]
            self.assertEqual(len(indexed), 1)
            self.assertEqual(indexed[0]["sense_index"], 0)
            self.assertRegex(
                indexed[0]["sense_fingerprint"],
                r"^[0-9a-f]{24}$",
            )
            self.assertEqual(
                indexed[0]["relations"],
                {
                    "form_of": ["color"],
                    "coordinate_terms": ["visual property"],
                    "hypernyms": ["software release"],
                },
            )
            full = [
                value for value in senses if "sense_index" not in value
            ]
            self.assertEqual(len(full), 1)
            self.assertEqual(
                full[0]["relations"],
                {"alt_of": ["color in"]},
            )

            first = apply_delta(remote, delta, apply=True, batch_size=1)
            second = apply_delta(remote, delta, apply=True, batch_size=1)
            self.assertEqual(first["changedRows"], 2)
            self.assertEqual(second["changedRows"], 0)

    def test_remote_apply_preserves_collected_data_and_is_idempotent(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local = root / "local.sqlite"
            remote = root / "remote.sqlite"
            dump = root / "kaikki.jsonl.gz"
            delta = root / "relations.sqlite"
            create_dataset(local)
            create_dataset(remote, remote=True)
            create_dump(dump)
            build_delta(local, dump, output=delta)
            before = digest(remote)

            dry_run = apply_delta(
                remote,
                delta,
                start_id=1,
                end_id=1,
                batch_size=1,
            )
            self.assertEqual(dry_run["mode"], "dry-run")
            self.assertEqual(dry_run["selectedRows"], 1)
            self.assertEqual(before, digest(remote))

            first = apply_delta(
                remote,
                delta,
                apply=True,
                start_id=1,
                end_id=1,
                batch_size=1,
            )
            second = apply_delta(
                remote,
                delta,
                apply=True,
                start_id=1,
                end_id=1,
                batch_size=1,
            )

            self.assertEqual(first["changedRows"], 1)
            self.assertEqual(second["changedRows"], 0)
            database = sqlite3.connect(remote)
            try:
                row = database.execute(
                    """
                    SELECT related_words_json,related_entries_json,
                           phrases_json,phrase_entries_json,senses_json,
                           source_json,scope_json,enrichment_json
                    FROM entries WHERE id=1
                    """
                ).fetchone()
                self.assertEqual(
                    json.loads(row[0]),
                    ["server-only", "visual property"],
                )
                self.assertEqual(
                    json.loads(row[1])[0]["word"],
                    "visual property",
                )
                self.assertEqual(
                    json.loads(row[2]),
                    ["server phrase", "visual property"],
                )
                self.assertEqual(
                    json.loads(row[3])[0]["word"],
                    "visual property",
                )
                senses = json.loads(row[4])
                self.assertEqual(senses[0]["custom"], "preserve")
                self.assertEqual(
                    senses[0]["relations"]["form_of"],
                    ["color"],
                )
                self.assertEqual(
                    json.loads(row[5]),
                    ["datamuse", "kaikki"],
                )
                scope = json.loads(row[6])
                self.assertTrue(scope["remote"])
                self.assertEqual(
                    scope["kaikkiRelationPrefill"]["license"],
                    "CC BY-SA 4.0",
                )
                self.assertRegex(
                    scope["kaikkiRelationPrefill"]["sourceSha256"],
                    r"^[0-9a-f]{64}$",
                )
                self.assertEqual(
                    scope["kaikkiRelationPrefill"]["custom"],
                    "preserve",
                )
                self.assertEqual(
                    json.loads(row[7]),
                    {"status": "completed", "remote": "preserve"},
                )
                # The shard boundary excluded id=2.
                beta = database.execute(
                    "SELECT related_words_json FROM entries WHERE id=2"
                ).fetchone()[0]
                self.assertEqual(json.loads(beta), ["remote-beta"])
            finally:
                database.close()

    def test_full_rich_columns_never_emit_unpaired_flat_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local = root / "local.sqlite"
            dump = root / "kaikki.jsonl.gz"
            delta = root / "relations.sqlite"
            create_dataset(local)
            create_dump(dump)
            full = [
                {
                    "word": f"occupied-{index}",
                    "definition": "Existing definition.",
                }
                for index in range(40)
            ]
            database = sqlite3.connect(local)
            database.execute(
                """
                UPDATE entries
                SET related_entries_json=?,phrase_entries_json=?
                WHERE id=1
                """,
                (json.dumps(full), json.dumps(full)),
            )
            database.commit()
            database.close()

            build_delta(local, dump, output=delta)

            database = sqlite3.connect(delta)
            try:
                row = database.execute(
                    """
                    SELECT related_add_json,related_entries_add_json,
                           phrases_add_json,phrase_entries_add_json
                    FROM relation_delta WHERE entry_id=1
                    """
                ).fetchone()
            finally:
                database.close()
            self.assertIsNotNone(row)
            self.assertEqual([json.loads(value) for value in row], [[], [], [], []])

    def test_remote_rich_capacity_skips_both_sides_of_each_pair(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local = root / "local.sqlite"
            remote = root / "remote.sqlite"
            dump = root / "kaikki.jsonl.gz"
            delta = root / "relations.sqlite"
            create_dataset(local)
            create_dataset(remote, remote=True)
            create_dump(dump)
            build_delta(local, dump, output=delta)
            full = [
                {
                    "word": f"remote-{index}",
                    "definition": "Remote definition.",
                }
                for index in range(40)
            ]
            database = sqlite3.connect(remote)
            database.execute(
                """
                UPDATE entries
                SET related_entries_json=?,phrase_entries_json=?
                WHERE id=1
                """,
                (json.dumps(full), json.dumps(full)),
            )
            database.commit()
            database.close()

            apply_delta(remote, delta, apply=True, start_id=1, end_id=1)

            database = sqlite3.connect(remote)
            try:
                row = database.execute(
                    """
                    SELECT related_words_json,related_entries_json,
                           phrases_json,phrase_entries_json
                    FROM entries WHERE id=1
                    """
                ).fetchone()
            finally:
                database.close()
            self.assertEqual(json.loads(row[0]), ["server-only"])
            self.assertEqual(len(json.loads(row[1])), 40)
            self.assertEqual(json.loads(row[2]), ["server phrase"])
            self.assertEqual(len(json.loads(row[3])), 40)

    def test_atomic_publish_refuses_file_created_during_scan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "local.sqlite"
            dump = root / "kaikki.jsonl.gz"
            delta = root / "relations.sqlite"
            create_dataset(dataset)
            create_dump(dump)
            real_open = gzip.open

            def occupy_output(*args: object, **kwargs: object):
                delta.write_text("do not overwrite", encoding="utf-8")
                return real_open(*args, **kwargs)

            with mock.patch(
                "build_kaikki_relation_delta.gzip.open",
                side_effect=occupy_output,
            ):
                with self.assertRaises(FileExistsError):
                    build_delta(dataset, dump, output=delta)

            self.assertEqual(
                delta.read_text(encoding="utf-8"),
                "do not overwrite",
            )
            self.assertEqual(list(root.glob(".*.tmp-*")), [])

    def test_applier_rejects_untraceable_source_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local = root / "local.sqlite"
            remote = root / "remote.sqlite"
            dump = root / "kaikki.jsonl.gz"
            delta = root / "relations.sqlite"
            create_dataset(local)
            create_dataset(remote, remote=True)
            create_dump(dump)
            build_delta(local, dump, output=delta)
            database = sqlite3.connect(delta)
            original = dict(
                database.execute("SELECT key,value FROM metadata")
            )
            database.close()

            cases = (
                ("source_sha256", "not-a-digest", "SHA-256"),
                ("source_url", "https://invalid.example", "source URL"),
                ("source_provider", "unknown", "source provider"),
                ("source_license", "unknown", "source license"),
            )
            for key, bad_value, message in cases:
                with self.subTest(key=key):
                    database = sqlite3.connect(delta)
                    database.execute(
                        "UPDATE metadata SET value=? WHERE key=?",
                        (bad_value, key),
                    )
                    database.commit()
                    database.close()
                    with self.assertRaisesRegex(ValueError, message):
                        apply_delta(remote, delta)
                    database = sqlite3.connect(delta)
                    database.execute(
                        "UPDATE metadata SET value=? WHERE key=?",
                        (original[key], key),
                    )
                    database.commit()
                    database.close()

    def test_identity_mismatch_is_rejected_before_any_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local = root / "local.sqlite"
            remote = root / "remote.sqlite"
            dump = root / "kaikki.jsonl.gz"
            delta = root / "relations.sqlite"
            create_dataset(local)
            create_dataset(remote, remote=True)
            create_dump(dump)
            build_delta(local, dump, output=delta)
            patch = sqlite3.connect(delta)
            patch.execute(
                "UPDATE relation_delta SET normalized_word='wrong' "
                "WHERE entry_id=2"
            )
            patch.commit()
            patch.close()
            before = digest(remote)

            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                apply_delta(
                    remote,
                    delta,
                    apply=True,
                    batch_size=1,
                )

            self.assertEqual(before, digest(remote))

    def test_unpaired_flat_value_is_rejected_before_any_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local = root / "local.sqlite"
            remote = root / "remote.sqlite"
            dump = root / "kaikki.jsonl.gz"
            delta = root / "relations.sqlite"
            create_dataset(local)
            create_dataset(remote, remote=True)
            create_dump(dump)
            build_delta(local, dump, output=delta)
            patch = sqlite3.connect(delta)
            patch.execute(
                """
                UPDATE relation_delta
                SET related_entries_add_json='[]'
                WHERE entry_id=1
                """
            )
            patch.commit()
            patch.close()
            before = digest(remote)

            with self.assertRaisesRegex(
                ValueError,
                "unsynchronized related",
            ):
                apply_delta(remote, delta, apply=True)

            self.assertEqual(before, digest(remote))


if __name__ == "__main__":
    unittest.main()
