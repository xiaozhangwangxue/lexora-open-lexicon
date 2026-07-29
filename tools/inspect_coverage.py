#!/usr/bin/env python3
"""Print compact coverage statistics for a Lexora SQLite dataset."""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


FIELDS = (
    "definition",
    "definition_zh",
    "us_phonetic",
    "uk_phonetic",
    "synonyms_json",
    "antonyms_json",
    "examples_json",
    "phrases_json",
    "phrase_entries_json",
    "related_words_json",
    "related_entries_json",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=Path)
    args = parser.parse_args()
    database = sqlite3.connect(args.dataset)
    try:
        coverage_sql = ", ".join(
            [
                "COUNT(*)",
                *(
                    f"""
                    SUM(CASE WHEN {field} IS NOT NULL
                                  AND TRIM({field}) NOT IN ('', '[]', '{{}}')
                             THEN 1 ELSE 0 END)
                    """
                    for field in FIELDS
                ),
                "SUM(CASE WHEN instr(normalized_word, ' ') > 0 THEN 1 ELSE 0 END)",
            ]
        )
        coverage = database.execute(f"SELECT {coverage_sql} FROM entries").fetchone()
        total = coverage[0]
        fields = dict(zip(FIELDS, coverage[1 : 1 + len(FIELDS)]))
        statuses = dict(
            database.execute(
                """
                SELECT COALESCE(json_extract(enrichment_json, '$.status'), 'pending'),
                       COUNT(*)
                FROM entries
                GROUP BY 1
                """
            ).fetchall()
        )
        phrase_terms = coverage[-1]
        print(
            json.dumps(
                {
                    "total": total,
                    "fields": fields,
                    "enrichment": statuses,
                    "phraseTerms": phrase_terms,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    finally:
        database.close()


if __name__ == "__main__":
    main()
