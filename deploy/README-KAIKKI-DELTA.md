# Kaikki sense-relation delta

These tools reuse the downloaded English Wiktionary Wiktextract dump without
copying its 2.6 GB compressed source to every collector.

## Safety model

- `prefill_kaikki_sense_relations.py` defaults to a read-only dry-run. Its
  standalone `--apply` flag is deliberately rejected, so this command cannot
  edit the canonical dataset. Typed relations are transferred only through
  the validated compact-delta workflow below.
- `build_kaikki_relation_delta.py` also defaults to a read-only dry-run.
  `--write-delta PATH` is required to create a new SQLite delta, and an
  existing output is never replaced.
- `apply_kaikki_relation_delta.py` defaults to a read-only validation pass.
  `--apply` is required to write the target database.
- The applier validates the delta's SQLite integrity, schema, row count,
  source filename and SHA-256, official URL, provider/license metadata, entry
  ID, normalized term, and compact sense fingerprint before writes. The same
  source digest, URL, license, dump dates and modification notice are retained
  in `scope_json.kaikkiRelationPrefill`. Updates are append-only, idempotent,
  and use bounded transactions.
- The delta never contains definitions or enrichment state for the source
  entry, so applying it cannot overwrite server-side collection progress.

`form_of` and `alt_of` remain typed relations only. The legacy flat related
columns receive only semantic relations in this order: hypernyms, hyponyms,
coordinate terms, meronyms, holonyms, and troponyms. A flat word is emitted
only when its canonical target has an English definition and both target
columns have capacity. Every `related_words_json` addition carries the same
normalized target in `related_entries_json`; every `phrases_json` addition
does the same in `phrase_entries_json`. The remote applier rechecks both
capacities and merges each flat/rich pair atomically.

## Local generation

First inspect the complete plan without writing:

```sh
python3 tools/build_kaikki_relation_delta.py \
  --dataset build/lexora-open-oxford-scope.sqlite
```

Then explicitly create a new compact delta:

```sh
python3 tools/build_kaikki_relation_delta.py \
  --dataset build/lexora-open-oxford-scope.sqlite \
  --write-delta build/kaikki-sense-relations.sqlite
```

`--start-id` and `--end-id` are inclusive and can build only one server's
range. A full delta can instead be copied once and filtered during application.

## Server-side application

Stop the collector for the selected shard and checkpoint its WAL before
applying any delta. Validate first:

```sh
python3 tools/apply_kaikki_relation_delta.py \
  --dataset /srv/lexora/lexora-open-oxford-scope.sqlite \
  --delta /srv/lexora/kaikki-sense-relations.sqlite \
  --start-id START \
  --end-id END
```

Only after the dry-run succeeds, repeat with `--apply`. Run SQLite
`PRAGMA quick_check`, verify the shard counters and FTS rows, and then restart
the collector. Never replace the server database with the local canonical
copy; the delta is designed to preserve already-collected values.

The source is the English Wiktionary dump dated 2026-07-06, extracted by
Wiktextract/Kaikki on 2026-07-25. Wiktionary text is available under
CC BY-SA 4.0; see `LICENSES.md` and `licenses/KAikki-rawdata.md`.
