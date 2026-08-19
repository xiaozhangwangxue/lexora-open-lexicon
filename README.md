# Lexora Open Lexicon

This is the open dictionary and offline-data project behind
[Lexora, a free bilingual dictionary and personal vocabulary book generator](https://lexora.12323456.xyz/en/vocabulary-book-generator).
The main application source is available in the
[Lexora GitHub repository](https://github.com/xiaozhangwangxue/lexora).

Reproducible, open-data SQLite snapshots for Lexora:

* `build/lexora-english-600k.sqlite` — 600,000 English headwords.
* `build/lexora-frequency-20k.sqlite` — the first 20,000 rows of the same
  deterministic frequency ranking.

## Build

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python tools/build_dataset.py
```

The builder streams the compressed Wiktionary dump and never loads it all into
memory. It writes FTS5 indexes and `build/manifest.json` plus a coverage report.

## Oxford-oriented open-data scope

```bash
python tools/build_oxford_scope.py
python tools/enrich_oxford_scope.py --limit 100
python tools/finalize_oxford_scope.py
```

The second command is resumable and rate-limited. Omit `--limit` to process
all pending terms. It creates `lexora-open-oxford-scope.sqlite`, the extracted
`lexora-open-oxford-frequency-20k.sqlite`, and a separate
`build/oxford-enrichment-state.sqlite`. This scope is an open-data approximation
of Oxford-oriented vocabulary, not an official Oxford/OED dataset.

## API

```bash
uvicorn service.server:app --host 0.0.0.0 --port 8080
curl http://127.0.0.1:8080/v1/lookup?term=word
curl http://127.0.0.1:8080/v1/suggest?prefix=wor
```

See `LICENSES.md` before redistributing. This is an open-data English lexicon,
not a copy of Oxford/OED proprietary definitions.

## Live relay

Lexora's public read-only relay is available at:

```text
https://dict.12323456.xyz/v1/lookup?term=word
https://dict.12323456.xyz/v1/suggest?prefix=wor
```

Cloudflare caches successful lookups and automatically fails over between two
Always Free OCI origins. Full open-data enrichment runs in two resumable
shards. Each shard persists progress locally and has a watchdog that restarts a
stalled collector without discarding completed entries.

## Downloads

The current SQLite snapshots are published as release assets:

https://github.com/xiaozhangwangxue/lexora-open-lexicon/releases/tag/lexicon-2026.07

## Safe fast-20k candidate

The raw global ranking must not be packaged directly. `wordfreq` scores for
arbitrary multi-token strings are not comparable with its single-word scores,
so they previously allowed phrases to dominate the first 20,000 rows. Build a
separate candidate instead:

```bash
python tools/fast20k_pipeline.py select \
  --canonical build/lexora-open-oxford-scope.sqlite \
  --output build/lexora-open-oxford-safe-20k.sqlite \
  --limit 20000 \
  --phrase-target 4000 \
  --repair-shards 2
```

The default policy keeps 16,000 words and up to 4,000 reliable phrases. Words
retain their single-token `wordfreq` order. Phrases require explicit dictionary
evidence, are ranked only within a bounded phrase stream, and are interleaved
deterministically. Canonical phrase rank is recorded only as an auditable
tie-breaker; it is not treated as globally comparable frequency evidence.

Selection uses bounded pages and disk-backed SQLite staging. Legitimate forms
such as `U.S.` and `people-to-people` remain distinct, while ellipses, affixes,
broken hyphens, unsupported punctuation, and phrases without dictionary
evidence are rejected. Every selected entry records its canonical ID, original
rank, conservative term key, ranking evidence, and content/identity digests.
The complete candidate, provenance and repair queue are published atomically;
`--replace` is required to replace an existing candidate.

The embedded queue contains exact canonical IDs, including replacement words
whose original rank is greater than 20,000. Every row receives a persisted
`shard_owner = canonical_id % shard_count`; both servers therefore use the
same partition even if their local database ID ranges differ. At startup the
consumer validates every identity in its complete shard before constructing
an HTTP client. Its provider-state file is permanently bound to the candidate
contract digest, so a different queue requires a fresh state file.

```bash
python tools/enrich_oxford_scope.py \
  --dataset build/lexora-open-oxford-scope.sqlite \
  --state state/fast20k-repair-0.sqlite \
  --repair-queue build/lexora-open-oxford-safe-20k.sqlite \
  --quality-repair-only \
  --shard-index 0 --shard-count 2
```

Export only the exact fixed queue rows from each server, require a complete,
non-overlapping union, and apply that union to a new canonical snapshot:

```bash
python tools/fast20k_repair_delta.py export \
  --dataset build/server-0.sqlite \
  --candidate build/lexora-open-oxford-safe-20k.sqlite \
  --output build/repair-0.sqlite --shard-index 0 --shard-count 2

python tools/fast20k_repair_delta.py validate-union \
  --candidate build/lexora-open-oxford-safe-20k.sqlite \
  build/repair-0.sqlite build/repair-1.sqlite

python tools/fast20k_repair_delta.py apply-union \
  --canonical build/canonical-snapshot.sqlite \
  --candidate build/lexora-open-oxford-safe-20k.sqlite \
  --output build/canonical-repaired.sqlite \
  build/repair-0.sqlite build/repair-1.sqlite

python tools/fast20k_pipeline.py refresh \
  --canonical build/canonical-repaired.sqlite \
  --template build/lexora-open-oxford-safe-20k.sqlite \
  --output build/lexora-open-oxford-safe-20k-refreshed.sqlite

python tools/fast20k_pipeline.py validate \
  --canonical build/canonical-repaired.sqlite \
  --candidate build/lexora-open-oxford-safe-20k-refreshed.sqlite \
  --expected-rows 20000
```

`refresh` preserves every selected canonical ID, selected rank, stream and
term key. It updates only canonical content and the remaining repair queue; it
never re-runs selection after enrichment, so POS repairs cannot silently drift
the 20,000-entry set.

Packaging accepts the same CLI plus `--fast-source`. The gate runs before the
output directory is created and verifies the exact candidate against the
canonical database. It checks required fields, reliable IPA for ordinary
words, JSON and source provenance, continuous ranks, FTS and SQLite integrity,
repair-queue consistency, and canonical identity/content digests. Omitting
`--fast-source` remains parse-compatible but fails closed because a canonical
database has no candidate provenance.

```bash
python tools/package_offline_lexicons.py \
  --source build/canonical-repaired.sqlite \
  --fast-source build/lexora-open-oxford-safe-20k-refreshed.sqlite \
  --output-dir build/offline-release \
  --version YYYY.MM.DD \
  --fast-only
```

The packager uses SQLite online backups to stage both inputs, gates those exact
snapshots, copies the candidate into a private release directory, and gates
that actual release copy again before compression. Fast-only is the default;
`--include-full` is currently a fail-closed reserved flag and always exits
before creating output. A future release may enable it only after separate
full-collection and full-quality gates are implemented.
