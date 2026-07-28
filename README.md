# Lexora Open Lexicon

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

## API

```bash
uvicorn service.server:app --host 0.0.0.0 --port 8080
curl http://127.0.0.1:8080/v1/lookup?term=word
curl http://127.0.0.1:8080/v1/suggest?prefix=wor
```

See `LICENSES.md` before redistributing. This is an open-data English lexicon,
not a copy of Oxford/OED proprietary definitions.
