# Lexora Open Lexicon 2026.07

## Assets

| Asset | Rows | Size | SHA-256 |
|---|---:|---:|---|
| `lexora-english-600k.sqlite` | 600,000 | 509,063,168 bytes | `385b4f24711f0a1c678c20ea2b0002f83fce138edd582b166b1db038f80178c2` |
| `lexora-frequency-20k.sqlite` | 20,000 | 26,075,136 bytes | `557bdede2e07d30ec7123ffa817b24e95197ce07592b68d5088121fc51a367d8` |

Both snapshots include exact, normalized, prefix and FTS5 indexes. The builder
uses wordfreq Zipf scores, falling back to ECDICT frequency rank only when a
word is absent from wordfreq.

For faster downloads, matching `.zst` assets are included:

| Compressed asset | Size | SHA-256 |
|---|---:|---|
| `lexora-english-600k.sqlite.zst` | 195,376,454 bytes | `810a1b381a0b825e23b8a48c814a92191e01be28bf57aa517a375bf69dfab105` |
| `lexora-frequency-20k.sqlite.zst` | 9,044,979 bytes | `8c185ae19c9ff97d9d7818c34f1b928c04a8c9c30a9816746a03def9be0465c3` |

## Attribution

ECDICT is MIT; English Wiktionary/Wiktextract and wordfreq data retain their
CC BY-SA/attribution requirements. See `LICENSES.md` and `licenses/`.

This is an open-data English lexicon, not a redistribution of Oxford/OED
proprietary definitions.
