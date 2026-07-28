# Lexora Open Lexicon 2026.07

## Assets

| Asset | Rows | Size | SHA-256 |
|---|---:|---:|---|
| `lexora-english-600k.sqlite` | 600,000 | 411,316,224 bytes | `00830f50a3f0adc5ba7bdb0b4c8a8fe28ad3951dd11efeadc70249e03cba42cb` |
| `lexora-frequency-20k.sqlite` | 20,000 | 22,900,736 bytes | `58942c18a67a8c924802543f2d000aeb259b215c22ef725e319a5be243fc1789` |

Both snapshots include exact, normalized, prefix and FTS5 indexes. The builder
uses wordfreq Zipf scores, falling back to ECDICT frequency rank only when a
word is absent from wordfreq.

## Attribution

ECDICT is MIT; English Wiktionary/Wiktextract and wordfreq data retain their
CC BY-SA/attribution requirements. See `LICENSES.md` and `licenses/`.

This is an open-data English lexicon, not a redistribution of Oxford/OED
proprietary definitions.
