#!/usr/bin/env python3
"""Finalize an enrichment run: rank, rebuild FTS, extract top 20k and manifest."""
from __future__ import annotations
import datetime as dt
import hashlib
import json
import sqlite3
from pathlib import Path
from build_oxford_scope import BUILD, FULL_NAME, TOP_NAME, SCHEMA, make_top, sha256

def main() -> None:
    full = BUILD / FULL_NAME
    if not full.exists():
        raise SystemExit(f"missing {full}")
    db = sqlite3.connect(full)
    rows = db.execute("SELECT id,frequency FROM entries").fetchall()
    ranked = sorted(((float(score or -20.0), row_id) for row_id, score in rows), key=lambda item: (-item[0], item[1]))
    db.execute("BEGIN")
    for rank, (_score, row_id) in enumerate(ranked, 1):
        db.execute("UPDATE entries SET frequency_rank=? WHERE id=?", (rank, row_id))
    db.execute("DELETE FROM entries_fts")
    db.execute("""INSERT INTO entries_fts(rowid,word,definition,definition_zh,examples,phrases)
      SELECT id,word,definition,definition_zh,examples_json,phrases_json FROM entries""")
    db.commit(); db.execute("PRAGMA journal_mode=DELETE"); db.commit(); db.close()
    top = BUILD / TOP_NAME
    make_top(full, top)
    manifest = {"schema_version": 2, "scope": "open-data approximation of Oxford-oriented vocabulary", "enriched_at": dt.datetime.now(dt.timezone.utc).isoformat(), "datasets": {}}
    for key, path in (("scope", full), ("frequency_20k", top)):
        con = sqlite3.connect(path)
        total = con.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        enriched = con.execute("SELECT COUNT(*) FROM entries WHERE enrichment_json NOT IN ('','{}')").fetchone()[0]
        complete = con.execute("SELECT COUNT(*) FROM entries WHERE enrichment_json LIKE '%\"status\":\"completed\"%'").fetchone()[0]
        manifest["datasets"][key] = {"file": path.name, "rows": total, "enriched_rows": enriched, "completed_rows": complete, "bytes": path.stat().st_size, "sha256": sha256(path)}
        con.close()
    (BUILD / "oxford-scope-manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
