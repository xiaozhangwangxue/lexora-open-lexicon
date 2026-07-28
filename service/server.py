#!/usr/bin/env python3
from __future__ import annotations
import json
import os
import sqlite3
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build"
app = FastAPI(title="Lexora Open Lexicon API", version="1.0")

def db_path(dataset: str) -> Path:
    name = {
        "full": "lexora-english-600k.sqlite",
        "top20k": "lexora-frequency-20k.sqlite",
        "oxford": "lexora-open-oxford-scope.sqlite",
        "oxford20k": "lexora-open-oxford-frequency-20k.sqlite",
    }.get(dataset, "lexora-frequency-20k.sqlite")
    path = BUILD / name
    if not path.exists():
        raise HTTPException(503, "dataset is not built")
    return path

def row_json(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    for field in ("synonyms_json", "antonyms_json", "examples_json", "phrases_json", "related_words_json", "senses_json", "source_json", "scope_json", "enrichment_json"):
        key = field[:-5] if field.endswith("_json") else field
        result[key] = json.loads(result.pop(field) or "[]")
    return result

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

@app.get("/manifest")
def manifest():
    path = BUILD / "manifest.json"
    if not path.exists():
        raise HTTPException(503, "manifest is not built")
    return JSONResponse(json.loads(path.read_text(encoding="utf-8")))

@app.get("/oxford-manifest")
def oxford_manifest():
    path = BUILD / "oxford-scope-manifest.json"
    if not path.exists():
        raise HTTPException(503, "Oxford-oriented scope is still building")
    return JSONResponse(json.loads(path.read_text(encoding="utf-8")))

@app.get("/v1/lookup")
def lookup(term: str = Query(min_length=1, max_length=120), dataset: str = "top20k"):
    path = db_path(dataset)
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM entries WHERE normalized_word=? LIMIT 1", (term.strip().lower(),)).fetchone()
        if not row:
            row = db.execute("SELECT * FROM entries WHERE normalized_word LIKE ? ORDER BY frequency_rank LIMIT 1", (term.strip().lower() + "%",)).fetchone()
        if not row:
            # Bounded fuzzy fallback: use the first character to keep the
            # candidate set small, then rank spelling similarity in Python.
            query = term.strip().lower()
            candidates = db.execute("SELECT normalized_word FROM entries WHERE normalized_word LIKE ? ORDER BY frequency_rank LIMIT 5000", (query[:1] + "%",)).fetchall()
            ranked = sorted(((SequenceMatcher(None, query, c[0]).ratio(), c[0]) for c in candidates), reverse=True)
            if ranked and ranked[0][0] >= 0.72:
                row = db.execute("SELECT * FROM entries WHERE normalized_word=? LIMIT 1", (ranked[0][1],)).fetchone()
        if not row:
            raise HTTPException(404, "word not found")
        result = row_json(row)
        if result["normalized_word"] != term.strip().lower():
            result["match_type"] = "fuzzy"
            result["matched_word"] = result["word"]
        else:
            result["match_type"] = "exact"
        return result

@app.get("/v1/suggest")
def suggest(prefix: str = Query(min_length=1, max_length=80), dataset: str = "top20k", limit: int = Query(10, ge=1, le=50)):
    path = db_path(dataset)
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute("SELECT word,normalized_word,frequency,frequency_rank FROM entries WHERE normalized_word LIKE ? ORDER BY frequency_rank LIMIT ?", (prefix.strip().lower() + "%", limit)).fetchall()
        return [dict(row) for row in rows]

@app.api_route("/downloads/{filename}", methods=["GET", "HEAD"])
def download(filename: str):
    if filename not in {"lexora-english-600k.sqlite", "lexora-frequency-20k.sqlite", "lexora-open-oxford-scope.sqlite", "lexora-open-oxford-frequency-20k.sqlite", "manifest.json", "oxford-scope-manifest.json"}:
        raise HTTPException(404, "file not found")
    path = BUILD / filename
    if not path.exists():
        raise HTTPException(404, "file not found")
    return FileResponse(path, filename=filename, media_type="application/octet-stream")
