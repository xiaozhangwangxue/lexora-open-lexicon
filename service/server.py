#!/usr/bin/env python3
from __future__ import annotations
import json
import os
import sqlite3
import hashlib
import re
import threading
import time
from datetime import date, datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Query, Request, UploadFile
from pydantic import BaseModel, Field, field_validator
from fastapi.responses import FileResponse, JSONResponse

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build"
STATE = ROOT / "state"
ORIGIN_TOKEN = os.environ.get("LEXORA_ORIGIN_TOKEN", "")
USAGE_DB = Path(os.environ.get("LEXORA_WEB_USAGE_DB", ROOT / "state" / "web-usage.sqlite"))
WEB_LOOKUP_DAILY_LIMIT = int(os.environ.get("LEXORA_WEB_LOOKUP_DAILY_LIMIT", "10000"))
WEB_PDF_DAILY_LIMIT = int(os.environ.get("LEXORA_WEB_PDF_DAILY_LIMIT", "250"))
WEB_PDF_CONCURRENCY = max(1, int(os.environ.get("LEXORA_WEB_PDF_CONCURRENCY", "1")))
PDF_SEMAPHORE = threading.BoundedSemaphore(WEB_PDF_CONCURRENCY)
app = FastAPI(title="Lexora Open Lexicon API", version="1.0")

@app.middleware("http")
async def protect_origin(request: Request, call_next):
    """Keep the public OCI origin private behind the Cloudflare relay."""
    if (
        ORIGIN_TOKEN
        and request.url.path != "/health"
        and request.headers.get("x-lexora-origin-token") != ORIGIN_TOKEN
    ):
        return JSONResponse({"detail": "forbidden"}, status_code=403)
    return await call_next(request)

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
    for field in ("synonyms_json", "antonyms_json", "examples_json", "phrases_json", "phrase_entries_json", "related_words_json", "related_entries_json", "senses_json", "source_json", "scope_json", "enrichment_json"):
        if field not in result:
            continue
        key = field[:-5] if field.endswith("_json") else field
        result[key] = json.loads(result.pop(field) or "[]")
    return result

def init_usage_db() -> None:
    USAGE_DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(USAGE_DB) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE IF NOT EXISTS daily_usage (day TEXT NOT NULL, client_hash TEXT NOT NULL, kind TEXT NOT NULL, count INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL, PRIMARY KEY(day,client_hash,kind))")

def client_hash(request: Request) -> str:
    value = request.headers.get("x-lexora-client-hash", "")
    if not re.fullmatch(r"[a-f0-9]{64}", value):
        raise HTTPException(400, "invalid client identity")
    return value

def quota_limit(kind: str) -> int:
    return WEB_PDF_DAILY_LIMIT if kind == "pdf" else WEB_LOOKUP_DAILY_LIMIT

def quota_remaining(identity: str, kind: str) -> int:
    init_usage_db()
    with sqlite3.connect(USAGE_DB) as db:
        row = db.execute("SELECT count FROM daily_usage WHERE day=? AND client_hash=? AND kind=?", (date.today().isoformat(), identity, kind)).fetchone()
    return max(0, quota_limit(kind) - int(row[0] if row else 0))

def consume_quota(identity: str, kind: str, amount: int = 1) -> int:
    init_usage_db()
    today = date.today().isoformat()
    with sqlite3.connect(USAGE_DB, timeout=5) as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT count FROM daily_usage WHERE day=? AND client_hash=? AND kind=?", (today, identity, kind)).fetchone()
        count = int(row[0] if row else 0)
        limit = quota_limit(kind)
        if count + amount > limit:
            db.rollback()
            raise HTTPException(429, f"daily {kind} quota exceeded")
        next_count = count + amount
        db.execute("INSERT INTO daily_usage(day,client_hash,kind,count,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(day,client_hash,kind) DO UPDATE SET count=excluded.count,updated_at=excluded.updated_at", (today, identity, kind, next_count, int(time.time())))
        db.commit()
    return limit - next_count

def lookup_entry(term: str, dataset: str = "oxford") -> dict[str, Any]:
    path = db_path(dataset)
    query = term.strip().lower()
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM entries WHERE normalized_word=? LIMIT 1", (query,)).fetchone()
        if not row:
            row = db.execute("SELECT * FROM entries WHERE normalized_word LIKE ? ORDER BY frequency_rank LIMIT 1", (query + "%",)).fetchone()
        if not row:
            candidates = db.execute("SELECT normalized_word FROM entries WHERE normalized_word LIKE ? ORDER BY frequency_rank LIMIT 5000", (query[:1] + "%",)).fetchall()
            ranked = sorted(((SequenceMatcher(None, query, c[0]).ratio(), c[0]) for c in candidates), reverse=True)
            if ranked and ranked[0][0] >= 0.72:
                row = db.execute("SELECT * FROM entries WHERE normalized_word=? LIMIT 1", (ranked[0][1],)).fetchone()
        if not row:
            raise HTTPException(404, "word not found")
        result = row_json(row)
        result["match_type"] = "fuzzy" if result["normalized_word"] != query else "exact"
        if result["match_type"] == "fuzzy":
            result["matched_word"] = result["word"]
        return result

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

@app.get("/v1/progress")
def collection_progress() -> dict[str, Any]:
    """Return this origin's cached shard progress without touching the live DB."""
    snapshots = sorted(
        STATE.glob("progress-shard-*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not snapshots:
        raise HTTPException(503, "progress snapshot is not ready")
    try:
        payload = json.loads(snapshots[0].read_text(encoding="utf-8"))
        finished = max(0, int(payload["finished"]))
        total = max(0, int(payload["total"]))
        shard = int(snapshots[0].stem.rsplit("-", 1)[-1])
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as error:
        raise HTTPException(503, "progress snapshot is invalid") from error
    result: dict[str, Any] = {
        "shard": shard,
        "finished": min(finished, total) if total else finished,
        "total": total,
        "remaining": max(0, total - finished),
        "percent": round((finished / total * 100) if total else 100.0, 3),
        "updatedAt": payload.get("updatedAt"),
    }
    for source, target in (
        ("entry_status", "entryStatus"),
        ("provider_status", "providerStatus"),
    ):
        value = payload.get(source)
        if isinstance(value, dict):
            result[target] = {
                str(key): max(0, int(count))
                for key, count in value.items()
                if isinstance(count, (int, float))
            }
    attempts = payload.get("provider_attempts")
    if isinstance(attempts, (int, float)):
        result["providerAttempts"] = max(0, int(attempts))

    quality_path = STATE / f"top20k-quality-shard-{shard}.json"
    try:
        quality = json.loads(quality_path.read_text(encoding="utf-8"))
        quality_updated_at = datetime.fromisoformat(
            str(quality["updatedAt"]).replace("Z", "+00:00")
        )
        if quality_updated_at.tzinfo is None:
            quality_updated_at = quality_updated_at.replace(tzinfo=timezone.utc)
        if (datetime.now(timezone.utc) - quality_updated_at).total_seconds() > 3 * 3600:
            raise ValueError("quality snapshot is stale")
        identity = quality["datasetIdentity"]
        dataset_stat = (BUILD / "lexora-open-oxford-scope.sqlite").stat()
        if (
            int(identity["device"]) != dataset_stat.st_dev
            or int(identity["inode"]) != dataset_stat.st_ino
        ):
            raise ValueError("quality snapshot belongs to a different dataset")
        quality_total = max(0, int(quality["total"]))
        quality_complete = max(0, int(quality["complete"]))
        quality_incomplete = max(0, int(quality["incomplete"]))
        if quality_complete + quality_incomplete != quality_total:
            raise ValueError("quality counts do not add up")
        result["top20k"] = {
            "total": quality_total,
            "complete": quality_complete,
            "incomplete": quality_incomplete,
            "percent": round(
                (quality_complete / quality_total * 100)
                if quality_total
                else 100.0,
                3,
            ),
            "terms": quality.get("terms", {}),
            "missing": quality.get("missing", {}),
            "entryStatus": quality.get("entryStatus", {}),
            "unresolved": quality.get("unresolved", []),
            "updatedAt": quality.get("updatedAt"),
            "qualityGateVersion": int(quality.get("qualityGateVersion", 1)),
        }
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        result["top20k"] = None
    return result

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
def lookup(term: str = Query(min_length=1, max_length=120), dataset: str = "oxford"):
    return lookup_entry(term, dataset)

class PdfRequest(BaseModel):
    title: str = Field(default="My vocabulary book", max_length=60)
    terms: list[str] = Field(min_length=1)
    fontPreset: str = "medium"
    examples: int = Field(default=1, ge=0, le=3)
    format: str = "pdf"
    pageSize: str = "a4"
    smartReorder: bool = False
    typography: dict[str, float] = Field(default_factory=dict)

    @field_validator("terms")
    @classmethod
    def validate_terms(cls, values: list[str]) -> list[str]:
        normalized = []
        seen = set()
        for value in values:
            term = re.sub(r"\s+", " ", value.strip().lower())
            if not re.fullmatch(r"[a-z][a-z' .-]{0,119}", term):
                raise ValueError(f"invalid term: {value}")
            if term not in seen:
                normalized.append(term)
                seen.add(term)
        return normalized

    @field_validator("fontPreset")
    @classmethod
    def validate_preset(cls, value: str) -> str:
        if value not in {"small", "medium", "large"}:
            raise ValueError("invalid font preset")
        return value

    @field_validator("format")
    @classmethod
    def validate_format(cls, value: str) -> str:
        if value not in {"pdf", "epub", "docx", "images", "longImage"}:
            raise ValueError("invalid export format")
        return value

    @field_validator("pageSize")
    @classmethod
    def validate_page_size(cls, value: str) -> str:
        if value.lower() not in {"a4", "a5", "b5"}:
            raise ValueError("invalid page size")
        return value.lower()

@app.get("/v1/web/quota")
def web_quota(request: Request):
    identity = client_hash(request)
    return {"lookupsRemaining": quota_remaining(identity, "lookup"), "pdfsRemaining": quota_remaining(identity, "pdf")}

@app.get("/v1/web/lookup")
def web_lookup(request: Request, term: str = Query(min_length=1, max_length=120)):
    identity = client_hash(request)
    remaining = consume_quota(identity, "lookup")
    result = lookup_entry(term)
    return JSONResponse(result, headers={"X-Lexora-Daily-Remaining": str(remaining)})

@app.get("/v1/web/suggest")
def web_suggest(request: Request, prefix: str = Query(min_length=1, max_length=80), limit: int = Query(12, ge=1, le=20)):
    identity = client_hash(request)
    remaining = consume_quota(identity, "lookup")
    rows = suggest(prefix=prefix, dataset="oxford", limit=limit)
    return JSONResponse(rows, headers={"X-Lexora-Daily-Remaining": str(remaining)})

@app.post("/v1/web/import")
async def web_import(request: Request, file: UploadFile = File(...)):
    client_hash(request)
    raw = await file.read(10 * 1024 * 1024 + 1)
    if len(raw) > 10 * 1024 * 1024:
        raise HTTPException(413, "file is larger than 10 MB")
    from service.web_documents import extract_terms
    try:
        terms = extract_terms(raw, file.filename or "upload.txt")
    except ValueError as error:
        raise HTTPException(422, str(error)) from error
    return {"terms": terms, "count": len(terms)}

@app.post("/v1/web/generate")
def web_generate(payload: PdfRequest, request: Request, background_tasks: BackgroundTasks):
    identity = client_hash(request)
    remaining = consume_quota(identity, "pdf")
    if not PDF_SEMAPHORE.acquire(blocking=False):
        raise HTTPException(503, "PDF service is busy; please retry shortly")
    try:
        entries = []
        missing = []
        for term in payload.terms:
            try:
                entry = lookup_entry(term)
                entry["requested_term"] = term
                entries.append(entry)
            except HTTPException as error:
                if error.status_code == 404:
                    missing.append(term)
                else:
                    raise
        if not entries:
            raise HTTPException(422, "no terms could be found")
        from service.web_documents import build_document
        output, filename, media_type = build_document(
            entries,
            title=payload.title,
            preset=payload.fontPreset,
            example_count=payload.examples,
            output_format=payload.format,
            page_size=payload.pageSize,
            smart_reorder=payload.smartReorder,
            typography=payload.typography,
        )
        background_tasks.add_task(output.unlink, missing_ok=True)
        headers = {
            "X-Lexora-Filename": filename,
            "X-Lexora-Daily-Remaining": str(remaining),
            "X-Lexora-Skipped": ",".join(missing[:20]),
            "Cache-Control": "no-store",
        }
        return FileResponse(output, media_type=media_type, filename=filename, headers=headers, background=background_tasks)
    finally:
        PDF_SEMAPHORE.release()

@app.get("/v1/suggest")
def suggest(prefix: str = Query(min_length=1, max_length=80), dataset: str = "oxford", limit: int = Query(10, ge=1, le=50)):
    path = db_path(dataset)
    normalized_prefix = prefix.strip().lower()
    upper_bound = normalized_prefix + "\uffff"
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        # A LIKE prefix combined with frequency ordering made SQLite scan the
        # 1.7M-row frequency index on the micro instances. The bounded binary
        # range uses the unique normalized-word index and then sorts only the
        # matching prefix, reducing common suggestions from ~1s to ~10ms.
        rows = db.execute(
            """
            SELECT word,normalized_word,frequency,frequency_rank
            FROM entries
            WHERE normalized_word >= ? AND normalized_word < ?
            ORDER BY frequency_rank
            LIMIT ?
            """,
            (normalized_prefix, upper_bound, limit),
        ).fetchall()
        return [dict(row) for row in rows]

@app.api_route("/downloads/{filename}", methods=["GET", "HEAD"])
def download(filename: str):
    if filename not in {"lexora-english-600k.sqlite", "lexora-frequency-20k.sqlite", "lexora-open-oxford-scope.sqlite", "lexora-open-oxford-frequency-20k.sqlite", "manifest.json", "oxford-scope-manifest.json"}:
        raise HTTPException(404, "file not found")
    path = BUILD / filename
    if not path.exists():
        raise HTTPException(404, "file not found")
    return FileResponse(path, filename=filename, media_type="application/octet-stream")
