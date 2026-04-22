"""
Krishi-Setu Dashboard API
-------------------------
Run:
  uvicorn dashboard_app:app --reload --port 8008

Open:
  http://127.0.0.1:8008
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

try:
    from scraper import SchemeDatabase
except Exception:
    SchemeDatabase = None


BASE_DIR = Path(__file__).resolve().parent
UI_DIR = BASE_DIR / "dashboard_ui"
OUTPUT_DIR = BASE_DIR / "output"
REPORT_DIR = OUTPUT_DIR / "reports"
DATA_DIR = OUTPUT_DIR / "data"
DEFAULT_DB_PATH = BASE_DIR / "output" / "db" / "agri_schemes.db"
SCRAPER_PATH = BASE_DIR / "scraper.py"

MAX_LOG_LINES = 1500

SMART_SYNONYMS = {
    "insurance": ["bima", "risk", "crop cover", "premium"],
    "credit": ["loan", "kcc", "kisan credit card", "finance"],
    "subsidy": ["support", "grant", "assistance", "benefit"],
    "income": ["direct benefit", "dbt", "cash transfer"],
    "farmer": ["kisan", "cultivator", "agri"],
    "irrigation": ["water", "micro irrigation", "sprinkler", "drip"],
    "organic": ["natural farming", "bio", "chemical free"],
}

SEARCH_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "how",
    "in",
    "into",
    "is",
    "it",
    "low",
    "of",
    "on",
    "or",
    "that",
    "the",
    "their",
    "these",
    "this",
    "to",
    "under",
    "with",
    "scheme",
    "schemes",
    "program",
    "programme",
}

INTENT_TYPE_HINTS = {
    "insurance": ["insurance", "bima", "crop_insurance", "risk"],
    "credit": ["credit", "loan", "finance", "kcc"],
    "subsidy": ["subsidy", "support", "grant", "assistance"],
    "income": ["income", "cash", "dbt", "support"],
    "irrigation": ["irrigation", "water", "sprinkler", "drip"],
    "organic": ["organic", "natural", "bio"],
    "farmer": ["farmer", "kisan", "agri"],
}

STRONG_SCHEME_KEYWORDS = ["yojana", "mission", "bima", "kisan", "card", "fund"]

NOISE_TITLE_MARKERS = [
    "implementation period",
    "scheme guidelines",
    "mission document",
    "district mission committee",
    "for scheme code",
    "consultative group",
    "program advocacy",
    "mission objectives",
    "mission structure",
    "mission interventions",
    "centrally sponsored scheme",
    "central sector scheme",
    "objectives of scheme",
    "support to state extension programme",
    "programme for extension reforms",
    "cabinet approves",
    "conference on implementing",
    "permissible components",
    "accept it",
    "prime minister shri",
]

JOB_LOCK = threading.Lock()
SCRAPE_JOBS: Dict[str, Dict[str, Any]] = {}


class ScrapeJobRequest(BaseModel):
    url: str = Field(..., min_length=8)
    depth: int = Field(1, ge=0, le=5)
    max_pages: int = Field(20, ge=1, le=500)
    max_files: int = Field(80, ge=0, le=1000)
    js: bool = False
    selenium_site: bool = True
    no_ai: bool = False
    all_domains: bool = False
    model: str = ""
    api_key: Optional[str] = None
    db_path: Optional[str] = None


class SchemeEligibilityRequest(BaseModel):
    age: Optional[int] = Field(None, ge=0, le=120)
    annual_income: Optional[float] = Field(None, ge=0)
    category: Optional[str] = Field("", max_length=100)
    state: Optional[str] = Field("", max_length=100)
    district: Optional[str] = Field("", max_length=100)
    landholding_hectares: Optional[float] = Field(None, ge=0)
    has_aadhaar: Optional[bool] = None
    has_land_record: Optional[bool] = None
    has_bank_account: Optional[bool] = None
    has_insurance: Optional[bool] = None


app = FastAPI(title="Krishi-Setu Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if UI_DIR.exists():
    app.mount("/ui", StaticFiles(directory=UI_DIR), name="ui")
if OUTPUT_DIR.exists():
    app.mount("/artifacts", StaticFiles(directory=OUTPUT_DIR), name="artifacts")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def normalize_url_input(raw_url: str) -> str:
    text = (raw_url or "").strip()
    if not text:
        return ""
    if not text.lower().startswith(("http://", "https://")):
        text = "https://" + text
    return text


def resolve_db_path(override: Optional[str] = None) -> Path:
    if override:
        return Path(override).expanduser().resolve()
    env_override = os.environ.get("KRISHI_DB_PATH", "").strip()
    if env_override:
        return Path(env_override).expanduser().resolve()
    return DEFAULT_DB_PATH


def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"Database not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def parse_stats(raw_stats: str) -> Dict[str, Any]:
    try:
        return json.loads(raw_stats or "{}")
    except Exception:
        return {}


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row)


def parse_filter_terms(raw_text: str) -> List[str]:
    terms: List[str] = []
    for part in re.split(r"[,/|]+", (raw_text or "").lower()):
        normalized = re.sub(r"\s+", " ", part).strip()
        if normalized and normalized not in terms:
            terms.append(normalized)
    return terms


def tokenize(text: str) -> List[str]:
    return [token for token in re.findall(r"[a-z0-9]{2,}", (text or "").lower()) if token]


def filter_query_tokens(tokens: List[str]) -> List[str]:
    filtered = [token for token in tokens if token not in SEARCH_STOPWORDS]
    return filtered or tokens


def smart_tokens(query_text: str, mode: str) -> List[str]:
    base = filter_query_tokens(tokenize(query_text))
    if mode != "smart":
        return base

    expanded = set(base)
    for token in base:
        for root, variants in SMART_SYNONYMS.items():
            root_tokens = tokenize(root)
            variant_tokens = []
            for variant in variants:
                variant_tokens.extend(tokenize(variant))

            if token in root_tokens or token in variant_tokens:
                expanded.update(root_tokens)
                expanded.update(variant_tokens)
    return sorted(filter_query_tokens(list(expanded)))


def infer_query_intents(tokens: List[str]) -> List[str]:
    intents = set()
    for token in tokens:
        for root, variants in SMART_SYNONYMS.items():
            if root == "farmer":
                continue
            root_tokens = set(tokenize(root))
            variant_tokens: set[str] = set()
            for variant in variants:
                variant_tokens.update(tokenize(variant))
            if token in root_tokens or token in variant_tokens:
                intents.add(root)
    return sorted(intents)


def match_token_score(text: str, tokens: List[str], per_token: int) -> int:
    lower = (text or "").lower()
    score = 0
    for token in tokens:
        if not token:
            continue
        if len(token) >= 3 and re.search(rf"\b{re.escape(token)}\b", lower):
            score += per_token
        elif token in lower:
            score += max(1, per_token // 3)
    return score


def intent_alignment_score(item: Dict[str, Any], intents: List[str]) -> int:
    if not intents:
        return 0

    scheme_type = str(item.get("scheme_type", "") or "").lower()
    name = str(item.get("scheme_name", "") or "").lower()

    score = 0
    intent_hits = 0
    for intent in intents:
        hints = INTENT_TYPE_HINTS.get(intent, [intent])
        if any(hint in scheme_type for hint in hints):
            score += 18
            intent_hits += 1
            continue
        if any(hint in name for hint in hints):
            score += 12
            intent_hits += 1
            continue

    if intent_hits >= 2:
        score += 6
    return score


def looks_like_noise_title(name: str) -> bool:
    text = (name or "").strip()
    lower = text.lower()
    if not text:
        return True
    if len(text) < 8 or len(text) > 180:
        return True
    if re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", lower):
        return True
    if re.search(r"\b\d{1,2}:\d{2}\b", lower):
        return True
    if re.match(r"^\s*([ivxlcdm]+|\d+|[a-z])\s*[\).:-]\s+", lower):
        return True
    if lower.startswith(("chapter ", "step ", "table ", "figure ", "link ", "section ", "annexure")):
        return True
    if re.search(r"\b(sl\.?\s*no\.?|sno|scheme\s*code|refer)\b", lower):
        return True
    if re.search(r"\blink\s*\d+\b", lower):
        return True
    if text.count("(") != text.count(")") and len(re.findall(r"[A-Za-z]{2,}", text)) <= 10:
        return True

    words = re.findall(r"[A-Za-z]{2,}", text)
    if len(words) < 2 or len(words) > 16:
        return True
    if re.search(r"\b\d+\s*$", lower) and len(words) <= 6:
        return True
    if lower.startswith(("with ", "while ", "towards ", "the ", "to ")) and len(words) >= 7:
        return True
    if lower.startswith(("cabinet approves", "prime minister", "conference on", "backend ", "permissible components", "accept it")):
        return True
    if re.search(r"[.;:]", text) and len(words) >= 6:
        return True

    if text.count(",") >= 2 or text.endswith((":", ";", ".")):
        return True

    sentence_words = {
        "the",
        "this",
        "that",
        "while",
        "with",
        "for",
        "from",
        "to",
        "in",
        "of",
        "on",
        "under",
        "has",
        "have",
        "had",
        "is",
        "are",
        "was",
        "were",
        "will",
        "would",
        "may",
        "can",
        "shall",
        "been",
    }
    sentence_hits = sum(1 for token in re.findall(r"[a-z]+", lower) if token in sentence_words)
    if sentence_hits >= max(3, len(words) // 3) and len(words) >= 7:
        return True

    return False


def is_obvious_noise_master_row(item: Dict[str, Any]) -> bool:
    name = str(item.get("scheme_name", "") or "")
    lower = name.lower()
    score = int(item.get("confidence_score", 0) or 0)
    scheme_type = str(item.get("scheme_type", "") or "").lower()
    ministry = str(item.get("nodal_ministry", "") or "").strip()
    website = str(item.get("official_website", "") or "").strip().lower().startswith("http")
    source_count = int(item.get("source_count", 0) or 0)

    if looks_like_noise_title(name) and score < 80:
        return True

    if any(marker in lower for marker in NOISE_TITLE_MARKERS) and score < 90:
        return True

    if scheme_type == "other" and score < 45 and not ministry and not website and source_count <= 3 and looks_like_noise_title(name):
        return True

    if len(re.findall(r"[A-Za-z]{2,}", name)) > 16 and score < 90:
        return True

    return False


def looks_like_scheme_name(name: str) -> bool:
    text = (name or "").strip()
    lower = text.lower()
    words = re.findall(r"[A-Za-z]{2,}", text)

    if len(words) < 2 or len(words) > 14:
        return False
    if re.search(r"[.;]", text):
        return False
    if lower.startswith(
        (
            "the ",
            "this ",
            "while ",
            "with ",
            "towards ",
            "to ",
            "under ",
            "existing ",
            "presented ",
            "implemented ",
        )
    ):
        return False

    keyword_hits = sum(
        1
        for keyword in ["yojana", "mission", "bima", "insurance", "credit", "kisan", "fund", "subvention", "scheme"]
        if keyword in lower
    )
    if keyword_hits <= 0:
        return False

    chunks = [chunk for chunk in re.split(r"\s+", text) if chunk]
    title_like = sum(1 for chunk in chunks if chunk[:1].isupper())
    title_ratio = title_like / max(1, len(chunks))
    if title_ratio < 0.35 and not text.isupper():
        return False

    if len(re.findall(r"\d", text)) > 6:
        return False
    return True


def is_trusted_master_row(item: Dict[str, Any]) -> bool:
    name = str(item.get("scheme_name", "") or "")
    if int(item.get("curated_flag", 0) or 0) == 1:
        return True

    if is_obvious_noise_master_row(item):
        return False

    score = int(item.get("confidence_score", 0) or 0)
    scheme_type = str(item.get("scheme_type", "") or "").lower()
    ministry = str(item.get("nodal_ministry", "") or "").strip()
    website = str(item.get("official_website", "") or "").strip().lower().startswith("http")
    source_count = int(item.get("source_count", 0) or 0)

    if score >= 75:
        return True
    if score >= 60 and (scheme_type != "other" or ministry or website) and source_count >= 2:
        return True
    if score >= 50 and scheme_type != "other" and (ministry or website) and source_count >= 2:
        return True
    if score >= 40 and source_count >= 4 and (ministry or website) and not looks_like_noise_title(name):
        return True
    if source_count >= 4 and looks_like_scheme_name(name):
        return True
    if source_count >= 5 and ministry and scheme_type != "other" and any(
        keyword in name.lower() for keyword in STRONG_SCHEME_KEYWORDS
    ):
        return True
    return False


def is_strict_trusted_master_row(item: Dict[str, Any]) -> bool:
    if not is_trusted_master_row(item):
        return False

    name = str(item.get("scheme_name", "") or "")
    score = int(item.get("confidence_score", 0) or 0)
    curated = int(item.get("curated_flag", 0) or 0) == 1
    scheme_type = str(item.get("scheme_type", "") or "").lower()
    ministry = str(item.get("nodal_ministry", "") or "").strip()
    website = str(item.get("official_website", "") or "").strip().lower().startswith("http")
    source_count = int(item.get("source_count", 0) or 0)

    if curated and (ministry or website):
        return True
    if score >= 75 and (ministry or website):
        return True
    if score >= 60 and source_count >= 2 and scheme_type != "other" and (ministry or website):
        return True
    if source_count >= 6 and website and looks_like_scheme_name(name):
        return True
    return False


def is_balanced_master_row(item: Dict[str, Any]) -> bool:
    if is_trusted_master_row(item):
        return True

    if is_obvious_noise_master_row(item):
        return False

    name = str(item.get("scheme_name", "") or "")
    score = int(item.get("confidence_score", 0) or 0)
    scheme_type = str(item.get("scheme_type", "") or "").lower()
    ministry = str(item.get("nodal_ministry", "") or "").strip()
    website = str(item.get("official_website", "") or "").strip().lower().startswith("http")
    source_count = int(item.get("source_count", 0) or 0)

    if source_count >= 3 and looks_like_scheme_name(name):
        return True
    if score >= 30 and scheme_type != "other" and (ministry or website):
        return True
    if score >= 25 and source_count >= 2 and any(keyword in name.lower() for keyword in STRONG_SCHEME_KEYWORDS):
        return True
    return False


def ensure_master_index_ready(db_path: Path) -> None:
    conn = connect_db(db_path)
    try:
        has_master = table_exists(conn, "master_schemes")
        has_curated = table_exists(conn, "curated_schemes")
        master_count = int(conn.execute("SELECT COUNT(*) FROM master_schemes").fetchone()[0]) if has_master else 0
    finally:
        conn.close()

    if not has_master or not has_curated or master_count == 0:
        refresh_master_index(db_path)


def refresh_curated_index(db_path: Path) -> int:
    return refresh_master_index(db_path).get("curated_count", -1)


def refresh_master_index(db_path: Path) -> Dict[str, int]:
    if SchemeDatabase is None:
        return {"curated_count": -1, "master_count": -1}
    db = SchemeDatabase(db_path)
    try:
        curated_count = db.refresh_curated_schemes()
        master_count = db.refresh_master_dataset()
        db.export_curated_csv(BASE_DIR / "output" / "data" / "schemes_curated_latest.csv")
        db.export_master_csv(BASE_DIR / "output" / "data" / "schemes_master_latest.csv")
        return {"curated_count": int(curated_count), "master_count": int(master_count)}
    finally:
        db.close()


def get_overview(db_path: Path) -> Dict[str, Any]:
    conn = connect_db(db_path)
    try:
        total_schemes = int(conn.execute("SELECT COUNT(*) FROM schemes").fetchone()[0])
        total_runs = int(conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0])
        total_sources = int(conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0])

        curated_total = 0
        if table_exists(conn, "curated_schemes"):
            curated_total = int(
                conn.execute("SELECT COUNT(*) FROM curated_schemes WHERE curated_flag = 1").fetchone()[0]
            )

        master_total = total_schemes
        if table_exists(conn, "master_schemes"):
            master_total = int(conn.execute("SELECT COUNT(*) FROM master_schemes").fetchone()[0])

        latest_run_row = conn.execute(
            """
            SELECT id, task_id, source_url, status, started_at, finished_at, stats_json
            FROM runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

        latest_run = None
        if latest_run_row:
            latest_run = {
                "id": int(latest_run_row["id"]),
                "task_id": latest_run_row["task_id"],
                "source_url": latest_run_row["source_url"],
                "status": latest_run_row["status"],
                "started_at": latest_run_row["started_at"],
                "finished_at": latest_run_row["finished_at"],
                "stats": parse_stats(latest_run_row["stats_json"]),
            }

        top_types_rows = conn.execute(
            """
            SELECT scheme_type, COUNT(*) AS count
            FROM schemes
            GROUP BY scheme_type
            ORDER BY count DESC
            LIMIT 6
            """
        ).fetchall()

        top_types = [
            {
                "scheme_type": (row["scheme_type"] or "other"),
                "count": int(row["count"]),
            }
            for row in top_types_rows
        ]

        return {
            "database_path": str(db_path),
            "totals": {
                "schemes": total_schemes,
                "curated": curated_total,
                "master": master_total,
                "runs": total_runs,
                "sources": total_sources,
            },
            "latest_run": latest_run,
            "top_types": top_types,
        }
    finally:
        conn.close()


def get_runs(db_path: Path, limit: int = 20) -> List[Dict[str, Any]]:
    conn = connect_db(db_path)
    safe_limit = max(1, min(int(limit), 300))
    try:
        rows = conn.execute(
            """
            SELECT id, task_id, source_url, status, started_at, finished_at, stats_json
            FROM runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        runs: List[Dict[str, Any]] = []
        for row in rows:
            runs.append(
                {
                    "id": int(row["id"]),
                    "task_id": row["task_id"],
                    "source_url": row["source_url"],
                    "status": row["status"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                    "stats": parse_stats(row["stats_json"]),
                }
            )
        return runs
    finally:
        conn.close()


def get_reports(db_path: Path, limit: int = 20) -> List[Dict[str, Any]]:
    conn = connect_db(db_path)
    safe_limit = max(1, min(int(limit), 200))
    try:
        rows = conn.execute(
            """
            SELECT id, task_id, source_url, status, started_at, finished_at, stats_json
            FROM runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        items: List[Dict[str, Any]] = []
        for row in rows:
            task_id = str(row["task_id"] or "").strip()
            report_file = REPORT_DIR / f"report_{task_id}.md"
            json_file = DATA_DIR / f"schemes_{task_id}.json"
            csv_file = DATA_DIR / f"schemes_{task_id}.csv"

            stats = parse_stats(row["stats_json"])
            items.append(
                {
                    "id": int(row["id"]),
                    "task_id": task_id,
                    "source_url": row["source_url"],
                    "status": row["status"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                    "stats": stats,
                    "report_url": f"/artifacts/reports/{report_file.name}" if report_file.exists() else "",
                    "json_url": f"/artifacts/data/{json_file.name}" if json_file.exists() else "",
                    "csv_url": f"/artifacts/data/{csv_file.name}" if csv_file.exists() else "",
                }
            )
        return items
    finally:
        conn.close()


def query_schemes(
    db_path: Path,
    query_text: str = "",
    ministry: str = "",
    scheme_type: str = "",
    scope: str = "curated",
    min_score: int = 45,
    limit: int = 30,
    offset: int = 0,
) -> Dict[str, Any]:
    conn = connect_db(db_path)
    try:
        if not table_exists(conn, "curated_schemes"):
            raise HTTPException(status_code=400, detail="Curated index not found. Run scraper at least once.")

        where_parts: List[str] = []
        params: List[Any] = []

        if scope == "curated":
            where_parts.append("cs.curated_flag = 1")
            where_parts.append("cs.confidence_score >= ?")
            params.append(int(min_score))
        else:
            where_parts.append("cs.confidence_score >= ?")
            params.append(max(0, int(min_score)))

        q = (query_text or "").strip().lower()
        if q:
            like = f"%{q}%"
            where_parts.append(
                "(lower(cs.scheme_name) LIKE ? OR lower(cs.short_name) LIKE ? OR lower(cs.nodal_ministry) LIKE ? OR lower(cs.benefit_amount) LIKE ? OR lower(cs.rationale) LIKE ?)"
            )
            params.extend([like, like, like, like, like])

        ministry_q = (ministry or "").strip().lower()
        if ministry_q:
            where_parts.append("lower(cs.nodal_ministry) LIKE ?")
            params.append(f"%{ministry_q}%")

        type_q = (scheme_type or "").strip().lower()
        if type_q:
            where_parts.append("lower(cs.scheme_type) LIKE ?")
            params.append(f"%{type_q}%")

        where_sql = " AND ".join(where_parts) if where_parts else "1=1"
        safe_limit = max(1, min(int(limit), 300))
        safe_offset = max(0, int(offset))

        total_row = conn.execute(
            f"SELECT COUNT(*) FROM curated_schemes cs WHERE {where_sql}",
            tuple(params),
        ).fetchone()
        total = int(total_row[0]) if total_row else 0

        rows = conn.execute(
            f"""
            SELECT
                cs.scheme_id,
                cs.scheme_name,
                cs.short_name,
                cs.scheme_type,
                cs.nodal_ministry,
                cs.launch_year,
                cs.benefit_amount,
                cs.official_website,
                cs.eligibility_count,
                cs.requirements_count,
                cs.source_runs_count,
                cs.high_quality_hits,
                cs.medium_quality_hits,
                cs.low_quality_hits,
                cs.heuristic_hits,
                cs.confidence_score,
                cs.curated_flag,
                cs.rationale,
                cs.updated_at
            FROM curated_schemes cs
            WHERE {where_sql}
            ORDER BY cs.confidence_score DESC, cs.source_runs_count DESC, cs.scheme_name COLLATE NOCASE
            LIMIT ? OFFSET ?
            """,
            tuple(params + [safe_limit, safe_offset]),
        ).fetchall()

        items = [dict(row) for row in rows]
        return {
            "total": total,
            "limit": safe_limit,
            "offset": safe_offset,
            "scope": scope,
            "items": items,
        }
    finally:
        conn.close()


def get_scheme_detail(db_path: Path, scheme_id: int) -> Dict[str, Any]:
    conn = connect_db(db_path)
    try:
        row = conn.execute(
            """
            SELECT
                s.id,
                s.scheme_name,
                s.short_name,
                s.scheme_type,
                s.nodal_ministry,
                s.launch_year,
                s.objective,
                s.target_beneficiaries,
                s.benefit_amount,
                s.premium_or_cost,
                s.application_process,
                s.application_deadline,
                s.official_website,
                s.helpline,
                s.budget_allocation,
                s.coverage_stats,
                s.updated_at,
                cs.confidence_score,
                cs.curated_flag,
                cs.rationale
            FROM schemes s
            LEFT JOIN curated_schemes cs ON cs.scheme_id = s.id
            WHERE s.id = ?
            """,
            (int(scheme_id),),
        ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Scheme not found")

        def fetch_list(table: str, col: str) -> List[str]:
            values = conn.execute(
                f"SELECT {col} FROM {table} WHERE scheme_id = ? ORDER BY id",
                (int(scheme_id),),
            ).fetchall()
            return [str(v[0]).strip() for v in values if str(v[0]).strip()]

        sources = conn.execute(
            """
            SELECT
                src.url,
                src.source_type,
                src.title,
                src.status_code,
                src.local_path,
                src.notes,
                ss.quality,
                ss.extraction_model,
                r.task_id,
                r.started_at
            FROM scheme_sources ss
            JOIN sources src ON src.id = ss.source_id
            JOIN runs r ON r.id = ss.run_id
            WHERE ss.scheme_id = ?
            ORDER BY r.id DESC, src.id DESC
            LIMIT 40
            """,
            (int(scheme_id),),
        ).fetchall()

        detail = dict(row)
        detail["eligibility_rules"] = fetch_list("scheme_eligibility", "rule_text")
        detail["exclusions"] = fetch_list("scheme_exclusions", "exclusion_text")
        detail["requirements"] = fetch_list("scheme_requirements", "requirement_text")
        detail["key_facts"] = fetch_list("scheme_benefits", "benefit_text")
        detail["keywords"] = fetch_list("scheme_keywords", "keyword_text")
        detail["sources"] = [dict(src) for src in sources]
        return detail
    finally:
        conn.close()


def normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def contains_keyword(source: str, keywords: List[str]) -> bool:
    text = normalize_text(source)
    return any(keyword.lower() in text for keyword in keywords if keyword)


def extract_numbers(source: str) -> List[float]:
    import re

    text = normalize_text(source).replace(",", "")
    numbers = re.findall(r"\d+(?:\.\d+)?", text)
    return [float(number) for number in numbers]


def evaluate_scheme_eligibility(request: SchemeEligibilityRequest, scheme_detail: Dict[str, Any]) -> Dict[str, Any]:
    user = {
        "age": request.age,
        "annual_income": request.annual_income,
        "category": normalize_text(request.category),
        "state": normalize_text(request.state),
        "district": normalize_text(request.district),
        "landholding_hectares": request.landholding_hectares,
        "has_aadhaar": request.has_aadhaar,
        "has_land_record": request.has_land_record,
        "has_bank_account": request.has_bank_account,
        "has_insurance": request.has_insurance,
    }

    combined_text = " ".join(
        [
            normalize_text(scheme_detail.get("objective", "")),
            normalize_text(scheme_detail.get("target_beneficiaries", "")),
            normalize_text(" ".join(scheme_detail.get("eligibility_rules", []) or [])),
            normalize_text(" ".join(scheme_detail.get("requirements", []) or [])),
            normalize_text(" ".join(scheme_detail.get("exclusions", []) or [])),
        ]
    )

    requirement_checks = []
    if contains_keyword(combined_text, ["aadhaar"]):
        requirement_checks.append(("Aadhaar", user["has_aadhaar"], "Aadhaar is required for this scheme."))
    if contains_keyword(combined_text, ["land record", "patta", "landholding", "land details", "land/lease"]):
        requirement_checks.append(("Land record", user["has_land_record"], "Land records or lease documents are required."))
    if contains_keyword(combined_text, ["bank details", "bank passbook", "bank account", "bank"]):
        requirement_checks.append(("Bank account", user["has_bank_account"], "A bank account is required to receive scheme benefits."))
    if contains_keyword(combined_text, ["notified crop", "notified area", "crop and notified", "crop information"]):
        requirement_checks.append(("Crop information", bool(user["state"] or user["district"]), "Crop or location details are required for this scheme."))
    if contains_keyword(combined_text, ["income limit", "income below", "income less", "annual income", "income does not exceed", "income up to"]):
        requirement_checks.append(("Income verification", user["annual_income"] is not None, "Income details are required to complete this eligibility check."))

    matched_requirements: List[str] = []
    missing_requirements: List[str] = []
    unknown_requirements: List[str] = []
    reason_items: List[str] = []

    for label, available, reason in requirement_checks:
        if available is True:
            matched_requirements.append(label)
            reason_items.append(f"{label} looks satisfied.")
        elif available is False:
            missing_requirements.append(label)
            reason_items.append(f"{label} is required but not provided.")
        else:
            unknown_requirements.append(label)
            reason_items.append(f"{label} appears relevant but has not been confirmed.")

    exclusion_warnings: List[str] = []
    for exclusion in scheme_detail.get("exclusions", []) or []:
        exclusion_text = normalize_text(exclusion)
        if user["category"] and user["category"] in exclusion_text:
            exclusion_warnings.append(exclusion)
        if user["state"] and user["state"] in exclusion_text:
            exclusion_warnings.append(exclusion)
        if user["district"] and user["district"] in exclusion_text:
            exclusion_warnings.append(exclusion)

    if user["age"] is not None:
        if "below 18" in combined_text or "under 18" in combined_text:
            if user["age"] >= 18:
                missing_requirements.append("Age limit")
                reason_items.append("The scheme mentions an age restriction; the provided age does not match the accepted range.")
        if "18 and above" in combined_text or "at least 18" in combined_text or "minimum age" in combined_text:
            if user["age"] < 18:
                missing_requirements.append("Age limit")
                reason_items.append("The scheme requires a minimum age of 18 years.")

    if user["annual_income"] is not None:
        income_thresholds = extract_numbers(combined_text)
        if income_thresholds and contains_keyword(combined_text, ["less than", "below", "up to", "does not exceed"]):
            threshold = max(income_thresholds)
            if user["annual_income"] > threshold:
                missing_requirements.append("Income threshold")
                reason_items.append(
                    f"The scheme appears to require income below {threshold:.0f}; the provided income is higher."
                )

    score = 100
    score -= 25 * len(missing_requirements)
    score -= 20 * len(exclusion_warnings)
    score -= 10 * len(unknown_requirements)
    score = max(0, min(100, score))

    if missing_requirements:
        status = "Not Eligible"
    elif exclusion_warnings:
        status = "Review Recommended"
    elif unknown_requirements and not matched_requirements:
        status = "Review Recommended"
    elif not matched_requirements and not requirement_checks:
        status = "Review Recommended"
    else:
        status = "Eligible"

    recommendation = "Review the scheme rules and complete the missing information."
    if status == "Eligible":
        recommendation = "You appear eligible; gather documents and apply through the scheme portal."
    elif status == "Not Eligible":
        recommendation = "The available input indicates missing eligibility details for this scheme. Update your application data and check again."
    elif status == "Review Recommended":
        recommendation = "This scheme needs manual review with more details or documents."

    return {
        "eligible": status == "Eligible",
        "status": status,
        "confidence_score": score,
        "matched_requirements": matched_requirements,
        "missing_requirements": list(dict.fromkeys(missing_requirements)),
        "unknown_requirements": list(dict.fromkeys(unknown_requirements)),
        "exclusions": exclusion_warnings,
        "reasons": reason_items,
        "recommendation": recommendation,
    }


def get_master_overview(db_path: Path) -> Dict[str, Any]:
    conn = connect_db(db_path)
    try:
        if not table_exists(conn, "master_schemes"):
            raise HTTPException(status_code=400, detail="Master index not found. Run scraper once to initialize.")

        totals = {
            "master_schemes": int(conn.execute("SELECT COUNT(*) FROM master_schemes").fetchone()[0]),
            "curated_master": int(
                conn.execute("SELECT COUNT(*) FROM master_schemes WHERE curated_flag = 1").fetchone()[0]
            ),
            "source_links": int(conn.execute("SELECT COUNT(*) FROM scheme_sources").fetchone()[0]),
            "media_links": int(
                conn.execute("SELECT COALESCE(SUM(media_count), 0) FROM master_schemes").fetchone()[0] or 0
            ),
            "versions": int(conn.execute("SELECT COUNT(*) FROM scheme_versions").fetchone()[0])
            if table_exists(conn, "scheme_versions")
            else 0,
        }

        top_ministries = [
            dict(row)
            for row in conn.execute(
                """
                SELECT nodal_ministry, COUNT(*) AS count
                FROM master_schemes
                WHERE trim(COALESCE(nodal_ministry, '')) != ''
                GROUP BY nodal_ministry
                ORDER BY count DESC
                LIMIT 8
                """
            ).fetchall()
        ]

        top_types = [
            dict(row)
            for row in conn.execute(
                """
                SELECT scheme_type, COUNT(*) AS count
                FROM master_schemes
                GROUP BY scheme_type
                ORDER BY count DESC
                LIMIT 8
                """
            ).fetchall()
        ]

        recent_updates = [
            dict(row)
            for row in conn.execute(
                """
                SELECT scheme_id, scheme_name, scheme_type, nodal_ministry, confidence_score, updated_at
                FROM master_schemes
                ORDER BY updated_at DESC
                LIMIT 10
                """
            ).fetchall()
        ]

        return {
            "database_path": str(db_path),
            "totals": totals,
            "top_ministries": top_ministries,
            "top_types": top_types,
            "recent_updates": recent_updates,
        }
    finally:
        conn.close()


def search_master_schemes(
    db_path: Path,
    query_text: str = "",
    ministry: str = "",
    scheme_type: str = "",
    scope: str = "trusted",
    mode: str = "smart",
    min_score: int = 0,
    limit: int = 30,
    offset: int = 0,
) -> Dict[str, Any]:
    conn = connect_db(db_path)
    try:
        if not table_exists(conn, "master_schemes"):
            raise HTTPException(status_code=400, detail="Master index not found. Run scraper once to initialize.")

        effective_min_score = max(0, int(min_score))
        if scope in {"curated", "strict"}:
            effective_min_score = max(45, effective_min_score)

        where_parts: List[str] = ["confidence_score >= ?"]
        params: List[Any] = [effective_min_score]

        ministry_terms = parse_filter_terms(ministry)
        if ministry_terms:
            where_parts.append("(" + " OR ".join(["lower(nodal_ministry) LIKE ?"] * len(ministry_terms)) + ")")
            params.extend([f"%{term}%" for term in ministry_terms])

        type_terms = parse_filter_terms(scheme_type)
        if type_terms:
            where_parts.append("(" + " OR ".join(["lower(scheme_type) LIKE ?"] * len(type_terms)) + ")")
            params.extend([f"%{term}%" for term in type_terms])

        rows = conn.execute(
            f"""
            SELECT
                scheme_id,
                scheme_name,
                short_name,
                scheme_type,
                nodal_ministry,
                launch_year,
                objective,
                target_beneficiaries,
                benefit_amount,
                application_process,
                official_website,
                confidence_score,
                curated_flag,
                source_count,
                media_count,
                version_count,
                eligibility_count,
                requirements_count,
                benefits_count,
                search_blob,
                updated_at
            FROM master_schemes
            WHERE {' AND '.join(where_parts)}
            """,
            tuple(params),
        ).fetchall()

        q = (query_text or "").strip().lower()
        tokens = smart_tokens(q, mode)
        query_intents = infer_query_intents(tokens)
        ranked: List[Dict[str, Any]] = []

        for row in rows:
            item = dict(row)

            if scope == "curated" and int(item.get("curated_flag", 0) or 0) != 1:
                continue
            if scope == "strict" and not is_strict_trusted_master_row(item):
                continue
            if scope == "trusted" and not is_trusted_master_row(item):
                continue
            if scope == "balanced" and not is_balanced_master_row(item):
                continue
            if scope == "all" and is_obvious_noise_master_row(item):
                continue

            name = str(item.get("scheme_name", "")).lower()
            short_name = str(item.get("short_name", "")).lower()
            ministry_text = str(item.get("nodal_ministry", "")).lower()
            type_text = str(item.get("scheme_type", "")).lower()
            blob = str(item.get("search_blob", "")).lower()

            lexical_score = 0
            reason_bits: List[str] = []

            if q:
                if q in name:
                    lexical_score += 42
                    reason_bits.append("name-exact")
                if q and q in blob:
                    lexical_score += 12

                lexical_score += match_token_score(name, tokens, 14)
                lexical_score += match_token_score(short_name, tokens, 12)
                lexical_score += match_token_score(ministry_text, tokens, 10)
                lexical_score += match_token_score(type_text, tokens, 9)
                lexical_score += match_token_score(blob, tokens, 4)

                if lexical_score > 0 and not reason_bits:
                    reason_bits.append("semantic-token-match" if mode == "smart" else "keyword-match")

            intent_score = intent_alignment_score(item, query_intents)
            if intent_score > 0:
                reason_bits.append("intent-match")

            if q and query_intents and intent_score <= 0:
                lexical_score = int(lexical_score * 0.6)
                if lexical_score < 28:
                    continue

            if q and lexical_score <= 0 and intent_score <= 0:
                continue

            confidence_component = int(min(35, max(0, int(item.get("confidence_score", 0))) * 0.35))
            source_component = min(20, int(item.get("source_count", 0)) * 2)
            version_component = min(10, int(item.get("version_count", 0)))
            final_rank = lexical_score + intent_score + confidence_component + source_component + version_component

            item["smart_rank"] = int(final_rank)
            item["match_reason"] = ", ".join(reason_bits) if reason_bits else "quality-rank"
            ranked.append(item)

        ranked.sort(
            key=lambda r: (
                int(r.get("smart_rank", 0)),
                int(r.get("confidence_score", 0)),
                int(r.get("source_count", 0)),
                str(r.get("scheme_name", "")).lower(),
            ),
            reverse=True,
        )

        total = len(ranked)
        safe_limit = max(1, min(int(limit), 300))
        safe_offset = max(0, int(offset))
        items = ranked[safe_offset : safe_offset + safe_limit]

        return {
            "total": total,
            "limit": safe_limit,
            "offset": safe_offset,
            "scope": scope,
            "mode": mode,
            "effective_min_score": effective_min_score,
            "query": query_text,
            "tokens": tokens,
            "items": items,
        }
    finally:
        conn.close()


def get_master_scheme_detail(db_path: Path, scheme_id: int) -> Dict[str, Any]:
    conn = connect_db(db_path)
    try:
        row = conn.execute(
            """
            SELECT
                ms.scheme_id,
                ms.canonical_name,
                ms.scheme_name,
                ms.short_name,
                ms.scheme_type,
                ms.nodal_ministry,
                ms.launch_year,
                ms.objective,
                ms.target_beneficiaries,
                ms.benefit_amount,
                ms.application_process,
                ms.official_website,
                ms.confidence_score,
                ms.curated_flag,
                ms.source_count,
                ms.media_count,
                ms.version_count,
                ms.eligibility_count,
                ms.requirements_count,
                ms.benefits_count,
                ms.updated_at,
                s.premium_or_cost,
                s.application_deadline,
                s.helpline,
                s.budget_allocation,
                s.coverage_stats,
                cs.rationale
            FROM master_schemes ms
            JOIN schemes s ON s.id = ms.scheme_id
            LEFT JOIN curated_schemes cs ON cs.scheme_id = ms.scheme_id
            WHERE ms.scheme_id = ?
            """,
            (int(scheme_id),),
        ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Master scheme not found")

        def fetch_list(table: str, col: str) -> List[str]:
            values = conn.execute(
                f"SELECT {col} FROM {table} WHERE scheme_id = ? ORDER BY id",
                (int(scheme_id),),
            ).fetchall()
            return [str(v[0]).strip() for v in values if str(v[0]).strip()]

        source_rows = conn.execute(
            """
            SELECT
                src.id AS source_id,
                src.url,
                src.source_type,
                src.title,
                src.status_code,
                src.content_type,
                src.local_path,
                src.notes,
                ss.quality,
                ss.extraction_model,
                r.task_id,
                r.started_at
            FROM scheme_sources ss
            JOIN sources src ON src.id = ss.source_id
            JOIN runs r ON r.id = ss.run_id
            WHERE ss.scheme_id = ?
            ORDER BY r.id DESC, src.id DESC
            LIMIT 80
            """,
            (int(scheme_id),),
        ).fetchall()

        sources: List[Dict[str, Any]] = []
        media: List[Dict[str, Any]] = []
        for src_row in source_rows:
            src = dict(src_row)
            local_path = (src.get("local_path") or "").strip()
            is_media = bool(local_path)
            media_type = ""
            if local_path:
                media_type = Path(local_path).suffix.lower().lstrip(".")
            if not media_type:
                media_type = str(src.get("source_type") or "").lower()
            src["is_media"] = is_media
            src["media_type"] = media_type
            sources.append(src)
            if is_media:
                media.append(src)

        versions = [
            dict(v)
            for v in conn.execute(
                """
                SELECT
                    id,
                    run_id,
                    source_id,
                    incoming_scheme_name,
                    match_method,
                    similarity_score,
                    created_at,
                    snapshot_json
                FROM scheme_versions
                WHERE scheme_id = ?
                ORDER BY id DESC
                LIMIT 40
                """,
                (int(scheme_id),),
            ).fetchall()
        ]
        for version in versions:
            raw_snapshot = version.get("snapshot_json") or "{}"
            try:
                version["snapshot"] = json.loads(raw_snapshot)
            except Exception:
                version["snapshot"] = {}
            version.pop("snapshot_json", None)

        related = [
            dict(r)
            for r in conn.execute(
                """
                SELECT scheme_id, scheme_name, scheme_type, nodal_ministry, confidence_score, source_count
                FROM master_schemes
                WHERE scheme_id != ?
                  AND (
                    lower(scheme_type) = lower(?)
                    OR lower(nodal_ministry) = lower(?)
                  )
                ORDER BY confidence_score DESC, source_count DESC
                LIMIT 8
                """,
                (int(scheme_id), str(row["scheme_type"] or ""), str(row["nodal_ministry"] or "")),
            ).fetchall()
        ]

        detail = dict(row)
        detail["eligibility_rules"] = fetch_list("scheme_eligibility", "rule_text")
        detail["exclusions"] = fetch_list("scheme_exclusions", "exclusion_text")
        detail["requirements"] = fetch_list("scheme_requirements", "requirement_text")
        detail["key_facts"] = fetch_list("scheme_benefits", "benefit_text")
        detail["keywords"] = fetch_list("scheme_keywords", "keyword_text")
        detail["sources"] = sources
        detail["media"] = media
        detail["versions"] = versions
        detail["related"] = related
        return detail
    finally:
        conn.close()


def append_job_log(job: Dict[str, Any], line: str) -> None:
    msg = line.rstrip("\n")
    if not msg:
        return
    logs = job.setdefault("logs", [])
    logs.append(msg)
    if len(logs) > MAX_LOG_LINES:
        del logs[: len(logs) - MAX_LOG_LINES]


def run_scrape_job(job_id: str, payload: Dict[str, Any]) -> None:
    with JOB_LOCK:
        job = SCRAPE_JOBS[job_id]
        job["status"] = "running"
        job["started_at"] = now_iso()
        job["updated_at"] = now_iso()

    db_path = resolve_db_path(payload.get("db_path"))
    url = normalize_url_input(str(payload.get("url", "")))

    cmd = [
        sys.executable,
        str(SCRAPER_PATH),
        url,
        "--depth",
        str(int(payload.get("depth", 1))),
        "--max-pages",
        str(int(payload.get("max_pages", 20))),
        "--max-files",
        str(int(payload.get("max_files", 80))),
        "--db",
        str(db_path),
    ]

    if payload.get("js"):
        cmd.append("--js")
    if payload.get("selenium_site", True):
        cmd.append("--selenium-site")
    if payload.get("all_domains"):
        cmd.append("--all-domains")
    if payload.get("no_ai"):
        cmd.append("--no-ai")
    model = str(payload.get("model", "")).strip()
    if model:
        cmd.extend(["--model", model])

    env = os.environ.copy()
    api_key = str(payload.get("api_key", "") or "").strip()
    if api_key and not payload.get("no_ai"):
        env["GEMINI_API_KEY"] = api_key

    process = None
    try:
        with JOB_LOCK:
            job = SCRAPE_JOBS[job_id]
            job["command"] = " ".join(cmd)
            job["db_path"] = str(db_path)
            job["url"] = url

        process = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )

        with JOB_LOCK:
            job = SCRAPE_JOBS[job_id]
            job["pid"] = int(process.pid)
            job["updated_at"] = now_iso()

        if process.stdout is not None:
            for line in process.stdout:
                with JOB_LOCK:
                    job = SCRAPE_JOBS[job_id]
                    append_job_log(job, line)
                    job["updated_at"] = now_iso()

        exit_code = int(process.wait())
        curated_count = refresh_curated_index(db_path)

        with JOB_LOCK:
            job = SCRAPE_JOBS[job_id]
            job["status"] = "completed" if exit_code == 0 else "failed"
            job["exit_code"] = exit_code
            job["finished_at"] = now_iso()
            job["updated_at"] = now_iso()
            job["curated_count"] = curated_count
            append_job_log(job, f"[system] finished with exit code {exit_code}")
    except Exception as exc:
        with JOB_LOCK:
            job = SCRAPE_JOBS[job_id]
            job["status"] = "failed"
            job["error"] = str(exc)
            job["finished_at"] = now_iso()
            job["updated_at"] = now_iso()
            append_job_log(job, f"[system] job error: {exc}")
    finally:
        if process and process.stdout:
            process.stdout.close()


@app.get("/")
def serve_dashboard() -> FileResponse:
    index_file = UI_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="Dashboard UI file missing")
    return FileResponse(index_file)


@app.get("/master")
def serve_master_dashboard() -> FileResponse:
    page_file = UI_DIR / "master.html"
    if not page_file.exists():
        raise HTTPException(status_code=404, detail="Master Database UI file missing")
    return FileResponse(page_file)


@app.get("/crawler")
def serve_crawler_dashboard() -> FileResponse:
    page_file = UI_DIR / "crawler.html"
    if not page_file.exists():
        raise HTTPException(status_code=404, detail="Crawler UI file missing")
    return FileResponse(page_file)


@app.get("/finder")
def serve_finder_dashboard() -> FileResponse:
    page_file = UI_DIR / "finder.html"
    if not page_file.exists():
        raise HTTPException(status_code=404, detail="Scheme Finder UI file missing")
    return FileResponse(page_file)


@app.get("/eligibility")
def serve_eligibility_dashboard() -> FileResponse:
    page_file = UI_DIR / "eligibility.html"
    if not page_file.exists():
        raise HTTPException(status_code=404, detail="Eligibility Checker UI file missing")
    return FileResponse(page_file)


@app.get("/vault")
def serve_vault_dashboard() -> FileResponse:
    page_file = UI_DIR / "vault.html"
    if not page_file.exists():
        raise HTTPException(status_code=404, detail="Document Vault UI file missing")
    return FileResponse(page_file)


@app.get("/schemes")
def serve_schemes_page() -> FileResponse:
    page_file = UI_DIR / "schemes.html"
    if not page_file.exists():
        raise HTTPException(status_code=404, detail="Schemes page missing")
    return FileResponse(page_file)


@app.get("/analytics")
def serve_analytics_page() -> FileResponse:
    page_file = UI_DIR / "analytics.html"
    if not page_file.exists():
        raise HTTPException(status_code=404, detail="Analytics page missing")
    return FileResponse(page_file)


@app.get("/api/health")
def health() -> Dict[str, Any]:
    db_path = resolve_db_path()
    return {
        "status": "ok",
        "timestamp": now_iso(),
        "database_exists": db_path.exists(),
        "db": str(db_path),
        "database_path": str(db_path),
    }


@app.get("/api/overview")
def api_overview(db_path: str = "") -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    refresh_curated_index(path)
    return get_overview(path)


@app.get("/api/runs")
def api_runs(limit: int = Query(20, ge=1, le=300), db_path: str = "") -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    runs = get_runs(path, limit=limit)
    return {"items": runs, "total": len(runs)}


@app.get("/api/reports")
def api_reports(limit: int = Query(20, ge=1, le=200), db_path: str = "") -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    items = get_reports(path, limit=limit)
    return {"items": items, "total": len(items)}


@app.get("/api/schemes")
def api_schemes(
    query: str = "",
    ministry: str = "",
    scheme_type: str = "",
    scope: str = Query("curated", pattern="^(curated|all)$"),
    min_score: int = Query(45, ge=0, le=100),
    limit: int = Query(30, ge=1, le=300),
    offset: int = Query(0, ge=0),
    db_path: str = "",
) -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    refresh_curated_index(path)
    return query_schemes(
        db_path=path,
        query_text=query,
        ministry=ministry,
        scheme_type=scheme_type,
        scope=scope,
        min_score=min_score,
        limit=limit,
        offset=offset,
    )


@app.get("/api/schemes/{scheme_id}")
def api_scheme_detail(scheme_id: int, db_path: str = "") -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    return get_scheme_detail(path, scheme_id)


@app.post("/api/schemes/{scheme_id}/eligibility")
def api_scheme_eligibility(
    scheme_id: int,
    request: SchemeEligibilityRequest,
    db_path: str = "",
) -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    scheme_detail = get_scheme_detail(path, scheme_id)
    result = evaluate_scheme_eligibility(request, scheme_detail)
    return {
        "scheme_id": scheme_id,
        "scheme_name": scheme_detail.get("scheme_name", ""),
        "scheme_type": scheme_detail.get("scheme_type", ""),
        **result,
    }


@app.post("/api/curated/refresh")
def api_refresh_curated(db_path: str = "") -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    refreshed = refresh_master_index(path)
    return {
        "status": "ok",
        "curated_count": int(refreshed.get("curated_count", -1)),
        "master_count": int(refreshed.get("master_count", -1)),
        "database_path": str(path),
        "refreshed_at": now_iso(),
    }


@app.post("/api/master/refresh")
def api_refresh_master(db_path: str = "") -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    refreshed = refresh_master_index(path)
    return {
        "status": "ok",
        "curated_count": int(refreshed.get("curated_count", -1)),
        "master_count": int(refreshed.get("master_count", -1)),
        "database_path": str(path),
        "refreshed_at": now_iso(),
    }


@app.get("/api/master/overview")
def api_master_overview(db_path: str = "") -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    ensure_master_index_ready(path)
    return get_master_overview(path)


@app.get("/api/master/search")
def api_master_search(
    query: str = "",
    ministry: str = "",
    scheme_type: str = "",
    scope: str = Query("trusted", pattern="^(strict|trusted|balanced|curated|all|raw)$"),
    mode: str = Query("smart", pattern="^(smart|keyword)$"),
    min_score: int = Query(0, ge=0, le=100),
    limit: int = Query(30, ge=1, le=300),
    offset: int = Query(0, ge=0),
    db_path: str = "",
) -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    ensure_master_index_ready(path)
    return search_master_schemes(
        db_path=path,
        query_text=query,
        ministry=ministry,
        scheme_type=scheme_type,
        scope=scope,
        mode=mode,
        min_score=min_score,
        limit=limit,
        offset=offset,
    )


@app.get("/api/master-search")
def api_master_search_compat(
    q: str = "",
    ministry: str = "",
    scheme_type: str = "",
    scope: str = Query("trusted", pattern="^(strict|trusted|balanced|curated|all|raw)$"),
    mode: str = Query("smart", pattern="^(smart|keyword)$"),
    min_score: int = Query(0, ge=0, le=100),
    limit: int = Query(30, ge=1, le=300),
    page: int = Query(1, ge=1),
    db_path: str = "",
) -> Dict[str, Any]:
    offset = (page - 1) * limit
    result = api_master_search(
        query=q,
        ministry=ministry,
        scheme_type=scheme_type,
        scope=scope,
        mode=mode,
        min_score=min_score,
        limit=limit,
        offset=offset,
        db_path=db_path,
    )

    total = int(result.get("total") or 0)
    pages = max(1, (total + limit - 1) // limit)
    response_page = min(page, pages)
    result["query"] = q
    result["page"] = response_page
    result["pages"] = pages
    result["count"] = len(result.get("items") or [])
    return result


@app.get("/api/master/schemes/{scheme_id}")
def api_master_scheme_detail(scheme_id: int, db_path: str = "") -> Dict[str, Any]:
    path = resolve_db_path(db_path or None)
    ensure_master_index_ready(path)
    return get_master_scheme_detail(path, scheme_id)


@app.post("/api/jobs/scrape")
def api_start_scrape_job(request: ScrapeJobRequest) -> Dict[str, Any]:
    payload = request.model_dump()
    payload["url"] = normalize_url_input(payload.get("url", ""))
    if not payload["url"]:
        raise HTTPException(status_code=400, detail="Valid URL required")

    job_id = uuid.uuid4().hex[:12]
    created = now_iso()

    public_params = {
        "url": payload["url"],
        "depth": payload.get("depth", 1),
        "max_pages": payload.get("max_pages", 20),
        "max_files": payload.get("max_files", 80),
        "js": bool(payload.get("js")),
        "selenium_site": bool(payload.get("selenium_site", True)),
        "no_ai": bool(payload.get("no_ai")),
        "all_domains": bool(payload.get("all_domains")),
        "model": payload.get("model", ""),
        "db_path": str(resolve_db_path(payload.get("db_path"))),
    }

    with JOB_LOCK:
        SCRAPE_JOBS[job_id] = {
            "id": job_id,
            "status": "queued",
            "created_at": created,
            "updated_at": created,
            "started_at": None,
            "finished_at": None,
            "exit_code": None,
            "error": None,
            "curated_count": None,
            "params": public_params,
            "logs": ["[system] job queued"],
        }

    thread = threading.Thread(target=run_scrape_job, args=(job_id, payload), daemon=True)
    thread.start()

    return {
        "job_id": job_id,
        "status": "queued",
        "created_at": created,
        "params": public_params,
    }


@app.get("/api/jobs")
def api_list_jobs(limit: int = Query(20, ge=1, le=200)) -> Dict[str, Any]:
    with JOB_LOCK:
        jobs = list(SCRAPE_JOBS.values())

    jobs_sorted = sorted(jobs, key=lambda j: j.get("created_at", ""), reverse=True)[:limit]

    summary = []
    for job in jobs_sorted:
        summary.append(
            {
                "id": job.get("id"),
                "status": job.get("status"),
                "created_at": job.get("created_at"),
                "updated_at": job.get("updated_at"),
                "started_at": job.get("started_at"),
                "finished_at": job.get("finished_at"),
                "exit_code": job.get("exit_code"),
                "curated_count": job.get("curated_count"),
                "params": job.get("params", {}),
            }
        )

    return {"items": summary, "total": len(summary)}


@app.get("/api/jobs/{job_id}")
def api_get_job(job_id: str) -> Dict[str, Any]:
    with JOB_LOCK:
        job = SCRAPE_JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return dict(job)
