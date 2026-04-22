"""
KRISHI-SETU Advanced Scraper, Crawler, and Database Organizer
==============================================================
Usage:
  python scraper.py
  python scraper.py <url>
  python scraper.py <url> --depth 2 --max-pages 80 --max-files 200 --js
    python scraper.py <url> --depth 2 --max-pages 200 --selenium-site
    python scraper.py --query-db --query "pm-kisan"
  python scraper.py --test

Highlights:
  - Deep crawl with same-domain controls
  - Multi-format ingestion (HTML, PDF, JSON, CSV, XLSX, DOCX, TXT/XML)
  - Selenium fallback for JS-rendered pages
  - Persistent normalized SQLite database that gets richer every run
    - Curated high-confidence scheme table derived from raw extracts
    - Query mode for searching schemes by name/ministry/type/confidence
  - Markdown + JSON + CSV exports for downstream analytics/RAG
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from collections import deque
from difflib import SequenceMatcher
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urldefrag, urljoin, urlparse

try:
    import requests
    from bs4 import BeautifulSoup
    import pdfplumber
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
except ImportError as exc:
    print(
        f"Missing dependency: {exc}\n"
        "Run: pip install -r requirements.txt"
    )
    sys.exit(1)

HAS_GENAI = False
try:
    from google import genai as google_genai
    from google.genai import types as genai_types

    HAS_GENAI = True
except Exception:
    HAS_GENAI = False

HAS_SELENIUM = False
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager

    HAS_SELENIUM = True
except Exception:
    HAS_SELENIUM = False

HAS_PANDAS = False
try:
    import pandas as pd

    HAS_PANDAS = True
except Exception:
    HAS_PANDAS = False

HAS_DOCX = False
try:
    import docx

    HAS_DOCX = True
except Exception:
    HAS_DOCX = False


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
REPORT_DIR = OUTPUT_DIR / "reports"
DATA_DIR = OUTPUT_DIR / "data"
DB_DIR = OUTPUT_DIR / "db"
PDF_DIR = OUTPUT_DIR / "pdfs"
RAW_DIR = OUTPUT_DIR / "raw"
RAW_CSV_DIR = RAW_DIR / "csv"
RAW_JSON_DIR = RAW_DIR / "json"
RAW_EXCEL_DIR = RAW_DIR / "excel"
RAW_DOC_DIR = RAW_DIR / "doc"
RAW_OTHER_DIR = RAW_DIR / "other"

for _dir in [
    OUTPUT_DIR,
    REPORT_DIR,
    DATA_DIR,
    DB_DIR,
    PDF_DIR,
    RAW_DIR,
    RAW_CSV_DIR,
    RAW_JSON_DIR,
    RAW_EXCEL_DIR,
    RAW_DOC_DIR,
    RAW_OTHER_DIR,
]:
    _dir.mkdir(parents=True, exist_ok=True)


console = Console()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

DOWNLOADABLE_TYPES = {
    "pdf",
    "csv",
    "json",
    "xlsx",
    "xls",
    "docx",
    "doc",
    "xml",
    "zip",
    "txt",
}

RESOURCE_DIRS = {
    "pdf": PDF_DIR,
    "csv": RAW_CSV_DIR,
    "json": RAW_JSON_DIR,
    "xlsx": RAW_EXCEL_DIR,
    "xls": RAW_EXCEL_DIR,
    "docx": RAW_DOC_DIR,
    "doc": RAW_DOC_DIR,
    "xml": RAW_OTHER_DIR,
    "zip": RAW_OTHER_DIR,
    "txt": RAW_OTHER_DIR,
    "other": RAW_OTHER_DIR,
}

MODEL_CANDIDATES = [
    "gemini-2.5-flash",
    "gemini-2.0-flash",
    "gemini-1.5-flash",
]

SCHEME_KEYS = [
    "scheme_name",
    "short_name",
    "scheme_type",
    "nodal_ministry",
    "launch_year",
    "objective",
    "target_beneficiaries",
    "eligibility_rules",
    "exclusions",
    "benefit_amount",
    "premium_or_cost",
    "requirements",
    "documents_required",
    "application_process",
    "application_deadline",
    "official_website",
    "helpline",
    "budget_allocation",
    "coverage_stats",
    "key_facts",
    "keywords_hindi",
]

LIST_FIELDS = {
    "eligibility_rules",
    "exclusions",
    "requirements",
    "documents_required",
    "key_facts",
    "keywords_hindi",
}

SCHEME_NAME_HINTS = (
    "scheme",
    "yojana",
    "mission",
    "programme",
    "program",
    "fund",
    "subsidy",
    "insurance",
    "credit",
    "support",
    "kisan",
    "agri",
    "agriculture",
    "pm-",
    "pm ",
)

SCHEME_NAME_BLOCKLIST = (
    "click",
    "download",
    "annexure",
    "table",
    "figure",
    "copyright",
    "privacy",
    "cookie",
    "cid:",
    "sql query",
)

GENERIC_SCHEME_NAMES = {
    "centrally sponsored scheme",
    "central sector scheme",
    "support to state extension programme for extension reforms",
    "atma scheme guidelines",
    "district mission committee",
    "district horticulture mission document",
    "agricultural mission",
}

MASTER_FUZZY_MATCH_THRESHOLD = 72

MYSCHEME_SEARCH_API = "https://api.myscheme.gov.in/search/v6/schemes"
MYSCHEME_PUBLIC_API_KEY = os.environ.get("MYSCHEME_PUBLIC_API_KEY", "tYTy5eEhlu9rFjyxuCr7ra7ACp4dv1RH8gWuHTDc")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def task_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_headers(seed: int = 0) -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENTS[seed % len(USER_AGENTS)],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
        "Referer": "https://www.google.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r", " ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def trim_text(value: str, limit: int = 100_000) -> str:
    return value[:limit] if len(value) > limit else value


def stable_hash(text: str, length: int = 12) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:length]


def canonicalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()


def normalize_url(raw_url: str) -> str:
    url = clean_text(raw_url)
    if not url:
        return ""
    if not re.match(r"^https?://", url, re.IGNORECASE):
        url = "https://" + url
    url, _ = urldefrag(url)
    return url


def safe_filename_fragment(value: str, default: str = "resource") -> str:
    name = re.sub(r"[^a-zA-Z0-9._-]+", "_", value).strip("._-")
    return name[:80] if name else default


def host_matches(candidate: str, base: str) -> bool:
    candidate = candidate.lower().strip()
    base = base.lower().strip()
    if not candidate or not base:
        return False
    return candidate == base or candidate.endswith("." + base) or base.endswith("." + candidate)


def should_skip_url(url: str) -> bool:
    lower = url.lower()
    blocked = [
        "javascript:",
        "mailto:",
        "tel:",
        "#",
        "logout",
        "/signin",
        "/register",
        "/wp-admin",
    ]
    return any(token in lower for token in blocked)


def is_myscheme_category_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path or ""
    return "myscheme.gov.in" in host and path.startswith("/search/category/")


def myscheme_category_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or ""
    marker = "/search/category/"
    if marker not in path:
        return ""
    raw = path.split(marker, 1)[1].strip("/")
    return clean_text(unquote(raw))


def fetch_myscheme_category_scheme_urls(
    category_name: str,
    page_size: int = 100,
    max_records: int = 2000,
) -> Tuple[List[str], List[str]]:
    category = clean_text(category_name)
    if not category:
        return [], ["missing category name"]

    safe_size = max(10, min(100, int(page_size)))
    safe_max = max(10, min(5000, int(max_records)))
    referer_category = quote(category, safe=",")

    headers = {
        "x-api-key": MYSCHEME_PUBLIC_API_KEY,
        "Origin": "https://www.myscheme.gov.in",
        "Referer": f"https://www.myscheme.gov.in/search/category/{referer_category}",
        "User-Agent": USER_AGENTS[0],
        "Accept": "application/json, text/plain, */*",
    }

    collected: List[str] = []
    seen: set[str] = set()
    errors: List[str] = []
    offset = 0
    total_hint = 0

    while offset < safe_max:
        params = {
            "lang": "en",
            "q": json.dumps([
                {
                    "identifier": "schemeCategory",
                    "value": category,
                }
            ], separators=(",", ":")),
            "keyword": "",
            "sort": "",
            "from": str(offset),
            "size": str(safe_size),
        }

        try:
            resp = requests.get(MYSCHEME_SEARCH_API, params=params, headers=headers, timeout=35)
        except Exception as exc:
            errors.append(f"request failed at offset={offset}: {exc}")
            break

        if resp.status_code >= 400:
            errors.append(f"HTTP {resp.status_code} at offset={offset}")
            break

        try:
            payload = resp.json()
        except Exception as exc:
            errors.append(f"invalid JSON at offset={offset}: {exc}")
            break

        data = payload.get("data") if isinstance(payload, dict) else {}
        if not isinstance(data, dict):
            errors.append(f"unexpected payload shape at offset={offset}")
            break

        summary = data.get("summary")
        if isinstance(summary, dict):
            try:
                total_hint = max(total_hint, int(summary.get("total") or 0))
            except Exception:
                pass

        hits = data.get("hits")
        items = hits.get("items", []) if isinstance(hits, dict) else []
        if not isinstance(items, list) or not items:
            break

        for item in items:
            if not isinstance(item, dict):
                continue
            fields = item.get("fields") if isinstance(item.get("fields"), dict) else {}
            slug = clean_text(item.get("slug") or fields.get("slug"))
            detail_url = ""
            if slug:
                detail_url = normalize_url(f"https://www.myscheme.gov.in/schemes/{slug}")
            else:
                maybe_url = normalize_url(clean_text(item.get("url")))
                if maybe_url and "/schemes/" in urlparse(maybe_url).path.lower():
                    detail_url = maybe_url

            if detail_url and detail_url not in seen:
                seen.add(detail_url)
                collected.append(detail_url)

        if len(items) < safe_size:
            break

        offset += safe_size
        if total_hint and offset >= min(total_hint, safe_max):
            break

    return collected, errors


def coerce_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            text = clean_text(item)
            if text:
                out.append(text)
        return out
    text = clean_text(value)
    return [text] if text else []


def classify_resource(url: str, content_type: str = "") -> str:
    path = urlparse(url).path.lower()
    ctype = content_type.lower()
    if path.endswith(".pdf") or "application/pdf" in ctype:
        return "pdf"
    if path.endswith(".csv") or "text/csv" in ctype or "application/csv" in ctype:
        return "csv"
    if path.endswith(".json") or "application/json" in ctype or ctype.endswith("+json"):
        return "json"
    if path.endswith(".xlsx") or "application/vnd.openxmlformats-officedocument" in ctype:
        return "xlsx"
    if path.endswith(".xls") or "application/vnd.ms-excel" in ctype:
        return "xls"
    if path.endswith(".docx"):
        return "docx"
    if path.endswith(".doc"):
        return "doc"
    if path.endswith(".xml") or "application/xml" in ctype or "text/xml" in ctype:
        return "xml"
    if path.endswith(".zip") or "application/zip" in ctype:
        return "zip"
    if path.endswith(".txt") or "text/plain" in ctype:
        return "txt"
    if "text/html" in ctype or "application/xhtml+xml" in ctype:
        return "html"
    if any(path.endswith(ext) for ext in [".htm", ".html", "/"]):
        return "html"
    return "other"


def extension_for_type(resource_type: str, content_type: str = "") -> str:
    mapping = {
        "pdf": ".pdf",
        "csv": ".csv",
        "json": ".json",
        "xlsx": ".xlsx",
        "xls": ".xls",
        "docx": ".docx",
        "doc": ".doc",
        "xml": ".xml",
        "zip": ".zip",
        "txt": ".txt",
        "html": ".html",
        "other": ".bin",
    }
    ext = mapping.get(resource_type, ".bin")
    if resource_type == "other" and "json" in content_type.lower():
        return ".json"
    return ext


def parse_json_block(raw_text: str) -> Dict[str, Any]:
    text = raw_text.strip()
    text = re.sub(r"^```json\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^```\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {"schemes": data}
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start : end + 1]
        try:
            data = json.loads(candidate)
            return data if isinstance(data, dict) else {"schemes": data}
        except Exception:
            pass
    return {"schemes": []}


def normalize_scheme_record(raw_scheme: Dict[str, Any]) -> Dict[str, Any]:
    scheme: Dict[str, Any] = {}
    for key in SCHEME_KEYS:
        value = raw_scheme.get(key)
        if key in LIST_FIELDS:
            scheme[key] = coerce_list(value)
        else:
            scheme[key] = clean_text(value)

    if not scheme.get("scheme_name") and scheme.get("short_name"):
        scheme["scheme_name"] = scheme["short_name"]

    # Normalize noisy wrappers from LLM output such as [Scheme Name] or quoted names.
    cleaned_name = clean_text(scheme.get("scheme_name", ""))
    cleaned_name = re.sub(r"^[A-Za-z]\s*\.\s*", "", cleaned_name)
    cleaned_name = re.sub(r"^[\[\(\{\"']+", "", cleaned_name)
    cleaned_name = re.sub(r"[\]\)\}\"']+$", "", cleaned_name)
    scheme["scheme_name"] = clean_text(cleaned_name)

    if not scheme.get("requirements") and scheme.get("documents_required"):
        scheme["requirements"] = scheme["documents_required"]
    return scheme


def scheme_detail_signal_count(scheme: Dict[str, Any]) -> int:
    score = 0
    for key in [
        "nodal_ministry",
        "target_beneficiaries",
        "benefit_amount",
        "application_process",
        "official_website",
    ]:
        if clean_text(scheme.get(key, "")):
            score += 1

    objective = clean_text(scheme.get("objective", ""))
    if objective and len(objective) >= 35 and "heuristic extraction from crawled content" not in objective.lower():
        score += 1

    for key in ["eligibility_rules", "requirements", "documents_required", "exclusions", "key_facts"]:
        if coerce_list(scheme.get(key)):
            score += 1
    return score


def is_probable_scheme_name(name: str) -> bool:
    text = clean_text(name)
    if not text:
        return False
    if len(text) < 8 or len(text) > 180:
        return False
    if not re.match(r"^[A-Z0-9]", text):
        return False
    if text.startswith(("/", "-", ":", "|")):
        return False
    if re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", text):
        return False
    if re.search(r"\b\d{1,2}:\d{2}\b", text):
        return False

    letters = sum(ch.isalpha() for ch in text)
    digits = sum(ch.isdigit() for ch in text)
    if letters < 5:
        return False
    if digits > letters:
        return False

    lower = text.lower()
    if any(token in lower for token in SCHEME_NAME_BLOCKLIST):
        return False
    if re.search(r"\b(am|pm)\b", lower):
        return False

    fragment_patterns = [
        r"\bas one of the\b",
        r"\bduring the\b",
        r"\bkeeping in view\b",
        r"\bunder the scheme\b",
        r"\bthe scheme envisages\b",
        r"\bhowever\b",
        r"\bfor the scheme code\b",
    ]
    if any(re.search(pattern, lower) for pattern in fragment_patterns):
        return False

    words = re.findall(r"[A-Za-z]{2,}", text)
    if len(words) > 16:
        return False
    if len(words) > 10 and re.search(r"\b(is|are|was|were|will|would|has|have|had|includes?|including|provides?|envisages?)\b", lower):
        return False
    if text.count(",") >= 2:
        return False
    if text.endswith((":", ".", ";")):
        return False
    return len(words) >= 2


def is_compact_scheme_title(name: str) -> bool:
    text = clean_text(name)
    lower = text.lower()
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9-]*", text)
    if len(words) < 2 or len(words) > 11:
        return False
    if any(ch in text for ch in [",", ":", ";"]):
        return False

    banned_sentence_words = {
        "this",
        "that",
        "these",
        "those",
        "during",
        "under",
        "where",
        "which",
        "however",
        "therefore",
        "accordingly",
        "because",
        "while",
        "will",
        "would",
        "has",
        "have",
        "had",
        "was",
        "were",
        "is",
        "are",
    }
    if any(token in banned_sentence_words for token in re.findall(r"[a-z]+", lower)):
        return False

    alpha_tokens = re.findall(r"[A-Za-z][A-Za-z-]*", text)
    if not alpha_tokens:
        return False
    titled = sum(1 for token in alpha_tokens if token[0].isupper() or token.isupper())
    if titled / len(alpha_tokens) < 0.6:
        return False

    canonical = canonicalize_name(text)
    return canonical not in GENERIC_SCHEME_NAMES


def is_high_quality_scheme_record(scheme: Dict[str, Any]) -> bool:
    name = clean_text(scheme.get("scheme_name", ""))
    if not is_probable_scheme_name(name):
        return False

    lower = name.lower()
    has_name_hint = any(token in lower for token in SCHEME_NAME_HINTS)
    detail_score = scheme_detail_signal_count(scheme)
    scheme_type = clean_text(scheme.get("scheme_type", "")).lower()

    if has_name_hint and detail_score >= 1:
        return True
    if has_name_hint and is_compact_scheme_title(name):
        return True
    if scheme_type and scheme_type != "other" and detail_score >= 1:
        return True
    if scheme_type and scheme_type != "other" and is_compact_scheme_title(name):
        return True
    return detail_score >= 4


def filter_extracted_schemes(schemes: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    filtered: List[Dict[str, Any]] = []
    dropped = 0
    seen: set[str] = set()

    for scheme in schemes:
        normalized = normalize_scheme_record(scheme)
        if not is_high_quality_scheme_record(normalized):
            dropped += 1
            continue

        key = canonicalize_name(normalized.get("scheme_name", ""))
        if not key or key in seen:
            continue
        seen.add(key)
        filtered.append(normalized)

    return filtered, dropped


def extract_table_data(soup: BeautifulSoup, max_tables: int = 8, max_rows: int = 30) -> List[Dict[str, Any]]:
    tables: List[Dict[str, Any]] = []
    for table in soup.find_all("table")[:max_tables]:
        headers = [clean_text(th.get_text(" ", strip=True)) for th in table.find_all("th")][:15]
        rows: List[List[str]] = []
        for tr in table.find_all("tr")[:max_rows]:
            cells = [clean_text(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])][:15]
            if any(cells):
                rows.append(cells)
        if rows:
            tables.append({"headers": headers, "rows": rows})
    return tables


def extract_html_text(soup: BeautifulSoup, limit: int = 80_000) -> str:
    for tag in soup(["script", "style", "noscript", "iframe", "footer", "header", "nav", "aside"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{2,}", "\n", text)
    return trim_text(text, limit=limit)


def fetch_page_with_selenium(url: str, wait_seconds: float = 4.0) -> Tuple[str, str, str]:
    if not HAS_SELENIUM:
        return "", "", "Selenium not installed"

    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver = None
    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(50)
        driver.get(url)
        time.sleep(wait_seconds)
        return driver.page_source or "", driver.title or "", ""
    except Exception as exc:
        return "", "", str(exc)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def crawl_site_with_selenium(
    start_url: str,
    max_depth: int = 1,
    max_pages: int = 40,
    same_domain_only: bool = True,
    wait_seconds: float = 2.0,
) -> Dict[str, Any]:
    normalized_start = normalize_url(start_url)
    if not normalized_start:
        return {"pages": [], "file_links": [], "errors": ["Invalid start URL"]}

    if classify_resource(normalized_start) in DOWNLOADABLE_TYPES:
        return {
            "pages": [],
            "file_links": [{"url": normalized_start, "label": "direct-resource", "resource_type": classify_resource(normalized_start)}],
            "errors": [],
        }

    if not HAS_SELENIUM:
        return {
            "pages": [],
            "file_links": [],
            "errors": ["Selenium not installed. Install selenium + webdriver-manager."],
        }

    base_host = urlparse(normalized_start).netloc.lower()
    myscheme_category_mode = is_myscheme_category_url(normalized_start)
    myscheme_category_name = myscheme_category_name_from_url(normalized_start) if myscheme_category_mode else ""
    queue: deque[Tuple[str, int]] = deque([(normalized_start, 0)])
    visited: set[str] = set()
    pages: List[Dict[str, Any]] = []
    files: List[Dict[str, Any]] = []
    errors: List[str] = []

    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver = None
    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(50)

        if myscheme_category_mode and myscheme_category_name and max_depth >= 1:
            seeded_urls, seed_errors = fetch_myscheme_category_scheme_urls(
                category_name=myscheme_category_name,
                page_size=100,
                max_records=max(100, max_pages * 3),
            )
            if seeded_urls:
                console.log(
                    f"[cyan]Selenium Crawl[/] seeded {len(seeded_urls)} MyScheme scheme links from category API"
                )
                for link in seeded_urls:
                    queue.append((link, 1))
            for err in seed_errors[:5]:
                errors.append(f"myscheme-category-api :: {err}")

        while queue and len(pages) < max_pages:
            current_url, depth = queue.popleft()
            if current_url in visited:
                continue
            visited.add(current_url)

            console.log(f"[cyan]Selenium Crawl[/] d={depth} {current_url[:110]}")

            page: Dict[str, Any] = {
                "url": current_url,
                "title": "",
                "text": "",
                "tables": [],
                "page_links": [],
                "file_links": [],
                "status_code": 200,
                "content_type": "text/html",
                "fetch_mode": "selenium-site",
                "error": None,
            }

            try:
                driver.get(current_url)
                time.sleep(wait_seconds)
                html = driver.page_source or ""
                title = clean_text(driver.title or "")
                soup = BeautifulSoup(html, "lxml")

                page["title"] = title or (clean_text(soup.title.get_text()) if soup.title else urlparse(current_url).netloc)
                page["text"] = extract_html_text(soup)
                page["tables"] = extract_table_data(soup)

                seen_page_links: set[str] = set()
                seen_file_links: set[str] = set()

                for anchor in soup.find_all("a", href=True):
                    href = clean_text(anchor.get("href"))
                    if not href:
                        continue
                    absolute = normalize_url(urljoin(current_url, href))
                    if not absolute or should_skip_url(absolute):
                        continue

                    resource = classify_resource(absolute)
                    label = clean_text(anchor.get_text(" ", strip=True))[:160]

                    if resource in DOWNLOADABLE_TYPES:
                        if absolute not in seen_file_links:
                            seen_file_links.add(absolute)
                            page["file_links"].append(
                                {
                                    "url": absolute,
                                    "label": label,
                                    "resource_type": resource,
                                }
                            )
                    elif resource in {"html", "other"}:
                        if myscheme_category_mode:
                            path_lower = urlparse(absolute).path.lower()
                            if absolute != normalized_start and "/schemes/" not in path_lower:
                                continue
                        if absolute not in seen_page_links and absolute != current_url:
                            if same_domain_only and not host_matches(urlparse(absolute).netloc, base_host):
                                continue
                            seen_page_links.add(absolute)
                            page["page_links"].append(absolute)
            except Exception as exc:
                page["error"] = str(exc)
                errors.append(f"{current_url} :: {exc}")

            pages.append(page)
            files.extend(page.get("file_links", []))

            if depth < max_depth:
                for link in page.get("page_links", []):
                    if link not in visited:
                        queue.append((link, depth + 1))

            time.sleep(0.15)
    except Exception as exc:
        errors.append(f"selenium-site-init :: {exc}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    deduped_files: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for item in files:
        url = item.get("url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        deduped_files.append(item)

    return {"pages": pages, "file_links": deduped_files, "errors": errors}


def crawl_single_page(url: str, session: requests.Session, enable_js: bool = False, ua_seed: int = 0) -> Dict[str, Any]:
    page: Dict[str, Any] = {
        "url": url,
        "title": "",
        "text": "",
        "tables": [],
        "page_links": [],
        "file_links": [],
        "status_code": 0,
        "content_type": "",
        "fetch_mode": "requests",
        "error": None,
    }

    try:
        response = session.get(url, headers=get_headers(ua_seed), timeout=30, allow_redirects=True)
        page["status_code"] = response.status_code
        ctype = response.headers.get("Content-Type", "")
        page["content_type"] = ctype
        resource_type = classify_resource(url, ctype)

        if resource_type in DOWNLOADABLE_TYPES:
            page["file_links"].append(
                {
                    "url": url,
                    "label": "direct-resource",
                    "resource_type": resource_type,
                }
            )
            return page

        if response.status_code >= 400:
            page["error"] = f"HTTP {response.status_code}"
            if enable_js and response.status_code in (401, 403, 429):
                html, title, js_error = fetch_page_with_selenium(url)
                if html:
                    page["fetch_mode"] = "selenium"
                    page["title"] = clean_text(title)
                    soup = BeautifulSoup(html, "lxml")
                    page["text"] = extract_html_text(soup)
                    page["tables"] = extract_table_data(soup)
                else:
                    page["error"] = f"HTTP {response.status_code}; JS fallback failed: {js_error}"
            return page

        html = response.text
        if enable_js and len(clean_text(html)) < 600:
            html_js, title_js, js_error = fetch_page_with_selenium(url)
            if html_js:
                html = html_js
                page["title"] = clean_text(title_js)
                page["fetch_mode"] = "selenium"
            elif js_error:
                page["error"] = f"JS fallback failed: {js_error}"

        soup = BeautifulSoup(html, "lxml")
        page["title"] = page["title"] or clean_text(soup.title.get_text()) if soup.title else urlparse(url).netloc
        page["text"] = extract_html_text(soup)
        page["tables"] = extract_table_data(soup)

        seen_page_links: set[str] = set()
        seen_file_links: set[str] = set()

        for anchor in soup.find_all("a", href=True):
            href = clean_text(anchor.get("href"))
            if not href:
                continue
            absolute = normalize_url(urljoin(url, href))
            if not absolute or should_skip_url(absolute):
                continue

            resource = classify_resource(absolute)
            label = clean_text(anchor.get_text(" ", strip=True))[:160]

            if resource in DOWNLOADABLE_TYPES:
                if absolute not in seen_file_links:
                    seen_file_links.add(absolute)
                    page["file_links"].append(
                        {
                            "url": absolute,
                            "label": label,
                            "resource_type": resource,
                        }
                    )
            elif resource in {"html", "other"}:
                if absolute not in seen_page_links and absolute != url:
                    seen_page_links.add(absolute)
                    page["page_links"].append(absolute)

    except Exception as exc:
        page["error"] = str(exc)

    return page


def crawl_site(
    start_url: str,
    max_depth: int = 1,
    max_pages: int = 40,
    same_domain_only: bool = True,
    enable_js: bool = False,
    selenium_site: bool = False,
) -> Dict[str, Any]:
    if selenium_site:
        if not HAS_SELENIUM:
            console.log("[yellow]Selenium-site mode requested but selenium is unavailable; using requests crawler.[/]")
        else:
            return crawl_site_with_selenium(
                start_url=start_url,
                max_depth=max_depth,
                max_pages=max_pages,
                same_domain_only=same_domain_only,
            )

    normalized_start = normalize_url(start_url)
    if not normalized_start:
        return {"pages": [], "file_links": [], "errors": ["Invalid start URL"]}

    if classify_resource(normalized_start) in DOWNLOADABLE_TYPES:
        return {
            "pages": [],
            "file_links": [{"url": normalized_start, "label": "direct-resource", "resource_type": classify_resource(normalized_start)}],
            "errors": [],
        }

    base_host = urlparse(normalized_start).netloc.lower()
    queue: deque[Tuple[str, int]] = deque([(normalized_start, 0)])
    visited: set[str] = set()
    pages: List[Dict[str, Any]] = []
    files: List[Dict[str, Any]] = []
    errors: List[str] = []

    session = requests.Session()
    ua_seed = 0

    while queue and len(pages) < max_pages:
        current_url, depth = queue.popleft()
        if current_url in visited:
            continue
        visited.add(current_url)

        console.log(f"[cyan]Crawl[/] d={depth} {current_url[:110]}")
        page = crawl_single_page(current_url, session, enable_js=enable_js, ua_seed=ua_seed)
        ua_seed += 1
        pages.append(page)

        if page.get("error"):
            errors.append(f"{current_url} :: {page['error']}")

        files.extend(page.get("file_links", []))

        if depth >= max_depth:
            continue

        for link in page.get("page_links", []):
            if link in visited:
                continue
            if same_domain_only and not host_matches(urlparse(link).netloc, base_host):
                continue
            queue.append((link, depth + 1))

        time.sleep(0.25)

    deduped_files: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for item in files:
        url = item.get("url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        deduped_files.append(item)

    return {"pages": pages, "file_links": deduped_files, "errors": errors}


def build_download_path(url: str, resource_type: str, content_type: str = "") -> Path:
    path_obj = Path(urlparse(url).path)
    stem = safe_filename_fragment(path_obj.stem or "resource")
    ext = extension_for_type(resource_type, content_type)
    filename = f"{stable_hash(url)}_{stem}{ext}"
    target_dir = RESOURCE_DIRS.get(resource_type, RAW_OTHER_DIR)
    return target_dir / filename


def download_resource(
    session: requests.Session,
    link_item: Dict[str, Any],
    timeout: int = 45,
    max_bytes: int = 120 * 1024 * 1024,
) -> Dict[str, Any]:
    url = link_item.get("url", "")
    declared = link_item.get("resource_type", "other")
    meta: Dict[str, Any] = {
        "url": url,
        "label": clean_text(link_item.get("label", "")),
        "resource_type": declared,
        "content_type": "",
        "status_code": 0,
        "local_path": "",
        "size_bytes": 0,
        "cached": False,
        "error": None,
    }

    try:
        response = session.get(url, headers=get_headers(), timeout=timeout, stream=True, allow_redirects=True)
        meta["status_code"] = response.status_code
        meta["content_type"] = response.headers.get("Content-Type", "")
        if response.status_code >= 400:
            meta["error"] = f"HTTP {response.status_code}"
            return meta

        guessed = classify_resource(url, meta["content_type"])
        if guessed == "html" and declared in DOWNLOADABLE_TYPES:
            guessed = declared
        meta["resource_type"] = guessed

        if guessed == "html":
            meta["error"] = "resolved-to-html"
            return meta

        dest = build_download_path(url, guessed, meta["content_type"])
        if dest.exists() and dest.stat().st_size > 0:
            meta["cached"] = True
            meta["local_path"] = str(dest)
            meta["size_bytes"] = int(dest.stat().st_size)
            return meta

        bytes_written = 0
        with open(dest, "wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                bytes_written += len(chunk)
                if bytes_written > max_bytes:
                    raise ValueError("file exceeded max size limit")
                handle.write(chunk)

        meta["local_path"] = str(dest)
        meta["size_bytes"] = bytes_written
        return meta
    except Exception as exc:
        meta["error"] = str(exc)
        return meta


def download_resources(file_links: List[Dict[str, Any]], max_files: int = 100) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    unique: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in file_links:
        url = item.get("url", "")
        if not url or url in seen:
            continue
        seen.add(url)
        unique.append(item)

    selected = unique[:max_files]
    downloaded: List[Dict[str, Any]] = []
    session = requests.Session()

    stats = {
        "discovered": len(unique),
        "selected": len(selected),
        "downloaded": 0,
        "cached": 0,
        "failed": 0,
    }

    for item in selected:
        meta = download_resource(session, item)
        downloaded.append(meta)
        if meta.get("error"):
            stats["failed"] += 1
            console.log(f"[red]File fail[/] {meta['url'][:90]} :: {meta['error']}")
        else:
            if meta.get("cached"):
                stats["cached"] += 1
                console.log(f"[yellow]Cached[/] {meta['resource_type']} {meta['url'][:90]}")
            else:
                stats["downloaded"] += 1
                console.log(f"[green]Downloaded[/] {meta['resource_type']} {meta['url'][:90]}")
        time.sleep(0.1)

    return downloaded, stats


def extract_text_from_pdf(path: Path, max_pages: int = 40, max_chars: int = 90_000) -> str:
    chunks: List[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages[:max_pages]:
            text = page.extract_text() or ""
            if text:
                chunks.append(text)
            if sum(len(x) for x in chunks) > max_chars:
                break
    return trim_text("\n".join(chunks), max_chars)


def extract_text_from_csv(path: Path, max_rows: int = 250, max_chars: int = 90_000) -> str:
    lines: List[str] = []
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.reader(handle)
        for idx, row in enumerate(reader):
            if idx >= max_rows:
                break
            lines.append(" | ".join(clean_text(cell) for cell in row))
    return trim_text("\n".join(lines), max_chars)


def extract_text_from_json(path: Path, max_chars: int = 90_000) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        data = json.load(handle)
    return trim_text(json.dumps(data, indent=2, ensure_ascii=False), max_chars)


def extract_text_from_excel(path: Path, max_rows: int = 200, max_chars: int = 90_000) -> str:
    if not HAS_PANDAS:
        return "Excel parser skipped (install pandas + openpyxl)."
    chunks: List[str] = []
    sheets = pd.read_excel(path, sheet_name=None, dtype=str)
    for sheet_name, frame in list(sheets.items())[:5]:
        frame = frame.fillna("").astype(str).head(max_rows)
        chunks.append(f"SHEET: {sheet_name}")
        chunks.append(" | ".join(frame.columns.tolist()))
        for _, row in frame.iterrows():
            chunks.append(" | ".join(clean_text(x) for x in row.tolist()))
    return trim_text("\n".join(chunks), max_chars)


def extract_text_from_docx(path: Path, max_chars: int = 90_000) -> str:
    if not HAS_DOCX:
        return "DOCX parser skipped (install python-docx)."
    document = docx.Document(path)
    parts: List[str] = []
    for paragraph in document.paragraphs[:500]:
        text = clean_text(paragraph.text)
        if text:
            parts.append(text)
    return trim_text("\n".join(parts), max_chars)


def extract_text_from_plain(path: Path, max_chars: int = 90_000) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        return trim_text(handle.read(), max_chars)


def extract_text_from_download(meta: Dict[str, Any]) -> Tuple[str, str]:
    if meta.get("error"):
        return "", meta["error"]

    local_path = meta.get("local_path")
    if not local_path:
        return "", "missing-local-path"

    resource_type = meta.get("resource_type", "other")
    path = Path(local_path)

    try:
        if resource_type == "pdf":
            return extract_text_from_pdf(path), "pdf"
        if resource_type == "csv":
            return extract_text_from_csv(path), "csv"
        if resource_type == "json":
            return extract_text_from_json(path), "json"
        if resource_type in {"xlsx", "xls"}:
            return extract_text_from_excel(path), "excel"
        if resource_type == "docx":
            return extract_text_from_docx(path), "docx"
        if resource_type in {"txt", "xml"}:
            return extract_text_from_plain(path), resource_type
    except Exception as exc:
        return "", f"parse-error: {exc}"

    return "", "unsupported-type"


def source_payloads_from_crawl(pages: List[Dict[str, Any]], downloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    payloads: List[Dict[str, Any]] = []

    for page in pages:
        content_parts: List[str] = []
        if page.get("text"):
            content_parts.append(page["text"])
        for table in page.get("tables", []):
            headers = table.get("headers", [])
            rows = table.get("rows", [])
            if headers:
                content_parts.append("TABLE HEADERS: " + " | ".join(headers))
            for row in rows[:30]:
                content_parts.append("TABLE ROW: " + " | ".join(row))
        merged = trim_text("\n".join(content_parts), 90_000)

        payloads.append(
            {
                "url": page.get("url", ""),
                "source_type": "webpage",
                "title": page.get("title", ""),
                "status_code": page.get("status_code", 0),
                "content_type": page.get("content_type", "text/html"),
                "local_path": "",
                "notes": page.get("error") or page.get("fetch_mode", "requests"),
                "content": merged,
            }
        )

    for meta in downloads:
        text, parser_note = extract_text_from_download(meta)
        payloads.append(
            {
                "url": meta.get("url", ""),
                "source_type": meta.get("resource_type", "file"),
                "title": meta.get("label", ""),
                "status_code": meta.get("status_code", 0),
                "content_type": meta.get("content_type", ""),
                "local_path": meta.get("local_path", ""),
                "notes": parser_note,
                "content": trim_text(text, 90_000),
            }
        )

    return payloads


def heuristic_extract_schemes(content: str) -> List[Dict[str, Any]]:
    candidates: List[str] = []
    lines = [clean_text(line) for line in content.splitlines()]

    line_pattern = re.compile(r"\b(scheme|yojana|mission|programme|program|subsidy|pm-[a-z0-9]+)\b", re.IGNORECASE)
    for line in lines:
        if len(line) < 8 or len(line) > 140:
            continue
        if not line_pattern.search(line):
            continue
        if re.search(r"\b(login|copyright|privacy|cookie|download|file)\b", line, re.IGNORECASE):
            continue
        line = re.sub(r"^[\-•*\d\.\)\(\s]+", "", line)
        if line:
            candidates.append(line)

    sentence_pattern = re.compile(r"\b[A-Z][A-Za-z0-9\-\(\)\s]{6,90}(?:Scheme|Yojana|Mission)\b")
    for match in sentence_pattern.finditer(content):
        candidates.append(clean_text(match.group(0)))

    unique_names: List[str] = []
    seen: set[str] = set()
    for name in candidates:
        key = canonicalize_name(name)
        if not key or key in seen:
            continue
        seen.add(key)
        unique_names.append(name)
        if len(unique_names) >= 30:
            break

    schemes: List[Dict[str, Any]] = []
    for name in unique_names:
        schemes.append(
            normalize_scheme_record(
                {
                    "scheme_name": name,
                    "scheme_type": "other",
                    "objective": "Heuristic extraction from crawled content. Verify from official source.",
                }
            )
        )
    return schemes


def build_ai_prompt(source_url: str, content: str) -> str:
    schema = {
        "schemes": [
            {
                "scheme_name": "",
                "short_name": "",
                "scheme_type": "income_support|crop_insurance|credit|machinery_subsidy|irrigation|organic_farming|market_linkage|infrastructure|other",
                "nodal_ministry": "",
                "launch_year": "",
                "objective": "",
                "target_beneficiaries": "",
                "eligibility_rules": [""],
                "exclusions": [""],
                "benefit_amount": "",
                "premium_or_cost": "",
                "requirements": [""],
                "documents_required": [""],
                "application_process": "",
                "application_deadline": "",
                "official_website": "",
                "helpline": "",
                "budget_allocation": "",
                "coverage_stats": "",
                "key_facts": [""],
                "keywords_hindi": [""],
            }
        ],
        "source_summary": "",
        "data_quality": "high|medium|low",
        "extraction_notes": "",
    }
    return (
        "You are building a production agriculture scheme database for India.\n"
        "Extract only explicit government scheme/program entries from this source.\n"
        "Exclude timestamps, press headline fragments, sentence snippets, generic headings, and table labels.\n"
        "Each item must be a concrete scheme name with verifiable details.\n"
        "Return STRICT JSON only, no markdown.\n"
        f"Required schema: {json.dumps(schema, ensure_ascii=False)}\n\n"
        f"SOURCE URL: {source_url}\n\n"
        "CONTENT START\n"
        f"{trim_text(content, 80_000)}\n"
        "CONTENT END"
    )


def ai_extract_schemes(
    content: str,
    source_url: str,
    api_key: str,
    preferred_model: str = "",
) -> Dict[str, Any]:
    if len(clean_text(content)) < 120:
        return {
            "schemes": [],
            "data_quality": "low",
            "model": "none",
            "extraction_notes": "content-too-short",
            "dropped_count": 0,
            "raw_count": 0,
        }

    if not api_key or not HAS_GENAI:
        fallback = heuristic_extract_schemes(content)
        filtered, dropped = filter_extracted_schemes(fallback)
        return {
            "schemes": filtered,
            "data_quality": "low",
            "model": "heuristic",
            "extraction_notes": "API key/model unavailable, used heuristic extraction",
            "dropped_count": dropped,
            "raw_count": len(fallback),
        }

    prompt = build_ai_prompt(source_url, content)
    client = google_genai.Client(api_key=api_key)

    model_order: List[str] = []
    if preferred_model:
        model_order.append(preferred_model)
    for model in MODEL_CANDIDATES:
        if model not in model_order:
            model_order.append(model)

    errors: List[str] = []
    for model in model_order:
        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    temperature=0.1,
                    max_output_tokens=4096,
                ),
            )
            raw = clean_text(response.text or "")
            parsed = parse_json_block(raw)
            raw_schemes = parsed.get("schemes", [])

            normalized: List[Dict[str, Any]] = []
            for item in raw_schemes:
                if isinstance(item, dict):
                    record = normalize_scheme_record(item)
                    if record.get("scheme_name"):
                        normalized.append(record)

            filtered, dropped = filter_extracted_schemes(normalized)

            if filtered:
                return {
                    "schemes": filtered,
                    "data_quality": clean_text(parsed.get("data_quality", "medium")).lower() or "medium",
                    "model": model,
                    "extraction_notes": clean_text(parsed.get("extraction_notes", "")),
                    "dropped_count": dropped,
                    "raw_count": len(normalized),
                }

            errors.append(f"{model}: empty-or-filtered-response")
        except Exception as exc:
            errors.append(f"{model}: {exc}")

    fallback = heuristic_extract_schemes(content)
    filtered, dropped = filter_extracted_schemes(fallback)
    return {
        "schemes": filtered,
        "data_quality": "low",
        "model": "heuristic",
        "extraction_notes": "; ".join(errors)[:1200],
        "dropped_count": dropped,
        "raw_count": len(fallback),
    }


class SchemeDatabase:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT UNIQUE NOT NULL,
                source_url TEXT NOT NULL,
                depth INTEGER NOT NULL,
                max_pages INTEGER NOT NULL,
                max_files INTEGER NOT NULL,
                js_enabled INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT DEFAULT 'running',
                stats_json TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                source_type TEXT NOT NULL,
                title TEXT DEFAULT '',
                status_code INTEGER DEFAULT 0,
                content_type TEXT DEFAULT '',
                local_path TEXT DEFAULT '',
                content_hash TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                extracted_text_chars INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                UNIQUE(run_id, url, source_type, local_path),
                FOREIGN KEY(run_id) REFERENCES runs(id)
            );

            CREATE TABLE IF NOT EXISTS schemes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_name TEXT UNIQUE NOT NULL,
                scheme_name TEXT NOT NULL,
                short_name TEXT DEFAULT '',
                scheme_type TEXT DEFAULT 'other',
                nodal_ministry TEXT DEFAULT '',
                launch_year TEXT DEFAULT '',
                objective TEXT DEFAULT '',
                target_beneficiaries TEXT DEFAULT '',
                benefit_amount TEXT DEFAULT '',
                premium_or_cost TEXT DEFAULT '',
                application_process TEXT DEFAULT '',
                application_deadline TEXT DEFAULT '',
                official_website TEXT DEFAULT '',
                helpline TEXT DEFAULT '',
                budget_allocation TEXT DEFAULT '',
                coverage_stats TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scheme_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                source_id INTEGER NOT NULL,
                scheme_id INTEGER NOT NULL,
                quality TEXT DEFAULT 'unknown',
                extraction_model TEXT DEFAULT '',
                raw_payload TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                UNIQUE(run_id, source_id, scheme_id),
                FOREIGN KEY(run_id) REFERENCES runs(id),
                FOREIGN KEY(source_id) REFERENCES sources(id),
                FOREIGN KEY(scheme_id) REFERENCES schemes(id)
            );

            CREATE TABLE IF NOT EXISTS scheme_eligibility (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scheme_id INTEGER NOT NULL,
                rule_text TEXT NOT NULL,
                UNIQUE(scheme_id, rule_text),
                FOREIGN KEY(scheme_id) REFERENCES schemes(id)
            );

            CREATE TABLE IF NOT EXISTS scheme_exclusions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scheme_id INTEGER NOT NULL,
                exclusion_text TEXT NOT NULL,
                UNIQUE(scheme_id, exclusion_text),
                FOREIGN KEY(scheme_id) REFERENCES schemes(id)
            );

            CREATE TABLE IF NOT EXISTS scheme_requirements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scheme_id INTEGER NOT NULL,
                requirement_text TEXT NOT NULL,
                UNIQUE(scheme_id, requirement_text),
                FOREIGN KEY(scheme_id) REFERENCES schemes(id)
            );

            CREATE TABLE IF NOT EXISTS scheme_benefits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scheme_id INTEGER NOT NULL,
                benefit_text TEXT NOT NULL,
                UNIQUE(scheme_id, benefit_text),
                FOREIGN KEY(scheme_id) REFERENCES schemes(id)
            );

            CREATE TABLE IF NOT EXISTS scheme_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scheme_id INTEGER NOT NULL,
                keyword_text TEXT NOT NULL,
                language TEXT DEFAULT 'en',
                UNIQUE(scheme_id, keyword_text, language),
                FOREIGN KEY(scheme_id) REFERENCES schemes(id)
            );

            CREATE TABLE IF NOT EXISTS curated_schemes (
                scheme_id INTEGER PRIMARY KEY,
                scheme_name TEXT NOT NULL,
                short_name TEXT DEFAULT '',
                scheme_type TEXT DEFAULT 'other',
                nodal_ministry TEXT DEFAULT '',
                launch_year TEXT DEFAULT '',
                benefit_amount TEXT DEFAULT '',
                official_website TEXT DEFAULT '',
                eligibility_count INTEGER DEFAULT 0,
                requirements_count INTEGER DEFAULT 0,
                benefits_count INTEGER DEFAULT 0,
                source_runs_count INTEGER DEFAULT 0,
                high_quality_hits INTEGER DEFAULT 0,
                medium_quality_hits INTEGER DEFAULT 0,
                low_quality_hits INTEGER DEFAULT 0,
                heuristic_hits INTEGER DEFAULT 0,
                confidence_score INTEGER DEFAULT 0,
                curated_flag INTEGER DEFAULT 0,
                rationale TEXT DEFAULT '',
                updated_at TEXT NOT NULL,
                FOREIGN KEY(scheme_id) REFERENCES schemes(id)
            );

            CREATE TABLE IF NOT EXISTS scheme_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scheme_id INTEGER NOT NULL,
                run_id INTEGER DEFAULT 0,
                source_id INTEGER DEFAULT 0,
                canonical_name TEXT NOT NULL,
                incoming_scheme_name TEXT NOT NULL,
                match_method TEXT NOT NULL,
                similarity_score INTEGER DEFAULT 0,
                snapshot_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(scheme_id) REFERENCES schemes(id),
                FOREIGN KEY(run_id) REFERENCES runs(id),
                FOREIGN KEY(source_id) REFERENCES sources(id)
            );

            CREATE TABLE IF NOT EXISTS master_schemes (
                scheme_id INTEGER PRIMARY KEY,
                canonical_name TEXT UNIQUE NOT NULL,
                scheme_name TEXT NOT NULL,
                short_name TEXT DEFAULT '',
                scheme_type TEXT DEFAULT 'other',
                nodal_ministry TEXT DEFAULT '',
                launch_year TEXT DEFAULT '',
                objective TEXT DEFAULT '',
                target_beneficiaries TEXT DEFAULT '',
                benefit_amount TEXT DEFAULT '',
                application_process TEXT DEFAULT '',
                official_website TEXT DEFAULT '',
                confidence_score INTEGER DEFAULT 0,
                curated_flag INTEGER DEFAULT 0,
                source_count INTEGER DEFAULT 0,
                media_count INTEGER DEFAULT 0,
                version_count INTEGER DEFAULT 0,
                eligibility_count INTEGER DEFAULT 0,
                requirements_count INTEGER DEFAULT 0,
                benefits_count INTEGER DEFAULT 0,
                search_blob TEXT DEFAULT '',
                updated_at TEXT NOT NULL,
                FOREIGN KEY(scheme_id) REFERENCES schemes(id)
            );

            CREATE INDEX IF NOT EXISTS idx_sources_run ON sources(run_id);
            CREATE INDEX IF NOT EXISTS idx_scheme_sources_run ON scheme_sources(run_id);
            CREATE INDEX IF NOT EXISTS idx_curated_score ON curated_schemes(confidence_score);
            CREATE INDEX IF NOT EXISTS idx_curated_flag_score ON curated_schemes(curated_flag, confidence_score);
            CREATE INDEX IF NOT EXISTS idx_scheme_versions_scheme ON scheme_versions(scheme_id, id DESC);
            CREATE INDEX IF NOT EXISTS idx_master_scheme_type ON master_schemes(scheme_type);
            CREATE INDEX IF NOT EXISTS idx_master_ministry ON master_schemes(nodal_ministry);
            CREATE INDEX IF NOT EXISTS idx_master_score ON master_schemes(confidence_score);
            """
        )
        self.conn.commit()

    def start_run(self, task_id: str, source_url: str, depth: int, max_pages: int, max_files: int, js_enabled: bool) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO runs (task_id, source_url, depth, max_pages, max_files, js_enabled, started_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (task_id, source_url, depth, max_pages, max_files, int(js_enabled), utc_now()),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str, stats: Dict[str, Any]) -> None:
        self.conn.execute(
            """
            UPDATE runs
               SET finished_at = ?,
                   status = ?,
                   stats_json = ?
             WHERE id = ?
            """,
            (utc_now(), status, json.dumps(stats, ensure_ascii=False), run_id),
        )
        self.conn.commit()

    def upsert_source(self, run_id: int, payload: Dict[str, Any]) -> int:
        url = clean_text(payload.get("url", ""))
        source_type = clean_text(payload.get("source_type", "unknown")) or "unknown"
        local_path = clean_text(payload.get("local_path", ""))
        content = payload.get("content", "") or ""
        content_hash = stable_hash(content) if content else ""

        self.conn.execute(
            """
            INSERT INTO sources (
                run_id, url, source_type, title, status_code, content_type,
                local_path, content_hash, notes, extracted_text_chars, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, url, source_type, local_path)
            DO UPDATE SET
                title = excluded.title,
                status_code = excluded.status_code,
                content_type = excluded.content_type,
                notes = excluded.notes,
                extracted_text_chars = excluded.extracted_text_chars,
                content_hash = excluded.content_hash
            """,
            (
                run_id,
                url,
                source_type,
                clean_text(payload.get("title", "")),
                int(payload.get("status_code", 0) or 0),
                clean_text(payload.get("content_type", "")),
                local_path,
                content_hash,
                clean_text(payload.get("notes", ""))[:2000],
                len(content),
                utc_now(),
            ),
        )

        row = self.conn.execute(
            """
            SELECT id FROM sources
             WHERE run_id = ? AND url = ? AND source_type = ? AND local_path = ?
            """,
            (run_id, url, source_type, local_path),
        ).fetchone()
        self.conn.commit()
        return int(row["id"])

    def _insert_list(self, table: str, column: str, scheme_id: int, values: List[str]) -> None:
        unique_values: List[str] = []
        seen: set[str] = set()
        for value in values:
            text = clean_text(value)
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            unique_values.append(text)

        for text in unique_values:
            self.conn.execute(
                f"INSERT OR IGNORE INTO {table} (scheme_id, {column}) VALUES (?, ?)",
                (scheme_id, text),
            )

    def _host_from_website(self, website: str) -> str:
        raw = clean_text(website)
        if not raw:
            return ""
        if not raw.lower().startswith(("http://", "https://")):
            raw = "https://" + raw
        try:
            return (urlparse(raw).netloc or "").lower().replace("www.", "").strip()
        except Exception:
            return ""

    def _scheme_merge_similarity(self, incoming: Dict[str, Any], existing: sqlite3.Row) -> int:
        incoming_name = canonicalize_name(clean_text(incoming.get("scheme_name", "")))
        existing_name = canonicalize_name(clean_text(existing["scheme_name"]))
        if not incoming_name or not existing_name:
            return 0

        score = 0.0
        name_ratio = SequenceMatcher(None, incoming_name, existing_name).ratio()
        score += name_ratio * 45.0

        incoming_ministry = canonicalize_name(clean_text(incoming.get("nodal_ministry", "")))
        existing_ministry = canonicalize_name(clean_text(existing["nodal_ministry"]))
        if incoming_ministry and existing_ministry:
            ministry_ratio = SequenceMatcher(None, incoming_ministry, existing_ministry).ratio()
            if ministry_ratio >= 0.92:
                score += 20.0
            elif ministry_ratio >= 0.75:
                score += 12.0
            elif incoming_ministry in existing_ministry or existing_ministry in incoming_ministry:
                score += 8.0

        incoming_type = clean_text(incoming.get("scheme_type", "")).lower()
        existing_type = clean_text(existing["scheme_type"]).lower()
        if incoming_type and existing_type and incoming_type != "other" and existing_type != "other":
            if incoming_type == existing_type:
                score += 15.0

        incoming_host = self._host_from_website(clean_text(incoming.get("official_website", "")))
        existing_host = self._host_from_website(clean_text(existing["official_website"]))
        if incoming_host and existing_host and incoming_host == existing_host:
            score += 10.0

        incoming_year = clean_text(incoming.get("launch_year", ""))
        existing_year = clean_text(existing["launch_year"])
        if incoming_year and existing_year and incoming_year == existing_year:
            score += 10.0

        incoming_short = canonicalize_name(clean_text(incoming.get("short_name", "")))
        existing_short = canonicalize_name(clean_text(existing["short_name"]))
        if incoming_short and existing_short and incoming_short == existing_short:
            score += 8.0

        return int(max(0, min(100, round(score))))

    def _find_best_scheme_match(self, scheme: Dict[str, Any]) -> Tuple[Optional[sqlite3.Row], int]:
        scheme_name = clean_text(scheme.get("scheme_name", ""))
        if not scheme_name:
            return None, 0

        canonical = canonicalize_name(scheme_name)
        token = canonical.split(" ")[0] if canonical else ""
        candidates: List[sqlite3.Row] = []

        if token and len(token) >= 3:
            candidates = self.conn.execute(
                """
                SELECT * FROM schemes
                WHERE canonical_name LIKE ? OR lower(scheme_name) LIKE ?
                LIMIT 500
                """,
                (f"%{token}%", f"%{token}%"),
            ).fetchall()

        if not candidates:
            candidates = self.conn.execute("SELECT * FROM schemes").fetchall()

        best_row: Optional[sqlite3.Row] = None
        best_score = 0
        for row in candidates:
            score = self._scheme_merge_similarity(scheme, row)
            if score > best_score:
                best_score = score
                best_row = row
        return best_row, best_score

    def _record_scheme_version(
        self,
        scheme_id: int,
        scheme: Dict[str, Any],
        run_id: int,
        source_id: int,
        canonical_name: str,
        match_method: str,
        similarity_score: int,
    ) -> None:
        snapshot: Dict[str, Any] = {}
        for key in SCHEME_KEYS:
            if key in LIST_FIELDS:
                snapshot[key] = coerce_list(scheme.get(key))
            else:
                snapshot[key] = clean_text(scheme.get(key, ""))

        self.conn.execute(
            """
            INSERT INTO scheme_versions (
                scheme_id, run_id, source_id, canonical_name, incoming_scheme_name,
                match_method, similarity_score, snapshot_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(scheme_id),
                int(run_id or 0),
                int(source_id or 0),
                clean_text(canonical_name),
                clean_text(scheme.get("scheme_name", "")),
                clean_text(match_method) or "unknown",
                int(similarity_score or 0),
                json.dumps(snapshot, ensure_ascii=False),
                utc_now(),
            ),
        )

    def upsert_scheme(self, scheme: Dict[str, Any], run_id: int = 0, source_id: int = 0) -> Optional[int]:
        scheme_name = clean_text(scheme.get("scheme_name", ""))
        if not scheme_name:
            return None

        canonical = canonicalize_name(scheme_name)
        now = utc_now()
        existing = self.conn.execute(
            "SELECT * FROM schemes WHERE canonical_name = ?",
            (canonical,),
        ).fetchone()
        match_method = "canonical_exact"
        similarity_score = 100

        if not existing:
            candidate, score = self._find_best_scheme_match(scheme)
            if candidate is not None and score >= MASTER_FUZZY_MATCH_THRESHOLD:
                existing = candidate
                match_method = "fuzzy_merge"
                similarity_score = score
            else:
                match_method = "new_insert"
                similarity_score = max(0, score)

        columns = [
            "scheme_name",
            "short_name",
            "scheme_type",
            "nodal_ministry",
            "launch_year",
            "objective",
            "target_beneficiaries",
            "benefit_amount",
            "premium_or_cost",
            "application_process",
            "application_deadline",
            "official_website",
            "helpline",
            "budget_allocation",
            "coverage_stats",
        ]

        if existing:
            merged: Dict[str, str] = {}
            for col in columns:
                new_val = clean_text(scheme.get(col, ""))
                old_val = clean_text(existing[col])
                merged[col] = new_val or old_val

            self.conn.execute(
                """
                UPDATE schemes
                   SET scheme_name = ?, short_name = ?, scheme_type = ?, nodal_ministry = ?,
                       launch_year = ?, objective = ?, target_beneficiaries = ?, benefit_amount = ?,
                       premium_or_cost = ?, application_process = ?, application_deadline = ?,
                       official_website = ?, helpline = ?, budget_allocation = ?, coverage_stats = ?,
                       updated_at = ?
                 WHERE id = ?
                """,
                (
                    merged["scheme_name"],
                    merged["short_name"],
                    merged["scheme_type"] or "other",
                    merged["nodal_ministry"],
                    merged["launch_year"],
                    merged["objective"],
                    merged["target_beneficiaries"],
                    merged["benefit_amount"],
                    merged["premium_or_cost"],
                    merged["application_process"],
                    merged["application_deadline"],
                    merged["official_website"],
                    merged["helpline"],
                    merged["budget_allocation"],
                    merged["coverage_stats"],
                    now,
                    int(existing["id"]),
                ),
            )
            scheme_id = int(existing["id"])
        else:
            cur = self.conn.cursor()
            cur.execute(
                """
                INSERT INTO schemes (
                    canonical_name, scheme_name, short_name, scheme_type, nodal_ministry,
                    launch_year, objective, target_beneficiaries, benefit_amount, premium_or_cost,
                    application_process, application_deadline, official_website, helpline,
                    budget_allocation, coverage_stats, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    canonical,
                    scheme_name,
                    clean_text(scheme.get("short_name", "")),
                    clean_text(scheme.get("scheme_type", "other")) or "other",
                    clean_text(scheme.get("nodal_ministry", "")),
                    clean_text(scheme.get("launch_year", "")),
                    clean_text(scheme.get("objective", "")),
                    clean_text(scheme.get("target_beneficiaries", "")),
                    clean_text(scheme.get("benefit_amount", "")),
                    clean_text(scheme.get("premium_or_cost", "")),
                    clean_text(scheme.get("application_process", "")),
                    clean_text(scheme.get("application_deadline", "")),
                    clean_text(scheme.get("official_website", "")),
                    clean_text(scheme.get("helpline", "")),
                    clean_text(scheme.get("budget_allocation", "")),
                    clean_text(scheme.get("coverage_stats", "")),
                    now,
                    now,
                ),
            )
            scheme_id = int(cur.lastrowid)

        self._insert_list("scheme_eligibility", "rule_text", scheme_id, coerce_list(scheme.get("eligibility_rules")))
        self._insert_list("scheme_exclusions", "exclusion_text", scheme_id, coerce_list(scheme.get("exclusions")))

        requirements = coerce_list(scheme.get("requirements")) + coerce_list(scheme.get("documents_required"))
        self._insert_list("scheme_requirements", "requirement_text", scheme_id, requirements)

        benefit_lines = []
        if clean_text(scheme.get("benefit_amount", "")):
            benefit_lines.append(clean_text(scheme.get("benefit_amount", "")))
        benefit_lines.extend(coerce_list(scheme.get("key_facts")))
        self._insert_list("scheme_benefits", "benefit_text", scheme_id, benefit_lines)

        seen_keywords: set[Tuple[str, str]] = set()
        for keyword in coerce_list(scheme.get("keywords_hindi")):
            language = "hi" if re.search(r"[\u0900-\u097F]", keyword) else "en"
            key = (keyword.lower(), language)
            if key in seen_keywords:
                continue
            seen_keywords.add(key)
            self.conn.execute(
                "INSERT OR IGNORE INTO scheme_keywords (scheme_id, keyword_text, language) VALUES (?, ?, ?)",
                (scheme_id, keyword, language),
            )

        self._record_scheme_version(
            scheme_id=scheme_id,
            scheme=scheme,
            run_id=run_id,
            source_id=source_id,
            canonical_name=canonical,
            match_method=match_method,
            similarity_score=similarity_score,
        )

        self.conn.commit()
        return scheme_id

    def link_scheme_source(
        self,
        run_id: int,
        source_id: int,
        scheme_id: int,
        quality: str,
        model: str,
        raw_payload: Dict[str, Any],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO scheme_sources (
                run_id, source_id, scheme_id, quality, extraction_model, raw_payload, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, source_id, scheme_id)
            DO UPDATE SET
                quality = excluded.quality,
                extraction_model = excluded.extraction_model,
                raw_payload = excluded.raw_payload
            """,
            (
                run_id,
                source_id,
                scheme_id,
                clean_text(quality) or "unknown",
                clean_text(model),
                json.dumps(raw_payload, ensure_ascii=False),
                utc_now(),
            ),
        )
        self.conn.commit()

    def _list_values(self, table: str, column: str, scheme_id: int) -> List[str]:
        rows = self.conn.execute(
            f"SELECT {column} FROM {table} WHERE scheme_id = ? ORDER BY id",
            (scheme_id,),
        ).fetchall()
        return [clean_text(row[0]) for row in rows if clean_text(row[0])]

    def get_run_schemes(self, run_id: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT s.*
              FROM schemes s
              JOIN scheme_sources ss ON ss.scheme_id = s.id
             WHERE ss.run_id = ?
             ORDER BY s.scheme_name COLLATE NOCASE
            """,
            (run_id,),
        ).fetchall()

        schemes: List[Dict[str, Any]] = []
        for row in rows:
            scheme_id = int(row["id"])
            schemes.append(
                {
                    "id": scheme_id,
                    "scheme_name": row["scheme_name"],
                    "short_name": row["short_name"],
                    "scheme_type": row["scheme_type"],
                    "nodal_ministry": row["nodal_ministry"],
                    "launch_year": row["launch_year"],
                    "objective": row["objective"],
                    "target_beneficiaries": row["target_beneficiaries"],
                    "benefit_amount": row["benefit_amount"],
                    "premium_or_cost": row["premium_or_cost"],
                    "application_process": row["application_process"],
                    "application_deadline": row["application_deadline"],
                    "official_website": row["official_website"],
                    "helpline": row["helpline"],
                    "budget_allocation": row["budget_allocation"],
                    "coverage_stats": row["coverage_stats"],
                    "eligibility_rules": self._list_values("scheme_eligibility", "rule_text", scheme_id),
                    "exclusions": self._list_values("scheme_exclusions", "exclusion_text", scheme_id),
                    "requirements": self._list_values("scheme_requirements", "requirement_text", scheme_id),
                    "key_facts": self._list_values("scheme_benefits", "benefit_text", scheme_id),
                    "keywords_hindi": self._list_values("scheme_keywords", "keyword_text", scheme_id),
                }
            )
        return schemes

    def export_master_csv(self, csv_path: Path) -> None:
        self.refresh_master_dataset()
        rows = self.conn.execute(
            """
            SELECT
                scheme_name,
                short_name,
                scheme_type,
                nodal_ministry,
                launch_year,
                benefit_amount,
                official_website,
                confidence_score,
                curated_flag,
                source_count,
                media_count,
                version_count,
                eligibility_count,
                requirements_count,
                benefits_count,
                updated_at
            FROM master_schemes
            ORDER BY confidence_score DESC, source_count DESC, scheme_name COLLATE NOCASE
            """
        ).fetchall()

        with open(csv_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "scheme_name",
                    "short_name",
                    "scheme_type",
                    "nodal_ministry",
                    "launch_year",
                    "benefit_amount",
                    "official_website",
                    "confidence_score",
                    "curated_flag",
                    "source_count",
                    "media_count",
                    "version_count",
                    "eligibility_count",
                    "requirements_count",
                    "benefits_count",
                    "updated_at",
                ]
            )
            for row in rows:
                writer.writerow([row[col] for col in row.keys()])

    def refresh_master_dataset(self) -> int:
        rows = self.conn.execute(
            """
            SELECT
                s.id,
                s.canonical_name,
                s.scheme_name,
                s.short_name,
                s.scheme_type,
                s.nodal_ministry,
                s.launch_year,
                s.objective,
                s.target_beneficiaries,
                s.benefit_amount,
                s.application_process,
                s.official_website,
                s.updated_at,
                COALESCE(cs.confidence_score, 0) AS confidence_score,
                COALESCE(cs.curated_flag, 0) AS curated_flag,
                (SELECT COUNT(*) FROM scheme_sources ss WHERE ss.scheme_id = s.id) AS source_count,
                (
                    SELECT COUNT(*)
                    FROM scheme_sources ss
                    JOIN sources src ON src.id = ss.source_id
                    WHERE ss.scheme_id = s.id AND trim(COALESCE(src.local_path, '')) != ''
                ) AS media_count,
                (SELECT COUNT(*) FROM scheme_versions sv WHERE sv.scheme_id = s.id) AS version_count,
                (SELECT COUNT(*) FROM scheme_eligibility e WHERE e.scheme_id = s.id) AS eligibility_count,
                (SELECT COUNT(*) FROM scheme_requirements r WHERE r.scheme_id = s.id) AS requirements_count,
                (SELECT COUNT(*) FROM scheme_benefits b WHERE b.scheme_id = s.id) AS benefits_count
            FROM schemes s
            LEFT JOIN curated_schemes cs ON cs.scheme_id = s.id
            ORDER BY s.id
            """
        ).fetchall()

        for row in rows:
            search_blob = " | ".join(
                [
                    clean_text(row["scheme_name"]),
                    clean_text(row["short_name"]),
                    clean_text(row["scheme_type"]),
                    clean_text(row["nodal_ministry"]),
                    clean_text(row["objective"]),
                    clean_text(row["target_beneficiaries"]),
                    clean_text(row["benefit_amount"]),
                    clean_text(row["application_process"]),
                ]
            )[:6000]

            self.conn.execute(
                """
                INSERT INTO master_schemes (
                    scheme_id, canonical_name, scheme_name, short_name, scheme_type,
                    nodal_ministry, launch_year, objective, target_beneficiaries,
                    benefit_amount, application_process, official_website,
                    confidence_score, curated_flag, source_count, media_count,
                    version_count, eligibility_count, requirements_count, benefits_count,
                    search_blob, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scheme_id)
                DO UPDATE SET
                    canonical_name = excluded.canonical_name,
                    scheme_name = excluded.scheme_name,
                    short_name = excluded.short_name,
                    scheme_type = excluded.scheme_type,
                    nodal_ministry = excluded.nodal_ministry,
                    launch_year = excluded.launch_year,
                    objective = excluded.objective,
                    target_beneficiaries = excluded.target_beneficiaries,
                    benefit_amount = excluded.benefit_amount,
                    application_process = excluded.application_process,
                    official_website = excluded.official_website,
                    confidence_score = excluded.confidence_score,
                    curated_flag = excluded.curated_flag,
                    source_count = excluded.source_count,
                    media_count = excluded.media_count,
                    version_count = excluded.version_count,
                    eligibility_count = excluded.eligibility_count,
                    requirements_count = excluded.requirements_count,
                    benefits_count = excluded.benefits_count,
                    search_blob = excluded.search_blob,
                    updated_at = excluded.updated_at
                """,
                (
                    int(row["id"]),
                    clean_text(row["canonical_name"]),
                    clean_text(row["scheme_name"]),
                    clean_text(row["short_name"]),
                    clean_text(row["scheme_type"]),
                    clean_text(row["nodal_ministry"]),
                    clean_text(row["launch_year"]),
                    clean_text(row["objective"]),
                    clean_text(row["target_beneficiaries"]),
                    clean_text(row["benefit_amount"]),
                    clean_text(row["application_process"]),
                    clean_text(row["official_website"]),
                    int(row["confidence_score"] or 0),
                    int(row["curated_flag"] or 0),
                    int(row["source_count"] or 0),
                    int(row["media_count"] or 0),
                    int(row["version_count"] or 0),
                    int(row["eligibility_count"] or 0),
                    int(row["requirements_count"] or 0),
                    int(row["benefits_count"] or 0),
                    search_blob,
                    clean_text(row["updated_at"]) or utc_now(),
                ),
            )

        self.conn.execute("DELETE FROM master_schemes WHERE scheme_id NOT IN (SELECT id FROM schemes)")
        self.conn.commit()
        row = self.conn.execute("SELECT COUNT(*) FROM master_schemes").fetchone()
        return int(row[0]) if row else 0

    def _noise_penalty_for_name(self, name: str) -> int:
        lower = clean_text(name).lower()
        penalty = 0

        if re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", lower):
            penalty += 20
        if re.search(r"\b\d{1,2}:\d{2}\b", lower):
            penalty += 15
        if re.search(r"\b\d{2,4}\b", lower) and len(lower.split()) <= 4:
            penalty += 6

        for token in [
            "refer here",
            "for scheme code",
            "implementation period",
            "chapter",
            "annexure",
            "document",
            "committee",
            "program management unit",
            "guidelines",
            "cid:",
        ]:
            if token in lower:
                penalty += 8

        if len(lower.split()) > 14:
            penalty += 6
        if lower.endswith((":", ";", ".")):
            penalty += 5
        return min(penalty, 35)

    def _confidence_for_scheme(self, row: sqlite3.Row, counts: Dict[str, int]) -> Tuple[int, int, str]:
        score = 0
        reasons: List[str] = []

        scheme_type = clean_text(row["scheme_type"])
        objective = clean_text(row["objective"])
        ministry = clean_text(row["nodal_ministry"])
        benefit = clean_text(row["benefit_amount"])
        website = clean_text(row["official_website"])
        beneficiaries = clean_text(row["target_beneficiaries"])
        apply_process = clean_text(row["application_process"])
        name = clean_text(row["scheme_name"])

        if scheme_type and scheme_type != "other":
            score += 10
            reasons.append("typed")
        if ministry:
            score += 15
            reasons.append("ministry")
        if benefit:
            score += 12
            reasons.append("benefit")
        if website.startswith("http"):
            score += 12
            reasons.append("website")
        if beneficiaries:
            score += 6
        if apply_process:
            score += 6

        if objective and len(objective) >= 35 and "heuristic extraction from crawled content" not in objective.lower():
            score += 8
            reasons.append("objective")

        if counts["eligibility_count"] > 0:
            score += 8
            reasons.append("eligibility")
        if counts["requirements_count"] > 0:
            score += 8
            reasons.append("requirements")
        if counts["benefits_count"] > 0:
            score += 5

        if counts["high_quality_hits"] > 0:
            score += 14
            reasons.append("high-quality-source")
        elif counts["medium_quality_hits"] > 0:
            score += 8

        if counts["source_runs_count"] > 1:
            score += 5

        if counts["heuristic_hits"] > 0 and counts["high_quality_hits"] == 0 and counts["medium_quality_hits"] == 0:
            score -= 10

        if not is_probable_scheme_name(name):
            score -= 24
            reasons.append("name-pattern-penalty")
        elif not is_compact_scheme_title(name):
            score -= 6

        noise_penalty = self._noise_penalty_for_name(name)
        if noise_penalty:
            score -= noise_penalty
            reasons.append(f"noise-{noise_penalty}")

        score = max(0, min(100, score))

        has_detail = bool(
            ministry
            or benefit
            or website
            or beneficiaries
            or apply_process
            or counts["eligibility_count"] > 0
            or counts["requirements_count"] > 0
            or counts["high_quality_hits"] > 0
            or counts["medium_quality_hits"] > 0
        )

        curated_flag = 1 if score >= 45 and has_detail and noise_penalty < 20 else 0
        rationale = ", ".join(reasons[:8]) if reasons else "insufficient-signals"
        return score, curated_flag, rationale

    def refresh_curated_schemes(self) -> int:
        rows = self.conn.execute(
            """
            SELECT
                s.*,
                (SELECT COUNT(*) FROM scheme_eligibility e WHERE e.scheme_id = s.id) AS eligibility_count,
                (SELECT COUNT(*) FROM scheme_requirements r WHERE r.scheme_id = s.id) AS requirements_count,
                (SELECT COUNT(*) FROM scheme_benefits b WHERE b.scheme_id = s.id) AS benefits_count,
                (SELECT COUNT(DISTINCT ss.run_id) FROM scheme_sources ss WHERE ss.scheme_id = s.id) AS source_runs_count,
                (SELECT COUNT(*) FROM scheme_sources ss WHERE ss.scheme_id = s.id AND lower(ss.quality) = 'high') AS high_quality_hits,
                (SELECT COUNT(*) FROM scheme_sources ss WHERE ss.scheme_id = s.id AND lower(ss.quality) = 'medium') AS medium_quality_hits,
                (SELECT COUNT(*) FROM scheme_sources ss WHERE ss.scheme_id = s.id AND lower(ss.quality) = 'low') AS low_quality_hits,
                (SELECT COUNT(*) FROM scheme_sources ss WHERE ss.scheme_id = s.id AND lower(ss.extraction_model) = 'heuristic') AS heuristic_hits
            FROM schemes s
            ORDER BY s.id
            """
        ).fetchall()

        for row in rows:
            counts = {
                "eligibility_count": int(row["eligibility_count"] or 0),
                "requirements_count": int(row["requirements_count"] or 0),
                "benefits_count": int(row["benefits_count"] or 0),
                "source_runs_count": int(row["source_runs_count"] or 0),
                "high_quality_hits": int(row["high_quality_hits"] or 0),
                "medium_quality_hits": int(row["medium_quality_hits"] or 0),
                "low_quality_hits": int(row["low_quality_hits"] or 0),
                "heuristic_hits": int(row["heuristic_hits"] or 0),
            }

            confidence_score, curated_flag, rationale = self._confidence_for_scheme(row, counts)

            self.conn.execute(
                """
                INSERT INTO curated_schemes (
                    scheme_id, scheme_name, short_name, scheme_type, nodal_ministry, launch_year,
                    benefit_amount, official_website, eligibility_count, requirements_count, benefits_count,
                    source_runs_count, high_quality_hits, medium_quality_hits, low_quality_hits,
                    heuristic_hits, confidence_score, curated_flag, rationale, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scheme_id)
                DO UPDATE SET
                    scheme_name = excluded.scheme_name,
                    short_name = excluded.short_name,
                    scheme_type = excluded.scheme_type,
                    nodal_ministry = excluded.nodal_ministry,
                    launch_year = excluded.launch_year,
                    benefit_amount = excluded.benefit_amount,
                    official_website = excluded.official_website,
                    eligibility_count = excluded.eligibility_count,
                    requirements_count = excluded.requirements_count,
                    benefits_count = excluded.benefits_count,
                    source_runs_count = excluded.source_runs_count,
                    high_quality_hits = excluded.high_quality_hits,
                    medium_quality_hits = excluded.medium_quality_hits,
                    low_quality_hits = excluded.low_quality_hits,
                    heuristic_hits = excluded.heuristic_hits,
                    confidence_score = excluded.confidence_score,
                    curated_flag = excluded.curated_flag,
                    rationale = excluded.rationale,
                    updated_at = excluded.updated_at
                """,
                (
                    int(row["id"]),
                    clean_text(row["scheme_name"]),
                    clean_text(row["short_name"]),
                    clean_text(row["scheme_type"]),
                    clean_text(row["nodal_ministry"]),
                    clean_text(row["launch_year"]),
                    clean_text(row["benefit_amount"]),
                    clean_text(row["official_website"]),
                    counts["eligibility_count"],
                    counts["requirements_count"],
                    counts["benefits_count"],
                    counts["source_runs_count"],
                    counts["high_quality_hits"],
                    counts["medium_quality_hits"],
                    counts["low_quality_hits"],
                    counts["heuristic_hits"],
                    confidence_score,
                    curated_flag,
                    rationale,
                    utc_now(),
                ),
            )

        self.conn.commit()
        row = self.conn.execute("SELECT COUNT(*) FROM curated_schemes WHERE curated_flag = 1").fetchone()
        return int(row[0]) if row else 0

    def export_curated_csv(self, csv_path: Path, min_score: int = 45) -> None:
        rows = self.conn.execute(
            """
            SELECT
                scheme_name,
                short_name,
                scheme_type,
                nodal_ministry,
                launch_year,
                benefit_amount,
                official_website,
                eligibility_count,
                requirements_count,
                source_runs_count,
                high_quality_hits,
                medium_quality_hits,
                confidence_score,
                rationale,
                updated_at
            FROM curated_schemes
            WHERE curated_flag = 1 AND confidence_score >= ?
            ORDER BY confidence_score DESC, source_runs_count DESC, scheme_name COLLATE NOCASE
            """,
            (int(min_score),),
        ).fetchall()

        with open(csv_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "scheme_name",
                    "short_name",
                    "scheme_type",
                    "nodal_ministry",
                    "launch_year",
                    "benefit_amount",
                    "official_website",
                    "eligibility_count",
                    "requirements_count",
                    "source_runs_count",
                    "high_quality_hits",
                    "medium_quality_hits",
                    "confidence_score",
                    "rationale",
                    "updated_at",
                ]
            )
            for row in rows:
                writer.writerow([row[col] for col in row.keys()])

    def search_scheme_index(
        self,
        query_text: str = "",
        ministry: str = "",
        scheme_type: str = "",
        min_score: int = 0,
        limit: int = 25,
        curated_only: bool = True,
    ) -> List[Dict[str, Any]]:
        where_parts: List[str] = []
        params: List[Any] = []

        if curated_only:
            where_parts.append("curated_flag = 1")

        where_parts.append("confidence_score >= ?")
        params.append(int(min_score))

        q = clean_text(query_text).lower()
        if q:
            like = f"%{q}%"
            where_parts.append(
                "(lower(scheme_name) LIKE ? OR lower(short_name) LIKE ? OR lower(nodal_ministry) LIKE ? OR lower(benefit_amount) LIKE ? OR lower(rationale) LIKE ?)"
            )
            params.extend([like, like, like, like, like])

        ministry_q = clean_text(ministry).lower()
        if ministry_q:
            where_parts.append("lower(nodal_ministry) LIKE ?")
            params.append(f"%{ministry_q}%")

        type_q = clean_text(scheme_type).lower()
        if type_q:
            where_parts.append("lower(scheme_type) LIKE ?")
            params.append(f"%{type_q}%")

        where_sql = " AND ".join(where_parts) if where_parts else "1=1"
        safe_limit = max(1, min(int(limit), 200))

        rows = self.conn.execute(
            f"""
            SELECT
                scheme_id,
                scheme_name,
                short_name,
                scheme_type,
                nodal_ministry,
                benefit_amount,
                official_website,
                confidence_score,
                source_runs_count,
                high_quality_hits,
                medium_quality_hits,
                heuristic_hits,
                rationale,
                updated_at
            FROM curated_schemes
            WHERE {where_sql}
            ORDER BY confidence_score DESC, source_runs_count DESC, scheme_name COLLATE NOCASE
            LIMIT ?
            """,
            (*params, safe_limit),
        ).fetchall()
        return [dict(row) for row in rows]


def write_exports(
    task_id: str,
    source_url: str,
    run_id: int,
    db_path: Path,
    schemes: List[Dict[str, Any]],
    source_payloads: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> Tuple[Path, Path, Path]:
    json_path = DATA_DIR / f"schemes_{task_id}.json"
    csv_path = DATA_DIR / f"schemes_{task_id}.csv"
    md_path = REPORT_DIR / f"report_{task_id}.md"

    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "task_id": task_id,
                "run_id": run_id,
                "source_url": source_url,
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "database": str(db_path),
                "stats": stats,
                "schemes": schemes,
            },
            handle,
            indent=2,
            ensure_ascii=False,
        )

    with open(csv_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "scheme_name",
                "short_name",
                "scheme_type",
                "nodal_ministry",
                "launch_year",
                "benefit_amount",
                "eligibility_rules",
                "requirements",
                "official_website",
            ]
        )
        for scheme in schemes:
            writer.writerow(
                [
                    scheme.get("scheme_name", ""),
                    scheme.get("short_name", ""),
                    scheme.get("scheme_type", ""),
                    scheme.get("nodal_ministry", ""),
                    scheme.get("launch_year", ""),
                    scheme.get("benefit_amount", ""),
                    " | ".join(scheme.get("eligibility_rules", [])),
                    " | ".join(scheme.get("requirements", [])),
                    scheme.get("official_website", ""),
                ]
            )

    lines: List[str] = [
        "# Krishi-Setu Advanced Extraction Report",
        "",
        f"**Task ID:** `{task_id}`  ",
        f"**Run ID:** `{run_id}`  ",
        f"**Source URL:** [{source_url}]({source_url})  ",
        f"**Database:** `{db_path}`  ",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        "",
        "---",
        "## Pipeline Summary",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Pages Crawled | {stats.get('pages_crawled', 0)} |",
        f"| Files Discovered | {stats.get('files_discovered', 0)} |",
        f"| Files Downloaded | {stats.get('files_downloaded', 0)} |",
        f"| Files Cached | {stats.get('files_cached', 0)} |",
        f"| Source Payloads | {stats.get('source_payloads', 0)} |",
        f"| Raw Scheme Candidates | {stats.get('raw_scheme_candidates', 0)} |",
        f"| Rejected Noisy Candidates | {stats.get('rejected_noisy_candidates', 0)} |",
        f"| Schemes Extracted This Run | {len(schemes)} |",
        f"| Curated Master Rows | {stats.get('curated_master_rows', '-')} |",
        f"| Extraction Quality | {stats.get('quality_breakdown', '-')} |",
        f"| Elapsed | {stats.get('elapsed', '-') } |",
        "",
        "---",
        f"## Scheme Table ({len(schemes)})",
        "",
        "| Scheme | Type | Ministry | Benefit | Eligibility Count | Requirements Count |",
        "|---|---|---|---|---:|---:|",
    ]

    for scheme in schemes:
        lines.append(
            "| "
            + f"{scheme.get('scheme_name', 'Unknown')}"
            + " | "
            + f"{scheme.get('scheme_type', '')}"
            + " | "
            + f"{scheme.get('nodal_ministry', '')[:70]}"
            + " | "
            + f"{scheme.get('benefit_amount', '')[:70]}"
            + " | "
            + str(len(scheme.get("eligibility_rules", [])))
            + " | "
            + str(len(scheme.get("requirements", [])))
            + " |"
        )

    lines.extend([
        "",
        "---",
        "## Detailed Schemes",
    ])

    for idx, scheme in enumerate(schemes, start=1):
        lines.extend(
            [
                "",
                f"### {idx}. {scheme.get('scheme_name', 'Unknown Scheme')}",
                f"- Short Name: {scheme.get('short_name', '-') or '-'}",
                f"- Type: {scheme.get('scheme_type', '-') or '-'}",
                f"- Ministry: {scheme.get('nodal_ministry', '-') or '-'}",
                f"- Launch Year: {scheme.get('launch_year', '-') or '-'}",
                f"- Objective: {scheme.get('objective', '-') or '-'}",
                f"- Beneficiaries: {scheme.get('target_beneficiaries', '-') or '-'}",
                f"- Benefit: {scheme.get('benefit_amount', '-') or '-'}",
                f"- Premium/Cost: {scheme.get('premium_or_cost', '-') or '-'}",
                f"- Apply Process: {scheme.get('application_process', '-') or '-'}",
                f"- Deadline: {scheme.get('application_deadline', '-') or '-'}",
                f"- Website: {scheme.get('official_website', '-') or '-'}",
                f"- Helpline: {scheme.get('helpline', '-') or '-'}",
            ]
        )

        if scheme.get("eligibility_rules"):
            lines.append("- Eligibility Rules:")
            for rule in scheme["eligibility_rules"][:20]:
                lines.append(f"  - {rule}")
        if scheme.get("requirements"):
            lines.append("- Requirements:")
            for req in scheme["requirements"][:20]:
                lines.append(f"  - {req}")
        if scheme.get("exclusions"):
            lines.append("- Exclusions:")
            for ex in scheme["exclusions"][:20]:
                lines.append(f"  - {ex}")

    lines.extend([
        "",
        "---",
        "## Source Inventory",
        "",
        "| Type | URL | Local Path | Notes |",
        "|---|---|---|---|",
    ])

    for payload in source_payloads:
        lines.append(
            "| "
            + f"{payload.get('source_type', '')}"
            + " | "
            + f"{payload.get('url', '')}"
            + " | "
            + f"{payload.get('local_path', '')}"
            + " | "
            + f"{clean_text(payload.get('notes', ''))[:120]}"
            + " |"
        )

    lines.extend(
        [
            "",
            "---",
            "Krishi-Setu upgraded scraper and organizer",
        ]
    )

    with open(md_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))

    return md_path, json_path, csv_path


def print_console_summary(
    task_id: str,
    stats: Dict[str, Any],
    db_path: Path,
    report_path: Path,
    json_path: Path,
    csv_path: Path,
    schemes: List[Dict[str, Any]],
) -> None:
    console.print(
        Panel(
            f"[bold green]Run Complete[/]\n\n"
            f"Task: {task_id}\n"
            f"Pages crawled: {stats.get('pages_crawled', 0)}\n"
            f"Files downloaded: {stats.get('files_downloaded', 0)} / {stats.get('files_discovered', 0)}\n"
            f"Rejected noisy candidates: {stats.get('rejected_noisy_candidates', 0)}\n"
            f"Schemes in this run: {len(schemes)}\n"
            f"Curated master rows: {stats.get('curated_master_rows', '-')}\n"
            f"Quality: {stats.get('quality_breakdown', '-')}\n"
            f"Elapsed: {stats.get('elapsed', '-')}\n\n"
            f"DB: {db_path}\n"
            f"Report: {report_path}\n"
            f"JSON: {json_path}\n"
            f"CSV: {csv_path}"
        )
    )

    if schemes:
        table = Table(title="Extracted Schemes", header_style="bold green", show_lines=True)
        table.add_column("#", width=4)
        table.add_column("Scheme", style="cyan", max_width=42)
        table.add_column("Type", max_width=18)
        table.add_column("Ministry", max_width=30)
        table.add_column("Benefit", max_width=32)

        for idx, scheme in enumerate(schemes[:20], start=1):
            table.add_row(
                str(idx),
                clean_text(scheme.get("scheme_name", ""))[:42],
                clean_text(scheme.get("scheme_type", ""))[:18],
                clean_text(scheme.get("nodal_ministry", ""))[:30],
                clean_text(scheme.get("benefit_amount", ""))[:32],
            )
        console.print(table)


MOCK_SCHEMES: List[Dict[str, Any]] = [
    {
        "scheme_name": "Pradhan Mantri Kisan Samman Nidhi",
        "short_name": "PM-KISAN",
        "scheme_type": "income_support",
        "nodal_ministry": "Ministry of Agriculture and Farmers Welfare",
        "launch_year": "2019",
        "objective": "Income support to landholding farmer families.",
        "target_beneficiaries": "All landholding farmer families",
        "eligibility_rules": [
            "Valid Aadhaar and bank linkage",
            "Landholding farmer family",
        ],
        "exclusions": ["Institutional landholders", "Tax payers in exclusion category"],
        "benefit_amount": "INR 6,000 per year via DBT",
        "requirements": ["Aadhaar", "Land record", "Bank details"],
        "documents_required": ["Aadhaar", "Land record", "Bank passbook"],
        "application_process": "Apply online or via CSC",
        "official_website": "https://pmkisan.gov.in",
        "key_facts": ["3 installments per year"],
    },
    {
        "scheme_name": "Pradhan Mantri Fasal Bima Yojana",
        "short_name": "PMFBY",
        "scheme_type": "crop_insurance",
        "nodal_ministry": "Ministry of Agriculture and Farmers Welfare",
        "launch_year": "2016",
        "objective": "Crop insurance support against losses.",
        "target_beneficiaries": "Farmers growing notified crops",
        "eligibility_rules": ["Notified crop and notified area"],
        "benefit_amount": "Insurance claim as per loss assessment",
        "premium_or_cost": "Farmer premium rates vary by crop season",
        "requirements": ["Aadhaar", "Land/lease details", "Bank details"],
        "application_process": "Apply via portal, bank, or CSC",
        "official_website": "https://pmfby.gov.in",
    },
    {
        "scheme_name": "Kisan Credit Card",
        "short_name": "KCC",
        "scheme_type": "credit",
        "nodal_ministry": "Ministry of Agriculture / NABARD / Banks",
        "objective": "Affordable short-term agricultural credit.",
        "target_beneficiaries": "Farmers, tenant farmers, and allied activities",
        "benefit_amount": "Short-term credit line based on scale of finance",
        "requirements": ["ID proof", "Address proof", "Land or tenancy proof"],
        "application_process": "Apply through eligible banks",
    },
]


def run_mock_mode(db: SchemeDatabase, db_path: Path) -> None:
    task_id = task_timestamp() + "_mock"
    run_id = db.start_run(task_id, "mock://agri-schemes", depth=0, max_pages=0, max_files=0, js_enabled=False)

    payload = {
        "url": "mock://source",
        "source_type": "mock",
        "title": "Mock Source",
        "status_code": 200,
        "content_type": "text/plain",
        "local_path": "",
        "notes": "mock data",
        "content": "mock",
    }
    source_id = db.upsert_source(run_id, payload)

    for item in MOCK_SCHEMES:
        scheme = normalize_scheme_record(item)
        scheme_id = db.upsert_scheme(scheme, run_id=run_id, source_id=source_id)
        if scheme_id:
            db.link_scheme_source(run_id, source_id, scheme_id, "high", "mock", scheme)

    schemes = db.get_run_schemes(run_id)
    stats = {
        "pages_crawled": 0,
        "files_discovered": 0,
        "files_downloaded": 0,
        "files_cached": 0,
        "source_payloads": 1,
        "quality_breakdown": "high:1 medium:0 low:0",
        "elapsed": "demo",
    }
    db.finish_run(run_id, "success", stats)

    report_path, json_path, csv_path = write_exports(
        task_id,
        "mock://agri-schemes",
        run_id,
        db_path,
        schemes,
        [payload],
        stats,
    )
    db.export_master_csv(DATA_DIR / "schemes_master_latest.csv")
    curated_count = db.refresh_curated_schemes()
    master_count = db.refresh_master_dataset()
    db.export_curated_csv(DATA_DIR / "schemes_curated_latest.csv")
    stats["curated_master_rows"] = curated_count
    stats["master_rows"] = master_count
    print_console_summary(task_id, stats, db_path, report_path, json_path, csv_path, schemes)


def run_pipeline(args: argparse.Namespace) -> None:
    url = normalize_url(args.url)
    if not url:
        raise ValueError("A valid URL is required")

    task_id = task_timestamp()
    db_path = Path(args.db)
    db = SchemeDatabase(db_path)
    run_id = db.start_run(task_id, url, args.depth, args.max_pages, args.max_files, args.js)

    t0 = time.time()
    status = "failed"
    stats: Dict[str, Any] = {}

    try:
        console.print(
            Panel(
                "[bold green]KRISHI-SETU ADVANCED PIPELINE[/]\n"
                f"Task: {task_id}\n"
                f"URL: {url}\n"
                f"Depth: {args.depth}\n"
                f"Max pages: {args.max_pages}\n"
                f"Max files: {args.max_files}\n"
                f"Crawler mode: {'selenium-site' if args.selenium_site else 'requests'}\n"
                f"JS fallback: {args.js}\n"
                f"AI extraction: {not args.no_ai}"
            )
        )

        console.rule("[bold cyan]Phase 1 - Crawl")
        crawl_result = crawl_site(
            url,
            max_depth=args.depth,
            max_pages=args.max_pages,
            same_domain_only=not args.all_domains,
            enable_js=args.js,
            selenium_site=args.selenium_site,
        )
        pages = crawl_result.get("pages", [])
        file_links = crawl_result.get("file_links", [])

        console.rule("[bold cyan]Phase 2 - Download Resources")
        downloads, download_stats = download_resources(file_links, max_files=args.max_files)

        console.rule("[bold cyan]Phase 3 - Build Source Payloads")
        source_payloads = source_payloads_from_crawl(pages, downloads)

        quality_counter = {"high": 0, "medium": 0, "low": 0}
        total_schemes_linked = 0
        raw_scheme_candidates = 0
        rejected_noisy_candidates = 0

        console.rule("[bold cyan]Phase 4 - Extract + Store")
        for payload in source_payloads:
            source_id = db.upsert_source(run_id, payload)
            content = payload.get("content", "")
            extraction = ai_extract_schemes(
                content,
                payload.get("url", ""),
                api_key="" if args.no_ai else args.api_key,
                preferred_model=args.model,
            )
            quality = clean_text(extraction.get("data_quality", "low")).lower() or "low"
            if quality not in quality_counter:
                quality = "low"
            quality_counter[quality] += 1

            raw_scheme_candidates += int(extraction.get("raw_count", len(extraction.get("schemes", []))))
            rejected_noisy_candidates += int(extraction.get("dropped_count", 0))

            seen_payload_keys: set[str] = set()

            for scheme in extraction.get("schemes", []):
                scheme_key = canonicalize_name(scheme.get("scheme_name", ""))
                if not scheme_key or scheme_key in seen_payload_keys:
                    continue
                seen_payload_keys.add(scheme_key)

                scheme_id = db.upsert_scheme(scheme, run_id=run_id, source_id=source_id)
                if not scheme_id:
                    continue
                db.link_scheme_source(
                    run_id,
                    source_id,
                    scheme_id,
                    quality,
                    clean_text(extraction.get("model", "")),
                    scheme,
                )
                total_schemes_linked += 1

            time.sleep(0.15)

        run_schemes = db.get_run_schemes(run_id)
        curated_count = db.refresh_curated_schemes()
        master_count = db.refresh_master_dataset()

        elapsed = f"{time.time() - t0:.1f}s"
        stats = {
            "pages_crawled": len(pages),
            "files_discovered": download_stats.get("discovered", 0),
            "files_downloaded": download_stats.get("downloaded", 0),
            "files_cached": download_stats.get("cached", 0),
            "files_failed": download_stats.get("failed", 0),
            "source_payloads": len(source_payloads),
            "raw_scheme_candidates": raw_scheme_candidates,
            "rejected_noisy_candidates": rejected_noisy_candidates,
            "linked_scheme_records": total_schemes_linked,
            "run_unique_schemes": len(run_schemes),
            "curated_master_rows": curated_count,
            "master_rows": master_count,
            "quality_breakdown": f"high:{quality_counter['high']} medium:{quality_counter['medium']} low:{quality_counter['low']}",
            "crawl_errors": crawl_result.get("errors", [])[:25],
            "elapsed": elapsed,
        }

        db.finish_run(run_id, "success", stats)
        status = "success"

        report_path, json_path, csv_path = write_exports(
            task_id,
            url,
            run_id,
            db_path,
            run_schemes,
            source_payloads,
            stats,
        )
        db.export_master_csv(DATA_DIR / "schemes_master_latest.csv")
        db.export_curated_csv(DATA_DIR / "schemes_curated_latest.csv")
        print_console_summary(task_id, stats, db_path, report_path, json_path, csv_path, run_schemes)
    except Exception as exc:
        stats = {
            "error": str(exc),
            "elapsed": f"{time.time() - t0:.1f}s",
        }
        db.finish_run(run_id, "failed", stats)
        raise
    finally:
        db.close()
        if status != "success":
            console.print("[red]Run failed. Check logs and report output.[/]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Krishi-Setu advanced scraper and database organizer")
    parser.add_argument("url", nargs="?", default="", help="Root URL to crawl")
    parser.add_argument("--depth", type=int, default=1, help="Crawl depth: 0, 1, 2...")
    parser.add_argument("--max-pages", type=int, default=40, help="Maximum pages to crawl")
    parser.add_argument("--max-files", type=int, default=120, help="Maximum files to download")
    parser.add_argument("--all-domains", action="store_true", help="Allow crawling links outside source domain")
    parser.add_argument("--js", action="store_true", help="Enable Selenium JS fallback")
    parser.add_argument("--selenium-site", action="store_true", help="Use Selenium to crawl all rendered pages of the site")
    parser.add_argument("--no-ai", action="store_true", help="Disable Gemini and use heuristic extraction")
    parser.add_argument("--api-key", default=os.environ.get("GEMINI_API_KEY", ""), help="Gemini API key")
    parser.add_argument("--model", default="", help="Preferred Gemini model name")
    parser.add_argument("--db", default=str(DB_DIR / "agri_schemes.db"), help="SQLite database path")
    parser.add_argument("--query-db", action="store_true", help="Run database query mode instead of crawling")
    parser.add_argument("--query", default="", help="Search text for database query mode")
    parser.add_argument("--query-ministry", default="", help="Filter query mode by ministry text")
    parser.add_argument("--query-type", default="", help="Filter query mode by scheme type")
    parser.add_argument("--query-limit", type=int, default=25, help="Maximum rows returned in query mode")
    parser.add_argument("--query-scope", choices=["curated", "all"], default="curated", help="Query curated or all indexed rows")
    parser.add_argument("--query-min-score", type=int, default=45, help="Minimum confidence score in query mode")
    parser.add_argument("--query-export", default="", help="Optional JSON export path for query mode results")
    parser.add_argument("--test", action="store_true", help="Run mock demo mode")
    return parser.parse_args()


def run_query_mode(args: argparse.Namespace) -> None:
    db = SchemeDatabase(Path(args.db))
    try:
        curated_count = db.refresh_curated_schemes()
        master_count = db.refresh_master_dataset()
        db.export_curated_csv(DATA_DIR / "schemes_curated_latest.csv")
        curated_only = args.query_scope == "curated"
        min_score = int(args.query_min_score if curated_only else 0)

        results = db.search_scheme_index(
            query_text=args.query,
            ministry=args.query_ministry,
            scheme_type=args.query_type,
            min_score=min_score,
            limit=args.query_limit,
            curated_only=curated_only,
        )

        console.print(
            Panel(
                "[bold green]DATABASE QUERY MODE[/]\n"
                f"DB: {args.db}\n"
                f"Scope: {args.query_scope}\n"
                f"Query: {args.query or '(none)'}\n"
                f"Ministry filter: {args.query_ministry or '(none)'}\n"
                f"Type filter: {args.query_type or '(none)'}\n"
                f"Min score: {min_score}\n"
                f"Curated master rows: {curated_count}\n"
                f"Master indexed rows: {master_count}\n"
                f"Matched rows: {len(results)}"
            )
        )

        if results:
            table = Table(title="Scheme Query Results", header_style="bold green", show_lines=True)
            table.add_column("#", width=4)
            table.add_column("Scheme", style="cyan", max_width=44)
            table.add_column("Type", max_width=16)
            table.add_column("Ministry", max_width=28)
            table.add_column("Score", justify="right", width=7)
            table.add_column("Website", max_width=28)

            for idx, row in enumerate(results, start=1):
                table.add_row(
                    str(idx),
                    clean_text(row.get("scheme_name", ""))[:44],
                    clean_text(row.get("scheme_type", ""))[:16],
                    clean_text(row.get("nodal_ministry", ""))[:28],
                    str(row.get("confidence_score", "")),
                    clean_text(row.get("official_website", ""))[:28],
                )
            console.print(table)
        else:
            console.print("[yellow]No schemes matched the provided query filters.[/]")

        if args.query_export:
            export_path = Path(args.query_export)
            export_path.parent.mkdir(parents=True, exist_ok=True)
            with open(export_path, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "queried_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "database": str(args.db),
                        "scope": args.query_scope,
                        "query": args.query,
                        "query_ministry": args.query_ministry,
                        "query_type": args.query_type,
                        "query_min_score": min_score,
                        "total_matches": len(results),
                        "results": results,
                    },
                    handle,
                    indent=2,
                    ensure_ascii=False,
                )
            console.print(f"[green]Query results exported:[/] {export_path}")
    finally:
        db.close()


def main() -> None:
    args = parse_args()

    if args.query_db:
        run_query_mode(args)
        return

    if args.test:
        db = SchemeDatabase(Path(args.db))
        try:
            run_mock_mode(db, Path(args.db))
        finally:
            db.close()
        return

    if not args.url:
        args.url = console.input("URL to scrape: ").strip()

    if not args.no_ai and not args.api_key:
        maybe_key = console.input("Gemini API key (optional, press Enter to continue without AI): ").strip()
        args.api_key = maybe_key

    if not args.api_key:
        args.no_ai = True
        console.print("[yellow]No API key detected. Using heuristic extraction mode.[/]")

    if args.js and not HAS_SELENIUM:
        console.print("[yellow]Selenium not installed, JS fallback disabled for this run.[/]")
        args.js = False

    if args.selenium_site and not HAS_SELENIUM:
        console.print("[yellow]Selenium not installed, selenium-site crawl disabled for this run.[/]")
        args.selenium_site = False

    run_pipeline(args)


if __name__ == "__main__":
    main()