# Krishi-Setu Advanced Scraper

Advanced crawler + multi-format downloader + agri scheme database organizer.

This version upgrades the old single-run extractor into a persistent data pipeline that keeps building your agriculture scheme knowledge base every run.

## What Is Upgraded

- Deep crawler (BFS) with configurable depth/pages
- Multi-format ingestion:
  - Web pages (HTML)
  - PDF
  - JSON
  - CSV
  - XLS/XLSX
  - DOCX
  - TXT/XML
- Selenium fallback for JS-heavy pages (`--js`)
- AI extraction with Gemini model fallback chain
- Heuristic extraction fallback when API/model fails
- Curated high-confidence scheme layer (auto-built each run)
- Canonical master indexing with fuzzy merge and version history
- Query CLI to search schemes by name/ministry/type/confidence
- Dashboard backend + frontend for visual control, testing, and DB exploration
- Dedicated Master Database page (`/master`) with AI smart search, source/media trace, and version timeline
- Persistent SQLite database with normalized tables:
  - schemes
  - eligibility
  - exclusions
  - requirements
  - benefits
  - keywords
  - sources
  - run tracking
- Rich exports per run:
  - report markdown
  - run JSON
  - run CSV
  - master CSV snapshot from DB
  - curated CSV snapshot from DB

## Install

```bash
pip install -r requirements.txt
```

## Run Dashboard (Backend + Frontend)

```bash
uvicorn dashboard_app:app --reload --port 8008
```

Then open:

```text
http://127.0.0.1:8008
```

UI pages:

- `http://127.0.0.1:8008/` Dashboard Analytics: overview metrics, quality signals, recent runs
- `http://127.0.0.1:8008/master` Master Data: schemes + search + pagination only
- `http://127.0.0.1:8008/crawler` Crawler Control: URL crawl form, live job logs, recent jobs, and report artifacts

Master Database page capabilities (`http://127.0.0.1:8008/master`):

- Canonical master scheme index built from all crawled URLs over time
- Smart search mode (semantic token expansion for natural-language queries)
- Multiple quality scopes: `strict`, `trusted`, `balanced`, `curated`, `all`, `raw`
- Trusted default scope that suppresses noisy/non-scheme fragments while keeping useful breadth
- Fast paginated scheme browsing for data operations

Crawler page capabilities (`http://127.0.0.1:8008/crawler`):

- Selenium full-site crawl mode (recommended)
- Optional requests+JS fallback mode
- AI extraction pipeline that keeps scheme-quality filtering before DB upsert
- Report artifact links (`report/json/csv`) for each run

## Quick Usage

```bash
# Interactive mode
python scraper.py

# Crawl URL with defaults
python scraper.py https://agriwelfare.gov.in/en/Major

# Deeper crawl + more files + JS rendering fallback
python scraper.py https://agriwelfare.gov.in/en/Major --depth 2 --max-pages 80 --max-files 250 --js

# Selenium full-site crawl mode (recommended for complex sites)
python scraper.py https://agriwelfare.gov.in/en/Major --depth 2 --max-pages 200 --max-files 300 --selenium-site

# Use Gemini API key
GEMINI_API_KEY=your_key python scraper.py https://vikaspedia.in/agriculture/policies-and-schemes

# Query curated schemes from DB
python scraper.py --query-db --query "pm kisan"

# Query by ministry with confidence filtering
python scraper.py --query-db --query-ministry "agriculture" --query-limit 30 --query-min-score 50

# Export query result JSON
python scraper.py --query-db --query "insurance" --query-export output/data/query_insurance.json

# Force heuristic mode (no AI)
python scraper.py https://agriwelfare.gov.in/en/Major --no-ai

# Demo mode
python scraper.py --test
```

## CLI Options

```text
url (positional)             Root URL to crawl
--depth                      Crawl depth (default: 1)
--max-pages                  Max crawled pages (default: 40)
--max-files                  Max downloaded files (default: 120)
--all-domains                Allow external domain links
--js                         Enable Selenium JS fallback
--no-ai                      Disable Gemini and use heuristic extraction
--api-key                    Gemini API key (or use GEMINI_API_KEY)
--model                      Preferred Gemini model name
--db                         SQLite DB path (default: output/db/agri_schemes.db)
--query-db                   Run database query mode (no crawl)
--query                      Text search for query mode
--query-ministry             Ministry text filter for query mode
--query-type                 Scheme type filter for query mode
--query-limit                Max rows in query mode (default: 25)
--query-scope                curated|all (default: curated)
--query-min-score            Min confidence score in query mode (default: 45)
--query-export               Export query result to JSON path
--test                       Run mock test
```

## Output Structure

```text
output/
  db/
    agri_schemes.db
  data/
    schemes_<task_id>.json
    schemes_<task_id>.csv
    schemes_master_latest.csv
    schemes_curated_latest.csv
    query_<name>.json
  reports/
    report_<task_id>.md
  pdfs/
    *.pdf
  raw/
    csv/
    json/
    excel/
    doc/
    other/
```

## Database Design

The organizer stores normalized information so you can query structured details:

- `schemes`: core scheme profile (name, ministry, objective, benefit, process)
- `scheme_eligibility`: 1:n eligibility rules
- `scheme_exclusions`: 1:n exclusions
- `scheme_requirements`: 1:n requirements/documents
- `scheme_benefits`: 1:n benefits/facts
- `scheme_keywords`: language-tagged keywords
- `sources`: every crawled/downloaded source record
- `scheme_sources`: mapping between extracted scheme and source
- `scheme_versions`: version/audit events for each upsert with match method and similarity score
- `master_schemes`: canonical master index used by dashboard smart search
- `runs`: per-run metadata and stats

## Curated Quality Layer

Every run now refreshes a `curated_schemes` table in SQLite.

- Confidence scoring uses field completeness + source quality + name-noise penalties
- Curated rows are exported to `output/data/schemes_curated_latest.csv`
- This gives you a cleaner, production-friendly subset for app and analytics usage

## Query Mode

Use query mode to search DB without crawling:

```bash
# Top curated rows
python scraper.py --query-db

# Search across curated rows
python scraper.py --query-db --query "pmfby"

# Search all indexed rows (including non-curated)
python scraper.py --query-db --query "horticulture" --query-scope all --query-min-score 0
```

## API Endpoints (Dashboard Backend)

- `GET /api/overview` system totals + latest run
- `GET /api/runs?limit=20` latest runs
- `GET /api/reports?limit=20` latest run reports + artifact links
- `GET /api/schemes?...` filtered scheme list
- `GET /api/schemes/{scheme_id}` full scheme details
- `POST /api/jobs/scrape` start a crawl job
- `GET /api/jobs` list UI-triggered jobs
- `GET /api/jobs/{job_id}` single job status and logs
- `POST /api/curated/refresh` rebuild curated index

Master API endpoints:

- `GET /api/master/overview` master totals, top ministries/types, recent updates
- `GET /api/master/search?...` smart/keyword search across canonical master schemes (`strict|trusted|balanced|curated|all|raw` scopes)
- `GET /api/master/schemes/{scheme_id}` full source-backed detail + media + version history
- `POST /api/master/refresh` rebuild curated + master indexes

## Why Your Last Run Had 0 Schemes

Your older script used a model value that failed with `404 NOT_FOUND`, so extraction produced zero scheme rows.

Upgraded script fixes this by:

- trying multiple Gemini model candidates
- gracefully falling back to heuristic extraction when AI is unavailable

## Best Targets For Large Agriculture Coverage

- `https://vikaspedia.in/agriculture/policies-and-schemes`
- `https://agriwelfare.gov.in`
- `https://pib.gov.in`
- Official PDF guideline links from ministry pages
- Structured data portals with CSV/JSON datasets

## Notes

- Some government websites block bots; use `--js` and increase `--max-pages`/`--max-files`.
- For production, run repeated crawls and keep the same DB path so your database accumulates over time.
