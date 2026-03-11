# CleanData — Full Context Dump

## What Is This

CleanData is a free web tool that cleans messy foodservice product catalogs. Upload a CSV of product data, get back cleaned names, extracted brands, pack/size parsing, quality scores, product categories, and GTIN validation. It's a self-service version of an internal pipeline that Pepper's team was running manually for supplier accounts.

## The Company

**Pepper** (trypepper.com) is a foodservice e-commerce platform. Suppliers (distributors like Sysco, US Foods, regional broadliners) list their product catalogs on Pepper. The platform connects these suppliers with buyers (restaurants, institutions, etc.).

The core problem: supplier product data is terrible. Names look like `CHKN BRST BNLS SKNLS FRZ 4OZ`. No brands extracted, no categories, no standardization. This hurts search, browsing, conversion rates, and the overall catalog quality that buyers see.

## Why This Exists

### The Business Case

CleanData is a **lead generation tool**, not a product. The value exchange:

1. Supplier uploads their catalog CSV (for free)
2. They see a preview of cleaned data — quality scores, brand extraction, category assignment
3. To download the full enriched CSV, they must provide **email + company name**
4. Pepper's sales team follows up with these leads

The cleaned data is genuinely useful to distributors, but the strategic goal is capturing contact info from potential Pepper customers. Every email submission is appended to `leads.jsonl` for the sales pipeline.

### The Internal Origin

The cleaning pipeline existed before CleanData as a CLI tool (`data_cleaning_v2/pipeline/`). Eduard's team ran it manually for each account — fetch products via `query_products.py`, run the pipeline, generate meeting notes showing coverage improvement opportunities. This manual workflow is documented extensively in the parent repo's `CLAUDE.md`.

CleanData makes this self-service: instead of Eduard running the CLI for each prospect, prospects run it themselves and Pepper captures the lead automatically.

## The Parent Repository

CleanData lives inside a larger monorepo (`auto_job/`) that contains Pepper's full product image acquisition toolkit:

- **Image classification** (`agents/classify.py`) — AI classifies products into acquisition strategies (AI generation, web scraping, skip)
- **AI image generation** (`ai_image_gen/`) — Gemini generates images for commodity products (produce, meat, etc.)
- **Web scraping** (`web_scrape/`) — Scrapes product images from the web for branded items
- **SALT matching** (`lib/get_matches.py`, `lib/assign_matches.py`) — Fuzzy-matches products against Pepper's existing product content database
- **Image upload** (`upload_imgs/`) — Bulk uploads approved images to SALT/S3
- **Human review** (`tmp/review_app.py`) — Tkinter GUI for manual image validation

CleanData uses the same pipeline infrastructure but wraps it in a web UI. The pipeline code was **copied** from `data_cleaning_v2/pipeline/` into `backend/pipeline/` (not symlinked, not submoduled) because the user explicitly wanted proper integration, not subprocess calls.

## What SALT Is

SALT is Pepper's internal **product content database**. It stores canonical product information: names, images, categories, nutrition data, allergens. When a supplier's product is "matched" to a SALT entry, it inherits rich content (especially images). The SALT taxonomy is a hierarchical category system specific to foodservice (Meats > Pork > Loins, Janitorial > Paper > Towels, etc.).

The pipeline's taxonomy stage classifies products using SALT's taxonomy. The GTIN stage validates barcodes against SALT's MongoDB database.

## The Pipeline — What It Actually Does

Three stages, each doing something fundamentally different:

### Stage 1: Cleaning (~70% of total runtime)

Uses **Google Gemini** to process each product row through AI. For `CHKN BRST BNLS SKNLS FRZ 4OZ` it produces:
- Cleaned name: `Chicken Breast Boneless Skinless Frozen 4oz`
- Extracted brand: `Tyson`
- Pack/size parsing: `2 x 10 LB`
- Clarity rating: 8/10 (how descriptive/clear the product name is)

This is the slow part. Gemini API calls are batched (10 items per batch, 4 workers) but still take ~40 minutes for 500 rows. Each row requires an AI inference.

### Stage 2: Taxonomy (~15% of runtime)

Calls an **external SALT API** to classify each product into the SALT category hierarchy. Takes the cleaned name and returns structured categories like `Meats > Poultry > Breast`. This is a network call to Pepper's internal service, not an AI call.

### Stage 3: GTIN Validation (~15% of runtime)

Validates barcodes (GTIN-14 format) against Pepper's **MongoDB database**. For products without GTINs, uses **Perplexity AI** as a fallback to look up product information. Determines whether each product has a valid barcode in the system.

### Why These Stages Matter for Lead Gen

The combination creates a compelling demo: "Your catalog had 8,863 products. We cleaned all the names, found 347 unique brands, assigned categories to everything, and validated 72% of your barcodes. Here's what the data looks like now." This is concrete value that makes the sales follow-up easier.

## Architecture Decisions and Why

### Direct Python Imports, Not Subprocess

The pipeline was originally a CLI tool. The first integration attempt used `subprocess.Popen` to shell out to `python -m pipeline`. This broke immediately (wrong Python path, module not found, no progress feedback). The user explicitly rejected this: *"I wanted the transfer of the code from cli to the website along with the cleanup of the code from cli rather than subprocess shenanigans."*

The pipeline package was copied into `backend/pipeline/` and imported directly. A `_ProgressHandler(logging.Handler)` intercepts the pipeline's log messages to feed real-time progress back to the job store. This is cleaner and gives row-level progress during cleaning.

### Filesystem Job Storage, Not a Database

Jobs are stored as directories on disk: `jobs/{id}/status.json`, `jobs/{id}/input.csv`, `jobs/{id}/output.csv`. This was chosen for simplicity in the MVP. The interface is clean enough that swapping to S3 + PostgreSQL/DynamoDB is straightforward — the docstrings in `job_store.py` document the exact migration path.

### SSE, Not WebSocket

Server-Sent Events are one-directional (server → client), which is all we need. The server polls `status.json` every 2 seconds and sends updates when the JSON changes. SSE works through proxies and load balancers more reliably than WebSocket. A heartbeat comment (`: heartbeat\n\n`) is sent every ~16 seconds to prevent proxy idle timeouts.

### Per-Requester Download Tokens

An external review found that the original email gate was **job-global**: once anyone submitted an email for a job, all future visitors to that URL could download without providing their own email. This defeated the lead-capture purpose entirely for shared/forwarded links.

The fix: each email submission generates a unique `download_token` stored in the job's `download_tokens[]` array. The download endpoint requires a valid token as a query parameter. Tokens are stored in the browser's `localStorage` (keyed by job ID) and never exposed in API responses (`download_tokens` is excluded from all public endpoints via `_STATUS_EXCLUDE`).

### Non-Daemon Threads

Pipeline jobs run for ~40 minutes. Using `daemon=True` threads meant any server restart (deploy, crash) would kill in-flight work immediately. Changed to `daemon=False` with a `wait_active_threads()` call on shutdown so the server waits up to 5 seconds for threads to finish gracefully. This isn't bulletproof (a `kill -9` still loses work), but it handles normal deploys. The `recover_stuck_jobs()` function marks any interrupted jobs as failed on startup.

### 4 Visual Stages, Not 6

The original UI had 6 fake stages to make progress look smoother. This caused the UI to skip stages (jumping from "Reading" to "Assigning categories") because the mapping from 3 real backend stages to 6 visual stages was fragile. Reduced to 4 visual stages that map 1:1 with reality: Reading → Cleaning → Categories → GTINs. A weighted progress bar (cleaning=70%, taxonomy=15%, gtin=10%, reading=5%) provides smooth movement within stages.

### The `pending` Stage Map Bug

A recurring UI bug: the progress display would skip directly to "Assigning categories" when cleaning started. Root cause: the `computeVisualStage` function used a `stageMap` that assigned visual indices to `pending` stages. When cleaning was `running` (index 1), gtin's `pending` value (index 2) was higher, so the visual jumped to stage 2. Fixed by only mapping `running` and `complete` statuses — `pending` stages don't advance the visual index.

## What the "40 Minutes" Means

The cleaning stage is the bottleneck. For 500 rows:
- 10 items per Gemini batch × 4 concurrent workers = 40 items in flight at a time
- Each batch takes ~5-8 seconds (Gemini API latency)
- 500 rows ÷ 40 per cycle × 6 seconds ≈ 75 seconds minimum... but error handling, retries, rate limits, and the taxonomy/GTIN stages add up

For real catalogs (5,000-10,000 rows), processing genuinely takes 30-60 minutes. This is why the email capture during processing says "Processing takes up to 40 minutes" and suggests bookmarking the page. Email notification ("we'll email you when it's ready") is **not implemented** — the UI was changed to be honest about this. The email capture during processing is purely for lead gen.

## The Email Capture Strategy

There are two email capture points, serving different purposes:

1. **During processing** ("Leave your email and we'll follow up with tips for your catalog") — lightweight, email-only, optional. The user gets a download token from this submission too, which means they won't see the gate form later.

2. **Results page download gate** (email + company name required) — the main lead capture. Blocks the full CSV download until submitted. This is the primary conversion point.

Both submissions append to `leads.jsonl` with email, company, job_id, and timestamp. This file is the sales team's lead list.

## The Results Page — What It Shows

After pipeline completion, the results page displays:
- **Summary stats**: row count, average quality score, unique brands extracted, GTIN validation rate
- **Before/After table**: 8 sample rows showing original messy names vs cleaned output
- **Quality score distribution**: bar chart of clarity ratings (1-10 scale)
- **Top brands**: horizontal bar chart of most common extracted brands
- **Category breakdown**: grid showing product distribution across SALT categories
- **CTA**: "Your catalog has X products ready for image sourcing. Want us to help?" → mailto link

The sample rows are selected by quality score (highest first) to showcase the best transformations.

## Environment Variables and Secrets

The backend needs these API keys (in `backend/.env`):
- `GEMINI_API_KEY` — Google Gemini for AI cleaning
- `PERPLEXITY_API_KEY` — Perplexity for GTIN fallback lookups
- `MONGODB_PASSWORD` — Pepper's SALT MongoDB for GTIN validation
- `AI_CLEANING_REQUEST_TIMEOUT_SECONDS` — timeout for Gemini calls (default 120)

Other config:
- `MAX_UPLOAD_MB` (default 50) — max upload size
- `JOB_TTL_HOURS` (default 24) — how long job results persist
- `MAX_CONCURRENT_JOBS` (default 3) — concurrent pipeline limit
- `CORS_ORIGINS` — allowed origins for CORS
- `CONTACT_EMAIL` (default hello@cleandata.com) — used in CTA buttons

## The GTIN Bug

The results page showed inflated GTIN validation percentages. Root cause: after CSV round-tripping, boolean values become strings. `pd.Series.astype(bool)` on strings like `"False"` returns `True` (any non-empty string is truthy in Python). Fixed with a proper `_is_gtin_truthy()` function that handles string representations: `"False"`, `"0"`, `"none"`, `"n/a"` → false.

## Data Retention

- Jobs are stored for 24 hours, measured from **completion time** (not upload time) for completed jobs. This prevents the scenario where a 40-minute pipeline run expires almost immediately after results become available.
- Failed and queued jobs expire 24 hours from creation time.
- Orphan directories (upload failed before `status.json` was created) are cleaned by filesystem mtime.
- Cleanup runs every hour as an asyncio background task.
- `leads.jsonl` persists indefinitely (it's outside job directories).

## Cloud Deployment Path

The code is structured for eventual AWS deployment:

| Current (Dev) | Cloud (AWS) |
|---|---|
| `status.json` on disk | PostgreSQL row or DynamoDB item with TTL |
| `input.csv` / `output.csv` on disk | S3 with lifecycle rules |
| Background thread per job | ECS Fargate task triggered via SQS |
| SSE polls filesystem | SSE polls database |
| `FileResponse` for download | S3 presigned URL |
| Background cleanup loop | S3 lifecycle + DynamoDB TTL |
| `leads.jsonl` append-only file | PostgreSQL table |

The `job_store.py` interface is the swap point. The public functions (`create_job`, `load_status`, `update_stage`, `mark_complete`, `set_email`, etc.) stay the same; only the storage implementation changes.

## Frontend Stack

- React 18 + TypeScript
- Vite (dev server on :8080, proxies `/api` to backend on :8000)
- Tailwind CSS with custom design tokens (score colors, hero gradients)
- shadcn/ui components (Button, Input, Tooltip, Toast)
- Framer Motion for animations
- Recharts for bar charts
- React Router with BrowserRouter (requires SPA fallback in production)

The design uses a dark hero section on the landing page with a light theme elsewhere. The color palette emphasizes trust and professionalism — this is a B2B tool for foodservice distributors, not a consumer app.

## The Landing Page

The landing page is a marketing funnel, not a dashboard:
1. Hero with upload widget ("Drop your catalog CSV here")
2. Trust bar (social proof)
3. Benefits section (what you get)
4. How it works (3 steps)
5. Sample results (hardcoded demo data showing before/after)
6. Benchmarks
7. FAQ
8. Final CTA

The "Try with sample data" link scrolls to the sample results section — it does NOT trigger a real pipeline run. The sample data is hardcoded marketing content showing idealized results from a broadline distributor catalog.

## What's NOT Implemented

- **Email notifications** — The UI used to promise "we'll email you when ready" but this was dishonest. Changed to "bookmark this page." No SMTP/SendGrid integration exists.
- **Multiple download formats** — Only CSV. The spec mentioned gated full report + brand summary + quality breakdown as separate downloads.
- **ETA display** — No time estimate shown during processing.
- **Real sample data flow** — `getSampleResults()` API exists but the landing page uses hardcoded data instead.
- **Pipeline tests** — The original `data_cleaning_v2` had tests; they were not migrated into the CleanData repo.
- **Authentication** — No user accounts. Everything is anonymous except email capture.

## Key People

- **Eduard** — The developer working on this. Also manages the broader image acquisition workflow for supplier accounts. His accounts are tracked in `meeting_notes/eduard_low_coverage.csv`.
- The broader team handles SALT operations, supplier onboarding, and image coverage improvement across Pepper's platform.

## The Review Process

An external AI review was conducted that found issues at P0-P2 severity:
- **P0**: Download gate was job-global (fixed with per-requester tokens)
- **P1**: Email notification lie, daemon threads, TTL from upload time, SSE no heartbeat, GTIN bool bug, dead CTA buttons
- **P2**: Missing category breakdown, SPA fallback, no pipeline tests, large JS bundle

All P0 and P1 issues have been addressed. Some P2 items remain (bundle splitting, pipeline test migration, real sample data flow).

## File Layout

```
CleanData/
├── backend/
│   ├── main.py              — FastAPI server, all API endpoints
│   ├── models.py            — Pydantic models (JobStatus, JobSummary, etc.)
│   ├── job_store.py          — Job lifecycle, filesystem storage, cleanup
│   ├── pipeline_runner.py    — Thread management, progress handler
│   ├── results.py            — Computes summary stats from pipeline output
│   ├── requirements.txt      — Python deps (fastapi, pandas, google-genai, pymongo, etc.)
│   ├── .env                  — API keys (GEMINI, PERPLEXITY, MONGODB)
│   ├── data/SALT Taxonomy.csv — Category hierarchy for taxonomy stage
│   ├── global-bundle.pem     — TLS cert for MongoDB connection
│   ├── jobs/                 — Runtime: job directories (gitignored)
│   └── pipeline/             — Copied from data_cleaning_v2/pipeline/
│       ├── orchestrator.py   — Runs stages in sequence
│       ├── config.py         — RunConfig dataclass
│       ├── io_files.py       — CSV/TSV/XLSX reader with GTIN preservation
│       ├── services/         — ai_cleaning.py, taxonomy.py, gtin.py
│       ├── providers/        — gemini, taxonomy API, mongodb, perplexity
│       └── assets/           — food_abbreviations.json
├── src/
│   ├── App.tsx               — Routes: /, /jobs/:jobId, legacy redirects
│   ├── main.tsx              — React entry point
│   ├── pages/
│   │   ├── Index.tsx         — Landing page (marketing funnel)
│   │   ├── JobPage.tsx       — Unified processing + results view
│   │   └── NotFound.tsx
│   ├── components/           — UI components (Navbar, HeroSection, UploadWidget, etc.)
│   ├── lib/api.ts            — Typed API client (uploadFile, getJobStatus, submitEmail, etc.)
│   └── hooks/                — Toast hook
├── vite.config.ts            — Dev server, proxy, SPA deployment notes
├── package.json
├── tailwind.config.ts
└── tsconfig.json
```
