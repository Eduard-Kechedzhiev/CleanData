# CleanData

Free catalog cleaning tool for foodservice distributors. Upload a CSV of product data and get standardized names, brand extraction, quality scores, category taxonomy, and GTIN enrichment — all powered by AI.

## Architecture

```
Browser → nginx (:9090) → FastAPI (:8000) → Pipeline → Results
                                               │
                              reading → cleaning → taxonomy → gtin
                               (5%)      (70%)     (15%)     (10%)
```

- **Frontend**: React 18 + TypeScript + Vite + Tailwind + shadcn/ui
- **Backend**: FastAPI + Python 3.11
- **Pipeline**: Gemini AI for cleaning, external API for taxonomy, MongoDB + Perplexity for GTIN
- **Progress**: SSE (Server-Sent Events) with polling fallback
- **Deployment**: Docker Compose (nginx + backend containers)

## Quick start

### Docker (recommended)

```bash
# 1. Copy env and add your Gemini key
cp backend/.env.example backend/.env
# edit backend/.env → set GEMINI_API_KEY

# 2. Build and run
docker compose up -d --build

# 3. Open http://localhost:9090
```

### Local development

```bash
# Frontend
npm install
npm run dev          # → http://localhost:5173

# Backend (separate terminal)
cd backend
pip install -r requirements.txt
cp .env.example .env # edit with your keys
uvicorn app.main:app --reload --port 8000
```

The Vite dev server proxies `/api/*` to `localhost:8000` automatically.

## Project structure

```
├── src/                          # React frontend
│   ├── components/               # UI components
│   │   ├── BenefitsSection.tsx   # Bento grid of features
│   │   ├── SampleResults.tsx     # Sample table + score distribution
│   │   ├── BenchmarksSection.tsx # How-it-works + data quality graphs
│   │   ├── HeroSection.tsx       # Landing hero with upload widget
│   │   ├── UploadWidget.tsx      # Drag-and-drop CSV upload
│   │   └── ...
│   ├── pages/
│   │   ├── Index.tsx             # Landing page
│   │   └── JobPage.tsx           # Processing + results page
│   └── lib/
│       └── api.ts                # API client (typed)
│
├── backend/
│   ├── app/
│   │   ├── main.py               # FastAPI app (CORS, lifespan, routes)
│   │   ├── settings.py           # Config from env vars
│   │   ├── api/routes/           # HTTP endpoints
│   │   │   ├── uploads.py        # POST /api/upload
│   │   │   ├── jobs.py           # GET /api/jobs/:id/status, results, events
│   │   │   └── leads.py          # POST /api/jobs/:id/email
│   │   ├── domain/
│   │   │   └── job_models.py     # Pydantic models (job state, pipeline stages)
│   │   ├── infrastructure/
│   │   │   ├── job_store.py      # File-based job persistence
│   │   │   ├── job_queue.py      # Job queue management
│   │   │   └── thread_runner.py  # Thread-based pipeline execution
│   │   └── services/
│   │       ├── job_service.py    # Job orchestration + lead capture
│   │       ├── job_executor.py   # Pipeline stage runner
│   │       └── results_service.py# Result aggregation
│   │
│   ├── pipeline/                 # Data processing pipeline
│   │   ├── orchestrator.py       # Stage sequencing
│   │   ├── services/
│   │   │   ├── ai_cleaning.py    # Gemini-powered name standardization + scoring
│   │   │   ├── taxonomy.py       # Category classification
│   │   │   └── gtin_lookup.py    # GTIN enrichment (MongoDB + Perplexity)
│   │   └── config.py             # Pipeline configuration
│   │
│   ├── requirements.txt
│   └── .env.example
│
├── docker/
│   ├── backend/Dockerfile        # Python 3.11 slim
│   └── nginx/
│       ├── Dockerfile            # Node build + nginx alpine
│       └── nginx.conf            # Proxy, SSE, gzip, security headers
│
├── docker-compose.yml            # backend + nginx on port 9090
└── .github/workflows/deploy.yml  # CI: build Docker images on push to main
```

## Pipeline stages

| Stage | Weight | What it does |
|-------|--------|-------------|
| **Reading** | 5% | Parse CSV/Excel, validate columns, count rows |
| **Cleaning** | 70% | Gemini AI standardizes names, extracts brands/pack sizes, assigns 0-10 clarity scores |
| **Taxonomy** | 15% | External API assigns 3-level category hierarchy |
| **GTIN** | 10% | Cross-references barcodes via MongoDB (SALT DB) with Perplexity API fallback |

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GEMINI_API_KEY` | Yes | — | Google Gemini API key for cleaning stage |
| `MONGODB_PASSWORD` | No | — | SALT DB password for GTIN lookups |
| `PERPLEXITY_API_KEY` | No | — | Fallback GTIN lookup via Perplexity |
| `MAX_UPLOAD_MB` | No | 50 | Max file upload size |
| `JOB_TTL_HOURS` | No | 24 | How long job results are kept |
| `MAX_CONCURRENT_JOBS` | No | 3 | Pipeline concurrency limit |
| `CORS_ORIGINS` | No | localhost | Allowed CORS origins |

## TODOs before production deployment

### Infrastructure
- [ ] Set up AWS infrastructure (Terraform in `infra/`)
- [ ] Configure ECS Fargate task definition (single task, no horizontal scaling needed for now)
- [ ] Set up ALB with HTTPS/TLS certificate
- [ ] Configure custom domain + DNS
- [ ] Set up persistent EFS or EBS volume for `/app/jobs` data

### Backend
- [ ] **Replace filesystem job store with a database.** Currently job state, pipeline progress, and uploaded files are stored on the local filesystem (`/app/jobs`). This works for a single Fargate task but breaks if we ever need multiple instances behind a load balancer — requests could hit a node that doesn't have the job. Moving to DynamoDB (job metadata + state) + S3 (uploaded files + results) would decouple state from the container and make horizontal scaling possible later
- [ ] Wire up rep notification on lead capture (currently logs only — see `_notify_rep()` in `job_service.py`). Options: Slack webhook, SES email, or a DB-backed queue
- [ ] Add rate limiting on upload endpoint
- [ ] Set up structured logging export (CloudWatch / Datadog)

### CI/CD
- [ ] Complete `deploy.yml` — push images to ECR and trigger ECS deployment
- [ ] Add staging environment
- [ ] Add health check monitoring / alerting

### Frontend
- [ ] Add error boundary for unhandled React errors
- [ ] Add analytics (PostHog, Mixpanel, or similar)
- [ ] Consider code-splitting for bundle size (currently ~250KB gzip)
