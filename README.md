# Newsvine

Newsvine is a local-first news platform built around personalized discovery, search, and engagement signals. It combines a FastAPI backend, a React frontend, and a streaming/data stack based on Kafka, Redis, Elasticsearch, PostgreSQL, dbt, and Spark-oriented batch assets.

## What It Is Used For

- Personalized news browsing with recommendations, trending feeds, bookmarks, and history.
- Search and discovery over indexed articles in Elasticsearch.
- Event-driven ranking and profile updates driven by user interactions.
- Local development and demos of a production-shaped data platform without needing cloud infrastructure first.

## Core Use Cases

- Browse a home feed with sections such as Recommended For You, Trending Now, and Most Read.
- Search articles and capture those search interactions for downstream ranking.
- Register, log in, bookmark stories, and review reading history.
- Run local pipelines that ingest articles, update user profiles, score trending items, and persist analytics-ready data.
- Exercise the system with integration tests and k6 load tests before deployment.

## Architecture Overview

Newsvine is organized as a thin frontend over an API, backed by both online serving stores and asynchronous event-processing jobs.

```text
Browser
	-> React/Vite frontend
	-> /api
	-> FastAPI application
			 -> PostgreSQL for users, auth, and persisted application data
			 -> Elasticsearch for article retrieval and search
			 -> Redis for rate limits, trending sets, profiles, and cached signals
			 -> Kafka for article ingestion and user interaction streams

Kafka topics
	-> pipeline consumers and scorers in src/newsvine_pipeline
	-> Redis, PostgreSQL, and Elasticsearch updates

Batch and analytics
	-> dbt models in analytics/dbt_newsvine
	-> Spark-oriented jobs in streaming
	-> orchestration and monitoring assets in orchestration
```

## Main Components

### Frontend

- The UI lives in `frontend` and is built with React 19 and Vite.
- Browser requests are made through `/api`, so local development and production deployments can keep a same-origin proxy model.
- Pages cover home feed, article view, auth, bookmarks, history, trending, search, and profile flows.

### API Layer

- The FastAPI app lives in `src/newsvine_api`.
- Routers expose auth, events, news, search, trending, recommendations, and users endpoints.
- Middleware adds request IDs, structured logging, and rate limiting.
- Error handling is standardized into a consistent `{error, message, code}` response shape.
- Observability includes `/metrics` and optional OpenTelemetry wiring.

### Online Data Stores

- PostgreSQL stores relational application data such as users and durable interaction records.
- Elasticsearch stores searchable article documents and powers retrieval plus freshness-aware ranking behavior.
- Redis stores fast-changing online state such as rate-limit buckets, user profile vectors, trending sorted sets, and recommendation-side signals.

### Event and Processing Layer

- Kafka is the event backbone.
- Article ingestion publishes to the `news-articles` topic.
- User behavior flows through `user-interactions`.
- Trending jobs publish and consume `trending-updates`.
- Background workers in `src/newsvine_pipeline` consume these topics and update serving stores.

### Analytics and Batch Layer

- `analytics/dbt_newsvine` contains staging, intermediate, and marts models for analytics portability.
- `streaming` contains Spark-oriented jobs for ALS training, embedding refresh, profile updates, and article consumption.
- Redis refresh scripts bridge offline outputs back into the online recommendation path.

### Operations Layer

- `orchestration/airflow` contains a nightly batch DAG skeleton.
- `orchestration/monitoring` contains dashboards, alert rules, and kube-prometheus values for a future Kubernetes-oriented deployment.
- `loadtests` contains k6 scenarios used to validate behavior under load.

## How The Parts Connect

### 1. Article ingestion flow

- The ingestor fetches external news and publishes normalized article events to Kafka.
- The article consumer reads from `news-articles`, persists the canonical article data, and indexes it into Elasticsearch.
- Search and article retrieval then read from Elasticsearch for low-latency serving.

### 2. User interaction flow

- The frontend sends events such as reads, bookmarks, and searches to the API.
- The API publishes or records those interactions onto the user interaction stream.
- The trending scorer consumes those events and updates Redis sorted sets used by trending endpoints.
- The profile updater consumes the same stream and updates per-user interest state in Redis.
- The interactions consumer persists raw interaction data for analytics and downstream model training.

### 3. Recommendation flow

- The recommendations router combines multiple online signals.
- Content-based signals, trending weight, and collaborative signals are blended using environment-configurable weights.
- Collaborative refresh jobs can write ALS-derived user vectors and article scores back into Redis so the API can serve them online.

### 4. Search flow

- Search requests hit the FastAPI search router.
- Elasticsearch handles retrieval and relevance scoring.
- Search events are also emitted so discovery behavior becomes part of the downstream personalization pipeline.

### 5. Analytics and model refresh flow

- Raw and curated data can be transformed with dbt models in the analytics project.
- Spark-oriented jobs in `streaming` produce offline training or refresh outputs.
- Refresh scripts publish those outputs back to online stores so the API can use them without a full redeploy.

## Local Setup

### Prerequisites

- Docker Desktop with Docker Compose on Windows or macOS, or Docker Engine with the Compose plugin on Linux.
- Python 3.11+.
- Node.js 20+.
- Optional: k6 for load tests.

### 1. Create The Local Environment File

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

macOS or Linux:

```bash
cp .env.example .env
```

Adjust values only if your local ports or services differ from the defaults.

### 2. Create And Activate A Virtual Environment

Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e .[dev]
```

macOS or Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

### 3. Start The Local Stack

The fastest way to bring up the backend and its dependencies is the same on every platform:

```bash
docker compose -f docker-compose.local.yml up -d --build
```

The local stack includes the API, PostgreSQL, Redis, Kafka, Kafka Connect, Elasticsearch, and supporting workers.

### 4. Verify The API

Windows PowerShell:

```powershell
Invoke-WebRequest -Method Get -Uri http://localhost:8000/health
Invoke-WebRequest -Method Get -Uri http://localhost:8000/ready
Invoke-WebRequest -Method Get -Uri http://localhost:8000/metrics
```

macOS or Linux:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/ready
curl http://localhost:8000/metrics
```

### 5. Start The Frontend For Local UI Work

Windows PowerShell:

```powershell
Set-Location frontend
npm install
npm run dev
```

macOS or Linux:

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server proxies `/api` requests to `http://localhost:8000`.

### 6. Optional Host-Run API Workflow

If you want Docker to provide only dependencies while the API runs on your host machine:

```bash
uvicorn newsvine_api.main:app --reload
```

That workflow is useful when iterating on backend code while keeping PostgreSQL, Redis, Kafka, and Elasticsearch in containers.

## Local Service Endpoints

- API: `http://localhost:8000`
- Frontend dev server: `http://localhost:5173`
- Elasticsearch: `http://localhost:9200`
- PostgreSQL: `localhost:5432`
- Redis: `localhost:6379`
- Kafka broker: `localhost:9092`
- Kafka Connect: `http://localhost:8083`
- Kafka UI: `http://localhost:8088`

## Useful Local Commands

Run the Python test suite:

```bash
python -m pytest -q
```

Run the frontend linter:

```bash
cd frontend
npm run lint
```

Run the phase 6 load test:

```bash
k6 run loadtests/phase6_full.js
```

## Key Endpoints

- `GET /health`
- `GET /ready`
- `GET /metrics`
- `GET /search?q=technology`
- `GET /trending/global`
- `GET /recommendations/for-you`
- `GET /users/me`
- `GET /users/me/history`
- `GET /users/me/bookmarks`

## Repository Layout

- `src/newsvine_api`: FastAPI app, routers, middleware, schemas, and API-level utilities.
- `src/newsvine_pipeline`: ingestion, consumers, ranking, and profile update jobs.
- `frontend`: React client, assets, and local dev proxy setup.
- `analytics`: dbt models and analytics project files.
- `streaming`: Spark batch jobs and Spark application specs.
- `orchestration`: Airflow and monitoring assets.
- `local`: Docker support files such as Elasticsearch mappings, init SQL, and service Dockerfiles.
- `tests`: unit and integration coverage.
- `loadtests`: k6 scenarios.
- `docs`: ADRs, runbooks, demo scripts, local guides, and exported OpenAPI artifacts.