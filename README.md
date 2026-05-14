# TikTok Recipe Intelligence

Portfolio-grade data engineering project for detecting recipe content from TikTok, ingesting it into Snowflake, enriching it with an LLM, transforming it into Gold serving models, running Spark analytics, and exposing the results through FastAPI and Streamlit.

The project is intentionally multi-service: Kafka demonstrates event-driven ingestion, Snowflake stores Bronze/Silver/Gold data, OpenRouter handles semantic enrichment, dbt builds consumption views, Spark computes batch analytics, Airflow orchestrates jobs, FastAPI exposes API endpoints, and Streamlit provides a lightweight analytics UI.

## Architecture

```text
CSV seed data / TikTok monitor
        |
        | batch CSV ingestion or Kafka events
        v
Snowflake BRONZE.BRONZE_TIKTOK_RECIPES
        |
        | OpenRouter LLM enrichment
        v
Snowflake SILVER.SILVER_TIKTOK_RECIPES
        |
        | dbt quality scoring + serving views     | Spark batch analytics
        v                                v
GOLD_API_RECIPE_CATALOG          RECIPE_ANALYTICS_* tables
GOLD_STREAMLIT_RECIPE_CATALOG
GOLD_INTERNAL_RECIPE_DEBUG
GOLD_DATA_QUALITY_DAILY
        |
        +--> FastAPI on container :8000 / host 127.0.0.1:18000
        +--> Streamlit on container :8501 / host 127.0.0.1:18501
```

## Services

The Docker Compose stack lives in `docker/docker-compose.yml`.

| Service | Role | Port |
| --- | --- | --- |
| `postgres` | Airflow metadata database | internal |
| `airflow-init` | Airflow database migration and admin user bootstrap | one-shot |
| `airflow-webserver` | Airflow UI | `127.0.0.1:18080` |
| `airflow-scheduler` | Airflow scheduler | internal |
| `zookeeper` | Kafka dependency | internal |
| `kafka` | Event broker | `127.0.0.1:19092` host, `kafka:29092` containers |
| `spark-analytics` | Long-lived Spark container used by Airflow via `docker exec` | internal |
| `recipe-api` | FastAPI recipe API | `127.0.0.1:18000` |
| `streamlit` | Streamlit analytics UI | `127.0.0.1:18501` |
| `tiktok-monitor` | TikTok creator monitor and Kafka producer | internal |
| `recipe-content-recovery` | Optional multimodal recovery worker for web captions, comments, audio transcript, OCR, and external recipe pages | profile `recovery` |
| `portainer` | Container management UI | `127.0.0.1:19000`, `127.0.0.1:19443` |

## Prerequisites

- Docker Desktop
- A Snowflake account, warehouse, database, schemas, and role
- An OpenRouter API key
- Optional: a TikTok `msToken` if you want to run the creator monitor

No local Python installation is required for the Docker path.

## Quick Start

1. Clone the repository.

2. Create your environment file:

```bash
cp .env.example .env
```

3. Fill in `.env` with your Snowflake, OpenRouter, Kafka, and Airflow values.

   On Linux servers, set Airflow's UID to the user that owns the cloned repository:

```bash
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
mkdir -p logs plugins
```

4. Create the Snowflake objects by running:

```sql
-- Run in Snowflake
sql/01_create_database_objects.sql
```

5. Start the stack:

```bash
cd docker
docker compose up airflow-init
docker compose up -d
```

6. Open the services:

- Airflow: <http://localhost:18080> (`admin` / value from `_AIRFLOW_WWW_USER_PASSWORD`)
- FastAPI docs: <http://localhost:18000/docs>
- Streamlit: <http://localhost:18501>
- Portainer: <http://localhost:19000>

7. Trigger the main DAG in Airflow:

```text
tiktok_recipe_intelligence_pipeline
```

This runs CSV Bronze ingestion, Silver LLM enrichment, and dbt Gold views.

8. Trigger the analytics DAG when Silver has data:

```text
tiktok_analytics_pipeline
```

This runs Spark analytics and writes `RECIPE_ANALYTICS_*` tables to the Gold schema.

## Environment Variables

Use `.env.example` as the source of truth. Required values:

- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DB`
- `SNOWFLAKE_ROLE`
- `OPENROUTER_API_KEY`

Important defaults:

- Inside Docker, Kafka should be `kafka:29092`
- From the host machine, Kafka should be `localhost:19092`
- Bronze schema defaults to `BRONZE`
- Silver schema defaults to `SILVER`
- Gold schema defaults to `GOLD`

## Manual Commands

Run these from the repository root when working locally or from `/opt/airflow` inside Airflow containers.

```bash
python -m scripts.load_bronze --input-dir data/raw
python -m scripts.enrich_silver --limit 100
python run_dbt.py run
```

Run TikTok discovery with resume state, creator-level daily skipping, and caption enrichment:

```bash
cd docker
docker compose run --no-deps --rm tiktok-monitor bash -lc "Xvfb :99 -screen 0 1280x1024x24 -nolisten tcp > /tmp/xvfb.log 2>&1 & export DISPLAY=:99; python -u -m scripts.tiktok_recipe_discovery --session-timeout 180 --max-rows 500 --per-creator 10 --sleep-min 12 --sleep-max 20 --caption-enrichment metadata"
```

For slower but more aggressive caption recovery, use `--caption-enrichment browser`. The scraper first uses TikTokApi captions, then tries TikTok web metadata, embedded JSON, oEmbed, and optional Playwright DOM extraction. It keeps the longest useful caption and records the source in Bronze `RAW_PAYLOAD`.

Silver keeps the data lineage explicit:

- `ORIGINAL_DESCRIPTION`: the original TikTokApi caption when available.
- `RECOVERED_TEXT`: optional text recovered from web metadata or future comments/OCR/audio jobs.
- `EVIDENCE_TEXT`: the combined evidence sent to the LLM.
- `EVIDENCE_QUALITY_SCORE`: deterministic pre-LLM score used to rank and filter evidence.
- `FINAL_RECIPE_TEXT`: a clean Markdown recipe card generated by the LLM from evidence only.
- `MISSING_RECIPE_INFO`: quantities, timings, or steps that were not specified in the source evidence.

Run adaptive content recovery for low-quality recipes:

```bash
cd docker
docker compose --profile recovery run --rm recipe-content-recovery python -u -m scripts.recover_recipe_content --method adaptive --limit 25
```

The default `adaptive` mode is a waterfall focused on useful evidence instead of noisy failures:

1. recover the best available web caption,
2. if the evidence score is still low, run local speech-to-text,
3. if confidence is still low, run OCR on sampled video frames,
4. try external recipe links only when the caption actually contains an external URL,
5. try comments only when explicitly requested with `--enable-comments`.

The worker skips methods that recently failed or returned empty output for the same video. The default retry window is 24 hours and can be overridden with `--retry-failed-after-hours` or bypassed with `--force-retry`. Audio/video downloads are cached under `RECOVERY_MEDIA_CACHE_DIR`, so audio and OCR do not redownload the same TikTok media on every run.

Targeted methods are still available:

```bash
docker compose --profile recovery run --rm recipe-content-recovery python -u -m scripts.recover_recipe_content --method audio_transcript --limit 10 --whisper-model tiny
docker compose --profile recovery run --rm recipe-content-recovery python -u -m scripts.recover_recipe_content --method ocr --limit 10 --frame-count 6 --ocr-engine auto
docker compose --profile recovery run --rm recipe-content-recovery python -u -m scripts.recover_recipe_content --method adaptive --limit 10 --enable-comments
```

Recovery attempts are stored in `SILVER.RECIPE_CONTENT_RECOVERY`. Usable evidence is stored separately in `SILVER.SILVER_RECIPE_EVIDENCE` with source type, length, quality score, and recipe-signal flags. The worker also updates `CONTROL.RECIPE_PROCESSING_QUEUE` with statuses such as `needs_ocr`, `needs_audio`, `ready_for_llm_classification`, and `ready_for_llm_extraction`. Re-run Silver afterwards so the LLM can generate better final recipe cards from the best scored evidence:

```bash
docker compose exec airflow-scheduler bash -lc "cd /opt/airflow && python -m scripts.enrich_silver --limit 250 --only-recovered"
```

Use `--reprocess-all` only when intentionally rebuilding Silver for every Bronze row. The normal pipeline uses `--only-recovered` to save OpenRouter calls.

By default, Silver enrichment hides verbose Snowflake connector logs and shows a `tqdm` progress bar. Set `SNOWFLAKE_LOG_LEVEL=INFO` only when debugging low-level Snowflake connectivity.
Third-party recovery logs are also quiet by default through `DEPENDENCY_LOG_LEVEL=WARNING`.

Run the Spark job from Docker:

```bash
docker exec recipe-spark-analytics bash -c "/opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages net.snowflake:snowflake-jdbc:3.15.1,net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4 /app/scripts/spark_recipe_analytics.py"
```

## API

FastAPI entrypoint: `api/main.py`.

Main routes:

- `GET /health`
- `GET /recipes`
- `GET /recipes/filters`
- `GET /recipes/{raw_id}`

The API reads from `GOLD.GOLD_API_RECIPE_CATALOG`.

## Streamlit

Streamlit entrypoint: `app/streamlit_app.py`.

The app reads from `GOLD.GOLD_STREAMLIT_RECIPE_CATALOG` and provides filters, metrics, recipe cards, and a tabular view.

## dbt

dbt project path: `dbt_project/`.

Current models:

- `stg_silver_tiktok_recipes`
- `stg_silver_recipe_evidence`
- `int_recipe_evidence_by_raw`
- `int_recipe_quality_scoring`
- `gold_tiktok_recipe_catalog`
- `gold_api_recipe_catalog`
- `gold_streamlit_recipe_catalog`
- `gold_internal_recipe_debug`
- `gold_data_quality_daily`

Run:

```bash
python run_dbt.py run
python run_dbt.py test
```

## Notebook Smoke Tests

Use `notebooks/component_smoke_tests.ipynb` to test the stack one component at a time:

- environment variables
- Docker Compose services
- Snowflake connectivity
- Bronze/Silver/Gold table visibility
- LLM response parsing
- dbt command wiring
- API health
- Streamlit availability
- Kafka host/container settings
- Spark command template

## Portfolio Pages

The `docs/` folder includes two English portfolio pages:

- `portfolio_project_overview.md`: high-level project story, architecture, value, and outcomes.
- `portfolio_technical_deep_dive.md`: detailed technology-by-technology explanation of design choices and impact.

## Known Limitations

- Airflow, API, and Streamlit currently install Python dependencies at container startup. This works for a portfolio demo but should become custom Docker images for a cleaner production story.
- API and Streamlit use slim runtime requirement files (`requirements-api.txt` and `requirements-streamlit.txt`) so they do not install the full development stack.
- The Spark container is kept alive and triggered with `docker exec`. This is pragmatic locally, but a dedicated Spark image or Spark-on-Kubernetes style runner would be cleaner.
- TikTok scraping is inherently unstable because sessions, bot detection, `msToken`, browser mode, and Playwright behavior change over time.
- The Snowflake DDL is intended for first-time setup. Do not run `CREATE OR REPLACE TABLE` against valuable existing data without backing it up.

## Portfolio Positioning

This project is built to demonstrate a realistic data platform shape without pretending to be a full production platform. It shows ingestion, eventing, warehouse modeling, orchestration, LLM enrichment, Spark analytics, API serving, and BI-style exploration in one coherent local stack.
