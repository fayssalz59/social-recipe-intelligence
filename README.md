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
        | dbt serving views              | Spark batch analytics
        v                                v
GOLD_API_RECIPE_CATALOG          RECIPE_ANALYTICS_* tables
GOLD_STREAMLIT_RECIPE_CATALOG
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
- `gold_tiktok_recipe_catalog`
- `gold_api_recipe_catalog`
- `gold_streamlit_recipe_catalog`

Run:

```bash
python run_dbt.py run
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
- The Spark container is kept alive and triggered with `docker exec`. This is pragmatic locally, but a dedicated Spark image or Spark-on-Kubernetes style runner would be cleaner.
- TikTok scraping is inherently unstable because sessions, bot detection, `msToken`, browser mode, and Playwright behavior change over time.
- The Snowflake DDL is intended for first-time setup. Do not run `CREATE OR REPLACE TABLE` against valuable existing data without backing it up.

## Portfolio Positioning

This project is built to demonstrate a realistic data platform shape without pretending to be a full production platform. It shows ingestion, eventing, warehouse modeling, orchestration, LLM enrichment, Spark analytics, API serving, and BI-style exploration in one coherent local stack.
