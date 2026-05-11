# Technical Audit - TikTok Recipe Intelligence

This audit is based on the current repository state and the runtime context from the Airflow logs. It is ordered by practical impact for making the project credible, reproducible, and portfolio-grade.

## 1. Executive Summary

The project already has a strong portfolio shape: Snowflake medallion modeling, Kafka events, OpenRouter enrichment, dbt Gold views, Spark analytics, Airflow orchestration, FastAPI, Streamlit, and Docker Compose are all present. The architecture is coherent as a demonstrative data platform.

The main gap is not ambition. The main gap is operational polish: runtime `pip install`, hardcoded container names, generated artifacts in the working tree, incomplete bootstrap DDL, limited tests, and a few config mismatches. These are fixable without removing Kafka, Spark, or Airflow.

## 2. Repo Map

```text
api/
  main.py                         FastAPI app reading Gold API catalog
app/
  streamlit_app.py                Streamlit app reading Gold Streamlit catalog
config/
  creators.json                   TikTok creators monitored by the crawler
dags/
  tiktok_main_pipeline.py         CSV Bronze -> Silver enrichment -> dbt Gold
  tiktok_analytics_pipeline.py    Silver enrichment -> Spark analytics
  tiktok_creator_monitor_pipeline.py TikTok monitor -> Kafka consumer -> Silver -> dbt
dbt_project/
  models/staging/                 dbt Silver staging model
  models/marts/                   Gold serving views for API and Streamlit
docker/
  docker-compose.yml              Main local stack
  airflow.env                     Airflow config
  tiktok-monitor/Dockerfile       Custom TikTok monitor image
scripts/
  load_bronze.py                  CSV to Bronze ingestion
  enrich_silver.py                OpenRouter enrichment to Silver
  spark_recipe_analytics.py       Spark analytics writer
  kafka_*                         Kafka producer/consumer scripts
  tiktok_*                        TikTok client and creator monitor
sql/
  01_create_database_objects.sql  Snowflake bootstrap DDL
docs/
  portfolio_project_overview.md   Portfolio project overview page
  portfolio_technical_deep_dive.md Technical portfolio deep-dive page
  technical_audit.md              This audit
notebooks/
  component_smoke_tests.ipynb     Brick-by-brick smoke tests
```

## 3. Critical Findings

### P0 - Silver enrichment parser rejected valid LLM payloads

Status: fixed in `scripts/enrich_silver.py`.

The code parsed a valid dict or single-item list, assigned it to `enrichment`, then accidentally validated `enrichment_raw`. Logs confirmed repeated failures such as valid payloads being rejected with `Invalid LLM schema`. The fix introduces `normalize_llm_enrichment`, accepts both `dict` and `[dict]`, and supports both short keys and business keys.

Impact: without this, Airflow can look successful while the enrichment layer is mostly empty or stale.

### P0 - Bootstrap DDL did not match Kafka consumer writes

Status: fixed in `sql/01_create_database_objects.sql`.

`kafka_tiktok_event_consumer.py` writes `PLATFORM`, `CONTENT_ID`, `CREATOR_USERNAME`, `DESCRIPTION_IS_PARTIAL`, and `RAW_PAYLOAD`, but the original Bronze table DDL did not create those columns. The DDL now includes the event-driven columns and creates `CONTROL.SEEN_TIKTOK_VIDEOS`.

Impact: a new user following README setup could start the stack and hit Snowflake column errors when trying the TikTok/Kafka path.

### P1 - Docker services install dependencies at runtime

Airflow, Streamlit, and API services run `pip install` in container startup commands. This works locally but is slow, fragile, and weak as a portfolio signal.

Recommended action:

- create `docker/airflow/Dockerfile`;
- create `docker/api/Dockerfile`;
- create `docker/streamlit/Dockerfile`;
- optionally create `docker/spark/Dockerfile` with Snowflake connector packages pre-resolved;
- change Compose services from `image: python:...` plus `pip install` to `build:`.

### P1 - Airflow Spark orchestration is pragmatic but brittle

The analytics DAG uses:

```text
docker exec docker-spark-analytics-1 ...
```

This assumes the Compose project name creates exactly `docker-spark-analytics-1`. That may change depending on checkout folder, Compose project name, or user settings.

Recommended action:

- set `container_name: spark-analytics` or use Compose service DNS plus a real job trigger;
- move the Spark command into a small script such as `scripts/run_spark_analytics.sh`;
- preinstall Python/Snowflake/Spark dependencies in a Spark image.

### P1 - Kafka bootstrap config must be explicit by execution context

The repo correctly distinguishes host `localhost:19092` and container `kafka:29092`, but logs show the creator monitor previously trying a host-style address from inside Airflow, which fails. `.env.example` now defaults to `kafka:29092`, which is the correct value for Docker-run jobs.

Recommended action:

- document host-vs-container values everywhere;
- override bootstrap explicitly in host commands;
- keep Docker `.env` pointed at `kafka:29092`.

### P1 - Generated/runtime artifacts should not live in the portfolio tree

Observed artifacts include `__pycache__`, Airflow logs, dbt `target`, dbt logs, `.venv`, `.venv2`, `.streamlit/secrets.toml`, and `airflow-webserver.pid`.

Status: `.gitignore` added.

Recommended action:

- keep these out of GitHub;
- remove stale `airflow-webserver.pid` before packaging the repo;
- avoid committing real Streamlit secrets.

## 4. Architecture Diagnosis

The intended architecture is sound:

- Kafka is for detection events and future multi-platform ingestion.
- Snowflake is the analytical store.
- Bronze/Silver/Gold gives a clear modeling story.
- OpenRouter/LLM enriches unstructured text.
- dbt builds serving-oriented views.
- Spark computes complementary batch analytics.
- Airflow orchestrates jobs.
- FastAPI and Streamlit expose results.

The main architectural improvement is to make boundaries more explicit:

- `scripts/` currently mixes app logic, jobs, clients, and utilities. For a polished repo, split later into `src/recipe_intelligence/` modules and keep `scripts/` as thin entrypoints.
- The TikTok monitor should emit a platform-neutral event model. `SourceContentItem` is a good start; extend it to support Instagram and YouTube Shorts without changing Bronze/Silver semantics.
- dbt and Spark should both write Gold, but with clearly named domains: dbt for serving views, Spark for analytical aggregates. The current table names support this separation.

## 5. Docker Review

What is good:

- almost every major service is represented in Compose;
- all services share a `data-network`;
- Kafka listeners are configured for host and container access;
- Portainer is useful for local debugging;
- the TikTok monitor has a custom image with browser dependencies.

What to improve:

- runtime dependency installs in Airflow/API/Streamlit;
- no API/Streamlit healthchecks;
- hardcoded Spark container name in Airflow;
- Docker socket mounted into Airflow is powerful and should be documented as a local-only tradeoff;
- Postgres dependency on API is unnecessary because API reads Snowflake, not Postgres;
- Streamlit depends implicitly on Snowflake/dbt but Compose cannot express that external dependency.

## 6. Airflow Review

What is good:

- DAGs are readable and scoped;
- Airflow is used for orchestration, not for permanent services;
- main batch DAG and Spark analytics DAG are separated.

Risks:

- `rm -rf target dbt_packages logs` inside DAGs is operationally noisy;
- DAG commands are shell-heavy and repeated;
- no task-level data quality gates after Bronze or Silver;
- Silver enrichment exceptions are logged per row but do not fail the task if all rows fail;
- dependency installation at startup increases the chance of unhealthy webserver/scheduler.

Recommended next step:

- make enrichment return counters and fail the job if `rows_found > 0` and `rows_success == 0`;
- add a dbt `test` task after `dbt run`;
- add Airflow Variables or env-driven limits for enrichment;
- move repeated Bash commands to versioned scripts.

## 7. Snowflake and Modeling Review

What is good:

- Bronze ingestion is idempotent using `RECORD_HASH`;
- Silver uses a merge on `RAW_ID`;
- dbt staging deduplicates by latest `processed_at`;
- API and Streamlit get their own Gold views.

Risks:

- Snowflake DDL uses `CREATE OR REPLACE TABLE`, which is fine for first setup but destructive if rerun on real data;
- unique constraints in Snowflake are metadata-only unless enforced by logic;
- no dbt tests currently enforce non-null IDs, accepted values, or uniqueness;
- Bronze schema evolved for events, but `load_bronze.py` only loads CSV fields, which is acceptable but should be documented.

Recommended dbt tests:

- `unique` and `not_null` on `raw_id`;
- accepted values or not-null checks for `recipe_language`;
- relationships from Gold back to staging;
- freshness checks on Silver if the source is expected to update.

## 8. Spark Review

What is good:

- Spark reads Silver directly from Snowflake;
- it avoids `CREATOR_USERNAME`, matching the real Silver schema;
- output tables are domain-specific and easy to explain.

Risks:

- Spark 3.5.7 with a connector package named for Spark 3.4 is a compatibility warning;
- mode `overwrite` is simple, but can drop historical aggregate snapshots if you later want trends;
- no row-count guard before overwriting Gold analytics;
- dependencies are downloaded at runtime.

Recommended action:

- pin a connector version tested with your Spark version or downgrade Spark image to the connector target;
- add `RUN_ID` or `GENERATED_AT` strategy if you want historical analytics snapshots;
- prebuild a Spark image.

## 9. API Review

What is good:

- route set is simple and relevant;
- query parameters are parameterized;
- `/health` validates Snowflake connectivity.

Status: Gold schema hardcoding fixed to use `SNOWFLAKE_SCHEMA_GOLD`.

Recommended action:

- add response models with Pydantic;
- add pagination metadata;
- avoid leaking raw exception strings in production responses;
- add a Docker healthcheck hitting `/health`.

## 10. Streamlit Review

What is good:

- app is small and focused;
- filters and metrics are aligned with the enriched fields;
- it reads the dbt Gold Streamlit view rather than raw Silver.

Status: Gold schema hardcoding fixed to use `SNOWFLAKE_SCHEMA_GOLD`.

Recommended action:

- add charts for cuisine, ingredient, language, and model confidence;
- optionally read Spark `RECIPE_ANALYTICS_*` tables for analytics sections;
- fix visible text encoding in the title if it appears garbled in the UI;
- use `.streamlit/secrets.toml` only locally, never in GitHub.

## 11. TikTok Monitor Review

What is good:

- platform-neutral `SourceContentItem` exists;
- monitor emits Kafka events;
- seen-content table avoids duplicate event publication;
- Dockerfile includes Playwright/browser dependencies and `xvfb-run`.

Risks:

- TikTok scraping is unstable by nature;
- `headless=False` inside containers is fragile but sometimes necessary;
- monitor run from Airflow uses the local Python path, while a dedicated `tiktok-monitor` service also exists, creating two execution modes.

Recommended action:

- keep TikTok as "best effort";
- make the monitor optional in README;
- add a fake/demo producer path so the platform can be evaluated without live TikTok;
- extend the source model before adding Instagram/YouTube.

## 12. Portfolio-Grade Quick Wins

1. Add screenshots or architecture diagram to README.
2. Add custom Docker images for Airflow, API, Streamlit, and Spark.
3. Add dbt tests and a `dbt test` Airflow task.
4. Add parser unit tests for `normalize_llm_enrichment`.
5. Add a `make` equivalent or scripts: `start`, `stop`, `bootstrap`, `smoke-test`.
6. Clean generated artifacts before pushing.
7. Add API response models and Docker healthchecks.
8. Add charts to Streamlit using Gold Spark analytics tables.
9. Add a `demo` mode that runs without TikTok scraping.
10. Add a short "interview talking points" section to the portfolio page.

## 13. Target Architecture

Keep the visible multi-brick architecture, but make packaging cleaner:

```text
docker/
  airflow/Dockerfile
  api/Dockerfile
  streamlit/Dockerfile
  spark/Dockerfile
  tiktok-monitor/Dockerfile

src/recipe_intelligence/
  config.py
  snowflake.py
  enrichment/
  ingestion/
  events/
  sources/
    tiktok.py
    instagram.py
    youtube_shorts.py

scripts/
  load_bronze.py
  enrich_silver.py
  run_spark_analytics.py

tests/
  test_enrich_silver.py
  test_kafka_event_normalization.py
```

This keeps the project demonstrative while making it look deliberate rather than experimental.

## 14. Recommended Next Sprint

Priority order:

1. Build custom Docker images and remove runtime `pip install`.
2. Replace hardcoded Spark container name with a stable name or script wrapper.
3. Add dbt tests and run them in Airflow.
4. Add unit tests for enrichment parser and Kafka normalization.
5. Add a demo mode that can run fully from bundled CSV data.
6. Add Streamlit charts from Spark analytics tables.
7. Add API response models and healthcheck.

This sprint would move the repo from "it runs on my machine" to "a reviewer can clone it, configure secrets, run it, and understand it."
