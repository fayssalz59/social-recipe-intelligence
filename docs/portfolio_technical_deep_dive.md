# TikTok Recipe Intelligence - Technical Deep Dive

## Technical Intent

This project was built to show a realistic data engineering architecture around social media recipe intelligence. The stack is intentionally broader than a minimal solution because the goal is to demonstrate how different data platform components work together and where each one belongs.

The key architectural principle is separation of responsibility:

- Kafka handles events.
- Snowflake stores analytical data.
- Bronze/Silver/Gold separates raw, enriched, and consumption-ready layers.
- OpenRouter provides semantic enrichment.
- dbt creates serving models.
- PySpark computes batch analytics.
- Airflow orchestrates jobs.
- FastAPI serves curated data.
- Streamlit gives an interactive analytics surface.
- Docker makes the platform reproducible locally.

## Snowflake

### Why I Used It

Snowflake is the central analytical warehouse of the project. It is a strong fit because the project needs a cloud data warehouse that can store raw records, enriched records, serving views, and analytics tables in a clean schema structure.

### What It Provides Here

Snowflake provides:

- a durable storage layer for all pipeline outputs;
- separate schemas for Bronze, Silver, Gold, and Control data;
- SQL-based modeling and querying;
- a shared source of truth for dbt, Spark, FastAPI, and Streamlit;
- easy integration with Python connectors and Spark connectors.

### How It Is Used

The warehouse is organized into schemas:

- `BRONZE` stores raw TikTok recipe records.
- `SILVER` stores LLM-enriched and standardized recipe records.
- `GOLD` stores serving views and analytics tables.
- `CONTROL` stores operational state such as seen TikTok videos.

The key tables include:

- `BRONZE.BRONZE_TIKTOK_RECIPES`
- `SILVER.SILVER_TIKTOK_RECIPES`
- `GOLD.GOLD_API_RECIPE_CATALOG`
- `GOLD.GOLD_STREAMLIT_RECIPE_CATALOG`
- `GOLD.RECIPE_ANALYTICS_SUMMARY`
- `GOLD.RECIPE_ANALYTICS_BY_CUISINE`
- `GOLD.RECIPE_ANALYTICS_BY_INGREDIENT`
- `GOLD.RECIPE_ANALYTICS_BY_LANGUAGE`
- `GOLD.RECIPE_ANALYTICS_BY_MODEL`

### What It Demonstrates

This demonstrates data warehouse design, schema separation, idempotent writes, downstream consumption patterns, and integration with multiple compute tools.

## Bronze / Silver / Gold Modeling

### Why I Used It

The medallion pattern makes the pipeline easier to understand and defend. Each layer answers a different question:

- Bronze: what did we receive?
- Silver: what did we understand from it?
- Gold: what can consumers use?

### Bronze

Bronze stores raw or near-raw source records. It keeps the original title, description, TikTok URL, source file, source metadata, raw payload, and record hash.

The purpose of Bronze is traceability. If an enrichment result looks wrong, I can go back to the original record and understand what the model received.

### Silver

Silver stores standardized and enriched records. The LLM extracts:

- language;
- vegetarian classification;
- cuisine style;
- main ingredient.

Silver also stores processing confidence, model name, raw LLM response, and record hash.

The purpose of Silver is semantic structure. This is where unstructured social content becomes analytical data.

### Gold

Gold stores consumption-oriented models:

- API catalog;
- Streamlit catalog;
- Spark analytics aggregates.

The purpose of Gold is usability. API routes and dashboards should not depend on raw operational tables.

### What It Demonstrates

This demonstrates clean data modeling, separation of concerns, traceability, and the ability to design data products from raw data.

## Kafka

### Why I Used It

Kafka is used to represent the event-driven part of the architecture. TikTok creator monitoring is naturally event-based: when a new video is detected, a new event can be emitted.

Even though the local dataset is small, Kafka is important because it shows how the platform could scale to more sources and more asynchronous ingestion flows.

### What It Provides Here

Kafka provides:

- decoupling between detection and ingestion;
- a normalized event stream for new videos;
- a future path for multi-platform ingestion;
- a realistic streaming component in the architecture.

### How It Is Used

The TikTok monitor publishes events to a topic such as `new_tiktok_video_detected`. A Kafka consumer reads the events, normalizes them, and merges them into Snowflake Bronze.

The project distinguishes two Kafka addresses:

- `localhost:19092` for host-machine commands;
- `kafka:29092` for container-to-container communication.

### What It Demonstrates

This demonstrates event-driven design, producer/consumer separation, schema normalization, and awareness of Docker networking.

## TikTok Monitor

### Why I Used It

The TikTok monitor makes the project feel connected to a real source rather than only a static CSV. It watches configured creators, detects recent videos, and emits events for new content.

### What It Provides Here

The monitor provides:

- a source connector pattern;
- creator-based content discovery;
- event publication to Kafka;
- deduplication through a `CONTROL.SEEN_TIKTOK_VIDEOS` table;
- a foundation for future Instagram Reels or YouTube Shorts connectors.

### Design Note

TikTok scraping is fragile by nature. Sessions, `msToken`, browser mode, bot detection, and TikTok page behavior can change. The project treats this component as a realistic but best-effort ingestion source.

### What It Demonstrates

This demonstrates source ingestion design, operational state tracking, async client usage, browser automation constraints, and practical handling of unstable external sources.

## OpenRouter / LLM Enrichment

### Why I Used It

The raw TikTok descriptions are unstructured. Traditional parsing would be brittle because captions vary widely by language, tone, format, and completeness.

An LLM is useful here because the enrichment task is semantic:

- identify the language;
- infer whether a recipe is vegetarian;
- infer a cuisine style;
- identify the main ingredient.

### What It Provides Here

The LLM turns messy text into structured fields that downstream analytics can use.

The expected response shape is:

```json
{
  "lang": "en",
  "is_veg": false,
  "cuisine": "italian",
  "ingredient": "pasta"
}
```

The parser accepts:

- a direct JSON object;
- a single-item list containing one object;
- short keys like `lang`;
- business keys like `recipe_language`.

### Reliability Work

The enrichment script validates model responses with Pydantic. A previous bug caused valid dictionaries to be rejected because the code normalized the payload but validated the original raw object. That has been corrected with a dedicated `normalize_llm_enrichment` function.

### What It Demonstrates

This demonstrates LLM integration in a data pipeline, schema validation, retry handling, confidence scoring, and the difference between model output and trusted warehouse data.

## dbt

### Why I Used It

dbt is used for SQL-based transformations and serving models. It is the right tool for clear, versioned transformations that sit close to the warehouse.

### What It Provides Here

dbt provides:

- a staging model over Silver;
- deduplication logic;
- clean Gold views for API and Streamlit;
- a transformation layer that is easy to test and document.

### How It Is Used

Current models include:

- `stg_silver_tiktok_recipes`
- `gold_tiktok_recipe_catalog`
- `gold_api_recipe_catalog`
- `gold_streamlit_recipe_catalog`

The staging model deduplicates records by `raw_id` and keeps the latest processed version. The Gold models rename and select fields for specific consumers.

### What It Demonstrates

This demonstrates analytics engineering practices: source definitions, staging models, marts, consumer-specific views, and a clean SQL transformation layer.

## PySpark

### Why I Used It

Spark is used for heavier batch analytics. In this project, Spark is not replacing dbt. It has a different role: computing aggregate analytics from the Silver layer and writing result tables back to Snowflake.

This distinction is important. dbt creates serving views. Spark demonstrates distributed batch processing.

### What It Provides Here

Spark computes:

- catalog summary metrics;
- recipes by cuisine;
- recipes by ingredient;
- recipes by language;
- recipes by LLM model.

The outputs are written to Gold tables such as:

- `RECIPE_ANALYTICS_SUMMARY`
- `RECIPE_ANALYTICS_BY_CUISINE`
- `RECIPE_ANALYTICS_BY_INGREDIENT`
- `RECIPE_ANALYTICS_BY_LANGUAGE`
- `RECIPE_ANALYTICS_BY_MODEL`

### What It Demonstrates

This demonstrates Spark/Snowflake integration, batch analytics design, aggregation logic, and the ability to use multiple transformation tools for different purposes.

## Airflow

### Why I Used It

Airflow orchestrates the pipeline. It does not host the API or dashboard. Its job is to coordinate batch steps and make pipeline execution visible.

### What It Provides Here

Airflow provides:

- scheduled pipeline execution;
- retries;
- task dependency graphs;
- operational visibility;
- separation between orchestration and service hosting.

### DAGs

The project has multiple DAGs:

- `tiktok_recipe_intelligence_pipeline`: Bronze ingestion, Silver enrichment, dbt Gold.
- `tiktok_analytics_pipeline`: Silver enrichment and Spark analytics.
- `tiktok_creator_monitor_pipeline`: TikTok monitor, Kafka consumer, Silver enrichment, dbt Gold.

### What It Demonstrates

This demonstrates orchestration, dependency management, local Airflow deployment, and awareness that Airflow should trigger jobs rather than behave like a permanent application server.

## FastAPI

### Why I Used It

FastAPI exposes the curated Gold catalog as an API. This proves that the pipeline produces data that can be consumed by applications, not only viewed in notebooks or dashboards.

### What It Provides Here

The API provides:

- `/health` for Snowflake connectivity;
- `/recipes` for filtered recipe retrieval;
- `/recipes/filters` for UI filter values;
- `/recipes/{raw_id}` for single-record lookup.

The API reads from `GOLD_API_RECIPE_CATALOG`, not from Bronze or raw Silver tables.

### What It Demonstrates

This demonstrates API serving, parameterized SQL queries, separation between warehouse models and application routes, and deployment through Docker.

## Streamlit

### Why I Used It

Streamlit gives the project an interactive analytics layer. It lets a reviewer explore the enriched data without writing SQL or calling API endpoints manually.

### What It Provides Here

The Streamlit app now includes:

- dashboard KPIs;
- filters for language, cuisine, ingredient, LLM model, vegetarian status, confidence, and text search;
- charts for cuisines, languages, ingredients, and model usage;
- recipe cards with TikTok links and enrichment metadata;
- full tabular exploration;
- Spark analytics visualizations when the aggregate tables exist;
- data quality checks for unknown, missing, and low-confidence values;
- Bronze/Silver/Gold layer counts when available.

### What It Demonstrates

This demonstrates data product thinking. The dashboard is not only a table; it helps inspect catalog coverage, enrichment quality, and analytics outputs.

## Docker and Docker Compose

### Why I Used It

The project uses Docker Compose to make a multi-service data platform runnable locally. A reviewer should be able to clone the repo, configure environment variables, and start the platform without manually installing every service.

### What It Provides Here

Docker Compose runs:

- Postgres for Airflow metadata;
- Airflow webserver and scheduler;
- Kafka and Zookeeper;
- Spark analytics container;
- TikTok monitor container;
- FastAPI container;
- Streamlit container;
- Portainer.

### What It Demonstrates

This demonstrates local platform engineering, container networking, service composition, environment-based configuration, and reproducible development workflows.

### Improvement Path

The next production-grade improvement is to replace runtime `pip install` commands with custom images:

- Airflow image with all DAG dependencies;
- API image with FastAPI dependencies;
- Streamlit image with dashboard dependencies;
- Spark image with the Snowflake connector setup.

## Portainer

### Why I Used It

Portainer is included to make the local Docker stack easier to inspect. It is useful in a portfolio project because it helps visualize running services and debug container status.

### What It Provides Here

Portainer provides:

- container visibility;
- logs and status inspection;
- easier local debugging;
- a UI for the Docker environment.

### What It Demonstrates

This demonstrates comfort with operational tooling around containers, not only writing pipeline code.

## Configuration Management

### Why It Matters

The project depends on external systems: Snowflake, OpenRouter, Kafka, Airflow, and optionally TikTok. Hardcoding credentials or environment-specific values would make the project unsafe and hard to reuse.

### How It Is Handled

The project uses:

- `.env` for local secrets and configuration;
- `.env.example` as the documented template;
- environment variables in Docker Compose;
- configurable Snowflake schema names;
- separate Kafka bootstrap settings for host and container contexts.

### What It Demonstrates

This demonstrates secure configuration patterns, portability, and awareness of local vs container execution contexts.

## Testing and Smoke Checks

### Why I Added It

Because this is a multi-service project, not every failure appears in one place. A notebook-based smoke test makes it possible to validate components independently.

### What It Covers

The smoke test notebook covers:

- environment variables;
- Docker Compose configuration;
- Snowflake connectivity;
- Bronze/Silver/Gold table visibility;
- LLM parser behavior without network calls;
- dbt command wiring;
- API health;
- Streamlit availability;
- Kafka bootstrap sanity;
- Spark command template.

### What It Demonstrates

This demonstrates practical debugging and component-level validation for a distributed local data stack.

## Key Engineering Tradeoffs

### Keeping Kafka

Kafka is not strictly necessary for a small local dataset, but it is valuable because the project is designed as an extensible social content platform. Keeping Kafka makes the event-driven architecture visible.

### Keeping Spark

Spark is not strictly necessary for small aggregate tables, but it demonstrates batch analytics and Snowflake/Spark integration. It is positioned as complementary to dbt, not as a replacement.

### Keeping Airflow

Airflow is not needed for one manual script, but it is appropriate for orchestrating repeatable data jobs and showing operational visibility.

### Treating TikTok as Best Effort

TikTok scraping is unstable. The project keeps it because it is realistic and useful, but it also supports CSV ingestion so the platform can still be demonstrated without live scraping.

## Interview Talking Points

In an interview, I would highlight:

- why I separated Bronze, Silver, and Gold;
- how LLM output is validated before entering Silver;
- why dbt and Spark both exist but serve different purposes;
- how Kafka enables future multi-platform ingestion;
- how Airflow orchestrates jobs without hosting permanent services;
- how API and Streamlit consume Gold models instead of raw tables;
- what I would improve next for production readiness;
- how I debugged real issues such as invalid LLM schema handling and Docker/Airflow/Spark orchestration.

## Production Readiness Roadmap

The current project is portfolio-grade and local-first. The next steps toward stronger production readiness are:

1. Build custom Docker images and remove runtime dependency installation.
2. Add dbt tests and run them in Airflow.
3. Add unit tests for parser and event normalization logic.
4. Add API response models and stricter error handling.
5. Add CI checks for Python syntax, dbt parsing, and Docker Compose validation.
6. Add a demo mode that does not require live TikTok access.
7. Add monitoring around enrichment failure rates and low-confidence records.
8. Add incremental or snapshot strategy for Spark analytics if historical trends are needed.

## Final Technical Summary

TikTok Recipe Intelligence is a compact but complete data platform. It uses the right tools for distinct jobs: Kafka for events, Snowflake for storage, LLMs for semantic enrichment, dbt for serving transformations, Spark for batch analytics, Airflow for orchestration, FastAPI for API access, Streamlit for exploration, and Docker for local reproducibility.

The project is valuable as a portfolio piece because it shows not only tool familiarity, but architectural judgment: each component has a reason to exist, each layer has a clear responsibility, and the system can be explained from raw ingestion to final user-facing data product.
