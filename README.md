# TikTok Recipe Intelligence Pipeline

Production-oriented starter project for ingesting TikTok recipe metadata into Snowflake, enriching records with OpenRouter, orchestrating the pipeline with Airflow, transforming Gold models with dbt, and exploring the catalog with Streamlit.

## Current Snowflake target

This repo is aligned to the schema you finalized:

- Database: `TIKTOK_PORTFOLIO_DB`
- Schemas: `BRONZE`, `SILVER`, `GOLD`
- Bronze table: `BRONZE.BRONZE_TIKTOK_RECIPES`
- Silver table: `SILVER.SILVER_TIKTOK_RECIPES`

The code assumes:

- Bronze contains: `RAW_ID`, `TITLE`, `DESCRIPTION`, `URL_TIKTOK`, `SOURCE_FILE`, `RECORD_HASH`, `INGESTED_AT`
- Silver contains: `SILVER_ID`, `RAW_ID`, `ORIGINAL_TITLE`, `ORIGINAL_DESCRIPTION`, `URL_TIKTOK`, `RECIPE_LANGUAGE`, `IS_VEGETARIAN`, `CUISINE_STYLE`, `MAIN_INGREDIENT`, `PROCESSING_CONFIDENCE`, `MODEL_NAME`, `LLM_RAW_RESPONSE`, `PROCESSED_AT`, `RECORD_HASH`

## What is included

- Docker Compose stack for Airflow, Spark, Kafka, Zookeeper, Postgres, and Streamlit.
- Snowflake DDL for Bronze and Silver layers.
- Python ingestion script for CSV -> Bronze.
- Python enrichment script for Bronze -> Silver using OpenRouter.
- Airflow DAG with TaskFlow gate and operational tasks.
- dbt project for Gold view generation.
- Streamlit app using `st.connection("snowflake")`.

## Security note

Do **not** keep credentials or API keys in source code. Put them in `.env`, `.streamlit/secrets.toml`, or your deployment secret store.
If a key was pasted into a conversation or committed to a repo, rotate it.

## Quick start

1. Copy `.env.example` to `.env` and fill in your real values.
2. If you already executed the Snowflake DDL in `sql/01_create_database_objects.sql`, do **not** recreate the tables.
3. Start the stack:

```bash
cd docker
docker compose up airflow-init
docker compose up -d