# DuckDB/Vercel Lite Variant

This branch keeps the demo path intentionally small:

- DuckDB replaces Snowflake for serving the curated recipe catalog.
- FastAPI reads `data/duckdb/recipes.duckdb` in read-only mode.
- Streamlit reads the same DuckDB file locally or in Docker.
- Airflow, Spark, Kafka, Zookeeper, Postgres, and Portainer are removed from the demo compose file.
- Vercel can deploy the FastAPI API via `api/index.py`.

Build the local demo database:

```bash
python scripts/build_duckdb_demo.py
```

Run the API locally:

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

Run the light Docker demo:

```bash
cd docker
docker compose up -d recipe-api streamlit tastagram
```

The historical Snowflake/Airflow/Spark scripts are left in the repo for reference, but they are no longer part of the default demo runtime.
