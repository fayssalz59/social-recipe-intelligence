"""Main Airflow DAG for the TikTok Recipe Intelligence pipeline."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "fayssal",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

REPO_ROOT = "/opt/airflow"


@dag(
    dag_id="tiktok_recipe_intelligence_pipeline",
    default_args=DEFAULT_ARGS,
    description="Bronze -> Silver -> Gold pipeline for TikTok recipes",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 5, 10),
    catchup=False,
    tags=["tiktok", "snowflake", "dbt", "llm", "airflow"],
)
def tiktok_recipe_pipeline():
    bronze_ingest_task = BashOperator(
        task_id="bronze_ingest_task",
        bash_command=(
            f"cd {REPO_ROOT} && "
            "python -m scripts.load_bronze --input-dir data/raw"
        ),
    )

    silver_enrich_task = BashOperator(
        task_id="silver_enrich_task",
        bash_command=(
            f"cd {REPO_ROOT} && "
            "python -m scripts.enrich_silver --limit 100"
        ),
    )

    gold_dbt_task = BashOperator(
        task_id="gold_dbt_task",
        bash_command=(
            f"cd {REPO_ROOT}/dbt_project && "
            "rm -rf target dbt_packages logs && "
            "dbt run --profiles-dir . --no-partial-parse"
        ),
    )

    bronze_ingest_task >> silver_enrich_task >> gold_dbt_task


pipeline_dag = tiktok_recipe_pipeline()