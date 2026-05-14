"""Manual Airflow DAG for multimodal recipe content recovery."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "fayssal",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

REPO_ROOT = "/opt/airflow"


@dag(
    dag_id="recipe_content_recovery_pipeline",
    default_args=DEFAULT_ARGS,
    description="Recover weak recipe evidence with adaptive caption, audio transcript, OCR, and optional comments",
    schedule=None,
    start_date=datetime(2026, 5, 14),
    catchup=False,
    tags=["tiktok", "recovery", "ocr", "audio", "snowflake"],
)
def content_recovery_pipeline():
    lightweight_recovery_task = BashOperator(
        task_id="lightweight_recovery_task",
        bash_command=(
            f"cd {REPO_ROOT}/docker && "
            "docker compose --profile recovery run --rm recipe-content-recovery "
            "python -u -m scripts.recover_recipe_content --method adaptive --limit 25"
        ),
    )

    silver_reprocess_task = BashOperator(
        task_id="silver_reprocess_task",
        bash_command=(
            f"cd {REPO_ROOT} && "
            "python -m scripts.enrich_silver --limit 250 --only-recovered"
        ),
    )

    gold_dbt_task = BashOperator(
        task_id="gold_dbt_task",
        bash_command=(
            f"cd {REPO_ROOT}/dbt_project && "
            "export DBT_TARGET_PATH=/tmp/dbt_target && "
            "export DBT_LOG_PATH=/tmp/dbt_logs && "
            "dbt run --profiles-dir . --no-partial-parse"
        ),
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test_task",
        bash_command=(
            f"cd {REPO_ROOT}/dbt_project && "
            "export DBT_TARGET_PATH=/tmp/dbt_target && "
            "export DBT_LOG_PATH=/tmp/dbt_logs && "
            "dbt test --profiles-dir . --no-partial-parse"
        ),
    )

    lightweight_recovery_task >> silver_reprocess_task >> gold_dbt_task >> dbt_test_task


pipeline_dag = content_recovery_pipeline()
