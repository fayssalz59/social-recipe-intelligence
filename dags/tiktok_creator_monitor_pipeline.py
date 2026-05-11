"""Airflow DAG for the TikTok creator monitoring -> Kafka -> Bronze -> Silver -> Gold pipeline."""
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
    dag_id="tiktok_creator_monitor_pipeline",
    default_args=DEFAULT_ARGS,
    description="TikTok creator monitor -> Kafka -> Bronze -> Silver -> Gold",
    schedule="0 11 * * *",
    start_date=datetime(2026, 5, 10),
    catchup=False,
    tags=["tiktok", "kafka", "snowflake", "dbt", "airflow"],
)
def tiktok_creator_monitor_pipeline():
    monitor_creator_videos_task = BashOperator(
        task_id="monitor_creator_videos_task",
        bash_command=(
            f"cd {REPO_ROOT} && "
            "python -m scripts.tiktok_creator_monitor"
        ),
    )

    consume_tiktok_kafka_events_task = BashOperator(
        task_id="consume_tiktok_kafka_events_task",
        bash_command=(
            f"cd {REPO_ROOT} && "
            "timeout 60s python -m scripts.kafka_tiktok_event_consumer --bootstrap-server kafka:29092"
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

    monitor_creator_videos_task >> consume_tiktok_kafka_events_task >> silver_enrich_task >> gold_dbt_task


pipeline_dag = tiktok_creator_monitor_pipeline()