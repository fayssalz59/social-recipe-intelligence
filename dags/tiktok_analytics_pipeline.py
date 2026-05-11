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
    dag_id="tiktok_analytics_pipeline",
    default_args=DEFAULT_ARGS,
    description="Silver -> Spark analytics -> Gold analytics tables",
    schedule="15 */6 * * *",
    start_date=datetime(2026, 5, 10),
    catchup=False,
    tags=["spark", "snowflake", "analytics", "airflow"],
)
def tiktok_analytics_pipeline():
    silver_enrich_task = BashOperator(
        task_id="silver_enrich_task",
        bash_command=(
            f"cd {REPO_ROOT} && "
            "python -m scripts.enrich_silver --limit 100"
        ),
    )

    spark_analytics_task = BashOperator(
        task_id="spark_analytics_task",
        bash_command=(
            'docker exec docker-spark-analytics-1 '
            'bash -c "python3 -m pip install --no-cache-dir python-dotenv pyspark && '
            '/opt/spark/bin/spark-submit '
            '--conf spark.jars.ivy=/tmp/.ivy2 '
            '--packages net.snowflake:snowflake-jdbc:3.15.1,net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4 '
            '/app/scripts/spark_recipe_analytics.py"'
        ),
    )

    silver_enrich_task >> spark_analytics_task


pipeline_dag = tiktok_analytics_pipeline()