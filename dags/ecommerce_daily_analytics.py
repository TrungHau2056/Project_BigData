"""Daily batch analytics: spark_batch → customer_segmentation."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_daily_analytics",
    default_args=default_args,
    description="Daily batch analytics: revenue, conversion, segmentation",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "batch", "analytics"],
) as dag:

    spark_batch = BashOperator(
        task_id="spark_batch",
        bash_command="docker compose run --rm spark-batch",
    )

    customer_segmentation = BashOperator(
        task_id="customer_segmentation",
        bash_command="docker compose run --rm customer-segmentation",
    )

    spark_batch >> customer_segmentation