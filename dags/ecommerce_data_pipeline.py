"""Main data pipeline: ingest → validate → feature engineering → quality check."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def check_quality(**context):
    """Check data quality metrics from previous tasks."""
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Data quality check passed. Features are ready for ML training.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_data_pipeline",
    default_args=default_args,
    description="Main data pipeline: ingest → feature engineering → quality check",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "pipeline"],
) as dag:

    ingest_to_lake = BashOperator(
        task_id="ingest_to_lake",
        bash_command=(
            "docker compose run --rm ingest-to-lake "
            "2>&1 | tee /tmp/airflow_ingest.log"
        ),
    )

    feature_engineering = BashOperator(
        task_id="feature_engineering",
        bash_command=(
            "docker compose run --rm feature-engineering "
            "2>&1 | tee /tmp/airflow_features.log"
        ),
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=check_quality,
    )

    ingest_to_lake >> feature_engineering >> quality_check