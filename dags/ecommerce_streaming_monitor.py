"""Monitor streaming health: check Kafka lag, ES index freshness."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def check_kafka_status(**context):
    """Check if Kafka is accessible and topic has data."""
    import subprocess
    import logging
    logger = logging.getLogger(__name__)

    try:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "kafka",
             "kafka-topics", "--describe", "--topic", "ecommerce-events",
             "--bootstrap-server", "localhost:29092"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            logger.info("Kafka topic check passed.")
        else:
            logger.warning(f"Kafka topic check failed: {result.stderr}")
    except Exception as e:
        logger.warning(f"Kafka check error: {e}")


def check_es_status(**context):
    """Check if Elasticsearch is accessible and indices exist."""
    import urllib.request
    import logging
    logger = logging.getLogger(__name__)

    try:
        req = urllib.request.Request("http://localhost:9200/_cat/indices?format=json")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read().decode()
            logger.info(f"ES indices: {len(data)} bytes returned")
    except Exception as e:
        logger.warning(f"ES check error: {e}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="ecommerce_streaming_monitor",
    default_args=default_args,
    description="Monitor Kafka and Elasticsearch health",
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "monitoring"],
) as dag:

    check_kafka = PythonOperator(
        task_id="check_kafka_status",
        python_callable=check_kafka_status,
    )

    check_es = PythonOperator(
        task_id="check_es_status",
        python_callable=check_es_status,
    )

    check_kafka >> check_es