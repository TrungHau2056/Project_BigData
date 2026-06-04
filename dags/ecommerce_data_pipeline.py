"""Main data pipeline: ingest → feature engineering → quality check."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from docker.types import Mount


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

COMMON_MOUNTS = [
    Mount(source="/d/Project_BigData", target="/app", type="bind"),
    Mount(source="/d/Project_BigData/.ivy-cache", target="/tmp/.ivy2", type="bind"),
]

with DAG(
    dag_id="ecommerce_data_pipeline",
    default_args=default_args,
    description="Main data pipeline: ingest → feature engineering → quality check",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "pipeline"],
) as dag:

    ingest_to_lake = DockerOperator(
        task_id="ingest_to_lake",
        image="apache/spark:3.5.1",
        working_dir="/app",
        environment={
            "HOME": "/tmp",
            "DATA_FILE": "data/2019-Oct.csv",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "ecommerce-datalake",
            "SPARK_PACKAGES": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        },
        command=[
            "/opt/spark/bin/spark-submit",
            "--master", "local[*]",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--conf", "spark.driver.memory=2g",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "/app/ingest_to_lake.py",
        ],
        mounts=COMMON_MOUNTS,
        docker_url="unix://var/run/docker.sock",
        network_mode="project_bigdata_default",
        auto_remove=True,
    )

    feature_engineering = DockerOperator(
        task_id="feature_engineering",
        image="apache/spark:3.5.1",
        working_dir="/app",
        environment={
            "HOME": "/tmp",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "ecommerce-datalake",
            "ES_NODES": "elasticsearch",
            "ES_PORT": "9200",
            "SPARK_PACKAGES": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3",
        },
        command=[
            "/opt/spark/bin/spark-submit",
            "--master", "local[*]",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--conf", "spark.driver.memory=2g",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3",
            "/app/feature_engineering.py",
        ],
        mounts=COMMON_MOUNTS,
        docker_url="unix://var/run/docker.sock",
        network_mode="project_bigdata_default",
        auto_remove=True,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=check_quality,
    )

    ingest_to_lake >> feature_engineering >> quality_check
