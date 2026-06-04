"""Daily batch analytics: spark_batch → customer_segmentation."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


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
    dag_id="ecommerce_daily_analytics",
    default_args=default_args,
    description="Daily batch analytics: revenue, conversion, segmentation",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "batch", "analytics"],
) as dag:

    spark_batch = DockerOperator(
        task_id="spark_batch",
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
            "--conf", "spark.driver.memory=4g",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3",
            "/app/spark_batch.py",
        ],
        mounts=COMMON_MOUNTS,
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode="project_bigdata_default",
        auto_remove=True,
    )

    customer_segmentation = DockerOperator(
        task_id="customer_segmentation",
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
            "NUM_CLUSTERS": "4",
            "SPARK_PACKAGES": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3",
        },
        entrypoint="/bin/bash",
        command=[
            "-c",
            "pip install numpy && /opt/spark/bin/spark-submit --master local[*] --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.driver.memory=4g --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3 /app/customer_segmentation.py",
        ],
        mounts=COMMON_MOUNTS,
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode="project_bigdata_default",
        auto_remove=True,
    )

    spark_batch >> customer_segmentation
