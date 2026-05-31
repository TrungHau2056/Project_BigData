import os
import json
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, lit, when

from schemas import ECOMMERCE_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

CONFIG = {
    "minio_endpoint": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
    "minio_access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "minio_secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    "minio_bucket": os.environ.get("MINIO_BUCKET", "ecommerce-datalake"),
    "kafka_bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    "topic_name": os.environ.get("TOPIC_NAME", "ecommerce-events"),
    "events_per_sec": int(os.environ.get("EVENTS_PER_SEC", "1000")),
    "spark_packages": os.environ.get(
        "SPARK_PACKAGES",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ),
}


def clean_nulls(df):
    """Replace null values with None for clean JSON serialization."""
    for c in df.columns:
        df = df.withColumn(c, when(col(c).isNull(), lit(None)).otherwise(col(c)))
    return df


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StreamReplay")
        .config("spark.jars.packages", CONFIG["spark_packages"])
        .config("spark.hadoop.fs.s3a.endpoint", CONFIG["minio_endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", CONFIG["minio_access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", CONFIG["minio_secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main():
    logger.info("Starting stream replay: MinIO → Kafka...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bucket = CONFIG["minio_bucket"]
    input_path = f"s3a://{bucket}/raw/"
    topic = CONFIG["topic_name"]
    kafka_servers = CONFIG["kafka_bootstrap_servers"]

    logger.info(f"Reading Parquet from {input_path}...")
    df = spark.read.parquet(input_path)
    df = df.orderBy("event_time")

    total = df.count()
    logger.info(f"Total events to replay: {total}")

    # Limit rows per batch for rate control
    events_per_sec = CONFIG["events_per_sec"]
    if events_per_sec > 0 and total > events_per_sec:
        limit = min(events_per_sec * 60, total)  # Send 1 minute of data at a time
        logger.info(f"Rate: {events_per_sec} events/sec. Sending {limit} rows in this batch.")
        df = df.limit(limit)
    else:
        logger.info(f"Sending all {total} rows.")

    # Convert all columns to JSON and write to Kafka
    # Select all original columns (drop event_date partition column if present)
    cols = [c for c in df.columns if c != "event_date"]
    value_col = to_json(struct(*[col(c) for c in cols]))

    kafka_df = df.select(value_col.alias("value"))

    logger.info(f"Writing {kafka_df.count()} events to Kafka topic '{topic}'...")
    (
        kafka_df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("topic", topic)
        .save()
    )

    logger.info(f"Replay complete: sent events to Kafka topic '{topic}'.")
    spark.stop()


if __name__ == "__main__":
    main()