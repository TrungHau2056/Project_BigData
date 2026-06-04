import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, sum, count, avg, stddev,
    window, lit, abs as spark_abs, when,
    round as spark_round, approx_count_distinct, current_timestamp,
    regexp_replace, date_format, concat_ws, broadcast
)
from pyspark.sql.types import DoubleType

from schemas import EVENT_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

CONFIG = {
    "spark_kafka_package": os.environ.get(
        "SPARK_KAFKA_PACKAGE",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ),
    "kafka_bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    "topic_name": os.environ.get("TOPIC_NAME", "ecommerce-events"),
    "es_nodes": os.environ.get("ES_NODES", "elasticsearch"),
    "es_port": os.environ.get("ES_PORT", "9200"),
    "minio_endpoint": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
    "minio_access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "minio_secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    "minio_bucket": os.environ.get("MINIO_BUCKET", "ecommerce-datalake"),
    "checkpoint_location": os.environ.get("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/anomaly"),
}

PRICE_ZSCORE_THRESHOLD = float(os.environ.get("PRICE_ZSCORE_THRESHOLD", "2.0"))
SESSION_EVENT_LIMIT = int(os.environ.get("SESSION_EVENT_LIMIT", "100"))

baseline_df = None

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StreamingAnomalyDetection")
        .config("spark.jars.packages", CONFIG["spark_kafka_package"])
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.hadoop.fs.s3a.endpoint", CONFIG["minio_endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", CONFIG["minio_access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", CONFIG["minio_secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def write_anomalies_to_es(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df = (
        batch_df
        .withColumn("processing_time", date_format(col("processing_time"), "yyyy-MM-dd'T'HH:mm:ss" ))
    )
    (
        batch_df.write
        .format("org.elasticsearch.spark.sql")
        .mode("append")
        .option("es.nodes", CONFIG["es_nodes"])
        .option("es.port", CONFIG["es_port"])
        .option("es.resource", "streaming-anomalies")
        .option("es.nodes.wan.only", "true")
        .option("es.index.auto.create", "true")
        .save()
    )
    logger.info(f"Anomaly batch {batch_id}: wrote {batch_df.count()} anomalies to ES.")


def detect_anomalies(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        logger.info(f"Anomaly batch {batch_id} is empty. Skipping.")
        return

    anomalies = []
    with_cat = batch_df.filter(col("category_code").isNotNull())

    price_anomalies = (
        with_cat
        .join(baseline_df, "category_code", "inner")
        .filter(
            col("baseline_avg").isNotNull() &
            col("baseline_std").isNotNull() &
            (col("baseline_std") > 0)
        )
        .withColumn(
            "z_score",
            spark_abs(col("price") - col("baseline_avg")) / col("baseline_std")
        )
        .filter(col("z_score") > PRICE_ZSCORE_THRESHOLD)
        .select(
            current_timestamp().alias("processing_time"),
            col("product_id"),
            col("category_code"),
            col("price"),
            col("baseline_avg"),
            col("baseline_std"),
            spark_round(col("z_score"), 2).alias("z_score"),
            lit("price_anomaly").alias("anomaly_type"),
        )
    )
    anomalies.append(price_anomalies)

    # Suspicious session: too many events per session
    with_session = batch_df.filter(col("user_session").isNotNull())
    if not with_session.rdd.isEmpty():
        session_stats = (
            with_session
            .groupBy("user_session", "user_id")
            .agg(count("*").alias("event_count"))
            .filter(col("event_count") > SESSION_EVENT_LIMIT)
            .select(
                current_timestamp().alias("processing_time"),
                col("user_session"),
                col("user_id"),
                col("event_count"),
                lit("suspicious_session").alias("anomaly_type"),
            )
        )
        anomalies.append(session_stats)

    # --- 1. ROOT LEVEL CHECK (e.g., "electronics") ---
    # High threshold: Catches site-wide script attacks or major system-wide events
    lvl1_stats = (
        batch_df
        .filter(col("category_level_1").isNotNull())
        .groupBy("category_level_1")
        .agg(count("*").alias("event_count"))
        .filter(col("event_count") > 200)
        .select(
            current_timestamp().alias("processing_time"),
            col("category_level_1").alias("category_value"),
            lit("root_level_1").alias("category_hierarchy"),
            col("event_count"),
            lit("volume_spike").alias("anomaly_type"),
        )
    )

    # --- 2. SUB LEVEL CHECK (e.g., "electronics.smartphone") ---
    # Medium threshold: Catches attacks targeting a specific family of products
    lvl2_stats = (
        batch_df
        .filter(col("category_level_2").isNotNull())
        .groupBy("category_level_1", "category_level_2")
        .agg(count("*").alias("event_count"))
        .filter(col("event_count") > 100)
        .select(
            current_timestamp().alias("processing_time"),
            concat_ws(".", col("category_level_1"), col("category_level_2")).alias("category_value"),
            lit("sub_level_2").alias("category_hierarchy"),
            col("event_count"),
            lit("volume_spike").alias("anomaly_type"),
        )
    )

    # --- 3. LEAF LEVEL CHECK (e.g., "electronics.smartphone.apple") ---
    # Low threshold: Lasers in on scalper bots hitting a single precise item drop
    lvl3_stats = (
        batch_df
        .filter(col("category_code").isNotNull())
        .groupBy("category_code")
        .agg(count("*").alias("event_count"))
        .filter(col("event_count") > 50)
        .select(
            current_timestamp().alias("processing_time"),
            col("category_code").alias("category_value"),
            lit("leaf_level_3").alias("category_hierarchy"),
            col("event_count"),
            lit("volume_spike").alias("anomaly_type"),
        )
    )
    volume_stats = lvl1_stats.unionByName(lvl2_stats).unionByName(lvl3_stats)
    anomalies.append(volume_stats)

    if anomalies:
        # Union all anomaly types (add missing columns with nulls for schema compatibility)
        from functools import reduce
        all_anomalies = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), anomalies)
        write_anomalies_to_es(all_anomalies, batch_id)

        # Write anomalies to MinIO (Parquet)
        (
            all_anomalies
            .withColumn("event_date", date_format(col("processing_time"), "yyyy-MM-dd"))
            .write
            .partitionBy("event_date")
            .mode("append")
            .parquet(f"s3a://{CONFIG['minio_bucket']}/anomalies/")
        )
        logger.info(f"Anomaly batch {batch_id}: wrote to MinIO s3a://{CONFIG['minio_bucket']}/anomalies/")

def main():
    logger.info("Starting Streaming Anomaly Detection...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    global baseline_df
    baseline_df = (
        spark.read
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", CONFIG["es_nodes"])
        .option("es.port", CONFIG["es_port"])
        .option("es.nodes.wan.only", "true")
        .load("batch-price-baseline")
        .select(
            "category_code",
            col("baseline_avg").cast("double").alias("baseline_avg"),
            col("baseline_std").cast("double").alias("baseline_std"),
            col("sample_size").cast("long").alias("sample_size"),
        )
    )

    baseline_df = broadcast(baseline_df)

    logger.info(f"Listening to Kafka topic '{CONFIG['topic_name']}'...")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", CONFIG["kafka_bootstrap_servers"])
        .option("subscribe", CONFIG["topic_name"])
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    clean_df = (
        parsed_df
        .filter(col("price").isNotNull())
    )

    query = (
        clean_df.writeStream
        .outputMode("append")
        .foreachBatch(detect_anomalies)
        .option("checkpointLocation", f"{CONFIG['checkpoint_location']}/all")
        .start()
    )

    logger.info("Anomaly detection query started. Waiting for data...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()