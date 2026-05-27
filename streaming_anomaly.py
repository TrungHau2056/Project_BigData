import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, sum, count, avg, stddev,
    window, lit, abs as spark_abs, when,
    round as spark_round, countDistinct, current_timestamp,
)
from pyspark.sql.types import DoubleType

from schemas import ECOMMERCE_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

CONFIG = {
    "spark_kafka_package": os.environ.get(
        "SPARK_KAFKA_PACKAGE",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3"
    ),
    "kafka_bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    "topic_name": os.environ.get("TOPIC_NAME", "ecommerce-events"),
    "es_nodes": os.environ.get("ES_NODES", "elasticsearch"),
    "es_port": os.environ.get("ES_PORT", "9200"),
    "checkpoint_location": os.environ.get("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/anomaly"),
}

# Anomaly thresholds
PRICE_ZSCORE_THRESHOLD = float(os.environ.get("PRICE_ZSCORE_THRESHOLD", "2.0"))
VOLUME_SPIKE_FACTOR = float(os.environ.get("VOLUME_SPIKE_FACTOR", "3.0"))
SESSION_EVENT_LIMIT = int(os.environ.get("SESSION_EVENT_LIMIT", "100"))


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StreamingAnomalyDetection")
        .config("spark.jars.packages", CONFIG["spark_kafka_package"])
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def write_anomalies_to_es(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
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


def main():
    logger.info("Starting Streaming Anomaly Detection...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
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
        .select(from_json(col("value"), ECOMMERCE_SCHEMA).alias("data"))
        .select("data.*")
    )

    clean_df = parsed_df.filter(col("price").isNotNull())

    # Add timestamp for windowing
    with_ts = clean_df.withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn("processing_time", current_timestamp())

    # === Anomaly 1: Price Anomaly ===
    # Products with price significantly different from category average (z-score)
    price_stats = (
        with_ts
        .filter(col("category_code").isNotNull())
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            window(col("event_timestamp"), "10 minutes"),
            col("category_code"),
        )
        .agg(
            avg("price").alias("cat_avg_price"),
            stddev("price").alias("cat_std_price"),
        )
    )

    price_anomalies = (
        with_ts
        .filter(col("category_code").isNotNull())
        .join(
            price_stats,
            (window(with_ts["event_timestamp"], "10 minutes") == price_stats["window"]) &
            (with_ts["category_code"] == price_stats["category_code"]),
            "left"
        )
        .filter(
            col("cat_std_price").isNotNull() &
            (col("cat_std_price") > 0) &
            (spark_abs(col("price") - col("cat_avg_price")) / col("cat_std_price") > PRICE_ZSCORE_THRESHOLD)
        )
        .select(
            col("processing_time"),
            col("event_time"),
            col("product_id"),
            col("category_code"),
            col("price"),
            col("cat_avg_price"),
            col("cat_std_price"),
            spark_round(
                spark_abs(col("price") - col("cat_avg_price")) / col("cat_std_price"), 2
            ).alias("z_score"),
            lit("price_anomaly").alias("anomaly_type"),
        )
    )

    # === Anomaly 2: Volume Spike ===
    # Event count spike (potential bot attack)
    volume_current = (
        with_ts
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(window(col("event_timestamp"), "5 minutes"))
        .agg(count("*").alias("current_count"))
    )

    volume_anomalies = (
        volume_current
        .filter(col("current_count") > 100)  # Only flag if meaningful volume
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("current_count").alias("event_count"),
            lit("volume_spike").alias("anomaly_type"),
            current_timestamp().alias("processing_time"),
        )
    )

    # === Anomaly 3: Suspicious Session ===
    # Too many events per session in a short time
    session_anomalies = (
        with_ts
        .filter(col("user_session").isNotNull())
        .withWatermark("event_timestamp", "1 minute")
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("user_session"),
            col("user_id"),
        )
        .agg(count("*").alias("event_count"))
        .filter(col("event_count") > SESSION_EVENT_LIMIT)
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("user_session"),
            col("user_id"),
            col("event_count"),
            lit("suspicious_session").alias("anomaly_type"),
            current_timestamp().alias("processing_time"),
        )
    )

    # Combine all anomalies
    # Note: Can't union streaming DataFrames with different schemas easily,
    # so we write each anomaly type separately

    # Start queries
    logger.info("Starting anomaly detection queries...")

    price_query = (
        price_anomalies.writeStream
        .outputMode("append")
        .foreachBatch(write_anomalies_to_es)
        .option("checkpointLocation", f"{CONFIG['checkpoint_location']}/price")
        .start()
    )

    volume_query = (
        volume_anomalies.writeStream
        .outputMode("append")
        .foreachBatch(write_anomalies_to_es)
        .option("checkpointLocation", f"{CONFIG['checkpoint_location']}/volume")
        .start()
    )

    session_query = (
        session_anomalies.writeStream
        .outputMode("append")
        .foreachBatch(write_anomalies_to_es)
        .option("checkpointLocation", f"{CONFIG['checkpoint_location']}/session")
        .start()
    )

    logger.info("Anomaly detection queries started. Waiting for data...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()