import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, sum, count, avg, stddev,
    window, lit, abs as spark_abs, when,
    round as spark_round, approx_count_distinct, current_timestamp,
    regexp_replace,
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

PRICE_ZSCORE_THRESHOLD = float(os.environ.get("PRICE_ZSCORE_THRESHOLD", "2.0"))
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


def detect_anomalies(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        logger.info(f"Anomaly batch {batch_id} is empty. Skipping.")
        return

    anomalies = []

    # Price anomaly: z-score > threshold vs category average
    with_cat = batch_df.filter(col("category_code").isNotNull())
    if not with_cat.rdd.isEmpty():
        price_stats = with_cat.groupBy("category_code").agg(
            avg("price").alias("cat_avg_price"),
            stddev("price").alias("cat_std_price"),
        ).filter(col("cat_std_price").isNotNull() & (col("cat_std_price") > 0))

        price_anomalies = (
            with_cat
            .join(price_stats, "category_code", "left")
            .filter(
                col("cat_std_price").isNotNull() &
                (col("cat_std_price") > 0) &
                (spark_abs(col("price") - col("cat_avg_price")) / col("cat_std_price") > PRICE_ZSCORE_THRESHOLD)
            )
            .select(
                current_timestamp().alias("processing_time"),
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

    # Volume anomaly: flag if batch has high event count per category
    volume_stats = (
        batch_df
        .filter(col("category_code").isNotNull())
        .groupBy("category_code")
        .agg(count("*").alias("event_count"))
        .filter(col("event_count") > 50)
        .select(
            current_timestamp().alias("processing_time"),
            col("category_code"),
            col("event_count"),
            lit("volume_spike").alias("anomaly_type"),
        )
    )
    if not volume_stats.rdd.isEmpty():
        anomalies.append(volume_stats)

    if anomalies:
        # Union all anomaly types (add missing columns with nulls for schema compatibility)
        from functools import reduce
        all_anomalies = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), anomalies)
        anomaly_count = all_anomalies.count()
        if anomaly_count > 0:
            (
                all_anomalies.write
                .format("org.elasticsearch.spark.sql")
                .mode("append")
                .option("es.nodes", CONFIG["es_nodes"])
                .option("es.port", CONFIG["es_port"])
                .option("es.resource", "streaming-anomalies")
                .option("es.nodes.wan.only", "true")
                .option("es.index.auto.create", "true")
                .save()
            )
            logger.info(f"Anomaly batch {batch_id}: wrote {anomaly_count} anomalies to ES.")


def main():
    logger.info("Starting Streaming Anomaly Detection...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

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

    clean_df = (
        parsed_df
        .filter(col("price").isNotNull())
        .withColumn(
            "event_timestamp",
            to_timestamp(
                regexp_replace(col("event_time"), r"\s+UTC$", ""),
                "yyyy-MM-dd HH:mm:ss",
            )
        )
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