import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, sum, count,
    window, round as spark_round, approx_count_distinct,
    regexp_replace, date_format
)

from schemas import EVENT_SCHEMA

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
    "kafka_bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "topic_name": os.environ.get("TOPIC_NAME", "ecommerce-events"),
    "es_nodes": os.environ.get("ES_NODES", "elasticsearch"),
    "es_port": os.environ.get("ES_PORT", "9200"),
    "es_index": os.environ.get("ES_INDEX", "ecommerce-events"),
    "checkpoint_location": os.environ.get("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/ecommerce-events"),
    "window_duration": os.environ.get("WINDOW_DURATION", "5 minutes"),
    "watermark_delay": os.environ.get("WATERMARK_DELAY", "10 minutes"),
}

def write_to_elasticsearch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id} is empty. Skipping.")
        return
    config = CONFIG
    batch_df = batch_df.withColumn("event_time", date_format(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss" ))
    (
        batch_df.write
        .format("org.elasticsearch.spark.sql")
        .mode("append")
        .option("es.nodes", config["es_nodes"])
        .option("es.port", config["es_port"])
        .option("es.resource", config["es_index"])
        .option("es.nodes.wan.only", "true")
        .option("es.index.auto.create", "true")
        .save()
    )
    logger.info(f"Batch {batch_id}: wrote {batch_df.count()} raw events to ES.")
    batch_df.show(5, False)



def write_windowed_to_elasticsearch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        logger.info(f"Windowed batch {batch_id} is empty. Skipping.")
        return
    config = CONFIG
    batch_df = (
        batch_df
        .withColumn(
            "window_start",
            date_format(col("window_start"), "yyyy-MM-dd'T'HH:mm:ss")
        )
        .withColumn(
            "window_end",
            date_format(col("window_end"), "yyyy-MM-dd'T'HH:mm:ss")
        )
    )
    (
        batch_df.write
        .format("org.elasticsearch.spark.sql")
        .mode("append")
        .option("es.nodes", config["es_nodes"])
        .option("es.port", config["es_port"])
        .option("es.resource", "streaming-windowed-revenue")
        .option("es.nodes.wan.only", "true")
        .option("es.index.auto.create", "true")
        .save()
    )
    logger.info(f"Windowed batch {batch_id}: wrote {batch_df.count()} aggregations to ES.")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EcommerceStreaming")
        .config("spark.jars.packages", CONFIG["spark_kafka_package"])
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def main():
    logger.info("Initializing Spark session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized.")

    config = CONFIG

    logger.info(f"Listening to Kafka topic '{config['topic_name']}'...")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"])
        .option("subscribe", config["topic_name"])
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )
    parsed_df.printSchema()

    clean_df = parsed_df.filter(col("price").isNotNull())
    clean_df.printSchema()


    # --- Query 1: Raw events to ES (original behavior) ---
    raw_query = (
        clean_df.writeStream
        .format("console")
        .outputMode("append")
        .foreachBatch(write_to_elasticsearch)
        .option("checkpointLocation", f"{config['checkpoint_location']}/raw")
        .option("truncate", False)
        .start()
    )``

    # --- Query 2: Windowed aggregations ---
    windowed_df = (
        clean_df
        .withWatermark("event_time", config["watermark_delay"])
        .groupBy(
            window(col("event_time"), config["window_duration"]),
            col("event_type"),
        )
        .agg(
            spark_round(sum("price"), 2).alias("total_revenue"),
            count("*").alias("event_count"),
            approx_count_distinct("user_id").alias("unique_users"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "event_type",
            "total_revenue",
            "event_count",
            "unique_users",
        )
    )

    windowed_query = (
        windowed_df.writeStream
        .outputMode("update")
        .foreachBatch(write_windowed_to_elasticsearch)
        .option("checkpointLocation", f"{config['checkpoint_location']}/windowed")
        .start()
    )

    logger.info("Streaming queries started. Waiting for data...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
