import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, desc, to_timestamp, to_date,
    hour, when, lit, round as spark_round, countDistinct,
    regexp_replace, date_trunc
)

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
    "es_nodes": os.environ.get("ES_NODES", "elasticsearch"),
    "es_port": os.environ.get("ES_PORT", "9200"),
    "spark_packages": os.environ.get(
        "SPARK_PACKAGES",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3"
    ),
}

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EcommerceBatchAnalytics")
        .config("spark.jars.packages", CONFIG["spark_packages"])
        .config("spark.hadoop.fs.s3a.endpoint", CONFIG["minio_endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", CONFIG["minio_access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", CONFIG["minio_secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def write_to_es(df, index_name: str):
    logger.info(f"Writing god knows many rows to ES index '{index_name}'...")
    (
        df.write
        .format("org.elasticsearch.spark.sql")
        .mode("overwrite")
        .option("es.nodes", CONFIG["es_nodes"])
        .option("es.port", CONFIG["es_port"])
        .option("es.resource", index_name)
        .option("es.nodes.wan.only", "true")
        .option("es.index.auto.create", "true")
        .save()
    )
    logger.info(f"Done writing to '{index_name}'.")


def revenue_by_category(purchases):
    return (
        purchases
        .filter(col("category_code").isNotNull())
        .groupBy(
            "category_level_1",
            "category_level_2",
            "category_level_3"
        )
        .agg(
            spark_round(sum("price"), 2).alias("total_revenue"),
            count("*").alias("order_count"),
            spark_round(avg("price"), 2).alias("avg_price"),
        )
        .orderBy(desc("total_revenue"))
    )

def revenue_by_brand(purchases):
    return (
        purchases
        .filter(col("brand").isNotNull())
        .groupBy("brand")
        .agg(
            spark_round(sum("price"), 2).alias("total_revenue"),
            count("*").alias("order_count"),
            spark_round(avg("price"), 2).alias("avg_price"),
        )
        .orderBy(desc("total_revenue"))
    )


def daily_revenue(purchases):

    daily = (
        purchases
        .withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date")
        .agg(
            spark_round(sum("price"), 2).alias("total_revenue"),
            count("*").alias("order_count"),
            countDistinct("user_id").alias("unique_buyers"),
            spark_round(avg("price"), 2).alias("aov"),
        )
        .orderBy("event_date")
    )

    return daily

def weekly_revenue(purchases):
    weekly = (
        purchases
        .withColumn(
            "week_start",
            date_trunc("week", col("event_time"))
        )
        .groupBy("week_start")
        .agg(
            spark_round(sum("price"), 2).alias("total_revenue"),
            count("*").alias("order_count"),
            countDistinct("user_id").alias("unique_buyers"),
        )
        .orderBy("week_start")
    )
    return weekly

def conversion_funnel(df):
    counts = df.groupBy("event_type").count()
    total = counts.agg(sum("count").alias("total")).collect()[0]["total"]
    return (
        counts
        .withColumn("percentage", spark_round(col("count") / lit(total) * 100, 2))
        .orderBy(desc("count"))
    )


def top_products(purchases, n=20):
    return (
        purchases
        .groupBy("product_id", "category_code", "brand")
        .agg(
            spark_round(sum("price"), 2).alias("total_revenue"),
            count("*").alias("order_count"),
        )
        .orderBy(desc("total_revenue"))
        .limit(n)
    )


def hourly_activity(df):
    return (
        df
        .withColumn(
            "event_hour",
            hour(col("event_time"))
        )
        .filter(col("event_hour").isNotNull())
        .groupBy("event_hour", "event_type")
        .agg(count("*").alias("event_count"))
        .orderBy("event_hour", "event_type")
    )


def main():
    logger.info("Starting batch analytics...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bucket = CONFIG["minio_bucket"]
    input_path = f"s3a://{bucket}/raw/"
    logger.info(f"Reading from data lake: {input_path}")
    df = spark.read.parquet(input_path)

    total_rows = df.count()
    logger.info(f"Total rows: {total_rows}")

    purchases = df.filter(
        (col("event_type") == "purchase") & col("price").isNotNull()
    )
    purchase_count = purchases.count()
    logger.info(f"Purchase rows: {purchase_count}")

    logger.info("Computing revenue by category...")
    write_to_es(revenue_by_category(purchases), "batch-revenue-category")

    logger.info("Computing revenue by brand...")
    write_to_es(revenue_by_brand(purchases), "batch-revenue-brand")

    logger.info("Computing daily and weekly revenue trend...")
    write_to_es(daily_revenue(purchases), "batch-daily-revenue")
    write_to_es(weekly_revenue(purchases), "batch-weekly-revenue")

    logger.info("Computing conversion funnel...")
    write_to_es(conversion_funnel(df), "batch-conversion-funnel")

    logger.info("Computing top products...")
    write_to_es(top_products(purchases), "batch-top-products")

    logger.info("Computing hourly activity pattern...")
    write_to_es(hourly_activity(df), "batch-hourly-activity")

    logger.info("All batch analytics complete.")
    spark.stop()


if __name__ == "__main__":
    main()
