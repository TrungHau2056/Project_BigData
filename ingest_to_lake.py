import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, count, sum as spark_sum, avg, min as spark_min, max as spark_max, round as spark_round, regexp_replace

from schemas import ECOMMERCE_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

CONFIG = {
    "data_file": os.environ.get("DATA_FILE", "data/2019-Oct.csv"),
    "minio_endpoint": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
    "minio_access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "minio_secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    "minio_bucket": os.environ.get("MINIO_BUCKET", "ecommerce-datalake"),
    "spark_packages": os.environ.get(
        "SPARK_PACKAGES",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ),
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IngestToLake")
        .config("spark.jars.packages", CONFIG["spark_packages"])
        .config("spark.hadoop.fs.s3a.endpoint", CONFIG["minio_endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", CONFIG["minio_access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", CONFIG["minio_secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def validate_data(df):
    """Run data quality checks and log metrics."""
    total = df.count()

    null_counts = df.select([
        count(col(c)).alias(c) for c in df.columns
    ]).collect()[0]

    logger.info("=== Data Quality Report ===")
    logger.info(f"Total rows: {total}")
    for c in df.columns:
        non_null = null_counts[c]
        null_rate = round((total - non_null) / total * 100, 2) if total > 0 else 0
        logger.info(f"  {c}: {non_null}/{total} non-null ({null_rate}% null)")

    # Price checks
    price_stats = df.filter(col("price").isNotNull()).select(
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price"),
        spark_round(avg("price"), 2).alias("avg_price"),
    ).collect()[0]

    logger.info(f"  Price range: {price_stats['min_price']} - {price_stats['max_price']} (avg: {price_stats['avg_price']})")

    negative_prices = df.filter(col("price") < 0).count()
    if negative_prices > 0:
        logger.warning(f"  WARNING: {negative_prices} rows with negative price!")

    # Event type distribution
    event_dist = df.groupBy("event_type").count().orderBy(col("count").desc()).collect()
    logger.info("  Event type distribution:")
    for row in event_dist:
        logger.info(f"    {row['event_type']}: {row['count']}")

    return {
        "total_rows": total,
        "negative_prices": negative_prices,
    }


def main():
    logger.info("Starting bulk ingest to data lake...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    file_path = CONFIG["data_file"]
    bucket = CONFIG["minio_bucket"]
    output_path = f"s3a://{bucket}/raw/"

    logger.info(f"Reading CSV: {file_path}")
    df = (
        spark.read
        .option("header", "true")
        .schema(ECOMMERCE_SCHEMA)
        .csv(file_path)
    )

    total_rows = df.count()
    logger.info(f"Total rows: {total_rows}")

    # Data quality validation
    logger.info("Running data quality checks...")
    quality = validate_data(df)

    # Filter out rows with negative prices
    if quality["negative_prices"] > 0:
        logger.info(f"Filtering out {quality['negative_prices']} rows with negative price")
        df = df.filter((col("price") >= 0) | col("price").isNull())

    # Partition by event_date for efficient querying
    # Some rows have "UTC" suffix in event_time — strip it before parsing
    df_clean = df.withColumn(
        "event_time_clean",
        regexp_replace(col("event_time"), r"\s+UTC$", "")
    )
    df_partitioned = df_clean.withColumn(
        "event_date",
        to_date(col("event_time_clean"), "yyyy-MM-dd HH:mm:ss")
    ).drop("event_time_clean")

    logger.info(f"Writing Parquet to {output_path} (partitioned by event_date)...")
    (
        df_partitioned.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(output_path)
    )

    logger.info("Bulk ingest complete.")

    # Verify
    logger.info("Verifying: reading back from data lake...")
    df_verify = spark.read.parquet(output_path)
    verified_rows = df_verify.count()
    logger.info(f"Verified rows: {verified_rows}")

    spark.stop()


if __name__ == "__main__":
    main()