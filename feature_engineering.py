import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count as spark_count, avg, when, lit,
    round as spark_round, datediff, max as spark_max, min as spark_min,
    countDistinct, first,
    to_timestamp, substring, unix_timestamp, hour
)
from pyspark.sql.types import IntegerType

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
    "data_file": os.environ.get("DATA_FILE", "data/2019-Oct.csv"),
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
        .appName("FeatureEngineering")
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
    logger.info(f"Writing to ES index '{index_name}'...")
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


def write_to_minio(df, path: str):
    logger.info(f"Writing Parquet to {path}...")
    df.write.mode("overwrite").parquet(path)
    logger.info(f"Done writing to {path}.")

# =========================================================================
# THÀNH PHẦN THÊM MỚI: TÍNH TOÁN CÁC ĐẶC TRƯNG THEO SESSION (TỪ NOTEBOOK)
# =========================================================================
def compute_session_features(df):
    """Compute session-based features for XGBoost Purchase Prediction."""
    bucket = CONFIG["minio_bucket"]
    output_path = f"s3a://{bucket}/features/session_features/"

    logger.info("Computing session features from notebook...")
    session_df = df.filter(col("user_session").isNotNull() & col("user_id").isNotNull())
    session_df = session_df.withColumn(
        "event_time_parsed", 
        to_timestamp(substring(col("event_time").cast("string"), 1, 19), "yyyy-MM-dd HH:mm:ss")
    )

    session_df = session_df.withColumn("is_view", when(col("event_type") == "view", 1).otherwise(0)) \
                           .withColumn("is_cart", when(col("event_type") == "cart", 1).otherwise(0)) \
                           .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0))

    grouped_df = session_df.groupBy("user_session").agg(
        spark_sum("is_view").alias("total_views"),
        spark_sum("is_cart").alias("total_carts"),
        spark_max("is_purchase").alias("label"),
        countDistinct("category_code").alias("unique_categories"),
        countDistinct("brand").alias("unique_brands"),
        spark_min("event_time_parsed").alias("start_time"),
        spark_max("event_time_parsed").alias("end_time")
    )

    final_session_features = grouped_df.withColumn(
        "session_duration", 
        unix_timestamp("end_time") - unix_timestamp("start_time")
    ).withColumn(
        "hour_of_day", hour("start_time")
    ).withColumn(
        "cart_to_view_ratio", 
        col("total_carts") / (col("total_views") + 1e-5)
    ).withColumn(
        "cart_to_view_ratio",
        when(col("cart_to_view_ratio") > 10.0, 10.0).otherwise(col("cart_to_view_ratio"))
    ).drop("start_time", "end_time")

    count = final_session_features.count()
    logger.info(f"Session features: {count} sessions")

    final_session_features.cache()
    final_session_features.show(5, truncate=False)

    write_to_minio(final_session_features, output_path)
    return final_session_features


def write_quality_report(user_features, product_features, interactions):
    """Write data quality metrics to ES."""
    logger.info("Writing data quality report...")

    u_count = user_features.count()
    p_count = product_features.count()
    i_count = interactions.count()

    u_nulls = user_features.filter(
        col("recency").isNull() | col("frequency").isNull() | col("monetary").isNull()
    ).count()

    i_invalid = interactions.filter(
        col("user_id").isNull() | col("product_id").isNull() |
        (col("rating") < 1) | (col("rating") > 3)
    ).count()

    report_data = [
        ("user_features", u_count, u_nulls),
        ("product_features", p_count, 0),
        ("interactions", i_count, i_invalid),
    ]

    # Write as a simple summary
    from pyspark.sql import Row
    spark = user_features.sparkSession
    report_df = spark.createDataFrame([
        Row(table=t, row_count=c, null_count=n, status="OK" if n == 0 else "WARNING")
        for t, c, n in report_data
    ])

    write_to_es(report_df, "data-quality-report")


def main():
    logger.info("Starting feature engineering...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bucket = CONFIG["minio_bucket"]
    raw_path = f"s3a://{bucket}/raw/"

    logger.info(f"Reading from data lake: {raw_path}")
    df = spark.read.parquet(raw_path)

    total = df.count()
    logger.info(f"Total rows: {total}")

    # Compute all feature tables
    user_features = compute_user_features(df)
    product_features = compute_product_features(df)
    interactions = compute_interactions(df)
    
    # Kích hoạt luồng xử lý Session Features phục vụ mô hình XGBoost
    session_features = compute_session_features(df)

    # Write quality report to ES
    try:
        write_quality_report(user_features, product_features, interactions)
    except Exception as e:
        logger.warning(f"Failed to write quality report to ES (ES may not be ready): {e}")

    logger.info("Feature engineering complete.")
    spark.stop()


if __name__ == "__main__":
    main()
