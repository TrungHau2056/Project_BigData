import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count as spark_count, avg, when, lit,
    round as spark_round, datediff, max as spark_max, min as spark_min,
    countDistinct, create_map,
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

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
    "num_clusters": int(os.environ.get("NUM_CLUSTERS", "4")),
    "spark_packages": os.environ.get(
        "SPARK_PACKAGES",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3"
    ),
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CustomerSegmentation")
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


def compute_rfm(df):
    """Compute RFM (Recency, Frequency, Monetary) features per user."""
    purchases = df.filter(
        (col("event_type") == "purchase") & col("user_id").isNotNull()
    )

    # Reference date = latest event_time + 1 day
    dates = purchases.select(
        spark_max(col("event_time")).alias("max_date")
    ).collect()
    ref_date = dates[0]["max_date"]

    rfm = (
        purchases.groupBy("user_id")
        .agg(
            (lit(datediff(lit(ref_date), spark_max(col("event_time"))))).alias("recency"),
            spark_count("*").alias("frequency"),
            spark_round(spark_sum("price"), 2).alias("monetary"),
            spark_round(avg("price"), 2).alias("avg_order_value"),
            countDistinct("product_id").alias("unique_products"),
        )
    )
    return rfm


def find_optimal_k(rfm_df, max_k=6):
    """Evaluate KMeans for k=2..max_k using Silhouette score."""
    assembler = VectorAssembler(
        inputCols=["recency", "frequency", "monetary"],
        outputCol="features_raw"
    )
    assembled = assembler.transform(rfm_df)

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    scaled = scaler.fit(assembled).transform(assembled)

    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="prediction",
        metricName="silhouette"
    )

    best_k = 2
    best_score = -1
    print("\nSilhouette Scores:")
    print(f"{'k':<5} {'Silhouette':>12}")
    print("-" * 18)

    for k in range(2, max_k + 1):
        kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=k, seed=42)
        model = kmeans.fit(scaled)
        predictions = model.transform(scaled)
        score = evaluator.evaluate(predictions)
        print(f"{k:<5} {score:>12.4f}")
        if score > best_score:
            best_score = score
            best_k = k

    print(f"\nBest k={best_k} (Silhouette={best_score:.4f})")
    return best_k, scaled


def run_kmeans(scaled_df, k):
    """Run KMeans with k clusters and return results."""
    kmeans = KMeans(featuresCol="features", predictionCol="segment", k=k, seed=42)
    model = kmeans.fit(scaled_df)
    predictions = model.transform(scaled_df)

    # Cluster centers
    centers = model.clusterCenters()
    print("\nCluster Centers (standardized):")
    for i, center in enumerate(centers):
        print(f"  Segment {i}: recency={center[0]:.2f}, frequency={center[1]:.2f}, monetary={center[2]:.2f}")

    return predictions, centers


def label_segments(predictions, centers):
    """Assign business labels based on cluster center values."""
    # Sort clusters by monetary (higher = more valuable)
    cluster_monetary = [(i, c[2]) for i, c in enumerate(centers)]
    cluster_monetary.sort(key=lambda x: x[1], reverse=True)

    labels = {}
    segment_names = ["Champions", "Loyal", "Potential", "At-Risk", "Lost", "Cold"]
    for rank, (cluster_id, _) in enumerate(cluster_monetary):
        labels[cluster_id] = segment_names[rank] if rank < len(segment_names) else f"Segment-{rank}"

    # Map segment number to label
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    label_map = create_map([lit(x) for pair in labels.items() for x in pair])

    result = predictions.withColumn(
        "segment_label",
        label_map[col("segment")]
    )
    return result


def compute_segment_summary(predictions):
    """Aggregate metrics per segment for dashboard."""
    return (
        predictions.groupBy("segment", "segment_label")
        .agg(
            spark_count("*").alias("user_count"),
            spark_round(avg("recency"), 1).alias("avg_recency"),
            spark_round(avg("frequency"), 1).alias("avg_frequency"),
            spark_round(avg("monetary"), 2).alias("avg_monetary"),
            spark_round(avg("avg_order_value"), 2).alias("avg_order_value"),
        )
        .orderBy("segment")
    )


def main():
    logger.info("Starting Customer Segmentation...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bucket = CONFIG["minio_bucket"]
    input_path = f"s3a://{bucket}/raw/"
    logger.info(f"Reading from data lake: {input_path}")
    df = spark.read.parquet(input_path)

    total = df.count()
    logger.info(f"Total rows: {total}")

    # Step 1: Compute RFM features
    logger.info("Computing RFM features...")
    rfm = compute_rfm(df)
    rfm.cache()
    rfm_count = rfm.count()
    logger.info(f"Unique users with purchases: {rfm_count}")

    rfm.summary().show()

    # Step 2: Find optimal k
    num_clusters = CONFIG["num_clusters"]
    if num_clusters <= 0:
        logger.info("Auto-detecting optimal k...")
        best_k, scaled_df = find_optimal_k(rfm, max_k=6)
    else:
        best_k = num_clusters
        assembler = VectorAssembler(
            inputCols=["recency", "frequency", "monetary"],
            outputCol="features_raw"
        )
        assembled = assembler.transform(rfm)
        scaler = StandardScaler(
            inputCol="features_raw", outputCol="features",
            withStd=True, withMean=True
        )
        scaled_df = scaler.fit(assembled).transform(assembled)

    # Step 3: Run KMeans
    logger.info(f"Running KMeans with k={best_k}...")
    predictions, centers = run_kmeans(scaled_df, best_k)

    # Step 4: Label segments
    predictions = label_segments(predictions, centers)

    # Step 5: Segment summary
    summary = compute_segment_summary(predictions)
    summary.show(truncate=False)

    # Step 6: Write results to ES
    # Per-user segment assignments
    user_segments = predictions.select(
        "user_id", "recency", "frequency", "monetary",
        "avg_order_value", "unique_products", "segment", "segment_label"
    )
    write_to_es(user_segments, "ml-customer-segments")

    # Segment summary for dashboard
    write_to_es(summary, "ml-segment-summary")

    logger.info("Customer Segmentation complete.")
    spark.stop()


if __name__ == "__main__":
    main()