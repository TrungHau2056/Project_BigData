import os
import time
import logging
import psutil

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count, desc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_session", StringType(), True),
])


def benchmark_pandas(file_path: str, nrows: int = None):
    logger.info(f"[Pandas] Reading {file_path}...")
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024 / 1024

    start = time.time()
    df = pd.read_csv(file_path, nrows=nrows)
    read_time = time.time() - start

    mem_after_read = process.memory_info().rss / 1024 / 1024

    # Revenue by brand (purchases only)
    start = time.time()
    purchases = df[df["event_type"] == "purchase"]
    result = (
        purchases[purchases["brand"].notna()]
        .groupby("brand")["price"]
        .agg(["sum", "count"])
        .sort_values("sum", ascending=False)
        .head(10)
    )
    agg_time = time.time() - start

    mem_after_agg = process.memory_info().rss / 1024 / 1024

    return {
        "rows": len(df),
        "read_time_sec": round(read_time, 2),
        "agg_time_sec": round(agg_time, 2),
        "total_time_sec": round(read_time + agg_time, 2),
        "peak_memory_mb": round(max(mem_after_read, mem_after_agg), 2),
        "memory_increase_mb": round(max(mem_after_read, mem_after_agg) - mem_before, 2),
    }


def benchmark_spark(file_path: str, nrows: int = None):
    logger.info(f"[Spark] Creating session and reading {file_path}...")
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024 / 1024

    start = time.time()
    spark = SparkSession.builder \
        .appName("PerformanceBenchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    reader = spark.read.option("header", "true").schema(SCHEMA)
    df = reader.csv(file_path)
    if nrows:
        df = df.limit(nrows)
    df.cache()
    row_count = df.count()
    read_time = time.time() - start

    mem_after_read = process.memory_info().rss / 1024 / 1024

    # Revenue by brand (purchases only)
    start = time.time()
    from pyspark.sql.functions import round as spark_round
    result = (
        df.filter((col("event_type") == "purchase") & col("brand").isNotNull())
        .groupBy("brand")
        .agg(
            spark_round(spark_sum("price"), 2).alias("total_revenue"),
            spark_count("*").alias("order_count"),
        )
        .orderBy(desc("total_revenue"))
        .limit(10)
    )
    result.show(truncate=False)
    agg_time = time.time() - start

    mem_after_agg = process.memory_info().rss / 1024 / 1024

    spark.stop()

    return {
        "rows": row_count,
        "read_time_sec": round(read_time, 2),
        "agg_time_sec": round(agg_time, 2),
        "total_time_sec": round(read_time + agg_time, 2),
        "peak_memory_mb": round(max(mem_after_read, mem_after_agg), 2),
        "memory_increase_mb": round(max(mem_after_read, mem_after_agg) - mem_before, 2),
    }


def main():
    file_path = os.environ.get("DATA_FILE", "data/2019-Oct.csv")
    sample_rows = int(os.environ.get("SAMPLE_ROWS", "500000"))

    print("=" * 60)
    print("PERFORMANCE COMPARISON: Pandas vs Spark")
    print("=" * 60)

    # Run on a sample first
    print(f"\n--- Sample: {sample_rows} rows ---")
    pandas_sample = benchmark_pandas(file_path, nrows=sample_rows)
    spark_sample = benchmark_spark(file_path, nrows=sample_rows)

    print(f"\n{'Metric':<25} {'Pandas':>12} {'Spark':>12}")
    print("-" * 50)
    for key in pandas_sample:
        print(f"{key:<25} {pandas_sample[key]:>12} {spark_sample[key]:>12}")

    # Full dataset — pandas (may fail on low memory)
    print(f"\n--- Full dataset ---")
    try:
        pandas_full = benchmark_pandas(file_path)
        pandas_status = "OK"
    except MemoryError:
        pandas_full = {"total_time_sec": "OOM", "peak_memory_mb": "OOM"}
        pandas_status = "OOM (Out of Memory)"

    spark_full = benchmark_spark(file_path)

    print(f"\n{'Metric':<25} {'Pandas':>12} {'Spark':>12}")
    print("-" * 50)
    if pandas_status == "OK":
        for key in pandas_full:
            print(f"{key:<25} {pandas_full[key]:>12} {spark_full[key]:>12}")
    else:
        print(f"{'Result':<25} {pandas_status:>12} {'OK':>12}")

    print("\n" + "=" * 60)
    print("Conclusion: Spark handles large datasets by distributing")
    print("across partitions and spilling to disk, while pandas")
    print("loads everything into memory and fails on OOM.")
    print("=" * 60)


if __name__ == "__main__":
    main()