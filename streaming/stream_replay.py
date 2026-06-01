import os
import json
import time
import logging
from datetime import datetime, timezone

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CONFIG = {
    "minio_endpoint":          os.environ.get("MINIO_ENDPOINT",          "http://minio:9000"),
    "minio_access_key":        os.environ.get("MINIO_ACCESS_KEY",        "minioadmin"),
    "minio_secret_key":        os.environ.get("MINIO_SECRET_KEY",        "minioadmin"),
    "minio_bucket":            os.environ.get("MINIO_BUCKET",            "ecommerce-datalake"),
    "kafka_bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    "topic_name":              os.environ.get("TOPIC_NAME",              "ecommerce-events"),
    "replay_start_date":       os.environ.get("REPLAY_START_DATE",       "2019-10-25"),
    "events_per_sec": int(os.environ.get("EVENTS_PER_SEC", "1000")),
}



def main():
    logger.info("Starting stream replay: MinIO → Kafka...")

    endpoint = CONFIG["minio_endpoint"].replace("http://", "")
    fs = pafs.S3FileSystem(
        access_key=CONFIG["minio_access_key"],
        secret_key=CONFIG["minio_secret_key"],
        endpoint_override=endpoint,
        scheme="http",
    )

    dataset = ds.dataset(
        f"{CONFIG['minio_bucket']}/raw/",
        filesystem=fs,
        format="parquet",
        partitioning="hive",
    )

    filter_expr = ds.field("event_date") >= CONFIG["replay_start_date"]
    columns = [name for name in dataset.schema.names if name != "event_date"]

    total = dataset.count_rows(filter=filter_expr)
    batch_size = CONFIG["events_per_sec"]

    logger.info(
        f"Total events to replay: {total:,} "
        f"at ~{batch_size:,} events/sec"
    )

    producer = KafkaProducer(
        bootstrap_servers=CONFIG["kafka_bootstrap_servers"],
        compression_type="gzip",
        batch_size=65536,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    sent = 0
    batch_num = 0

    try:
        batches = dataset.to_batches(
            batch_size=batch_size,
            filter=filter_expr,
            columns=columns,
        )

        for batch in batches:
            batch_num += 1
            batch_start = time.time()

            now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            for row in batch.to_pylist():
                row["event_time"] = now_ts
                producer.send(CONFIG["topic_name"], row)

            producer.flush()

            sent += batch.num_rows
            elapsed = time.time() - batch_start

            logger.info(
                f"Batch {batch_num}: sent {batch.num_rows:,} events "
                f"in {elapsed:.2f}s ({sent:,}/{total:,})"
            )

            sleep_for = max(0.0, 1.0 - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)

    finally:
        producer.flush()
        producer.close()

    logger.info(f"Replay complete: {sent:,} events sent to topic '{CONFIG['topic_name']}'.")

if __name__ == "__main__":
    main()