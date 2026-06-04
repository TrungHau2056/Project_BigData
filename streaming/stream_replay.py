import os
import json
import time
import logging
from datetime import datetime, timezone

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from kafka import KafkaProducer
import random

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

BOT_SESSION_CHANCE = 0.20
SCRAPER_SESSION_CHANCE = 0.20

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

            now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

            for row in batch.to_pylist():
                row["event_time"] = now_ts
                producer.send(CONFIG["topic_name"], row)

            # --- ANOMALY A: Suspicious Session Bot ---
            if random.random() < BOT_SESSION_CHANCE:
                bot_user = random.randint(90000, 99999)
                bot_sess = f"bot-session-{random.randint(100, 999)}"
                logger.warning(f"🚨 Injecting Bot Session")

                for _ in range(120):
                    producer.send(CONFIG["topic_name"], {
                        "event_time": now_ts,
                        "event_type": "view",
                        "product_id": random.randint(10000000, 40000000),
                        "category_code": "apparel.shoes.sneakers",
                        "category_level_1": "apparel",
                        "category_level_2": "shoes",
                        "category_level_3": "sneakers",
                        "price": 45.00,
                        "user_id": bot_user,
                        "user_session": bot_sess,
                        "brand": "Nike",
                        "category_id": 2053013565639492569
                    })

            # --- ANOMALY B: Hierarchical Volume Spikes ---
            if random.random() < SCRAPER_SESSION_CHANCE:
                spike_type = random.choice(["leaf_only", "sub_only", "root_only"])

                # Leaf Level Spike (Threshold > 50)
                if spike_type == "leaf_only":
                    logger.warning("🔥 Anomaly Triggered: Leaf-focused Spike (Target: GPU Scalper Bot)")
                    for _ in range(60):
                        producer.send(CONFIG["topic_name"], {
                            "event_time": now_ts,
                            "event_type": "view",
                            "product_id": random.randint(5000, 6000),
                            "price": 499.99,
                            "user_id": random.randint(532105499, 600000000),
                            "user_session": f"leaf-sess-{random.randint(100,999)}",
                            "category_code": "computers.components.gpu",
                            "category_level_1": "computers",
                            "category_level_2": "components",
                            "category_level_3": "gpu",
                            "brand": "NVIDIA",
                            "category_id": 2053013554658804075
                        })

                # Sub Level Spike (Threshold > 100, keeping leafs under 50)
                elif spike_type == "sub_only":
                    logger.warning("🔥 Anomaly Triggered: Distributed Sub-Level Spike (Target: Shoe Drop)")
                    for style in ["sneakers", "boots", "sandals"]:
                        for _ in range(40):
                            producer.send(CONFIG["topic_name"], {
                                "event_time": now_ts,
                                "event_type": "view",
                                "product_id": random.randint(5000, 6000),
                                "price": 85.00,
                                "user_id": random.randint(1000, 2000),
                                "user_session": f"sub-sess-{random.randint(100,999)}",
                                "category_code": f"apparel.shoes.{style}",
                                "category_level_1": "apparel",
                                "category_level_2": "shoes",
                                "category_level_3": style,
                                "brand": "Adidas",
                                "category_id": 2053013554658804075
                            })

                # Root Level Spike (Threshold > 200, keeping everything else lower)
                elif spike_type == "root_only":
                    logger.warning("🔥 Anomaly Triggered: Macro Root-Level Spike (Target: Broad Electronics Scraper)")
                    sub_departments = {
                        "smartphone": ["iphone", "android"],
                        "audio": ["headphones", "speakers"],
                        "video": ["tv", "projectors"]
                    }
                    for sub, leafs in sub_departments.items():
                        for leaf in leafs:
                            for _ in range(25): # 3 subs * 2 leafs * 25 = 150 + base data easily crosses 200
                                producer.send(CONFIG["topic_name"], {
                                    "event_time": now_ts,
                                    "event_type": "view",
                                    "product_id": random.randint(5000, 6000),
                                    "price": 150.00,
                                    "user_id": random.randint(1000, 2000),
                                    "user_session": f"root-sess-{random.randint(100,999)}",
                                    "category_code": f"electronics.{sub}.{leaf}",
                                    "category_level_1": "electronics",
                                    "category_level_2": sub,
                                    "category_level_3": leaf
                                })

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