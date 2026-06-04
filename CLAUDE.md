# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time e-commerce intelligence platform using Lambda Architecture with Airflow orchestration:
```
                        ┌─ Airflow (orchestration) ──────────────────────┐
                        │                                                │
CSV → Spark (bulk) → MinIO (Parquet) ──┬── Spark Batch (analytics) → ES → Kibana
                                        ├── Feature Engineering → MinIO (for ML)
                                        └── Stream Replay → Kafka
                                                            ├── Spark Streaming (aggregations) → ES
                                                            └── Anomaly Detection → ES

MinIO (features) → ML training (by other team member)
```

No local runtime setup — everything runs via Docker Compose.

## Development Commands

**Start the pipeline (staggered for dependency readiness):**
```bash
docker compose down --remove-orphans
docker compose up -d zookeeper kafka
docker compose up -d minio
# Wait for minio-init to finish creating bucket

# Step 1: Bulk ingest CSV → MinIO
docker compose up ingest-to-lake

# Step 2: Feature engineering
docker compose up feature-engineering

# Step 3: Start streaming infrastructure
docker compose up -d elasticsearch kibana
# IMPORTANT: Only run ONE Spark streaming container at a time (memory constraint)
# Option A: Spark Streaming (raw events + windowed aggregations)
docker compose up -d spark-streaming
docker compose up stream-replay   # send events, streaming will consume them
# Option B: Anomaly Detection (price/volume/session anomalies)
docker compose stop spark-streaming
docker compose up -d spark-anomaly
docker compose up stream-replay   # send events, anomaly will process them

# Step 4: Run batch analytics (one-shot job)
docker compose up spark-batch

# Step 5: Run customer segmentation (one-shot job)
docker compose up customer-segmentation

# Step 6: Airflow orchestration
docker compose up -d airflow-init
# Wait for init to complete
docker compose up -d airflow-webserver airflow-scheduler
# Airflow UI: http://localhost:8080 (airflow/airflow)
```

**Run batch analytics:**
```bash
docker compose up spark-batch
```

**Run customer segmentation (ML):**
```bash
docker compose up customer-segmentation
```

**Operational commands:**
```bash
docker compose logs -f ingest-to-lake        # Tail ingest logs
docker compose logs -f stream-replay          # Tail stream replay logs
docker compose logs -f spark-streaming        # Tail Spark streaming logs
docker compose logs -f spark-anomaly          # Tail anomaly detection logs
docker compose logs -f spark-batch            # Tail batch job logs
docker compose logs -f feature-engineering    # Tail feature engineering logs
docker compose ps                             # Check running services
```

**Verify data in Elasticsearch (Windows — use `curl.exe`, not `curl`):**
```bash
curl.exe "http://localhost:9200/_cat/indices?v"
curl.exe "http://localhost:9200/ecommerce-events/_count"
curl.exe "http://localhost:9200/streaming-anomalies/_count"
curl.exe "http://localhost:9200/data-quality-report/_count"
curl.exe "http://localhost:9200/ml-customer-segments/_count"
```

**Verify data in MinIO:** http://localhost:9001 (minioadmin/minioadmin)

**Airflow UI:** http://localhost:8080 (airflow/airflow)

**Stop and clean:**
```bash
docker compose down --remove-orphans
docker volume rm project_bigdata_minio-data project_bigdata_airflow-data
```

## Architecture

### Data Ingestion (Landing Zone)
- **Ingest to Lake** (`ingest_to_lake.py`): Spark reads CSV in distributed mode, validates data quality (null checks, negative prices, event type distribution), writes Parquet to MinIO partitioned by `event_date`. Replaces the old row-by-row pandas producer.
- **Stream Replay** (`stream_replay.py`): Reads Parquet from MinIO, sends events to Kafka at configurable rate (`EVENTS_PER_SEC`). Uses Spark for distributed read + Kafka producer for sending. Replaces the old `process_data.py`.

### Batch Layer
- **Spark Batch** (`spark_batch.py`): Reads Parquet from MinIO (data lake), computes 6 analytics and writes to ES indices:
  - `batch-revenue-category`: Revenue by category_code
  - `batch-revenue-brand`: Revenue by brand
  - `batch-daily-revenue`: Daily revenue trend
  - `batch-conversion-funnel`: Event type distribution (view/cart/purchase)
  - `batch-top-products`: Top 20 products by revenue
  - `batch-hourly-activity`: Activity pattern by hour of day
- **Feature Engineering** (`feature_engineering.py`): Reads Parquet from MinIO, computes feature tables for ML training:
  - User features (RFM + engagement) → `s3a://ecommerce-datalake/features/user_features/`
  - Product features (popularity + revenue) → `s3a://ecommerce-datalake/features/product_features/`
  - Interaction matrix (implicit feedback for ALS) → `s3a://ecommerce-datalake/features/interactions/`
  - Data quality report → ES index `data-quality-report`

### Speed Layer (Streaming)
- **Spark Streaming** (`spark_streaming.py`): Consumes from Kafka, two output queries:
  1. Raw events → ES index `ecommerce-events` (via `foreachBatch`)
  2. Windowed aggregations (revenue/event count by 5-min window + event_type, with watermark) → ES index `streaming-windowed-revenue`
- **Anomaly Detection** (`streaming_anomaly.py`): Consumes from Kafka, detects:
  1. Price anomaly (z-score > 2 vs category average)
  2. Volume spike (> 3x increase in event count)
  3. Suspicious session (> 100 events/min/session)
  - Results → ES index `streaming-anomalies`

### ML Layer
- **Customer Segmentation** (`customer_segmentation.py`): Reads from MinIO data lake, Spark MLlib pipeline:
  1. Compute RFM features per user
  2. StandardScaler normalization
  3. KMeans clustering with configurable k
  4. Business labels: Champions, Loyal, Potential, At-Risk, Lost
  5. Results → ES indices `ml-customer-segments` and `ml-segment-summary`

### Orchestration
- **Airflow**: 3 DAGs in `dags/` directory using `DockerOperator` to launch Spark containers:
  1. `ecommerce_data_pipeline`: ingest → feature_engineering → quality_check (manual trigger)
  2. `ecommerce_daily_analytics`: batch → segmentation (@daily)
  3. `ecommerce_streaming_monitor`: check Kafka + ES health (every 5 min, uses Docker API instead of docker compose CLI)
- **Custom Airflow image** (`Dockerfile.airflow`): Based on `apache/airflow:2.8.1` with `apache-airflow-providers-docker` and `docker` Python package installed

### Infrastructure
- **Kafka**: Dual-listener broker — `INTERNAL://kafka:29092` for container-to-container, `EXTERNAL://localhost:9092` for host access
- **MinIO**: S3-compatible object storage at `localhost:9000` (console at `:9001`)
- **Elasticsearch**: Single-node, security disabled, auto-creates indices
- **Kibana**: UI at http://localhost:5601
- **Shared Schema** (`schemas.py`): Common `ECOMMERCE_SCHEMA` used by all Spark scripts

## MinIO Data Structure

```
ecommerce-datalake/
  raw/                           # Landing zone — Parquet partitioned by event_date
  features/
    user_features/               # User feature table (RFM + engagement) — for ML
    product_features/            # Product feature table (popularity + revenue) — for ML
    interactions/                # User-Product interaction matrix (for ALS) — for ML
```

## Key Design Details

- **Lambda Architecture**: Batch layer (Spark reads MinIO) + Speed layer (Kafka → Spark Streaming) + Serving layer (ES + Kibana)
- **Startup ordering matters**: MinIO before ingest, ingest before feature-engineering, ES before streaming, Kafka before stream-replay
- **Spark `startingOffsets: latest`**: Data produced before Spark starts is **not consumed**. Start streaming before or concurrently with stream-replay.
- **Spark Ivy cache**: All spark containers set `HOME=/tmp`, `spark.jars.ivy=/tmp/.ivy2`, and share host directory `.ivy-cache/` so JARs are downloaded once and reused
- **Rate-controlled replay**: `stream_replay.py` sends events at configurable `EVENTS_PER_SEC` instead of slow row-by-row
- **Volume mount `./:/app`**: All services mount the project root, so code changes are reflected without rebuilding
- **Streaming watermarking**: `withWatermark("event_timestamp", "10 minutes")` handles late-arriving data
- **MinIO bucket auto-creation**: `minio-init` service uses `mc` to create the `ecommerce-datalake` bucket
- **Feature tables for ML**: Preprocessed feature tables in MinIO are the handoff point for ML training

## Runtime Configuration

| Service | Variable | Default |
|---------|----------|---------|
| Ingest to Lake | `DATA_FILE` | `data/2019-Oct.csv` |
| Ingest to Lake | `MINIO_ENDPOINT` | `http://minio:9000` |
| Ingest to Lake | `MINIO_BUCKET` | `ecommerce-datalake` |
| Stream Replay | `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| Stream Replay | `TOPIC_NAME` | `ecommerce-events` |
| Stream Replay | `MINIO_ENDPOINT` | `http://minio:9000` |
| Stream Replay | `MINIO_BUCKET` | `ecommerce-datalake` |
| Stream Replay | `EVENTS_PER_SEC` | `1000` |
| Feature Engineering | `MINIO_ENDPOINT` | `http://minio:9000` |
| Feature Engineering | `MINIO_BUCKET` | `ecommerce-datalake` |
| Feature Engineering | `ES_NODES` | `elasticsearch` |
| Feature Engineering | `ES_PORT` | `9200` |
| Spark Streaming | `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| Spark Streaming | `TOPIC_NAME` | `ecommerce-events` |
| Spark Streaming | `ES_NODES` | `elasticsearch` |
| Spark Streaming | `ES_PORT` | `9200` |
| Spark Streaming | `WINDOW_DURATION` | `5 minutes` |
| Spark Streaming | `WATERMARK_DELAY` | `10 minutes` |
| Spark Anomaly | `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| Spark Anomaly | `TOPIC_NAME` | `ecommerce-events` |
| Spark Anomaly | `ES_NODES` | `elasticsearch` |
| Spark Anomaly | `ES_PORT` | `9200` |
| Spark Anomaly | `PRICE_ZSCORE_THRESHOLD` | `2.0` |
| Spark Anomaly | `VOLUME_SPIKE_FACTOR` | `3.0` |
| Spark Anomaly | `SESSION_EVENT_LIMIT` | `100` |
| Spark Batch | `MINIO_ENDPOINT` | `http://minio:9000` |
| Spark Batch | `MINIO_BUCKET` | `ecommerce-datalake` |
| Spark Batch | `ES_NODES` | `elasticsearch` |
| Spark Batch | `ES_PORT` | `9200` |
| Customer Segmentation | `MINIO_ENDPOINT` | `http://minio:9000` |
| Customer Segmentation | `MINIO_BUCKET` | `ecommerce-datalake` |
| Customer Segmentation | `ES_NODES` | `elasticsearch` |
| Customer Segmentation | `ES_PORT` | `9200` |
| Customer Segmentation | `NUM_CLUSTERS` | `4` |

## Data Schema

CSV columns: `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`

Spark schema defined in `schemas.py`: `ECOMMERCE_SCHEMA`
- `event_time`/`event_type`/`category_id`/`category_code`/`brand`/`user_session` → StringType
- `product_id`/`user_id` → IntegerType
- `price` → DoubleType

## Elasticsearch Indices

| Index | Source | Content |
|-------|--------|---------|
| `ecommerce-events` | Streaming | Raw events from Kafka |
| `streaming-windowed-revenue` | Streaming | Windowed aggregations (5-min) |
| `streaming-anomalies` | Anomaly | Price/volume/session anomalies |
| `batch-revenue-category` | Batch | Revenue by category |
| `batch-revenue-brand` | Batch | Revenue by brand |
| `batch-daily-revenue` | Batch | Daily revenue trend |
| `batch-conversion-funnel` | Batch | View/cart/purchase counts |
| `batch-top-products` | Batch | Top 20 products by revenue |
| `batch-hourly-activity` | Batch | Activity by hour |
| `ml-customer-segments` | ML | Per-user segment assignment |
| `ml-segment-summary` | ML | Segment aggregate stats |
| `data-quality-report` | Feature Eng | Data quality stats + validation |

## Common Issues

- `index_not_found_exception`: Start streaming services and ensure stream-replay is sending data
- No data in Kibana: Expand time range to "Last 24 hours" and refresh Data View fields
- Source data contains string `"NaN"` in optional fields like `brand` — expected source data quality
- Spark Ivy `/nonexistent` errors: Ensure `HOME=/tmp` and `spark.jars.ivy=/tmp/.ivy2` are set
- PowerShell `curl` alias issues: Use `curl.exe` instead of `curl`
- MinIO port conflict: Change the host port mapping in docker-compose.yml
- KMeans takes long on full dataset: Reduce `NUM_CLUSTERS` or use a smaller `DATA_FILE` for testing
- Airflow can't reach Docker: Ensure Docker socket is mounted (`/var/run/docker.sock`) and `Dockerfile.airflow` has been built (`docker compose build airflow-init`)
- Airflow `DockerOperator` network error: Ensure `network_mode: project_bigdata_default` matches the actual Docker network name
- **OOM / container exit code 137**: Cannot run 2+ Spark streaming containers simultaneously. Run spark-streaming OR spark-anomaly, not both. Stop one before starting the other.
- **Maven JAR download failed**: Transient network issue. Retry the command. JARs are cached in `.ivy-cache/` host directory so subsequent runs are faster.
- **`DateTimeParseException: unparsed text found at index 19`**: Source data has "UTC" suffix in `event_time`. Code uses `regexp_replace(col("event_time"), r"\s+UTC$", "")` to handle this.
- **`countDistinct not supported on streaming DataFrames`**: Use `approx_count_distinct()` instead in streaming queries.