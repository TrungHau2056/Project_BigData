# Project_BigData

Docker-only real-time data pipeline:

Producer -> Kafka -> Spark Structured Streaming -> Elasticsearch -> Kibana

## What runs in Docker

- `zookeeper`: Kafka coordination
- `kafka`: event broker
- `producer`: reads `data/2019-Oct.csv` and publishes events to Kafka
- `spark-streaming`: consumes Kafka topic, cleans data, writes to Elasticsearch
- `elasticsearch`: stores indexed events for search/analytics
- `kibana`: UI for Discover/Data View/Dashboard

## Execution mode

- This project is intentionally **Docker-only**.
- Local manual runtime setup is intentionally excluded to avoid confusion.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)

## Architecture flow

1. `process_data.py` reads CSV by chunks and sends events to Kafka topic `ecommerce-events`.
2. Kafka stores events and exposes stream offsets.
3. `spark_streaming.py` parses JSON events, filters invalid rows, then writes to Elasticsearch index `ecommerce-events`.
4. Kibana reads from Elasticsearch for Discover and Dashboard.

## Reliable startup (recommended)

From project root:

```bash
docker compose down --remove-orphans
docker compose build producer
docker compose up -d zookeeper kafka
docker compose up -d producer
docker compose up -d spark-streaming
docker compose up -d elasticsearch kibana
```

Check running services:

```bash
docker compose ps
```

## Verification checkpoints

Producer emits events:

```bash
docker compose logs -f producer
```

Spark streaming query active:

```bash
docker compose logs -f spark-streaming
```

Elasticsearch index exists and grows:

```bash
curl.exe "http://localhost:9200/_cat/indices?v"
curl.exe "http://localhost:9200/ecommerce-events/_count"
```

Kibana UI:

- Open `http://localhost:5601`
- Create Data View: `ecommerce-events*`

## Useful commands

Tail a specific service:

```bash
docker compose logs -f spark-streaming
docker compose logs -f producer
docker compose logs -f kafka
```

Restart stream consumers after config/code changes:

```bash
docker compose restart producer spark-streaming
```

Stop and clean:

```bash
docker compose down --remove-orphans
```

## Runtime configuration

`producer`:

- `KAFKA_BOOTSTRAP_SERVERS` (default: `kafka:29092`)
- `TOPIC_NAME` (default: `ecommerce-events`)
- `DATA_FILE` (default: `data/2019-Oct.csv`)
- `SEND_DELAY_SEC` (default: `0.5`)

`spark-streaming`:

- `KAFKA_BOOTSTRAP_SERVERS` (default: `kafka:29092`)
- `TOPIC_NAME` (default: `ecommerce-events`)
- `ES_NODES` (default: `elasticsearch`)
- `ES_PORT` (default: `9200`)
- `ES_INDEX` (default: `ecommerce-events`)
- `SPARK_KAFKA_PACKAGE` (Spark + Kafka + Elasticsearch connector coordinates)

## Troubleshooting

`index_not_found_exception` on `ecommerce-events`:

1. Ensure `spark-streaming` is running.
2. Check Spark logs for connector errors.
3. Restart producer and spark services.

PowerShell `curl` prompt/warning:

- Use `curl.exe` instead of `curl` alias.

Spark dependency cache issues (`/nonexistent/.ivy...`):

- `docker-compose.yml` already sets `HOME=/tmp` and `spark.jars.ivy=/tmp/.ivy2`.

No data in Kibana Discover:

1. Verify `_count` endpoint returns `count > 0`.
2. Expand Kibana time range (Last 24 hours or larger).
3. Refresh Data View fields.

## Notes

- Source dataset may contain string values like `"NaN"` in optional fields (e.g. `brand`).
- This is source-data quality, not pipeline failure.