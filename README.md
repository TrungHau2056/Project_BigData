# Project_BigData

Run full pipeline with Docker so it can run on any machine without manual Python/Spark setup.

## What runs in Docker

- `zookeeper`: Kafka coordination
- `kafka`: event broker
- `producer`: reads `data/2019-Oct.csv` and publishes events to Kafka
- `spark-streaming`: consumes Kafka topic and prints cleaned stream to logs
- `elasticsearch` + `kibana`: optional analytics stack

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)

## Quick Start

From project root:

```bash
docker compose up --build
```

Stop and clean containers:

```bash
docker compose down
```

## Useful commands

Only start Kafka + streaming + producer:

```bash
docker compose up --build kafka zookeeper spark-streaming producer
```

Watch Spark logs only:

```bash
docker compose logs -f spark-streaming
```

Watch producer logs only:

```bash
docker compose logs -f producer
```

## Runtime configuration

Both scripts support environment variables:

- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `TOPIC_NAME` (default: `ecommerce-events`)

Producer-only variables:

- `DATA_FILE` (default: `data/2019-Oct.csv`)
- `SEND_DELAY_SEC` (default: `0.5`)

## Local (non-Docker) note for Windows

`spark_streaming.py` still supports Windows local execution and auto-detects `HADOOP_HOME`/`winutils.exe`.
Inside Linux Docker containers, this Windows patch is skipped automatically.