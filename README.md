# E-Commerce Big Data Platform - Lambda Architecture

Nền tảng phân tích dữ liệu thương mại điện tử thời gian thực sử dụng **Lambda Architecture** với Kafka, Spark, Elasticsearch và Airflow.

## Mục lục

- [Tổng quan](#tổng-quan)
- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Các thành phần](#các-thành-phần)
- [Luồng dữ liệu](#luồng-dữ-liệu)
- [Cài đặt và chạy](#cài-đặt-và-chạy)
- [Xem và phân tích dữ liệu](#xem-và-phân-tích-dữ-liệu)
- [Troubleshooting](#troubleshooting)
- [Scale với dữ liệu lớn](#scale-với-dữ-liệu-lớn)

---

## Tổng quan

Platform xử lý dữ liệu e-commerce từ dataset Kaggle với 3 lớp xử lý:

| Lớp | Chức năng | Công nghệ |
|-----|-----------|-----------|
| **Batch Layer** | Xử lý dữ liệu lịch sử toàn bộ | Spark + MinIO |
| **Speed Layer** | Xử lý streaming thời gian thực | Kafka + Spark Streaming |
| **Serving Layer** | Hiển thị & truy vấn | Elasticsearch + Kibana |

**Nguồn dữ liệu**: File CSV e-commerce events (view, cart, purchase) với các trường:
- `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`

---

## Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         LAMBDA ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    BATCH LAYER (Spark)                          │   │
│  │                                                                 │   │
│  │  CSV (15GB) ──► Spark Ingest ──► MinIO (Parquet)               │   │
│  │                              │                                  │   │
│  │                              ├──► Feature Engineering ──► ML    │   │
│  │                              │                                  │   │
│  │                              └──► Spark Batch Analytics         │   │
│  │                                   6 báo cáo                    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                   SPEED LAYER (Streaming)                       │   │
│  │                                                                 │   │
│  │  MinIO ──► Stream Replay ──► Kafka ──┬──► Spark Streaming      │   │
│  │                                        │    (windowed agg)      │   │
│  │                                        └──► Anomaly Detection  │   │
│  │                                             (z-score, spike)   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  SERVING LAYER (Kibana)                         │   │
│  │                                                                 │   │
│  │  Elasticsearch (11 indices) ◄── Spark / Streaming               │   │
│  │       │                                                         │   │
│  │       └──► Kibana Dashboard                                     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              ORCHESTRATION (Airflow)                            │   │
│  │                                                                 │   │
│  │  3 DAGs: Data Pipeline, Daily Analytics, Streaming Monitor      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Các thành phần

### 1. Hạ tầng (Infrastructure)

| Service | Port | Mô tả |
|---------|------|-------|
| `kafka` | 9092 (external), 29092 (internal) | Message broker (KRaft mode, không cần Zookeeper) |
| `kafka-init` | — | Tạo topic `ecommerce-events` (chạy 1 lần) |
| `minio` | 9000 (API), 9001 (console) | S3-compatible object storage |
| `minio-init` | — | Tạo bucket `ecommerce-datalake` (chạy 1 lần) |
| `elasticsearch` | 9200 | Search & analytics engine |
| `kibana` | 5601 | Visualization UI |

### 2. Batch Layer

| Service | File | Chức năng |
|---------|------|-----------|
| `ingest-to-lake` | `ingest_to_lake.py` | Đọc CSV, validate, ghi Parquet vào MinIO |
| `feature-engineering` | `feature_engineering.py` | Tính RFM, engagement features cho ML |
| `spark-batch` | `spark_batch.py` | 6 báo cáo analytics |
| `customer-segmentation` | `customer_segmentation.py` | KMeans clustering (4 nhóm) |

### 3. Speed Layer (Streaming)

| Service | File | Chức năng |
|---------|------|-----------|
| `stream-replay` | `stream_replay.py` | Replay Parquet → Kafka (1000 events/sec) |
| `spark-streaming` | `spark_streaming.py` | Streaming aggregations (5-min window) |
| `spark-anomaly` | `streaming_anomaly.py` | Phát hiện bất thường (price, volume, session) |

> **⚠️ Lưu ý**: Không thể chạy `spark-streaming` và `spark-anomaly` cùng lúc do giới hạn memory (OOM). Phải stop cái này trước khi start cái kia.

### 4. Orchestration

| Service | DAG | Lịch trình |
|---------|-----|------------|
| `airflow-webserver` | `ecommerce_data_pipeline` | Manual trigger |
| `airflow-scheduler` | `ecommerce_daily_analytics` | @daily |
| | `ecommerce_streaming_monitor` | Every 5 min |

---

## Luồng dữ liệu

### Batch Pipeline
```
CSV → Spark Ingest → MinIO (Parquet partitioned by event_date)
                    ↓
        ┌───────────┼───────────┐
        ↓           ↓           ↓
   Spark Batch   Feature Eng   ML Segmentation
        ↓           ↓           ↓
   ES Indices   MinIO Features ES Indices
```

### Streaming Pipeline
```
MinIO → Stream Replay → Kafka (ecommerce-events)
                        ↓
              ┌─────────┴─────────┐
              ↓                   ↓ (chọn 1, không chạy cùng lúc)
     Spark Streaming       Anomaly Detection
              ↓                   ↓
     ES: ecommerce-events   ES: streaming-anomalies
```

---

## Cài đặt và chạy

### Bước 1: Khởi động hạ tầng

```bash
# Dừng và cleanup (nếu có)
docker compose down --remove-orphans

# Start Kafka (KRaft mode, không cần Zookeeper)
docker compose up -d kafka

# Tạo topic ecommerce-events
docker compose up kafka-init

# Start MinIO (tự động tạo bucket ecommerce-datalake)
docker compose up -d minio

# Đợi minio-init tạo bucket xong
```

**Kiểm tra**:
```bash
docker compose ps
# Kafka và MinIO phải ở trạng thái "healthy" hoặc "running"
```

### Bước 2: Ingest dữ liệu vào Data Lake

```bash
docker compose up ingest-to-lake
```

**Giải thích**:
- Spark đọc file `data/2019-Oct.csv` (5.67 GB)
- Validate: null checks, giá âm, phân bố event type
- Ghi Parquet vào MinIO: `s3a://ecommerce-datalake/raw/` partition theo `event_date`

**Thời gian**: 5-10 phút

**Kiểm tra logs**:
```bash
docker compose logs ingest-to-lake
```

### Bước 3: Feature Engineering

```bash
docker compose up feature-engineering
```

**Giải thích**:
- Đọc Parquet từ MinIO
- Tính toán 3 bảng features:
  - **User features**: RFM (Recency, Frequency, Monetary) + engagement score
  - **Product features**: popularity + revenue
  - **Interactions**: user-product matrix cho ALS
- Ghi báo cáo chất lượng data vào ES index `data-quality-report`

**Thời gian**: 5-10 phút

### Bước 4: Khởi động Streaming

```bash
# Start Elasticsearch + Kibana
docker compose up -d elasticsearch kibana

# QUAN TRỌNG: Chỉ chạy MỘT Spark streaming container tại một thời điểm (giới hạn memory)

# Option A: Spark Streaming (raw events + windowed aggregations)
docker compose up -d spark-streaming
docker compose up stream-replay   # gửi events, streaming sẽ consume

# Option B: Anomaly Detection (price/volume/session anomalies)
docker compose stop spark-streaming
docker compose up -d spark-anomaly
docker compose up stream-replay   # gửi events, anomaly sẽ process
```

**⚠️ Lưu ý quan trọng**:
- `spark-streaming` dùng `startingOffsets: latest` — chỉ consume data **sau khi** Spark start
- Phải start `spark-streaming`/`spark-anomaly` **TRƯỚC** `stream-replay` để không mất data
- **Không chạy 2 Spark streaming container cùng lúc** — sẽ gây OOM (exit code 137). Chạy spark-streaming HOẶC spark-anomaly, không phải cả hai

**Kiểm tra streaming**:
```bash
# Xem logs stream-replay
docker compose logs -f stream-replay

# Xem logs spark-streaming
docker compose logs -f spark-streaming

# Kiểm tra Elasticsearch
curl.exe "http://localhost:9200/ecommerce-events/_count"
```

### Bước 5: Airflow Orchestration (tùy chọn)

```bash
# Initialize Airflow database + user
docker compose up -d airflow-init

# Chờ 60 giây cho init hoàn tất

# Start webserver + scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

**Truy cập Airflow UI**: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

---

## Chạy Batch Analytics

Sau khi đã ingest data (Bước 2), chạy bất kỳ lúc nào:

```bash
docker compose up spark-batch
```

**6 báo cáo được tạo**:

| ES Index | Nội dung |
|----------|----------|
| `batch-revenue-category` | Doanh thu theo category_code |
| `batch-revenue-brand` | Doanh thu theo brand |
| `batch-daily-revenue` | Xu hướng doanh thu theo ngày |
| `batch-conversion-funnel` | Phân bố view/cart/purchase |
| `batch-top-products` | Top 20 sản phẩm theo revenue |
| `batch-hourly-activity` | Hoạt động theo giờ trong ngày |

---

## Chạy Customer Segmentation (ML)

```bash
docker compose up customer-segmentation
```

**Output**:
- KMeans clustering với `k=4` (có thể config qua `NUM_CLUSTERS`)
- Business labels: **Champions, Loyal, Potential, At-Risk, Lost**
- ES indices: `ml-customer-segments`, `ml-segment-summary`

---

## Xem và phân tích dữ liệu

### 1. Kiểm tra Elasticsearch Indices

```bash
# Liệt kê tất cả indices
curl.exe "http://localhost:9200/_cat/indices?v"

# Kiểm tra số lượng documents
curl.exe "http://localhost:9200/ecommerce-events/_count"
curl.exe "http://localhost:9200/batch-revenue-category/_count"
curl.exe "http://localhost:9200/ml-customer-segments/_count"
curl.exe "http://localhost:9200/streaming-anomalies/_count"

# Xem sample data
curl.exe "http://localhost:9200/batch-conversion-funnel/_search?pretty"
```

### 2. Kibana - Visualization

**Truy cập**: http://localhost:5601

#### Tạo Data View (bắt buộc)

1. Vào **Management** → **Stack Management** → **Data Views**
2. Click **Create data view**
3. Nhập index pattern:
   - `batch-*` — xem tất cả batch reports
   - `ecommerce-events*` — xem raw streaming events
   - `streaming-anomalies*` — xem anomalies
   - `ml-customer-segments*` — xem customer segments
4. Chọn timestamp field (thường là `event_time` hoặc `@timestamp`)
5. Click **Save**

#### Xem dữ liệu trong Discover

1. Vào **Discover** (menu trái)
2. Chọn Data View từ dropdown (góc trái trên)
3. Chọn thời gian: **Last 24 hours** hoặc **Last 30 days** (góc phải trên)
4. Click **Refresh** nếu data không hiện

#### Tạo Visualization & Dashboard

1. Vào **Visualize Library** → **Create new visualization**
2. Chọn loại: **Data Table**, **Bar Chart**, **Line Chart**, **Pie Chart**
3. Chọn Data View đã tạo
4. Cấu hình metrics và buckets:
   - **Metrics**: Count, Sum, Average
   - **Buckets**: Aggregation by field (category, brand, event_type)
5. Click **Save** và đặt tên
6. Vào **Dashboard** → **Create new dashboard** → Add visualizations

#### Gợi ý dashboards

**A. Revenue Dashboard**
- Bar chart: Revenue by category (`batch-revenue-category`)
- Bar chart: Revenue by brand (`batch-revenue-brand`)
- Line chart: Daily revenue trend (`batch-daily-revenue`)

**B. Conversion Funnel Dashboard**
- Pie chart: Event type distribution (`batch-conversion-funnel`)
- Metric: Total views, carts, purchases

**C. Real-time Monitoring Dashboard**
- Line chart: Events per minute (`ecommerce-events`)
- Table: Recent anomalies (`streaming-anomalies`)
- Metric: Total events today

**D. Customer Segmentation Dashboard**
- Pie chart: Segment distribution (`ml-customer-segments`)
- Table: Segment summary stats (`ml-segment-summary`)

### 3. MinIO Console - Data Lake Browser

**Truy cập**: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`

**Cấu trúc bucket**:
```
ecommerce-datalake/
├── raw/
│   └── event_date=2019-10-01/
│   └── event_date=2019-10-02/
│   └── ...
├── features/
│   ├── user_features/
│   ├── product_features/
│   └── interactions/
```

### 4. Airflow UI - Pipeline Orchestration

**Truy cập**: http://localhost:8080 (airflow/airflow)

**3 DAGs**:

| DAG Name | Schedule | Mô tả |
|----------|----------|-------|
| `ecommerce_data_pipeline` | Manual | ingest → feature_engineering → quality_check |
| `ecommerce_daily_analytics` | @daily | batch_analytics → customer_segmentation |
| `ecommerce_streaming_monitor` | */5 * * * * | Check Kafka + ES health |

---

## Troubleshooting

### Elasticsearch index_not_found_exception

**Nguyên nhân**: Streaming services chưa chạy hoặc chưa có data.

**Giải pháp**:
```bash
# Start streaming services (chỉ MỘT Spark streaming container tại một thời điểm)
docker compose up -d spark-streaming
docker compose up stream-replay

# Chờ 2-3 phút cho data flow
curl.exe "http://localhost:9200/_cat/indices?v"
```

### Không có data trong Kibana Discover

1. **Kiểm tra data có trong ES chưa**:
   ```bash
   curl.exe "http://localhost:9200/batch-revenue-category/_count"
   ```

2. **Mở rộng time range**: Chọn "Last 30 days" hoặc "All time"

3. **Refresh Data View fields**:
   - Vào Stack Management → Data Views
   - Chọn Data View → Click refresh icon

4. **Đảm bảo batch job đã chạy**:
   ```bash
   docker compose up spark-batch
   ```

### Kibana không load được

```bash
# Kiểm tra Elasticsearch health
curl.exe "http://localhost:9200/_cluster/health?pretty"

# Restart Elasticsearch
docker compose restart elasticsearch

# Chờ 60 giây cho ES ready
```

### Spark job failed với OutOfMemoryError

**Tăng memory trong docker-compose.yml**:
```yaml
environment:
  HOME: /tmp
  SPARK_DRIVER_MEMORY: 4g
  SPARK_EXECUTOR_MEMORY: 4g
command:
  - --conf
  - spark.driver.memory=4g
  - --conf
  - spark.executor.memory=4g
```

### Kafka connection refused

**Kiểm tra listeners config**:
```bash
docker compose logs kafka | grep -i listener
```

Đảm bảo `KAFKA_ADVERTISED_LISTENERS` có cả `EXTERNAL://localhost:9092`

### curl là PowerShell alias

**Trên Windows PowerShell**, dùng `curl.exe` thay vì `curl`:
```powershell
# Sai (curl là alias của Invoke-WebRequest)
curl "http://localhost:9200/_cat/indices?v"

# Đúng
curl.exe "http://localhost:9200/_cat/indices?v"
```

---

## Scale với dữ liệu lớn

### Cấu hình cho 100 GB data

**1. Tăng memory cho Docker Desktop**:
- Settings → Resources → Memory: **16-24 GB**
- CPU: **6-8 cores**
- Disk: **150-200 GB free**

**2. Sửa docker-compose.yml**:

```yaml
# Spark services - tăng memory
spark-batch:
  environment:
    SPARK_DRIVER_MEMORY: 4g
    SPARK_EXECUTOR_MEMORY: 4g
  command:
    - --conf
    - spark.driver.memory=4g
    - --conf
    - spark.executor.memory=4g

# Elasticsearch - tăng heap
elasticsearch:
  environment:
    - ES_JAVA_OPTS=-Xms2g -Xmx2g  # Từ 512MB → 2GB
  deploy:
    resources:
      limits:
        memory: 4G

# Kafka - tăng partitions
kafka:
  environment:
    KAFKA_NUM_PARTITIONS: 6
    KAFKA_LOG_RETENTION_HOURS: 168
```

**3. Tuning Spark Streaming**:
```yaml
spark-streaming:
  environment:
    SPARK_BACKPRESSURE_ENABLED: "true"
    SPARK_MAX_RATE_PER_PARTITION: 1000
```

### So sánh scale

| Scale | RAM | Thời gian batch | ES indices | Khả thi |
|-------|-----|-----------------|------------|---------|
| 15 GB | 8 GB | 5-10 phút | ~2 GB | ✅ |
| 100 GB | 16-24 GB | 30-60 phút | ~10 GB | ✅ (với tuning) |
| 1 TB+ | Cluster | Giờ | 100+ GB | ⚠️ Cần multi-node |

---

## Dừng và dọn dẹp

```bash
# Dừng tất cả services
docker compose down --remove-orphans

# Xóa volumes (mất toàn bộ data đã xử lý)
docker volume rm project_bigdata_minio-data project_bigdata_airflow-data

# Xóa Elasticsearch indices (giữ data trong MinIO)
curl.exe -X DELETE "http://localhost:9200/*"
```

---

## Tài liệu tham khảo

- [CLAUDE.md](CLAUDE.md) - Chi tiết kiến trúc và cấu hình
- [Kafka docs](https://kafka.apache.org/documentation/)
- [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Elasticsearch Guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Kibana User Guide](https://www.elastic.co/guide/en/kibana/current/index.html)
