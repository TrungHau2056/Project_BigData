# E-Commerce Big Data Platform - Lambda Architecture

Nền tảng phân tích dữ liệu thương mại điện tử thời gian thực sử dụng **Lambda Architecture** với Kafka, Spark, Elasticsearch và Airflow. Bao gồm ML Serving layer (XGBoost + MLflow + FastAPI + Streamlit).

## Mục lục

- [Tổng quan](#tổng-quan)
- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Cấu trúc project](#cấu-trúc-project)
- [Các thành phần](#các-thành-phần)
- [Luồng dữ liệu](#luồng-dữ-liệu)
- [Cài đặt và chạy](#cài-đặt-và-chạy)
- [ML Model Training & Serving](#ml-model-training--serving)
- [Xem và phân tích dữ liệu](#xem-và-phân-tích-dữ-liệu)
- [Cấu hình runtime](#cấu-hình-runtime)
- [Troubleshooting](#troubleshooting)
- [Scale với dữ liệu lớn](#scale-với-dữ-lượng-lớn)

---

## Tổng quan

Platform xử lý dữ liệu e-commerce từ dataset Kaggle với 4 lớp xử lý:

| Lớp | Chức năng | Công nghệ |
|-----|-----------|-----------|
| **Batch Layer** | Xử lý dữ liệu lịch sử toàn bộ | Spark + MinIO |
| **Speed Layer** | Xử lý streaming thời gian thực | Kafka + Spark Streaming |
| **Serving Layer** | Hiển thị & truy vấn | Elasticsearch + Kibana |
| **ML Serving Layer** | Train + serve mô hình dự đoán | XGBoost + MLflow + FastAPI + Streamlit |

**Nguồn dữ liệu**: File CSV e-commerce events (view, cart, purchase) với các trường:
- `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`

---

## Kiến trúc hệ thống

```
                            ┌─ Airflow (orchestration) ──────────────────────┐
                            │                                                │
CSV → Spark (bulk) → MinIO (Parquet) ──┬── Spark Batch (analytics + price baseline) → ES → Kibana
                                        ├── Feature Engineering → MinIO + ES (Recommendation + XGBoost)
                                        ├── Customer Segmentation → ES (KMeans)
                                        └── Stream Replay → Kafka (+ fake anomalies)
                                                            ├── Spark Streaming (aggregations) → ES + MinIO
                                                            └── Anomaly Detection ← price baseline from ES → ES + MinIO

MinIO (features/session_features) → MLflow (train XGBoost) → MinIO (models/)
                                                            → FastAPI (serve) → Streamlit (UI)
```

---

## Cấu trúc project

```
Project_BigData/
├── ingest_to_lake.py           # Bulk ingest CSV → MinIO Parquet
├── feature_engineering.py      # Feature tables cho Recommendation + XGBoost → MinIO + ES
├── spark_batch.py              # 8 báo cáo batch analytics + price baseline → ES
├── customer_segmentation.py    # KMeans segmentation → ES
├── train_mlflow.py             # Train XGBoost với MLflow tracking → MinIO models/
├── app.py                      # FastAPI serving XGBoost model (load từ MinIO)
├── streamlit_app.py            # Streamlit UI gọi FastAPI predict endpoint
├── schemas.py                  # Shared ECOMMERCE_SCHEMA + EVENT_SCHEMA
├── compare_performance.py      # So sánh Pandas vs Spark (utility)
├── view_data.py                # Xem sample data từ CSV (utility)
├── requirements.txt            # Python dependencies
│
├── streaming/
│   ├── stream_replay.py        # Replay MinIO → Kafka (PyArrow + kafka-python) + fake anomalies
│   ├── spark_streaming.py      # Streaming aggregations → ES + MinIO
│   └── streaming_anomaly.py    # Anomaly detection (price/volume/session)
│
├── dags/
│   ├── ecommerce_data_pipeline.py      # ingest → feature_eng → quality_check
│   ├── ecommerce_daily_analytics.py    # batch → segmentation
│   └── ecommerce_streaming_monitor.py  # Kafka + ES health check
│
├── data/                       # Source CSV files (2019-Oct.csv, etc.)
├── Dockerfile                  # Base Spark image
├── Dockerfile.airflow          # Custom Airflow image with Docker provider
├── docker-compose.yml          # Toàn bộ services
├── CLAUDE.md                   # Chi tiết kiến trúc cho Claude Code
└── README.md                   # File này
```

---

## Các thành phần

### 1. Hạ tầng (Infrastructure)

| Service | Port | Mô tả |
|---------|------|-------|
| `kafka` | 9092 (external), 29092 (internal) | Message broker — KRaft mode, không cần Zookeeper |
| `kafka-init` | — | Tạo topic `ecommerce-events` (chạy 1 lần) |
| `minio` | 9000 (API), 9001 (console) | S3-compatible object storage |
| `minio-init` | — | Tạo bucket `ecommerce-datalake` (chạy 1 lần) |
| `elasticsearch` | 9200 | Search & analytics engine |
| `kibana` | 5601 | Visualization UI |

### 2. Batch Layer

| Service | File | Chức năng |
|---------|------|-----------|
| `ingest-to-lake` | `ingest_to_lake.py` | Đọc CSV, validate, tách category levels, ghi Parquet vào MinIO |
| `feature-engineering` | `feature_engineering.py` | Feature tables cho Recommendation + XGBoost → MinIO + ES |
| `spark-batch` | `spark_batch.py` | 8 báo cáo analytics + price baseline → ES |
| `customer-segmentation` | `customer_segmentation.py` | KMeans segmentation → ES |

### 3. Speed Layer (Streaming)

| Service | File | Chức năng |
|---------|------|-----------|
| `stream-replay` | `streaming/stream_replay.py` | Replay Parquet → Kafka + inject fake anomalies |
| `spark-streaming` | `streaming/spark_streaming.py` | Streaming aggregations → ES + MinIO |
| `spark-anomaly` | `streaming/streaming_anomaly.py` | Phát hiện bất thường (price ← ES baseline, hierarchical volume, session) |

> **Lưu ý quan trọng**: Không thể chạy `spark-streaming` và `spark-anomaly` cùng lúc do giới hạn memory (OOM). Phải stop cái này trước khi start cái kia.

### 4. ML Serving Layer

| File | Chức năng |
|------|-----------|
| `train_mlflow.py` | Đọc session_features từ MinIO, train XGBoost, log metrics/artifacts lên MLflow, upload model về MinIO |
| `app.py` | FastAPI load XGBoost model từ MinIO, expose `/predict` endpoint cho real-time inference |
| `streamlit_app.py` | Streamlit UI nhập session features, gọi FastAPI `/predict`, hiển thị xác suất mua hàng |

### 5. Orchestration

| Service | DAG | Lịch trình |
|---------|-----|------------|
| `airflow-webserver` | `ecommerce_data_pipeline` | Manual trigger |
| `airflow-scheduler` | `ecommerce_daily_analytics` | @daily |
| | `ecommerce_streaming_monitor` | Every 5 min |

### 6. Utilities

| File | Chức năng |
|------|-----------|
| `view_data.py` | Xem 5 dòng đầu + schema của file CSV nguồn |
| `compare_performance.py` | So sánh tốc độ xử lý Pandas vs Spark |
| `schemas.py` | Shared `ECOMMERCE_SCHEMA` + `EVENT_SCHEMA` dùng bởi tất cả Spark scripts |

---

## Luồng dữ liệu

### Batch Pipeline
```
CSV → Spark Ingest → MinIO (Parquet partitioned by event_date, + category_level_1/2/3)
                    ↓
        ┌───────────┼───────────┐
        ↓           ↓           ↓
   Spark Batch   Feature Eng   ML Segmentation
        ↓           ↓           ↓
   ES Indices   MinIO Features ES Indices
                (user/product/
                 interaction/
                 session)
```

### Streaming Pipeline
```
MinIO → Stream Replay (PyArrow + fake anomalies) → Kafka (ecommerce-events)
                                                    ↓
                                          ┌─────────┴─────────┐
                                          ↓                   ↓ (chỉ chọn 1, không chạy cùng lúc)
                                 Spark Streaming       Anomaly Detection ← batch-price-baseline (ES)
                                          ↓                   ↓
                                 ES: ecommerce-events   ES: streaming-anomalies
                                 MinIO: streaming/      MinIO: anomalies/
                                                        (price anomaly + hierarchical volume spike + suspicious session)
```

### ML Pipeline
```
MinIO (session_features/) → train_mlflow.py → MLflow (metrics + registry)
                                         ↓
                                    MinIO (models/xgboost_session_model.json)
                                         ↓
                                    app.py (FastAPI /predict)
                                         ↓
                                    streamlit_app.py (UI)
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

- Spark đọc file `data/2019-Oct.csv`
- Validate: null checks, giá âm, phân bố event type
- Tách `category_code` thành 3 cấp: `category_level_1`, `category_level_2`, `category_level_3`
- Ghi Parquet vào MinIO: `s3a://ecommerce-datalake/raw/` partition theo `event_date`

**Kiểm tra logs**:
```bash
docker compose logs ingest-to-lake
```

### Bước 3: Feature Engineering (cho Recommendation + XGBoost)

```bash
docker compose up feature-engineering
```

- Đọc Parquet từ MinIO
- Tính toán 4 bảng features:
  - **User features**: RFM (Recency, Frequency, Monetary) + engagement (view/cart/purchase count, active_days) + favorite category & brand → MinIO
  - **Product features**: popularity (view/cart/purchase count) + revenue + conversion_rate + unique_buyers → MinIO
  - **Interactions**: user-product implicit feedback matrix (view=1, cart=2, purchase=3) cho ALS → MinIO
  - **Session features**: session_duration, total_views, total_carts, label (purchase=1), unique_categories, unique_brands, cart_to_view_ratio, hour_of_day — cho XGBoost Purchase Prediction → MinIO
- Ghi báo cáo chất lượng data vào ES index `data-quality-report`

### Bước 4: Khởi động Streaming

```bash
# Start Elasticsearch + Kibana
docker compose up -d elasticsearch kibana

# QUAN TRỌNG: Chỉ chạy MỘT Spark streaming container tại một thời điểm (giới hạn memory)

# Option A: Spark Streaming (raw events + windowed aggregations)
docker compose up -d spark-streaming
docker compose up stream-replay   # gửi events, streaming sẽ consume

# Option B: Anomaly Detection (price/volume/session anomalies)
# QUAN TRỌNG: Phải chạy spark-batch TRƯỚC để tạo batch-price-baseline
docker compose stop spark-streaming
docker compose up -d spark-anomaly
docker compose up stream-replay   # gửi events + fake anomalies, anomaly sẽ process
```

**Lưu ý quan trọng**:
- `spark-streaming` dùng `startingOffsets: latest` — chỉ consume data **sau khi** Spark start
- Phải start `spark-streaming`/`spark-anomaly` **TRƯỚC** `stream-replay` để không mất data
- **Không chạy 2 Spark streaming container cùng lúc** — sẽ gây OOM (exit code 137)

**Kiểm tra streaming**:
```bash
docker compose logs -f stream-replay
docker compose logs -f spark-streaming
curl.exe "http://localhost:9200/ecommerce-events/_count"
```

### Bước 5: Chạy Batch Analytics

```bash
docker compose up spark-batch
```

**8 báo cáo được tạo**:

| ES Index | Nội dung |
|----------|----------|
| `batch-revenue-category` | Doanh thu theo category (3 cấp: level_1, level_2, level_3) |
| `batch-revenue-brand` | Doanh thu theo brand |
| `batch-daily-revenue` | Xu hướng doanh thu theo ngày |
| `batch-weekly-revenue` | Xu hướng doanh thu theo tuần |
| `batch-conversion-funnel` | Phân bố view/cart/purchase + conversion rates |
| `batch-price-baseline` | Giá trung bình + độ lệch chuẩn theo category (dùng cho anomaly detection) |
| `batch-top-products` | Top 20 sản phẩm theo revenue |
| `batch-hourly-activity` | Hoạt động theo giờ trong ngày |

### Bước 6: Chạy Customer Segmentation (ML)

```bash
docker compose up customer-segmentation
```

- KMeans clustering với `k=4` (config qua `NUM_CLUSTERS`, `0` = auto-detect bằng Silhouette)
- Business labels: **Champions, Loyal, Potential, At-Risk, Lost, Cold**
- ES indices: `ml-customer-segments`, `ml-segment-summary`

### Bước 7: Airflow Orchestration (tùy chọn)

```bash
# Build custom Airflow image (chỉ lần đầu)
docker compose build airflow-init

# Initialize Airflow database + user
docker compose up -d airflow-init

# Chờ init hoàn tất

# Start webserver + scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

**Truy cập Airflow UI**: http://localhost:8080 (airflow/airflow)

---

## ML Model Training & Serving

### 1. Train XGBoost Model (với MLflow)

**Yêu cầu**: Đã chạy Bước 2 + Bước 3 (data trong MinIO), MinIO chạy ở `localhost:9000`, MLflow server chạy ở `localhost:5000`.

```bash
# Start MLflow tracking server (nếu chưa có)
mlflow server --host 0.0.0.0 --port 5000 --backend-store-uri sqlite:///mlflow.db --default-artifact-root s3://ecommerce-datalake/mlflow/

# Train model
python train_mlflow.py
```

**Quá trình train**:
1. Đọc `session_features` từ MinIO (`s3://ecommerce-datalake/features/session_features/`)
2. Lấy 1000 rows cho demo, split 80/20
3. Train XGBoost (binary:logistic, scale_pos_weight=45, early stopping)
4. Log params + metrics (ROC-AUC, PR-AUC) lên MLflow
5. Log model vào MLflow registry
6. Export model → MinIO `models/xgboost_session_model.json`

### 2. Serve Model qua FastAPI

```bash
# Yêu cầu: MinIO chạy, model đã có trong MinIO
python -m uvicorn app:app --host 0.0.0.0 --port 8000
```

**API Endpoints**:
| Method | Path | Mô tả |
|--------|------|-------|
| GET | `/health` | Kiểm tra API + model status |
| POST | `/predict` | Nhận session features → trả về buy_probability + will_buy + recommendation |

**Request body** (`/predict`):
```json
{
  "total_views": 15,
  "total_carts": 2,
  "unique_categories": 3,
  "unique_brands": 2,
  "session_duration": 450,
  "hour_of_day": 14,
  "cart_to_view_ratio": 0.133
}
```

**Response**:
```json
{
  "status": "success",
  "buy_probability": 0.7234,
  "will_buy": true,
  "recommendation": "Gửi Voucher giảm 10%"
}
```

### 3. Streamlit Dashboard

```bash
streamlit run streamlit_app.py
```

**Truy cập**: http://localhost:8501

- Nhập hành vi khách hàng (views, carts, session duration, hour...)
- Gọi FastAPI `/predict` real-time
- Hiển thị xác suất chốt đơn + hành động đề xuất

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
curl.exe "http://localhost:9200/data-quality-report/_count"

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

#### Gợi ý dashboards

**A. Revenue Dashboard**
- Bar chart: Revenue by category (`batch-revenue-category`)
- Bar chart: Revenue by brand (`batch-revenue-brand`)
- Line chart: Daily revenue trend (`batch-daily-revenue`)
- Line chart: Weekly revenue trend (`batch-weekly-revenue`)

**B. Conversion Funnel Dashboard**
- Pie chart: Event type distribution (`batch-conversion-funnel`)
- Metric: Total views, carts, purchases + conversion rates

**C. Real-time Monitoring Dashboard**
- Line chart: Events per minute (`ecommerce-events`)
- Table: Recent anomalies (`streaming-anomalies`)
- Metric: Total events today

**D. Customer Segmentation Dashboard**
- Pie chart: Segment distribution (`ml-customer-segments`)
- Table: Segment summary stats (`ml-segment-summary`)

### 3. MinIO Console - Data Lake Browser

**Truy cập**: http://localhost:9001 (minioadmin/minioadmin)

**Cấu trúc bucket**:
```
ecommerce-datalake/
├── raw/
│   └── event_date=2019-10-01/
│   └── event_date=2019-10-02/
│   └── ...
├── streaming/                   # Streaming data ghi lại MinIO
│   └── event_date=2019-10-25/
│   └── ...
├── anomalies/                   # Anomaly detection results (Parquet)
│   └── event_date=2019-10-25/
│   └── ...
├── features/
│   ├── user_features/           # RFM + engagement (ALS)
│   ├── product_features/        # Popularity + revenue (ALS)
│   ├── interactions/            # User-Product implicit feedback (ALS)
│   └── session_features/        # Session features (XGBoost Purchase Prediction)
├── models/
│   └── xgboost_session_model.json   # Trained XGBoost model
└── mlflow/                      # MLflow artifacts
```

### 4. Airflow UI - Pipeline Orchestration

**Truy cập**: http://localhost:8080 (airflow/airflow)

**3 DAGs**:

| DAG Name | Schedule | Mô tả |
|----------|----------|-------|
| `ecommerce_data_pipeline` | Manual | ingest → feature_engineering → quality_check |
| `ecommerce_daily_analytics` | @daily | batch_analytics → customer_segmentation |
| `ecommerce_streaming_monitor` | */5 * * * * | Check Kafka + ES health |

### 5. MLflow UI - Experiment Tracking

**Truy cập**: http://localhost:5000 (khi đã start MLflow server)

- Xem experiments, runs, metrics (ROC-AUC, PR-AUC)
- So sánh các lần train
- Load model từ registry

---

## Cấu hình runtime

| Service | Variable | Default |
|---------|----------|---------|
| Ingest to Lake | `DATA_FILE` | `data/2019-Oct.csv` |
| Ingest to Lake | `MINIO_ENDPOINT` | `http://minio:9000` |
| Ingest to Lake | `MINIO_BUCKET` | `ecommerce-datalake` |
| Stream Replay | `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| Stream Replay | `TOPIC_NAME` | `ecommerce-events` |
| Stream Replay | `MINIO_ENDPOINT` | `http://minio:9000` |
| Stream Replay | `MINIO_BUCKET` | `ecommerce-datalake` |
| Stream Replay | `REPLAY_START_DATE` | `2019-10-25` |
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
| Spark Anomaly | `MINIO_ENDPOINT` | `http://minio:9000` |
| Spark Anomaly | `MINIO_BUCKET` | `ecommerce-datalake` |
| Spark Anomaly | `PRICE_ZSCORE_THRESHOLD` | `2.0` |
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
| FastAPI (app.py) | `MINIO_ENDPOINT` | `http://localhost:9000` |
| FastAPI (app.py) | `AWS_ACCESS_KEY_ID` | `minioadmin` |
| FastAPI (app.py) | `AWS_SECRET_ACCESS_KEY` | `minioadmin` |
| Train (train_mlflow.py) | `MLFLOW_S3_ENDPOINT_URL` | `http://localhost:9000` |

---

## Troubleshooting

### FastAPI: `RuntimeError: Không thể nạp mô hình`

MinIO chưa chạy hoặc chưa có model. Cần:
1. Start MinIO: `docker compose up -d minio`
2. Train model trước: `python train_mlflow.py`
3. Kiểm tra model trong MinIO console: `ecommerce-datalake/models/xgboost_session_model.json`

### FastAPI: `ModuleNotFoundError`

Chạy bằng `python -m uvicorn` thay vì `uvicorn` trực tiếp để dùng đúng venv:
```bash
python -m uvicorn app:app --host 0.0.0.0 --port 8000
```

### Elasticsearch `index_not_found_exception`

Streaming services chưa chạy hoặc chưa có data.
```bash
docker compose up -d spark-streaming
docker compose up stream-replay
# Chờ 2-3 phút
curl.exe "http://localhost:9200/_cat/indices?v"
```

### Không có data trong Kibana Discover

1. Kiểm tra data có trong ES chưa: `curl.exe "http://localhost:9200/batch-revenue-category/_count"`
2. Mở rộng time range: chọn "Last 30 days" hoặc "All time"
3. Refresh Data View fields: Stack Management → Data Views → refresh icon

### Spark OOM (exit code 137)

Không chạy 2 Spark streaming container cùng lúc. Stop `spark-streaming` trước khi start `spark-anomaly` và ngược lại.

Nếu vẫn OOM, tăng memory trong `docker-compose.yml`:
```yaml
command:
  - --conf
  - spark.driver.memory=4g
```

### Maven JAR download failed

Lỗi mạng tạm thời. Retry — JARs được cache trong `.ivy-cache/` nên lần sau nhanh hơn.

### `DateTimeParseException: unparsed text found at index 19`

Source data có suffix "UTC" trong `event_time`. Code dùng `regexp_replace` hoặc `to_timestamp(..., "yyyy-MM-dd HH:mm:ss 'UTC'")` để xử lý.

### `countDistinct not supported on streaming DataFrames`

Dùng `approx_count_distinct()` thay thế trong streaming queries.

### PowerShell `curl` alias

Dùng `curl.exe` thay vì `curl` (PowerShell alias `curl` → `Invoke-WebRequest`).

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
  command:
    - --conf
    - spark.driver.memory=4g

# Elasticsearch - tăng heap
elasticsearch:
  environment:
    - ES_JAVA_OPTS=-Xms2g -Xmx2g
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

### So sánh scale

| Scale | RAM | Thời gian batch | ES indices | Khả thi |
|-------|-----|-----------------|------------|---------|
| 15 GB | 8 GB | 5-10 phút | ~2 GB | OK |
| 100 GB | 16-24 GB | 30-60 phút | ~10 GB | OK (với tuning) |
| 1 TB+ | Cluster | Giờ | 100+ GB | Cần multi-node |

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

## Elasticsearch Indices Tổng hợp

| Index | Source | Nội dung |
|-------|--------|----------|
| `ecommerce-events` | Streaming | Raw events từ Kafka |
| `streaming-anomalies` | Anomaly | Price/volume/session anomalies (hierarchical volume spike) |
| `batch-revenue-category` | Batch | Doanh thu theo category (3 cấp) |
| `batch-revenue-brand` | Batch | Doanh thu theo brand |
| `batch-daily-revenue` | Batch | Xu hướng doanh thu theo ngày |
| `batch-weekly-revenue` | Batch | Xu hướng doanh thu theo tuần |
| `batch-conversion-funnel` | Batch | View/cart/purchase counts + conversion rates |
| `batch-price-baseline` | Batch | Giá trung bình + stddev theo category (cho anomaly) |
| `batch-top-products` | Batch | Top 20 sản phẩm theo revenue |
| `batch-hourly-activity` | Batch | Hoạt động theo giờ |
| `ml-customer-segments` | ML | Per-user segment assignment |
| `ml-segment-summary` | ML | Segment aggregate stats |
| `data-quality-report` | Feature Eng | Data quality stats + validation |

---

## Tài liệu tham khảo

- [CLAUDE.md](CLAUDE.md) - Chi tiết kiến trúc và cấu hình cho Claude Code
- [Kafka docs](https://kafka.apache.org/documentation/)
- [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Elasticsearch Guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Kibana User Guide](https://www.elastic.co/guide/en/kibana/current/index.html)
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)
