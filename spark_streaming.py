import os
import sys
from pathlib import Path

# --- 1. BẢN VÁ LỖI CHO WINDOWS (BẮT BUỘC PHẢI NẰM TRÊN CÙNG) ---
def resolve_hadoop_home():
    env_home = os.environ.get('HADOOP_HOME')
    candidates = []

    if env_home:
        candidates.append(Path(env_home))

    project_dir = Path(__file__).resolve().parent
    candidates.extend([
        project_dir / 'hadoop',
        Path('C:/hadoop'),
    ])

    for candidate in candidates:
        if (candidate / 'bin' / 'winutils.exe').exists():
            return candidate

    return None

def write_to_es(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id} is empty. Skipping write to Elasticsearch.")
        return

    (
        batch_df.write
        .format("org.elasticsearch.spark.sql")
        .mode("append")
        .option("es.nodes", es_nodes)
        .option("es.port", es_port)
        .option("es.resource", es_index)
        .option("es.nodes.wan.only", "true")
        .option("es.index.auto.create", "true")
        .save()
    )

if os.name == 'nt':
    hadoop_home = resolve_hadoop_home()
    if hadoop_home is not None:
        os.environ['HADOOP_HOME'] = str(hadoop_home)
        hadoop_bin = str(hadoop_home / 'bin')

        # Ép Windows phải nhìn thấy file hadoop.dll/winutils.exe
        current_path = os.environ.get('PATH', '')
        if hadoop_bin.lower() not in current_path.lower():
            os.environ['PATH'] = current_path + (';' if current_path else '') + hadoop_bin

        if hasattr(os, 'add_dll_directory'):
            os.add_dll_directory(hadoop_bin)
    else:
        print(
            "[WARN] Khong tim thay winutils.exe. "
            "Hay set bien moi truong HADOOP_HOME hoac dat vao ./hadoop/bin/winutils.exe"
        )

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col


# Create a SparkSession
spark_kafka_package = os.environ.get(
    'SPARK_KAFKA_PACKAGE',
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3'
)

spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .config("spark.jars.packages", spark_kafka_package) \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")

print("Khời tạo Spark xong")

# Khai báo cấu trúc dữ liệu
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_session", StringType(), True)    
])

# Kết nối vơi Kafka để hút dữ liệu liên tục (streaming)
print("Đang lắng nghe luồng dữ liệu từ kafka topic 'ecommerce-events'...")
kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
topic_name = os.environ.get('TOPIC_NAME', 'ecommerce-events')
es_nodes = os.environ.get('ES_NODES', 'elasticsearch')
es_port = os.environ.get('ES_PORT', '9200')
es_index = os.environ.get('ES_INDEX', 'ecommerce-events')

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "latest") \
    .load()
    
    
# Ép kiểu dữ liệu (chuyển mã hóa Binary của kafka -> chuỗi json và tách cột)

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
                    .select(from_json(col("value"), schema).alias("data")) \
                    .select("data.*")

# Làm sạch data (Lọc bỏ những giá trị bị lỗi)
clean_df = parsed_df.filter(col("price").isNotNull())

# In kết quả ra terminal
print("Dang cho du lieu chay vao Elasticsearch...")
query = (
    clean_df.writeStream
    .outputMode("append")
    .foreachBatch(write_to_es)
    .option("checkpointLocation", "/tmp/spark-checkpoints/ecommerce-events")
    .start()
)

# debug_query = (
#     clean_df.writeStream
#     .outputMode("append")
#     .format("console")
#     .option("truncate", "false")
#     .start()
# )

query.awaitTermination()