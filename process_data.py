import pandas as pd
import json
import time
import os
from kafka import KafkaProducer

bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
topic_name = os.environ.get('TOPIC_NAME', 'ecommerce-events')
file_path = os.environ.get('DATA_FILE', 'data/2019-Oct.csv')
send_delay = float(os.environ.get('SEND_DELAY_SEC', '0.5'))

print(" Connecting to Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda x: json.dumps(x, allow_nan=False).encode('utf-8')
    )
    
    print(" Connected to Kafka successfully.")
except Exception as e:
    print(f" Failed to connect to Kafka: {e}")
    exit()
    
print(f" Reading data from {file_path}...")

try:
    for chunk in pd.read_csv(file_path, chunksize=1000):
        for index, row in chunk.iterrows():
            # Convert pandas/numpy null-like values to JSON null for strict parsing in Spark.
            data_dict = {
                key: (None if pd.isna(value) else value.item() if hasattr(value, 'item') else value)
                for key, value in row.to_dict().items()
            }
            producer.send(topic_name, value=data_dict)
            
            event_time = data_dict.get('event_time', 'N/A')
            event_type = data_dict.get('event_type', 'N/A')
            product_id = data_dict.get('product_id', 'N/A')    
            print(f" Sent event: {event_time} | {event_type} | Product ID: {product_id}")
            time.sleep(send_delay)
except FileNotFoundError:
    print(f" File not found: {file_path}")
except KeyboardInterrupt:
    print(" Process interrupted by user.")
except Exception as e:
    print(f" An error occurred: {e}")
finally:
    if 'producer' in locals():
        producer.flush()
        producer.close()
        print(" Kafka producer closed.")