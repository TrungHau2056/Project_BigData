import pandas as pd
import json
import time
from kafka import KafkaProducer

print(" Connecting to Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    print(" Connected to Kafka successfully.")
except Exception as e:
    print(f" Failed to connect to Kafka: {e}")
    exit()
    
file_path = 'data/2019-Oct.csv'

topic_name = 'ecommerce-events'

print(f" Reading data from {file_path}...")

try:
    for chunk in pd.read_csv(file_path, chunksize=1000):
        for index, row in chunk.iterrows():
            
            data_dict = row.to_dict()
            producer.send(topic_name, value=data_dict)
            
            event_time = data_dict.get('event_time', 'N/A')
            event_type = data_dict.get('event_type', 'N/A')
            product_id = data_dict.get('product_id', 'N/A')    
            print(f" Sent event: {event_time} | {event_type} | Product ID: {product_id}")
            time.sleep(0.5)
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