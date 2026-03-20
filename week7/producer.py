import json
import pandas as pd
from kafka import KafkaProducer
from time import time

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'green-trips'
PARQUET_FILE = 'green_tripdata_2025-10.parquet'

COLUMNS_TO_KEEP = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]

def main():
    df = pd.read_parquet(PARQUET_FILE)
    df = df[COLUMNS_TO_KEEP]
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    t0 = time()
    
    for _, row in df.iterrows():
        record = {
            'lpep_pickup_datetime': str(row['lpep_pickup_datetime']),
            'lpep_dropoff_datetime': str(row['lpep_dropoff_datetime']),
            'PULocationID': int(row['PULocationID']),
            'DOLocationID': int(row['DOLocationID']),
            'passenger_count': float(row['passenger_count']),
            'trip_distance': float(row['trip_distance']),
            'tip_amount': float(row['tip_amount']),
            'total_amount': float(row['total_amount'])
        }
        producer.send(TOPIC_NAME, record)
    
    producer.flush()
    t1 = time()
    print(f'took {(t1 - t0):.2f} seconds')

if __name__ == '__main__':
    main()
