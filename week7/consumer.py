import json
from kafka import KafkaConsumer
import signal
import sys

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'green-trips'

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=60000
    )
    
    count = 0
    total = 0
    try:
        for message in consumer:
            total += 1
            record = message.value
            if record['trip_distance'] > 5.0:
                count += 1
    except StopIteration:
        pass
    
    print(f"Total messages: {total}")
    print(f"Trips with trip_distance > 5.0: {count}")

if __name__ == '__main__':
    main()
