from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udf
import json

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'green-trips'
POSTGRES_HOST = 'workshop-postgres-1'
POSTGRES_PORT = '5432'
POSTGRES_DB = 'postgres'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'postgres'

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    t_env = StreamTableEnvironment.create(env)
    
    t_env.execute_sql(f"""
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TOPIC_NAME}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-consumer-q4',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)
    
    t_env.execute_sql(f"""
        CREATE TABLE pickup_location_counts (
            window_start TIMESTAMP,
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}',
            'table-name' = 'pickup_location_counts',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql("""
        INSERT INTO pickup_location_counts
        SELECT
            TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
            PULocationID,
            COUNT(*) AS num_trips
        FROM green_trips
        GROUP BY
            TUMBLE(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID
    """).wait()

if __name__ == '__main__':
    main()
