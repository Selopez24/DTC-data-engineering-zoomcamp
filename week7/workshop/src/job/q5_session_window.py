from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col, lit, concat_ws
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
            'properties.group.id' = 'flink-consumer-q5',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)
    
    t_env.execute_sql(f"""
        CREATE TABLE session_window_counts (
            session_id VARCHAR(100),
            PULocationID INT,
            trip_count BIGINT,
            PRIMARY KEY (session_id, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}',
            'table-name' = 'session_window_counts',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql("""
        INSERT INTO session_window_counts
        SELECT
            CONCAT(CAST(PULocationID AS VARCHAR), '_', CAST(MIN(event_timestamp) AS VARCHAR)) AS session_id,
            PULocationID,
            COUNT(*) AS trip_count
        FROM green_trips
        GROUP BY
            SESSION(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID
    """).wait()

if __name__ == '__main__':
    main()
