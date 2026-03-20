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
            'properties.group.id' = 'flink-consumer-q6',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)
    
    t_env.execute_sql(f"""
        CREATE TABLE hourly_tips (
            window_start TIMESTAMP,
            total_tip_amount DECIMAL(10, 2),
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}',
            'table-name' = 'hourly_tips',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql("""
        INSERT INTO hourly_tips
        SELECT
            TUMBLE_START(event_timestamp, INTERVAL '1' HOUR) AS window_start,
            SUM(tip_amount) AS total_tip_amount
        FROM green_trips
        GROUP BY
            TUMBLE(event_timestamp, INTERVAL '1' HOUR)
    """).wait()

if __name__ == '__main__':
    main()
