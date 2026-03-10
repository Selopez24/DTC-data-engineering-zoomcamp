"""
Week 6 Homework - Batch Processing with Spark (November 2025 Data)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, unix_timestamp, max as spark_max, count

# Set Java environment
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# Initialize Spark session
spark = (
    SparkSession.builder.appName("Week6-Homework-Nov2025")
    .master("local[*]")
    .getOrCreate()
)

print(f"Spark Version: {spark.version}")

# Question 2: Read Yellow November 2025 data
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
print(f"\nOriginal DataFrame count: {df.count()}")
print(f"Original partitions: {df.rdd.getNumPartitions()}")

# Repartition to 4 partitions
df_repartitioned = df.repartition(4)
print(f"After repartition: {df_repartitioned.rdd.getNumPartitions()} partitions")

# Save to parquet
output_path = "yellow_tripdata_2025-11-repartitioned"
df_repartitioned.write.mode("overwrite").parquet(output_path)
print(f"\nData saved to: {output_path}")

# Check file sizes
import subprocess

result = subprocess.run(["ls", "-lh", output_path], capture_output=True, text=True)
print("\nParquet files created:")
print(result.stdout)

# Calculate average size
parquet_files = [f for f in os.listdir(output_path) if f.endswith(".parquet")]
total_size = sum(os.path.getsize(os.path.join(output_path, f)) for f in parquet_files)
avg_size_mb = (total_size / len(parquet_files)) / (1024 * 1024)
print(f"\nNumber of parquet files: {len(parquet_files)}")
print(f"Average file size: {avg_size_mb:.2f} MB")

# Question 3: Count records on November 15th
df_with_date = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
nov_15_count = df_with_date.filter(col("pickup_date") == "2025-11-15").count()
print(f"\nTrips on November 15th: {nov_15_count}")

# Question 4: Longest trip duration
df_with_duration = df.withColumn(
    "trip_duration_hours",
    (
        unix_timestamp(col("tpep_dropoff_datetime"))
        - unix_timestamp(col("tpep_pickup_datetime"))
    )
    / 3600,
)
max_duration = df_with_duration.select(spark_max("trip_duration_hours")).collect()[0][0]
print(f"\nLongest trip duration: {max_duration:.2f} hours")

# Question 6: Least frequent pickup location zone
# Load zone lookup data
zone_df = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)
zone_df.createOrReplaceTempView("zones")

# Count pickups by zone
pickup_counts = df.groupBy("PULocationID").agg(count("*").alias("trip_count"))
pickup_counts.createOrReplaceTempView("pickup_counts")

# Find least frequent
result = spark.sql("""
    SELECT z.Zone, z.LocationID, p.trip_count
    FROM pickup_counts p
    JOIN zones z ON p.PULocationID = z.LocationID
    ORDER BY p.trip_count ASC
    LIMIT 10
""")

print("\nLeast frequent pickup zones (top 10):")
result.show(truncate=False)

# Get the least frequent
least_frequent = result.collect()[0]
print(f"\nLeast frequent pickup zone: {least_frequent['Zone']}")
print(f"Location ID: {least_frequent['LocationID']}")
print(f"Trip count: {least_frequent['trip_count']}")

# Stop Spark session
spark.stop()
print("\nSpark session stopped.")
