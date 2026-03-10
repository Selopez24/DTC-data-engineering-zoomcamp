# Module 6 Homework Answers

## Setup

- **Location**: `week6/`
- **Environment**: PySpark with OpenJDK 17
- **Data**: Yellow taxi trip data for November 2025 (4,181,444 records)
- **Files**:
  - `yellow_tripdata_2025-11.parquet` - Original data (67.8 MB)
  - `taxi_zone_lookup.csv` - Zone lookup data
  - `spark_homework_nov2025.py` - Main homework script

---

## Answers

### Question 1: Install Spark and PySpark

Install Spark, run PySpark, create a local spark session, and execute `spark.version`.

**Answer:** 4.1.1

Code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Week6-Homework") \
    .master("local[*]") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")
```

Result: **4.1.1**

---

### Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe, repartition to 4 partitions, and save to parquet. What is the average size of the Parquet files?

Options:

- 6MB
- 25MB
- 75MB
- 100MB

**Answer:** 25MB

Code:

```python
# Read data
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Repartition to 4 partitions
df_repartitioned = df.repartition(4)

# Save to parquet
df_repartitioned.write.mode("overwrite").parquet("yellow_tripdata_2025-11-repartitioned")
```

Results:
- Number of parquet files: 4
- File sizes: 24MB each
- **Average file size: 24.41 MB** (closest to **25MB**)

---

### Question 3: Count records

How many taxi trips were there on the 15th of November? Consider only trips that started on the 15th of November.

Options:

- 62,610
- 102,340
- 162,604
- 225,768

**Answer:** 162,604

Code:

```python
from pyspark.sql.functions import to_date, col

df_with_date = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
nov_15_count = df_with_date.filter(col("pickup_date") == "2025-11-15").count()
print(f"Trips on November 15th: {nov_15_count}")
```

Result: **162,604 trips**

---

### Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

Options:

- 22.7
- 58.2
- 90.6
- 134.5

**Answer:** 90.6

Code:

```python
from pyspark.sql.functions import unix_timestamp, max as spark_max

df_with_duration = df.withColumn(
    "trip_duration_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600
)
max_duration = df_with_duration.select(spark_max("trip_duration_hours")).collect()[0][0]
print(f"Longest trip duration: {max_duration:.2f} hours")
```

Result: **90.65 hours** (closest to **90.6**)

---

### Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

Options:

- 80
- 443
- 4040
- 8080

**Answer:** 4040

Explanation: By default, Spark's UI runs on **port 4040**. This is where you can monitor your Spark jobs, stages, tasks, and other metrics.

---

### Question 6: Least frequent pickup location zone

Load the zone lookup data and find the name of the LEAST frequent pickup location Zone.

Options:

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

**Answer:** Governor's Island/Ellis Island/Liberty Island

Code:

```python
from pyspark.sql.functions import count

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
result.show()
```

Result: **Governor's Island/Ellis Island/Liberty Island** with only **1 trip**

Top 5 least frequent pickup zones:
1. Governor's Island/Ellis Island/Liberty Island - 1 trip
2. Arden Heights - 1 trip
3. Eltingville/Annadale/Prince's Bay - 1 trip
4. Port Richmond - 3 trips
5. Rossville/Woodrow - 4 trips

Note: Multiple zones tied with 1 trip. Governor's Island/Ellis Island/Liberty Island is one of the correct answers.

---

## Files Created

- `spark_homework_nov2025.py` - Main script for all questions
- `yellow_tripdata_2025-11-repartitioned/` - Directory with 4 repartitioned parquet files

---

## Submission

Submit your answers at: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw6
