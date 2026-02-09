# Module 3 Homework Answers

## Setup

- GCS Bucket: `data-engineering-zoomcamp-sebastian-lopez-week3`
- BigQuery Dataset: `zoomcamp`
- Tables Created:
  - `yellow_taxi_external` (external table from GCS)
  - `yellow_taxi_materialized` (regular table)
  - `yellow_taxi_partitioned` (partitioned by date, clustered by VendorID)

## Answers

### Question 1: Counting records

What is count of records for the 2024 Yellow Taxi Data?

Options:

- 65,623
- 840,402
- 20,332,093
- 85,431,289

**Answer:**: 20,332,093

Query:

```sql
SELECT COUNT(*) AS total_records
FROM `zoomcamp.yellow_taxi_materialized`;
```

---

### Question 2: Data read estimation

What is the estimated amount of data that will be read when counting distinct PULocationIDs?

**Answer:** 0 MB for the External Table and 155.12 MB for the Materialized Table

Options:

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

Query:

```sql
-- External Table
SELECT COUNT(DISTINCT PULocationID) AS distinct_pickup_locations
FROM `zoomcamp.yellow_taxi_external`;

-- Materialized Table
SELECT COUNT(DISTINCT PULocationID) AS distinct_pickup_locations
FROM `zoomcamp.yellow_taxi_materialized`;
```

---

### Question 3: Understanding columnar storage

Why are the estimated number of Bytes different when querying one vs two columns?

**Answer:**
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

Query:

```sql
SELECT PULocationID
FROM `zoomcamp.yellow_taxi_materialized`;

-- Query 2: Two columns
SELECT PULocationID, DOLocationID
FROM `zoomcamp.yellow_taxi_materialized`;
```

---

### Question 4: Counting zero fare trips

How many records have a fare_amount of 0?

Options:

- 128,210
- 546,578
- 20,188,016
- 8,333

**Answer:** 8,333

Query:

```sql
SELECT COUNT(*) AS zero_fare_trips
FROM `zoomcamp.yellow_taxi_materialized`
WHERE fare_amount = 0;
```

---

### Question 5: Partitioning and clustering

What is the best strategy to make an optimized table?

**Answer:**
Partition by tpep_dropoff_datetime and Cluster on VendorID

Explanation: Partition on the column used for filtering (tpep_dropoff_datetime), cluster on the column used for ordering (VendorID).

Query:

```sql
CREATE OR REPLACE TABLE `zoomcamp.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `zoomcamp.yellow_taxi_materialized`;
```

---

### Question 6: Partition benefits

What are the estimated bytes for querying March 2024 data?

**Answer:** 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

Options:

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

Query:

```sql
SELECT DISTINCT VendorID
FROM `zoomcamp.yellow_taxi_materialized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `zoomcamp.yellow_taxi_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

---

### Question 7: External table storage

Where is the data stored in the External Table?

**Answer:** GCP Bucket

Explanation: External tables reference data stored in Google Cloud Storage buckets, not in BigQuery storage.

---

### Question 8: Clustering best practices

Is it best practice to always cluster your data?

**Answer:** False

Explanation: Clustering is beneficial for large tables with specific query patterns, but adds overhead. Small tables or tables without consistent filtering/clustering patterns don't benefit from clustering.

---

### Question 9: Understanding table scans

How many bytes does `SELECT count(*)` estimate will be read?

**Answer:**
BigQuery will likely estimate **0 bytes** because it uses table metadata/statistics rather than scanning the actual data rows to get the count.

---

## Submission

Submit your answers at: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw3
