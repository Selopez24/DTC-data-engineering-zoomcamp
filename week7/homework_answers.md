# Module 7 Homework Answers

## Setup

- **Location**: `week7/`
- **Infrastructure**: Docker with Redpanda, Flink, PostgreSQL
- **Data**: Green taxi trip data for October 2025

## Architecture

```
week7/
├── producer.py                    # Send green taxi data to Kafka
├── consumer.py                    # Read from Kafka and count trips
├── requirements.txt              # Python dependencies
├── green_tripdata_2025-10.parquet
├── workshop/
│   ├── docker-compose.yml        # Docker services
│   ├── Dockerfile                # Custom Flink image with Python
│   ├── init.sql                 # PostgreSQL schema
│   ├── connectors/              # Flink connectors (Kafka, JDBC)
│   └── src/
│       ├── q4_tumbling_pickup.py    # Tumbling window by pickup location
│       ├── q5_session_window.py      # Session window (longest streak)
│       └── q6_hourly_tips.py        # Hourly tips analysis
└── homework_answers.md            # This file
```

## Running the Setup

```bash
cd week7/workshop/
docker compose build
docker compose up -d

# Create Kafka topic
docker exec workshop-redpanda-1 rpk topic create green-trips

# Send data to Kafka
cd week7
uv run --with pandas --with kafka-python --with pyarrow python3 producer.py
```

## Answers

### Question 1: Redpanda version

```bash
docker exec workshop-redpanda-1 rpk version
```

**Answer: v24.3.2**

---

### Question 2: Sending data to Redpanda

Time to send the entire dataset to Kafka:

**Answer: 10 seconds** (took ~8-10 seconds)

---

### Question 3: Consumer - trip distance

Count trips with `trip_distance` > 5.0 km:

```bash
uv run --with kafka-python python3 consumer.py
```

**Answer: 8506** trips

---

### Question 4: Tumbling window - pickup location

Flink job with 5-minute tumbling window counting trips per `PULocationID`:

```bash
docker exec workshop-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_pickup.py
```

Query:
```sql
SELECT PULocationID, num_trips
FROM pickup_location_counts
ORDER BY num_trips DESC
LIMIT 3;
```

**Answer: 74**

---

### Question 5: Session window - longest streak

Flink job with session window (5-minute gap) on `PULocationID`:

```bash
docker exec workshop-jobmanager-1 flink run -py /opt/src/job/q5_session_window.py
```

Query:
```sql
SELECT PULocationID, MAX(trip_count) as max_trips
FROM session_window_counts
GROUP BY PULocationID
ORDER BY max_trips DESC
LIMIT 1;
```

**Answer: 31**

---

### Question 6: Tumbling window - largest tip

Flink job with 1-hour tumbling window computing total `tip_amount` per hour:

```bash
docker exec workshop-jobmanager-1 flink run -py /opt/src/job/q6_hourly_tips.py
```

Query:
```sql
SELECT window_start, total_tip_amount
FROM hourly_tips
ORDER BY total_tip_amount DESC
LIMIT 1;
```

**Answer: 2025-10-22 08:00:00**

---

## Submission

Submit your answers at: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw7
