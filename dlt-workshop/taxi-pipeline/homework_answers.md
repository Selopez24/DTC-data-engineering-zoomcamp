# dlt Workshop Homework Answers

## Setup

- **Location**: `dlt-workshop/taxi-pipeline/`
- **Pipeline**: `taxi_pipeline_pipeline`
- **Database**: DuckDB (`taxi_pipeline_pipeline.duckdb`)
- **Dataset**: `taxi_pipeline_pipeline_dataset`
- **Source**: NYC Yellow Taxi trip data from custom API
- **API Endpoint**: `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api`
- **Records Loaded**: 10,000
- **Tables Created**:
  - `taxi_trips` (main table with all trip data)

---

## Pipeline Configuration

The dlt pipeline was built using the REST API source with the following configuration:

```python
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
    },
    "resources": [
        {
            "name": "taxi_trips",
            "endpoint": {
                "path": "/",
                "paginator": {
                    "type": "page_number",
                    "page": 1,
                    "base_page": 1,
                    "page_param": "page",
                    "total_path": None,
                    "stop_after_empty_page": True,
                },
            },
        }
    ],
}
```

**Key Features**:
- Page-based pagination (1,000 records per page)
- Automatic stopping when empty page is returned
- No authentication required
- Data automatically normalized into relational schema

---

## Answers

### Question 1: What is the start date and end date of the dataset?

Options:

- 2009-01-01 to 2009-01-31
- 2009-06-01 to 2009-07-01
- 2024-01-01 to 2024-02-01
- 2024-06-01 to 2024-07-01

**Answer:** 2009-06-01 to 2009-07-01

Query:

```sql
SELECT 
    MIN(trip_pickup_date_time) as start_date,
    MAX(trip_pickup_date_time) as end_date
FROM taxi_trips;
```

Result:
- Start Date: 2009-06-01 11:33:00
- End Date: 2009-06-30 23:58:00

The dataset spans the entire month of June 2009, which corresponds to option "2009-06-01 to 2009-07-01" (with the end date being exclusive).

---

### Question 2: What proportion of trips are paid with credit card?

Options:

- 16.66%
- 26.66%
- 36.66%
- 46.66%

**Answer:** 26.66%

Query:

```sql
SELECT 
    payment_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM taxi_trips), 2) as percentage
FROM taxi_trips
GROUP BY payment_type
ORDER BY count DESC;
```

Result:
- CASH: 7,235 trips (72.35%)
- Credit: 2,666 trips (26.66%)
- Cash: 97 trips (0.97%)
- Dispute: 1 trip (0.01%)
- No Charge: 1 trip (0.01%)

**26.66%** of trips are paid with credit card.

---

### Question 3: What is the total amount of money generated in tips?

Options:

- $4,063.41
- $6,063.41
- $8,063.41
- $10,063.41

**Answer:** $6,063.41

Query:

```sql
SELECT 
    ROUND(SUM(tip_amt), 2) as total_tips
FROM taxi_trips;
```

Result: **$6,063.41**

The total amount of tips across all 10,000 taxi trips is $6,063.41.

---

## Data Schema

The `taxi_trips` table contains the following columns:

| Column | Data Type | Description |
|--------|-----------|-------------|
| end_lat | double | Dropoff latitude |
| end_lon | double | Dropoff longitude |
| fare_amt | double | Fare amount |
| passenger_count | bigint | Number of passengers |
| payment_type | text | Payment method (Credit, CASH, Cash, etc.) |
| rate_code | text | Rate code (null for all records) |
| start_lat | double | Pickup latitude |
| start_lon | double | Pickup longitude |
| tip_amt | double | Tip amount |
| tolls_amt | double | Tolls amount |
| total_amt | double | Total amount |
| trip_distance | double | Trip distance in miles |
| trip_dropoff_date_time | timestamp | Dropoff datetime |
| trip_pickup_date_time | timestamp | Pickup datetime |
| mta_tax | text | MTA tax (null for all records) |
| surcharge | double | Surcharge amount |
| vendor_name | text | Vendor name (VTS, etc.) |
| store_and_forward | double | Store and forward flag |

---

## Submission

Submit your answers at: https://courses.datatalks.club/de-zoomcamp-2026/homework/dlt
