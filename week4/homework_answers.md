# Module 4 Homework Answers

## Setup

- Database: DuckDB (`taxi_rides_ny.duckdb`)
- dbt Project: `taxi_rides_ny`
- Target: `prod`
- Tables Created:
  - `prod.stg_green_tripdata` (staging view)
  - `prod.stg_yellow_tripdata` (staging view)
  - `prod.stg_fhv_tripdata` (staging view for FHV data)
  - `prod.int_trips_unioned` (intermediate table)
  - `prod.int_trips` (intermediate table)
  - `prod.fct_trips` (fact table - incremental)
  - `prod.fct_monthly_zone_revenue` (reporting table)
  - `prod.dim_zones` (dimension table)
  - `prod.dim_vendors` (dimension table)
  - Seed tables: `payment_type_lookup`, `taxi_zone_lookup`

---

## Answers

### Question 1: dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

Options:

- `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
- Any model with upstream and downstream dependencies to `int_trips_unioned`
- `int_trips_unioned` only
- `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)

**Answer:** `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)

Explanation: When you run `dbt run --select <model>`, dbt automatically includes all upstream dependencies (parent models) that the selected model depends on. It does NOT include downstream models (children). Since `int_trips_unioned` depends on both staging models, dbt will build all three models.

---

### Question 2: dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          values: [1, 2, 3, 4, 5]
          quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

Options:

- dbt will skip the test because the model didn't change
- dbt will fail the test, returning a non-zero exit code
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value

**Answer:** dbt will fail the test, returning a non-zero exit code

Explanation: The `accepted_values` test checks that all values in a column match the specified list. When a new value (6) appears that is not in the accepted list [1, 2, 3, 4, 5], the test will fail. dbt tests are designed to be strict - they fail when data doesn't meet the criteria, regardless of whether the model code changed. This is a data quality issue, not a model change issue.

---

### Question 3: Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

Options:

- 12,998
- 14,120
- 12,184
- 15,421

**Answer:** 12,184

Query:

```sql
SELECT COUNT(*) as record_count
FROM prod.fct_monthly_zone_revenue;
```

Result: 12,184 records

---

### Question 4: Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

Options:

- East Harlem North
- Morningside Heights
- East Harlem South
- Washington Heights South

**Answer:** East Harlem North

Query:

```sql
SELECT 
    pickup_zone,
    SUM(revenue_monthly_total_amount) as total_revenue
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green' 
    AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
```

Result: East Harlem North with $1,817,619.05 in total revenue

---

### Question 5: Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

Options:

- 500,234
- 350,891
- 384,624
- 421,509

**Answer:** 384,624

Query:

```sql
SELECT SUM(total_monthly_trips) as total_trips
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green' 
    AND EXTRACT(YEAR FROM revenue_month) = 2019
    AND EXTRACT(MONTH FROM revenue_month) = 10;
```

Result: 384,624 trips

---

### Question 6: Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

Options:

- 42,084,899
- 43,244,693
- 22,998,722
- 44,112,187

**Answer:** 43,244,693

Steps Taken:

1. **Created data ingestion script** (`ingest_fhv_data.py`) to download FHV data for 2019
2. **Loaded data into DuckDB** table `prod.fhv_tripdata`
3. **Created staging model** (`models/staging/stg_fhv_tripdata.sql`):
   - Filters out records with null `dispatching_base_num`
   - Renames columns to follow naming conventions:
     - `PUlocationID` → `pickup_location_id`
     - `DOlocationID` → `dropoff_location_id`
     - `dropOff_datetime` → `dropoff_datetime`
     - `SR_Flag` → `sr_flag`
     - `Affiliated_base_number` → `affiliated_base_number`
4. **Added source** to `sources.yml`
5. **Ran dbt** to create the view

Query:

```sql
SELECT COUNT(*) as record_count
FROM prod.stg_fhv_tripdata;
```

Result: 43,244,693 records

---

## Submission

Submit your answers at: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw4
