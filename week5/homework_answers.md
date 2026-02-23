# Module 5 Homework Answers

## Setup

- Bruin CLI: v0.11.466 installed
- Project: Initialized with `bruin init zoomcamp my-pipeline`
- Location: `week5/my-pipeline/`

---

**Note:** Answers have been verified against official Bruin documentation (https://getbruin.com/docs/bruin/).

## Answers

### Question 1: Bruin Pipeline Structure

In a Bruin project, what are the required files/directories?

Options:

- `bruin.yml` and `assets/`
- `.bruin.yml` and `pipeline.yml` (assets can be anywhere)
- `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`
- `pipeline.yml` and `assets/` only

**Answer:** `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`

Explanation: According to the official Bruin documentation:
- A project is defined by the `.bruin.yml` configuration file (Project page)
- A pipeline is defined with a `pipeline.yml` file, and all assets need to be under a folder called `assets` next to this file (Pipeline Definition page)
The README confirms this structure: `.bruin.yml` in the root directory, `pipeline.yml` in the `pipeline/` directory (or root), and `assets/` folder next to `pipeline.yml`.

---

### Question 2: Materialization Strategies

You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

Options:

- `append` - always add new rows
- `replace` - truncate and rebuild entirely
- `time_interval` - incremental based on a time column
- `view` - create a virtual table only

**Answer:** `time_interval` - incremental based on a time column

Explanation: The official Bruin documentation states: "The `time_interval` strategy is designed for incrementally loading time-based data. It deletes existing records within the specified time interval and inserts new records from the query given in the asset" (Materialization page). This matches the requirement of processing a specific interval period by deleting and inserting data for that time period.

---

### Question 3: Pipeline Variables

You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

Options:

- `bruin run --taxi-types yellow`
- `bruin run --var taxi_types=yellow`
- `bruin run --var 'taxi_types=["yellow"]'`
- `bruin run --set taxi_types=["yellow"]`

**Answer:** `bruin run --var 'taxi_types=["yellow"]'`

Explanation: The official Bruin documentation states: "Override variable values using the `--var` flag during `bruin run`" and shows examples like `--var env='"prod"'` and `--var '{"users": ["alice", "charlie"]}'` (Variables page). For an array variable, the value must be a valid JSON array, making `--var 'taxi_types=["yellow"]'` the correct syntax.

---

### Question 4: Running with Dependencies

You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

Options:

- `bruin run ingestion.trips --all`
- `bruin run ingestion/trips.py --downstream`
- `bruin run pipeline/trips.py --recursive`
- `bruin run --select ingestion.trips+`

**Answer:** `bruin run ingestion/trips.py --downstream`

Explanation: The official Bruin documentation states: "The `--downstream` flag runs all downstream assets as well" (Run Command page). The example shows running an asset with downstream dependencies: `bruin run ./pipelines/project1/assets/my_asset.sql`. The `--downstream` flag runs the specified asset and all assets that depend on it.

---

### Question 5: Quality Checks

You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

Options:

- `name: unique`
- `name: not_null`
- `name: positive`
- `name: accepted_values, value: [not_null]`

**Answer:** `name: not_null`

Explanation: The official Bruin documentation lists `not_null` as a quality check that "will verify that none of the values of the checked column are null" (Available Checks page). The example shows exactly this usage:

```yaml
columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
    primary_key: true
    checks:
      - name: not_null
```

The `not_null` check ensures the column never contains NULL values.

---

### Question 6: Lineage and Dependencies

After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

Options:

- `bruin graph`
- `bruin dependencies`
- `bruin lineage`
- `bruin show`

**Answer:** `bruin lineage`

Explanation: The official Bruin documentation states: "The `lineage` command helps you understand how a specific asset fits into your pipeline by showing its dependencies" (Lineage Command page). This command shows the dependency graph between assets.

---

### Question 7: First-Time Run

You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

Options:

- `--create`
- `--init`
- `--full-refresh`
- `--truncate`

**Answer:** `--full-refresh`

Explanation: The official Bruin documentation states: "The `--full-refresh` flag truncates the table before running. Also sets the `full_refresh` jinja variable to `True` and `BRUIN_FULL_REFRESH` environment variable to `1`" (Run Command page). This ensures tables are created from scratch on first run.

---

## Summary

| Question | Answer |
|----------|--------|
| **Q1** | `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/` |
| **Q2** | `time_interval` - incremental based on a time column |
| **Q3** | `bruin run --var 'taxi_types=["yellow"]'` |
| **Q4** | `bruin run ingestion/trips.py --downstream` |
| **Q5** | `name: not_null` |
| **Q6** | `bruin lineage` |
| **Q7** | `--full-refresh` |

---

## Submission

Submit your answers at: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw5
