# Data Engineering Assessment

## Overview

This project implements an end-to-end data pipeline for ingesting, transforming, and analyzing hiring workflow data.

The solution is divided into three layers:

1. **Ingestion (Local Python + SQLite)**
2. **Transformation (PySpark + Delta Lake on Databricks)**
3. **Data Quality & Metrics Layer**

---

## Architecture
Raw Files (CSV / JSON / JSONL)
↓
SQLite (Landing Layer - raw.db)
↓
Exported Bronze CSVs
↓
Databricks (Delta Lake)
↓
Dim / Fact Tables
↓
Metrics + Data Quality Outputs


---

## Design Decisions

### 1. Layering Strategy

- **Landing Layer (SQLite - `raw.db`)**
  - Stores ingested data with minimal transformation
  - Ensures idempotent ingestion

- **Bronze Layer (Logical)**
  - Represented via cleaned tables in SQLite and Delta
  - Data normalized and standardized

- **Silver/Gold (Databricks Delta)**
  - Dimensional modeling
  - Metrics and aggregations

---

### 2. Technology Choices

| Component | Technology | Reason |
|----------|----------|--------|
| Ingestion | Python + SQLite | Lightweight, easy to run locally |
| Transformation | PySpark (Databricks) | Scalable distributed processing |
| Storage | Delta Lake | ACID, schema enforcement |
| Config | YAML | Decouples logic from environment |

---

## Project Structure
src/
ingest.py
export_bronze.py
config.py
01_load_bronze
02_dim_job
03_dim_candidate
04_fct_workflow_events
05_fct_applications
06_time_to_hire_metrics
07_data_quality_checks

config/
config_local.yaml
config_databricks.yaml

---

## Task 1: Data Ingestion

- Data is ingested from:
  - CSV (jobs, education, applications)
  - JSON (candidates)
  - JSONL (workflow events)

- Stored in SQLite (`raw.db`)

### Key Features

- Idempotent ingestion using:
  - `INSERT OR REPLACE`
  - Primary keys

- Data normalization:
  - Date standardization
  - String trimming
  - JSON serialization (skills)

---

## Task 2: Data Modeling (PySpark)

### Dimensional Model

#### Dimension Tables
- `dim_job`
- `dim_candidate`

#### Fact Tables
- `fct_workflow_events`
- `fct_applications`

---

### Key Transformations

#### 1. Current Status Derivation
- Derived using latest `event_timestamp`

#### 2. Hired Date
- First occurrence of `"Hired"` status

#### 3. Event Processing
- Ensures correct mapping of timestamp → status

---

### Time-to-Hire Metric
time_to_hire_days = hired_date - apply_date


Outputs:
- `fct_time_to_hire_detail`
- `job_time_to_hire`
- `department_time_to_hire`

---

## Task 3: Data Quality & Validation

### Implemented Checks

#### Duplicate Checks
- job_id
- candidate_id
- application_id
- event_id

#### Null Checks
- critical keys
- timestamps

#### Volume Checks
- row counts for each table

---

### Anomaly Detection

#### Case: Hired Before Applied
hired_date < apply_date


- Stored in:
  - `dq_hired_before_applied`

#### Handling Strategy

- Data is **not modified**
- Anomalies are **flagged separately**
- Excluded from metric calculations

---

## Idempotency

### Ingestion Layer
- SQLite uses:
  - `INSERT OR REPLACE`
- Safe to rerun without duplicates

### Transformation Layer
- Delta tables use:
  - `.mode("overwrite")`
- Produces consistent outputs on reruns

---

## Scaling to 10TB (Design Considerations)

To scale this pipeline:

- Replace SQLite with:
  - Object storage (For example: S3)

- Use:
  - Partitioned Delta tables
  - Incremental ingestion

- Optimizations:
  - Partition by event date
  - Use Z-ordering
  - Cache hot datasets

- Avoid full recomputation:
  - Incremental updates
  - Streaming ingestion

---

## Configuration Management

- Local config: `config.yaml`
- Databricks config: `config_databricks.yaml`

Benefits:
- No hardcoding
- Easy environment switching

---

## Assumptions

- Each `application_id` is unique
- Workflow events are append-only
- "Hired" status represents final hire
- Dates are convertible to standard formats

---

## AI Usage

AI assistance was used for:
- Structuring code
- Debugging errors in logic
- Improving design patterns

All implementation decisions and validations were reviewed and verified.

---

## How to Run

### Step 1: Ingestion

```bash
python src/ingest.py

### Step 2: Export Bronze
python src/export_bronze.py

### Step 3: Run Databricks Notebooks

Execute in order:

1. 01_load_bronze
2. 02_dim_job
3. 03_dim_candidate
4. 04_fct_workflow_events
5. 05_fct_applications
6. 06_time_to_hire_metrics
7. 07_data_quality_checks

### Final Output Tables
1. dim_job
2. dim_candidate
3. fct_workflow_events
4. fct_applications
5. fct_time_to_hire_detail
6. job_time_to_hire
7. department_time_to_hire
8. dq_results
9. dq_hired_before_applied

### Conclusion

This pipeline demonstrates:

End-to-end data engineering workflow
Strong data modeling practices
Robust data quality handling
Scalable architecture design