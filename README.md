# Wind Turbine Data Pipeline (Databricks Lakehouse)

## Overview

This project implements a **data engineering pipeline for wind turbine telemetry** using **Databricks Lakeflow Declarative Pipelines (DLT)** and the **Medallion architecture**.

The pipeline ingests turbine telemetry data stored as CSV files and performs:

- data validation and cleansing
- anomaly detection
- summary aggregation
- data quality monitoring
- missing interval detection
- gap imputation for analytics

The implementation demonstrates **production-grade data engineering practices**, including:

- modular transformation logic
- unit testing
- pipeline observability
- infrastructure-as-code using **Databricks Asset Bundles (DAB)**

---

# Architecture

## Medallion Architecture

The pipeline follows a **Bronze → Silver → Gold** design.
Raw CSV Files
│
▼
Bronze Layer
Raw ingestion with lineage metadata
│
▼
Silver Layer
Validated telemetry
Quarantine table
Missing interval detection
Imputed dataset
│
▼
Gold Layer
Daily turbine summaries
Anomaly detection
Data quality monitoring



Bronze is intentionally implemented as a **full snapshot ingestion** because the source files are **mutated (appended) daily**.

Streaming ingestion would be unsafe for mutable files.

---

## Silver Layer

The Silver layer standardizes and validates turbine telemetry.

### silver_turbine_data

Operations:

- type standardization
- deduplication
- validation rules

Validation rules include:

| Rule | Example |
|-----|-----|
timestamp required | timestamp IS NOT NULL |
wind speed range | 0–60 m/s |
wind direction range | 0–360 |
power output range | 0–10 MW |

---

### silver_quarantine_turbine_data

Invalid rows are **not dropped**.

Instead they are written to a quarantine table with a reason column.

Example reasons:

missing_timestamp
negative_wind_speed
power_output_out_of_range


This allows investigation of upstream telemetry issues.

---

### silver_missing_intervals

Turbine sensors may fail and skip readings.

This table detects **missing expected telemetry intervals**.

For each turbine and date:
expected timestamps = hourly sequence
actual timestamps = telemetry records
missing intervals = expected - actual



This enables monitoring of turbine data completeness.

---

### silver_imputed_turbine_data

Some downstream analytics require **continuous time series data**.

This table forward-fills short telemetry gaps.

Rules:

- gaps ≤ 3 hours → forward filled
- longer gaps → left missing
- imputed rows flagged with `is_imputed = true`

This preserves data lineage while enabling analytics.

---

## Gold Layer

Gold tables provide business-facing datasets.

---

### gold_turbine_summary

Daily turbine statistics:
min_power
max_power
avg_power
std_power
reading_count
anomaly_count


Aggregated by:
data_group
turbine_id
date


---

### gold_turbine_anomalies

Anomalies are detected using the **2-standard-deviation rule**:
abs(power_output - avg_power) > 2 * std_power


This identifies turbines producing abnormal output.

---

### gold_turbine_data_quality_summary

Operational monitoring table containing:
valid_record_count
quarantine_record_count
missing_interval_count


Grouped by:
data_group
date


This provides visibility into overall pipeline health.

---

# Project Structure
turbines/
│
├── databricks.yml
│
├── resources/
│ ├── pipelines/
│ │ └── turbine_pipeline.yml
│ ├── schemas/
│ │ └── schema.yml
│ ├── volumes/
│ │ └── turbine_volume.yml
│
├── src/
│ └── turbines_etl/
│ ├── dlt_pipeline.py
│ └── utils/
│ └── turbine_transformer.py
│
├── notebooks/
│ └── test_turbine_transformer
│
├── tests/
│ └── test_turbine_transformer.py
│
├── scripts/
│ └── run_tests.py
│
└── README.md



---

# Transformation Logic

Business logic is isolated in:



This module contains reusable functions for:

- data standardization
- deduplication
- anomaly detection
- missing interval detection
- forward fill imputation
- quality reporting

Separating logic from pipeline orchestration allows:

- independent unit testing
- easier reuse
- cleaner pipeline code

src/turbines_etl/utils/turbine_transformer.py
---

# Deployment with Databricks Asset Bundles

Infrastructure is deployed using **Databricks Asset Bundles (DAB)**.

## Validate configuration
databricks bundle validate


---

## Deploy pipeline
databricks bundle deploy -t dev


The deployment will create:

- Unity Catalog schema
- Unity Catalog volume
- Lakeflow pipeline

---

# Running the Pipeline

After deployment, run the pipeline via:

Databricks UI:
Workflows → Pipelines → turbines_etl


Or CLI:
databricks bundle run bronze_silver_gold_pipeline -t dev


The pipeline will create all Bronze, Silver, and Gold tables automatically.

---

# Running Tests

The project includes **two testing approaches**.

---

## 1. CLI Unit Tests

Tests are written using **pytest**.

Run locally:
pytest tests -v

or 

python scripts/run_tests.py


These tests validate the transformation logic in:
notebooks/test_turbine_transformer


This allows tests to run directly inside the Databricks environment.

Example tests include:

- validation rules
- quarantine logic
- anomaly detection
- missing interval detection
- forward-fill imputation

---

# Design Decisions

## Batch Ingestion Instead of Streaming

The source system **appends rows to existing CSV files**.

Streaming ingestion assumes **immutable files**, so the pipeline uses:
spark.read()


instead of Auto Loader.

This guarantees consistent processing.

---

## Separate Invalid Data from Anomalies

Invalid telemetry and statistical anomalies are treated differently.

| Type | Handling |
|----|----|
Invalid data | Silver quarantine |
Missing telemetry | Silver missing intervals |
Short gaps | Silver imputed dataset |
Statistical anomalies | Gold anomaly table |

This preserves data lineage.

---

## Observability

The pipeline includes operational monitoring through:
gold_turbine_data_quality_summary

This helps detect:

- sensor failures
- ingestion issues
- abnormal telemetry patterns

---

# Potential Future Enhancements

Possible production improvements:

- wind turbine **power curve model** anomaly detection
- automated **data quality alerts**
- ML-based turbine performance prediction
- incremental ingestion if upstream changes to immutable files
- packaging utilities as a **Python wheel**

---

# Conclusion

This project demonstrates a **production-style Databricks data engineering pipeline** including:

- Medallion architecture
- modular transformation logic
- automated testing
- infrastructure-as-code deployment
- operational observability

The design emphasizes **maintainability, scalability, and data quality** — key requirements for modern data platforms.
