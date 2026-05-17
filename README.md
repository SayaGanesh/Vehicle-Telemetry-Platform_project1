# Vehicle Telemetry Data Processing Platform – Azure

## Overview

This project builds an end-to-end **Vehicle Telemetry Data Processing Platform** on Microsoft Azure. Raw telemetry signals from vehicle sensors (GPS, accelerometer, engine ECU, LiDAR proximity) are ingested, validated, transformed, and made available for analytics and ML model training.

The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold) using Azure Data Lake Storage Gen2, Azure Databricks, and Azure Data Factory.

---

## Architecture

```
Vehicle Sensors
      │
      ▼
Azure Event Hubs (Stream Ingestion)
      │
      ▼
ADLS Gen2 – Bronze Layer (raw JSON)
      │
      ▼
Azure Databricks – Silver Layer (cleaned, validated Parquet)
      │
      ▼
Azure Databricks – Gold Layer (aggregated Delta tables)
      │
      ▼
Azure Synapse Analytics / Power BI (reporting)
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Azure Event Hubs |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Databricks (PySpark) |
| Orchestration | Azure Data Factory |
| Serving | Azure Synapse Analytics |
| Format | Delta Lake, Parquet, JSON |
| Language | Python 3.10, SQL, PySpark |

---

## Project Structure

```
📁 data/              → Sample raw telemetry JSON files
📁 notebooks/         → Databricks notebooks (Bronze→Silver→Gold)
📁 sql/               → Synapse SQL views and aggregation queries
📄 README.md
📄 requirements.txt
```

---

## Pipeline Stages

### Bronze Layer
- Raw telemetry events land from Event Hubs into ADLS Gen2 as JSON
- No transformation — append-only, full fidelity

### Silver Layer
- Schema validation and null handling
- Unit normalization (speed: km/h, temperature: Celsius)
- Duplicate removal using `vehicle_id + event_timestamp`
- Written as Parquet with partitioning by `date` and `vehicle_id`

### Gold Layer
- Aggregated metrics per vehicle per day
- Anomaly flags: harsh braking, overspeed, engine overheat
- Delta Lake format for ACID compliance and time travel

---

## Sample Insights Produced

- Average speed and mileage per vehicle per day
- Harsh braking events count per trip
- Engine temperature anomaly detection
- Fleet-level health scoring dashboard

---

## How to Run

1. Upload `data/` folder to ADLS Gen2 Bronze container
2. Import notebooks from `notebooks/` into Azure Databricks
3. Run in order: `01_bronze_ingest` → `02_silver_transform` → `03_gold_aggregate`
4. Connect Synapse to Gold Delta tables
5. Schedule via ADF pipeline (JSON export in `adf_pipeline/`)

---

## Key Learnings

- Implemented schema drift handling for new sensor types
- Used Delta Lake `MERGE` for late-arriving telemetry corrections
- Partitioning strategy reduced query time by ~60% on large datasets
- Parameterized ADF pipelines for multi-vehicle fleet support
