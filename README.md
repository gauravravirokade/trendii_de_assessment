# Event Analytics Pipeline

This repository contains a **full data pipeline** that ingests event data, models it with dbt, and produces analytics-ready outputs.

---

## 📂 Structure

- **`data_ingestion/`** — Python scripts to ingest raw parquet files into PostgreSQL (`raw` schema), with JSON validation, logging, and file archiving.
- **`analytics/event_analytics/`** — dbt project that transforms raw events into business-ready datasets.
  - **Bronze (source)**: wraps raw tables (`src_events`, `src_product`, `src_campaign`)
  - **Silver (staging)**: cleansed, standardized, and deduplicated data
  - **Gold (end products)**: analytics-ready outputs (campaign metrics, product performance, dashboards)
- **`output.txt` & `test_file`** — auxiliary or temporary files

---

## ⚙️ Workflow

1. **Ingest** new parquet files via `data_ingestion__events.py`.
2. **Transform** data with dbt:
   - `dbt run` to build source → staging → end products
   - `dbt test` to validate data quality
3. **Access** curated outputs for reporting or analytics.

---

## 📌 Key Features

- Automatic table creation and JSON handling during ingestion
- Deduplication and incremental updates in dbt
- Robust logging and error handling
- Analytics-ready end products with campaign and product metrics

