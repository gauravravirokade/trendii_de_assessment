# Event Analytics — dbt Project

## 📌 Overview

The **Event Analytics** project is a **dbt (Data Build Tool)** implementation designed to model raw event data into curated, analytics-ready datasets.

It follows a layered architecture:

* **Bronze (Source models)** → Direct ingestion and casting of raw data.
* **Silver (Staging models)** → Cleansed, standardized, and deduplicated events.
* **Gold (End product models)** → Business-ready outputs answering key questions.

---

## 📂 Project Structure

```
event_analytics/
│── models/
│   ├── source/        # Bronze layer: raw sources wrapped as dbt models
│   │   ├── src_events.sql
│   │   ├── src_product.sql
│   │   └── src_campaign.sql
│   │
│   ├── staging/       # Silver layer: cleaned and standardized staging models
│   │   ├── stg_events.sql
│   │   ├── stg_product.sql
│   │   ├── stg_campaign.sql
│   │   ├── stg_event_mounts.sql
│   │   ├── stg_event_product_click.sql
│   │   ├── stg_event_product_impressions.sql
│   │   ├── stg_event_tag_loaded.sql
│   │   └── stg_event_data_profiling.sql
│   │
│   └── end_products/  # Gold layer: analytics-ready outputs
│       ├── product_impressions_per_campaign.sql
│       ├── product_clicks_per_campaign.sql
│       ├── mounts_per_campaign.sql
│       ├── tags_loaded_per_campaign.sql
│       └── event_summary_dashboard.sql
│
│── macros/            # Custom macros (if any)
│── tests/             # Generic and singular tests
```

---

## ⚙️ Key Models

### 🔹 Source Models (Bronze)

* **`src_events`** — Ingests raw `raw_events` data, parses JSON fields, and ensures unique `event_id`.
* **`src_product`** — Wraps raw `dim_product` data with type casting.
* **`src_campaign`** — Wraps raw `dim_campaign` data with type casting and filtering.

### 🔹 Staging Models (Silver)

* **`stg_events`** — Base cleaned events table.
* **`stg_product`** — Clean product dimension for joins.
* **`stg_campaign`** — Active campaign records only.
* **`stg_event_mounts`** — Extracts and flattens mount event arrays.
* **`stg_event_product_click`** — Standardized product click events.
* **`stg_event_product_impressions`** — Extracts products array, deduplicates by `event_id + product_id`, and flags product dimension matches.
* **`stg_event_tag_loaded`** — Standardized tag load events.
* **`stg_event_data_profiling`** — Profiling layer that validates event hypotheses (e.g., only mounts populated for Mounts events, etc.).

### 🔹 End Products (Gold)

* **`product_impressions_per_campaign`** — Identifies the top product impressions per campaign (brand attribution + validity window).
* **`product_clicks_per_campaign`** — Tracks product click-through performance per campaign.
* **`mounts_per_campaign`** — Aggregates mount events tied to campaigns.
* **`tags_loaded_per_campaign`** — Provides campaign-level breakdown of tag load events.
* **`event_summary_dashboard`** — Consolidated event metrics across campaigns for reporting.

---

## 🧪 Testing & Validation

* **Generic dbt tests**: `unique`, `not_null`, `relationships` applied across keys.
* **Custom profiling**: `stg_event_data_profiling` flags unexpected data patterns.
* **Constraints**: Deduplication using surrogate keys (e.g., `event_id + product_id`).

---


---

## 🔧 Technical Details

* **Adapter**: Postgres 
* **Materializations**:

  * Source: `incremental` or `view`
  * Staging: `view` / `table` / `incremental` (depending on event type)
  * End Products: `table`
* **Incremental strategy**: `unique_key` (e.g., `event_id`, or `event_id + mount_index`)
* **Schema naming**:

  * Bronze → `bronze`
  * Silver → `silver`
  * Gold → `gold`

---

## 📖 Setup & Usage

For installation, running, and documentation access, please refer to the dedicated guide:
👉 [Installation & Run Guide](INSTALLATION.md)

---

Would you like me to now also create the **`INSTALLATION.md`** file with installation, run, and docs hosting steps?
