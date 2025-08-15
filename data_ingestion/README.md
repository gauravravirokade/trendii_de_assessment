

# Data Ingestion — `data_ingestion__events.py`
````markdown
## Pre-requisites

1. **Python Version**
   Python 3.8 or higher.

2. **Required Python Packages**
   ```bash
   pip install pandas pyarrow numpy psycopg2-binary python-dotenv
```

*If using Conda:*

```bash
conda install -c conda-forge pandas pyarrow numpy psycopg2
````

3. **PostgreSQL Database**
   Ensure PostgreSQL is running locally (or on an accessible host) and credentials are set in `DB_CONFIG` or `.env`.

4. **Directory Structure**

   ```
   /source_data_folder   # Incoming parquet files
   /archive_folder       # Processed files are moved here
   logs/                 # Ingestion error logs
   ```

5. **Database Tables**

   * `raw.raw_events` — stores ingested data
   * `raw.ingestion_log` — tracks already processed files

---

## ⚙️ How It Works

1. Connects to PostgreSQL.
2. Ensures the `raw` schema and ingestion log table exist.
3. Scans the source folder for new `.parquet` files.
4. For each file:

   * Reads the parquet file (using **PyArrow**).
   * Adds `source_file_name` and `ingested_at` columns.
   * Validates JSON fields (`event_data`, `event_context`) and makes them serializable.
   * Creates the target table if not already present (all columns stored as `TEXT`).
   * Inserts all rows into `raw.raw_events`.
   * Logs the ingestion in `raw.ingestion_log`.
   * Moves the file to the archive folder.

---

## 🛡 Error Handling

The script handles:

* **Missing parquet engine** → Requires `pyarrow` or `fastparquet`.
* **Invalid JSON** → Converts NumPy objects to JSON-serializable formats; logs failures.
* **Database errors** → Rolls back failed transactions; logs details.
* **Duplicate ingestion** → Skips already processed files via `ingestion_log`.
* **File movement issues** → Ensures processed files are archived.

---

## ✅ Summary

This script ingests parquet files into PostgreSQL with:

* JSON validation and conversion
* Automatic schema and table creation
* Robust error handling and logging
* Prevention of duplicate ingestion
* Archiving of processed files

It’s suitable for development and production ingestion pipelines.

