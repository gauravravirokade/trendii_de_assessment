

# Data Ingestion — `data_ingestion__events.py`

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
   ```

3. **PostgreSQL Database**
   Ensure PostgreSQL is running locally or on an accessible host.

4. **Directory Structure**

   ```
   /source_data_folder   # Incoming parquet files
   /archive_folder       # Processed files are moved here
   /logs/                # Ingestion error logs
   ```

5. **Database Tables**

   * `raw.events` — stores ingested data
   * `raw.ingestion_log` — tracks already processed files

---

## ⚙️ Setup

1. **Create a `.env` file** in the root of your project.
   Copy the structure below and update values as needed:

   ```env
   # -------- Database Configuration --------
   DB_NAME=your_database_name
   DB_USER=your_database_user
   DB_PASSWORD=your_database_password
   DB_HOST=localhost
   DB_PORT=5432

   # -------- Directories --------
   SOURCE_DIR=/path/to/your/source/parquet/files
   ARCHIVE_DIR=/path/to/archive/directory

   # -------- Logging --------
   LOG_FILE=/path/to/log/ingestion.log

   # -------- Tables --------
   TARGET_TABLE=database_name.schema_name.table_name
   LOG_TABLE=raw.ingestion_log
   ```

   > The script automatically loads environment variables from `.env` using `python-dotenv`.

---

## ⚙️ How It Works

1. Connects to PostgreSQL using credentials from `.env`.
2. Ensures the `raw` schema exists.
3. Creates the ingestion log table (`raw.ingestion_log`) if it doesn’t exist.
4. Scans the source folder for new `.parquet` files that have not already been ingested.
5. For each new file:

   * Reads the parquet file (using **PyArrow**).
   * Adds metadata columns:

     * `source_file_name` — name of the file ingested
     * `ingested_at` — timestamp of ingestion
   * Converts `event_data` and `event_context` columns to JSONB-compatible strings.
   * Ensures the target table exists (`raw.events`) with appropriate column types.
   * Inserts all rows into the target table.
   * Logs ingestion in `raw.ingestion_log`.
   * Moves the processed file to the archive folder.

---

## 🛡 Error Handling

The script handles:

* **Missing parquet engine** → Requires `pyarrow` or `fastparquet`.
* **Invalid JSON** → Converts NumPy/Pandas types to JSON-serializable formats; logs failures.
* **Database errors** → Rolls back failed transactions and logs details.
* **Duplicate ingestion** → Skips already processed files via `raw.ingestion_log`.
* **File movement issues** → Ensures processed files are archived even if ingestion partially fails.

---

## ✅ Summary

This script ingests parquet files into PostgreSQL with:

* JSON validation and conversion
* Automatic schema and table creation
* Robust error handling and logging
* Prevention of duplicate ingestion
* Archiving of processed files

