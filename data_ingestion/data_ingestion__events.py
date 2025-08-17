import os
import json
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import ast

load_dotenv()  # Load .env file

# -------- CONFIG --------
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT"))
}

SOURCE_DIR = Path(os.getenv("SOURCE_DIR"))
ARCHIVE_DIR = Path(os.getenv("ARCHIVE_DIR"))
LOG_FILE = Path(os.getenv("LOG_FILE"))
TARGET_TABLE = os.getenv("TARGET_TABLE")
LOG_TABLE = os.getenv("LOG_TABLE")

# -------- DB SETUP --------
def get_connection():
    """Create a single reusable DB connection."""
    return psycopg2.connect(**DB_CONFIG)

def ensure_schema(conn):
    """Ensure raw schema exists."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    conn.commit()

def ensure_log_table(conn):
    """Create ingestion log table if not exists."""
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
                file_name TEXT PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT now(),
                row_count INTEGER
            );
        """)
    conn.commit()

# -------- HELPERS --------
def get_already_ingested(conn):
    """Return set of already ingested file names."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT file_name FROM {LOG_TABLE}")
        rows = cur.fetchall()
    return {r[0] for r in rows}

def log_error(file, error_msg):
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.now()} | {file} | {error_msg}\n")

def insert_log(conn, file_name, row_count):
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {LOG_TABLE} (file_name, row_count) VALUES (%s, %s)",
            (file_name, row_count)
        )
    conn.commit()

def move_to_archive(file_path):
    ARCHIVE_DIR.mkdir(exist_ok=True)
    dest = ARCHIVE_DIR / file_path.name
    file_path.rename(dest)

def make_json_serializable(obj):
    """Recursively convert NumPy and Pandas types to native Python types."""
    if isinstance(obj, np.ndarray):
        return [make_json_serializable(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(i) for i in obj]
    elif isinstance(obj, (np.integer, np.int32, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float32, np.float64)):
        return float(obj)
    elif obj is None or pd.isna(obj):
        return None
    else:
        return obj

def parse_to_json(value):
    """Convert string representations of dict/list to JSON-compatible string."""
    if pd.isna(value):
        return None
    try:
        # Convert Python-style string dict to actual dict
        if isinstance(value, str):
            parsed = ast.literal_eval(value)
        else:
            parsed = value
        # Convert any nested types to JSON-compatible types
        parsed = make_json_serializable(parsed)
        # Dump to JSON string
        return json.dumps(parsed)
    except Exception as e:
        raise ValueError(f"Failed to parse JSON-like value: {value}. Error: {e}")

# -------- INGESTION --------
def ingest_file(conn, file_path):
    print(f"Processing: {file_path.name}")
    try:
        df = pd.read_parquet(file_path, engine='pyarrow')

        # Add metadata columns
        df["source_file_name"] = file_path.name
        df["ingested_at"] = datetime.utcnow()

        # Ensure proper timestamp for PostgreSQL
        if 'event_created_at' in df.columns:
            df['event_created_at'] = pd.to_datetime(df['event_created_at'])

        # Fix JSON columns
        for json_col in ["event_data", "event_context"]:
            if json_col in df.columns:
                df[json_col] = df[json_col].apply(parse_to_json)

        # Create target table if not exists
        with conn.cursor() as cur:
            col_defs = []
            for c in df.columns:
                if c in ["event_data", "event_context"]:
                    col_defs.append(f'"{c}" JSONB')
                elif c in ["ingested_at", "event_created_at"]:
                    col_defs.append(f'"{c}" TIMESTAMP')
                else:
                    col_defs.append(f'"{c}" TEXT')

            # Check if table exists
            cur.execute(f"SELECT to_regclass('{TARGET_TABLE}');")
            if cur.fetchone()[0] is None:
                cur.execute(f"""
                    CREATE TABLE {TARGET_TABLE} (
                        {', '.join(col_defs)}
                    );
                """)
            conn.commit()

            # Insert rows using execute_batch
            columns = df.columns.tolist()
            placeholders = ", ".join(["%s"] * len(columns))
            data_to_insert = df.values.tolist()
            # Build the column list with double quotes safely
            quoted_columns = ', '.join(['"' + c + '"' for c in columns])
            insert_sql = f'INSERT INTO {TARGET_TABLE} ({quoted_columns}) VALUES ({placeholders})'

            execute_batch(cur, insert_sql, data_to_insert)

        conn.commit()
        insert_log(conn, file_path.name, len(df))
        move_to_archive(file_path)
        print(f"✅ Successfully ingested: {file_path.name}")

    except Exception as e:
        conn.rollback()
        log_error(file_path.name, str(e))
        print(f"❌ Failed: {file_path.name} — logged to {LOG_FILE}")

# -------- MAIN --------
def main():
    with get_connection() as conn:
        ensure_schema(conn)
        ensure_log_table(conn)
        already_ingested = get_already_ingested(conn)

        parquet_files = sorted(SOURCE_DIR.glob("*.parquet"))
        new_files = [f for f in parquet_files if f.name not in already_ingested]

        if not new_files:
            print("✅ No new files to ingest.")
            return

        for file_path in new_files:
            ingest_file(conn, file_path)

if __name__ == "__main__":
    main()
