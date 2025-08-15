import psycopg2
from psycopg2 import sql

# Database connection parameters
DB_CONFIG = {
    "dbname": "trendii_de_assessment",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",  # or your DB host
    "port": 5432          # default Postgres port
}

def connect_to_postgres():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connection to PostgreSQL established successfully.")
        return conn
    except Exception as e:
        print("❌ Failed to connect to PostgreSQL:", e)
        return None

def create_schema(conn):
    """Ensure raw schema exists."""
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
            conn.commit()
            print("✅ Schema 'raw' is ready.")
    except Exception as e:
        print("❌ Failed to create schema:", e)

def create_table(conn):
    """Create a table in the raw schema."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            print("✅ Table 'raw.test_table' is ready.")
    except Exception as e:
        print("❌ Failed to create table:", e)

def insert_sample_data(conn):
    """Insert sample data into raw.test_table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.test_table (name) VALUES (%s)",
                ("Test User",)
            )
            conn.commit()
            print("✅ Sample data inserted successfully.")
    except Exception as e:
        print("❌ Failed to insert data:", e)

def fetch_data(conn):
    """Fetch data from raw.test_table."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, created_at FROM raw.test_table")
            rows = cur.fetchall()
            print("\n📋 Data in 'raw.test_table':")
            for row in rows:
                print(row)
    except Exception as e:
        print("❌ Failed to fetch data:", e)

if __name__ == "__main__":
    connection = connect_to_postgres()
    if connection:
        create_schema(connection)
        create_table(connection)
        insert_sample_data(connection)
        fetch_data(connection)
        connection.close()
        print("🔒 Connection closed.")
