# tests/test_ingestion.py
import pytest
import psycopg2
import os

# We re-define config here just to check connection independently, 
# ensuring we use the ENV variable.
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": "5432",
    "dbname": "ecommerce_db",
    "user": "admin",
    "password": "password"
}

def test_database_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        assert conn is not None
        conn.close()
    except psycopg2.OperationalError as e:
        pytest.fail(f"Database connection failed: {e}")

def test_staging_tables_exist():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        tables = ["customers", "products", "transactions", "transaction_items"]
        for table in tables:
            cur.execute(f"SELECT to_regclass('staging.{table}');")
            result = cur.fetchone()
            assert result[0] is not None, f"Table staging.{table} does not exist"
        
        cur.close()
        conn.close()
    except Exception as e:
        pytest.fail(f"Test failed: {e}")

def test_data_loaded_into_staging():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM staging.customers")
    assert cur.fetchone()[0] > 0
    
    cur.close()
    conn.close()