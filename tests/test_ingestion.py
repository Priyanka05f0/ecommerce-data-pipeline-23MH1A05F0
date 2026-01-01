import psycopg2
import os

DB = {
    "host": os.getenv("DB_HOST", "localhost"),   # ✅ FIXED
    "database": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "port": int(os.getenv("DB_PORT", 5432))
}

def test_database_connection():
    conn = psycopg2.connect(**DB)
    assert conn is not None
    conn.close()

def test_staging_tables_exist():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging';
    """)
    tables = [t[0] for t in cur.fetchall()]
    assert "customers" in tables
    assert "products" in tables
    conn.close()

def test_data_loaded_into_staging():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM staging.customers;")
    count = cur.fetchone()[0]
    assert count > 0
    conn.close()
