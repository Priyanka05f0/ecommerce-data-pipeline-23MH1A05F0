import psycopg2
import os

DB = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password")
}

def test_warehouse_tables_exist():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='warehouse'
    """)
    tables = [r[0] for r in cur.fetchall()]
    assert "fact_sales" in tables
    conn.close()

def test_fact_sales_not_empty():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM warehouse.fact_sales")
    assert cur.fetchone()[0] > 0
    conn.close()
