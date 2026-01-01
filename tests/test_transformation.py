import psycopg2
import os

DB = {
    "host": os.getenv("DB_HOST", "localhost"),   # ✅ FIXED
    "database": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "port": int(os.getenv("DB_PORT", 5432))
}

def test_production_tables_populated():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM production.transactions;")
    assert cur.fetchone()[0] > 0
    conn.close()

def test_no_orphan_transaction_items():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM production.transaction_items ti
        LEFT JOIN production.transactions t
          ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL;
    """)
    assert cur.fetchone()[0] == 0
    conn.close()
