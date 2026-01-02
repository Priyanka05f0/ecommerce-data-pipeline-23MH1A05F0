import psycopg2

DB = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce_db",
    "user": "admin",
    "password": "password"
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
