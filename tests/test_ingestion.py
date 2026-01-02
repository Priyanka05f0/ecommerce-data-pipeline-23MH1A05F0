import psycopg2

DB = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce_db",
    "user": "admin",
    "password": "password"
}

def test_database_connection():
    conn = psycopg2.connect(**DB)
    conn.close()

def test_staging_tables_exist():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='staging'
    """)
    tables = [r[0] for r in cur.fetchall()]
    assert "customers" in tables
    conn.close()

def test_data_loaded_into_staging():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM staging.customers")
    assert cur.fetchone()[0] > 0
    conn.close()
