import psycopg2
import pandas as pd
import os
from pathlib import Path

# -----------------------------
# Database config (CI SAFE)
# -----------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

DATA_DIR = Path("data/raw")

# -----------------------------
# Create schemas + tables
# -----------------------------
def initialize_staging(cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.customers (
            customer_id INT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            registration_date DATE,
            city TEXT,
            state TEXT,
            country TEXT,
            age_group TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.products (
            product_id INT,
            product_name TEXT,
            category TEXT,
            sub_category TEXT,
            price NUMERIC,
            cost NUMERIC,
            brand TEXT,
            stock_quantity INT,
            supplier_id INT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.transactions (
            transaction_id INT,
            customer_id INT,
            transaction_date DATE,
            transaction_time TIME,
            payment_method TEXT,
            shipping_address TEXT,
            total_amount NUMERIC
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.transaction_items (
            item_id INT,
            transaction_id INT,
            product_id INT,
            quantity INT,
            unit_price NUMERIC,
            discount_percentage NUMERIC,
            line_total NUMERIC
        );
    """)

# -----------------------------
# Ingest CSV → Staging
# -----------------------------
def ingest():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    initialize_staging(cur)
    conn.commit()

    tables = {
        "customers": "customers.csv",
        "products": "products.csv",
        "transactions": "transactions.csv",
        "transaction_items": "transaction_items.csv"
    }

    for table, file in tables.items():
        df = pd.read_csv(DATA_DIR / file)

        cur.execute(f"DELETE FROM staging.{table};")

        cols = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))

        insert_query = f"""
            INSERT INTO staging.{table} ({cols})
            VALUES ({placeholders})
        """

        for row in df.itertuples(index=False):
            cur.execute(insert_query, tuple(row))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Data ingestion completed successfully")

# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    ingest()
