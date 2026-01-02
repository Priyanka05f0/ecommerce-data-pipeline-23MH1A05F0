import psycopg2
import os
from sqlalchemy import create_engine, text

# -----------------------------
# Database config
# -----------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password")
}

def transform():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        print("🔄 Starting Staging -> Production transformation...")
        
        cur.execute("CREATE SCHEMA IF NOT EXISTS production;")

        # -----------------------------
        # 1. Copy Customers & Products (Clean Copy)
        # -----------------------------
        cur.execute("DROP TABLE IF EXISTS production.customers;")
        cur.execute("CREATE TABLE production.customers AS SELECT * FROM staging.customers;")
        
        cur.execute("DROP TABLE IF EXISTS production.products;")
        cur.execute("CREATE TABLE production.products AS SELECT * FROM staging.products;")

        # -----------------------------
        # 2. Create ONE Master Transaction Table (The Fix)
        # -----------------------------
        print("   Creating production.transactions (Denormalized)...")
        cur.execute("DROP TABLE IF EXISTS production.transactions;")
        
        # We join Items + Transactions + Customers
        # This creates a single table with email, product_id, and price all in one row.
        cur.execute("""
            CREATE TABLE production.transactions AS
            SELECT 
                ti.item_id,
                ti.transaction_id,
                ti.product_id,
                ti.quantity,
                ti.unit_price,
                ti.line_total,
                t.transaction_date,
                c.customer_id,
                c.email AS customer_email
            FROM staging.transaction_items ti
            JOIN staging.transactions t ON ti.transaction_id = t.transaction_id
            JOIN staging.customers c ON t.customer_id = c.customer_id;
        """)

        conn.commit()
        print("✅ Staging → Production completed successfully")

    except Exception as e:
        print(f"❌ Error in Staging to Production: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    transform()