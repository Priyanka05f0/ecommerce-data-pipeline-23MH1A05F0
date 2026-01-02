import psycopg2
import os
from sqlalchemy import create_engine, text

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password")
}

def load():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        print("🔄 Starting Warehouse Load...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")

        # -----------------------------
        # 1. Customer Metrics (Aggregation)
        # -----------------------------
        print("   Creating warehouse.customer_metrics...")
        cur.execute("DROP TABLE IF EXISTS warehouse.customer_metrics;")
        cur.execute("""
            CREATE TABLE warehouse.customer_metrics AS
            SELECT 
                customer_email,
                COUNT(transaction_id) AS total_orders,
                SUM(line_total) AS total_spent
            FROM production.transactions
            GROUP BY customer_email;
        """)

        # -----------------------------
        # 2. Fact Sales (Copy relevant columns)
        # -----------------------------
        print("   Creating warehouse.fact_sales...")
        cur.execute("DROP TABLE IF EXISTS warehouse.fact_sales;")
        # Note: We rename unit_price -> price to match what analytics script expects
        cur.execute("""
            CREATE TABLE warehouse.fact_sales AS
            SELECT 
                transaction_id,
                product_id,
                quantity,
                unit_price AS price,
                line_total,
                transaction_date
            FROM production.transactions;
        """)

        conn.commit()
        print("✅ Warehouse Load completed successfully")

    except Exception as e:
        print(f"❌ Error in Warehouse Load: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    load()