import psycopg2
import os

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
        cur.execute("CREATE SCHEMA IF NOT EXISTS production;")

        # Customers
        cur.execute("""
            DROP TABLE IF EXISTS production.customers;
            CREATE TABLE production.customers AS
            SELECT * FROM staging.customers;
        """)

        # Products
        cur.execute("""
            DROP TABLE IF EXISTS production.products;
            CREATE TABLE production.products AS
            SELECT * FROM staging.products;
        """)

        # Transactions (ONLY REAL COLUMNS)
        cur.execute("""
            DROP TABLE IF EXISTS production.transactions;
            CREATE TABLE production.transactions AS
            SELECT
                item_id,
                transaction_id,
                product_id,
                quantity,
                unit_price,
                discount_percentage,
                line_total
            FROM staging.transaction_items;
        """)

        conn.commit()
        print("✅ Staging → Production completed successfully")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error in Staging → Production: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    transform()
