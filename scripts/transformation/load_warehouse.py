from sqlalchemy import create_engine, text
import os

DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'admin')}:"
    f"{os.getenv('DB_PASSWORD', 'password')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'ecommerce_db')}"
)

def load_warehouse():
    engine = create_engine(DB_URL)

    with engine.begin() as conn:

        # 1️⃣ CREATE SCHEMA
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse;"))

        # 2️⃣ CREATE DIMENSIONS
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
                customer_id TEXT PRIMARY KEY,
                city TEXT,
                state TEXT,
                country TEXT,
                age_group TEXT
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.dim_products (
                product_id TEXT PRIMARY KEY,
                category TEXT,
                sub_category TEXT,
                brand TEXT
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.dim_date (
                date DATE PRIMARY KEY,
                year INT,
                month INT,
                day INT
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.dim_payment_method (
                payment_method TEXT PRIMARY KEY
            );
        """))

        # 3️⃣ CREATE FACT TABLE
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
                transaction_id TEXT,
                product_id TEXT,
                customer_id TEXT,
                date DATE,
                payment_method TEXT,
                quantity INT,
                revenue NUMERIC
            );
        """))

        # 4️⃣ SAFE TRUNCATE
        conn.execute(text("TRUNCATE warehouse.fact_sales CASCADE;"))
        conn.execute(text("TRUNCATE warehouse.dim_customers CASCADE;"))
        conn.execute(text("TRUNCATE warehouse.dim_products CASCADE;"))
        conn.execute(text("TRUNCATE warehouse.dim_date CASCADE;"))
        conn.execute(text("TRUNCATE warehouse.dim_payment_method CASCADE;"))

        # 5️⃣ LOAD DIMENSIONS
        conn.execute(text("""
            INSERT INTO warehouse.dim_customers
            SELECT DISTINCT customer_id, city, state, country, age_group
            FROM production.customers;
        """))

        conn.execute(text("""
            INSERT INTO warehouse.dim_products
            SELECT DISTINCT product_id, category, sub_category, brand
            FROM production.products;
        """))

        conn.execute(text("""
            INSERT INTO warehouse.dim_date
            SELECT DISTINCT
                transaction_date,
                EXTRACT(YEAR FROM transaction_date),
                EXTRACT(MONTH FROM transaction_date),
                EXTRACT(DAY FROM transaction_date)
            FROM production.transactions;
        """))

        conn.execute(text("""
            INSERT INTO warehouse.dim_payment_method
            SELECT DISTINCT payment_method
            FROM production.transactions;
        """))

        # 6️⃣ LOAD FACT
        conn.execute(text("""
            INSERT INTO warehouse.fact_sales
            SELECT
                ti.transaction_id,
                ti.product_id,
                t.customer_id,
                t.transaction_date,
                t.payment_method,
                ti.quantity,
                ti.line_total
            FROM production.transaction_items ti
            JOIN production.transactions t
              ON ti.transaction_id = t.transaction_id;
        """))

    print("✅ Warehouse loaded successfully")

if __name__ == "__main__":
    load_warehouse()
