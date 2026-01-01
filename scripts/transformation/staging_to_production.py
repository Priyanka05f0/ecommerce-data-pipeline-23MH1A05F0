from sqlalchemy import create_engine, text
import os

# -----------------------------
# Database config (CI SAFE)
# -----------------------------
DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'admin')}:"
    f"{os.getenv('DB_PASSWORD', 'password')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'ecommerce_db')}"
)

# -----------------------------
# Main ETL
# -----------------------------
def run_staging_to_production():
    engine = create_engine(DB_URL)

    with engine.begin() as conn:

        # -----------------------------
        # Create schema
        # -----------------------------
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS production;"))

        # -----------------------------
        # Create tables
        # -----------------------------
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS production.customers (
                customer_id TEXT PRIMARY KEY,
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
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS production.products (
                product_id TEXT PRIMARY KEY,
                product_name TEXT,
                category TEXT,
                sub_category TEXT,
                price NUMERIC,
                cost NUMERIC,
                brand TEXT,
                stock_quantity INT,
                supplier_id TEXT
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS production.transactions (
                transaction_id TEXT PRIMARY KEY,
                customer_id TEXT,
                transaction_date DATE,
                transaction_time TIME,
                payment_method TEXT,
                shipping_address TEXT,
                total_amount NUMERIC
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS production.transaction_items (
                item_id TEXT PRIMARY KEY,
                transaction_id TEXT,
                product_id TEXT,
                quantity INT,
                unit_price NUMERIC,
                discount_percentage NUMERIC,
                line_total NUMERIC
            );
        """))

        # -----------------------------
        # Truncate existing data
        # -----------------------------
        conn.execute(text("TRUNCATE production.transaction_items CASCADE;"))
        conn.execute(text("TRUNCATE production.transactions CASCADE;"))
        conn.execute(text("TRUNCATE production.products CASCADE;"))
        conn.execute(text("TRUNCATE production.customers CASCADE;"))

        # -----------------------------
        # Load data
        # -----------------------------
        conn.execute(text("""
            INSERT INTO production.customers
            SELECT * FROM staging.customers;
        """))

        conn.execute(text("""
            INSERT INTO production.products
            SELECT * FROM staging.products;
        """))

        conn.execute(text("""
            INSERT INTO production.transactions
            SELECT * FROM staging.transactions;
        """))

        conn.execute(text("""
            INSERT INTO production.transaction_items
            SELECT * FROM staging.transaction_items;
        """))

    print("✅ Step completed successfully: Staging → Production")

# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    run_staging_to_production()
