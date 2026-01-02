import os
from sqlalchemy import create_engine, text

# -----------------------------
# Database config (Docker-safe)
# -----------------------------
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_db")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL)

# -----------------------------
# Warehouse Load
# -----------------------------
def load():
    with engine.begin() as conn:
        print("🏗️ Creating warehouse schema...")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse;"))

        # -----------------------------
        # FACT TABLE
        # -----------------------------
        print("📦 Creating warehouse.fact_sales...")

        conn.execute(text("""
            DROP TABLE IF EXISTS warehouse.fact_sales;

            CREATE TABLE warehouse.fact_sales AS
            SELECT
                product_id,
                quantity,
                unit_price AS price
            FROM production.transactions;
        """))

        print("✅ Warehouse Load completed successfully")

if __name__ == "__main__":
    load()
