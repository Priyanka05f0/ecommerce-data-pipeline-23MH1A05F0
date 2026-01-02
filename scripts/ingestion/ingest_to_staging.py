import os
import pandas as pd
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
# Ingestion
# -----------------------------
def ingest():
    # ---- Required CSV files ----
    customers_path = "data/raw/customers.csv"
    products_path = "data/raw/products.csv"
    transactions_path = "data/raw/transactions.csv"
    items_path = "data/raw/transaction_items.csv"

    required_files = [
        customers_path,
        products_path,
        transactions_path,
        items_path
    ]

    # Validate files exist
    for path in required_files:
        if not os.path.exists(path):
            raise FileNotFoundError(f"❌ Missing required file: {path}")

    print("📥 Reading CSV files...")
    customers = pd.read_csv(customers_path)
    products = pd.read_csv(products_path)
    transactions = pd.read_csv(transactions_path)
    items = pd.read_csv(items_path)

    with engine.begin() as conn:
        print("📂 Creating schemas...")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse;"))

        print("⬆️ Loading data into staging tables...")

        # 1️⃣ Customers
        customers.to_sql(
            name="customers",
            con=conn,
            schema="staging",
            if_exists="replace",
            index=False
        )

        # 2️⃣ Products
        products.to_sql(
            name="products",
            con=conn,
            schema="staging",
            if_exists="replace",
            index=False
        )

        # 3️⃣ Transactions (header-level info)
        transactions.to_sql(
            name="transactions",
            con=conn,
            schema="staging",
            if_exists="replace",
            index=False
        )

        # 4️⃣ Transaction items (line-level info)
        items.to_sql(
            name="transaction_items",
            con=conn,
            schema="staging",
            if_exists="replace",
            index=False
        )

    print("✅ Data ingestion completed successfully")

if __name__ == "__main__":
    ingest()
