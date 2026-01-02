import pandas as pd
from sqlalchemy import create_engine, text
import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_URL = f"postgresql://admin:password@{DB_HOST}:5432/ecommerce_db"

def main():
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        # ✅ SQLAlchemy 2.x FIX
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))

        customers = pd.read_csv("data/raw/customers.csv")
        products = pd.read_csv("data/raw/products.csv")
        transactions = pd.read_csv("data/raw/transactions.csv")
        items = pd.read_csv("data/raw/transaction_items.csv")

        customers.to_sql("customers", conn, schema="staging", if_exists="replace", index=False)
        products.to_sql("products", conn, schema="staging", if_exists="replace", index=False)
        transactions.to_sql("transactions", conn, schema="staging", if_exists="replace", index=False)
        items.to_sql("transaction_items", conn, schema="staging", if_exists="replace", index=False)

    print("✅ Data successfully loaded into staging")

if __name__ == "__main__":
    main()
