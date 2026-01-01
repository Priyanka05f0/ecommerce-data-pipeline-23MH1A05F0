import psycopg2
import pandas as pd
import os
from pathlib import Path

# -----------------------------
# CI-SAFE DB CONFIG (FINAL)
# -----------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

# -----------------------------
# Ingestion
# -----------------------------
def ingest():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    data_dir = Path("data/raw")

    tables = {
        "customers": "customers.csv",
        "products": "products.csv",
        "transactions": "transactions.csv",
        "transaction_items": "transaction_items.csv",
    }

    for table, file in tables.items():
        df = pd.read_csv(data_dir / file)

        cur.execute(f"DELETE FROM staging.{table};")

        for _, row in df.iterrows():
            cols = ",".join(df.columns)
            vals = ",".join(["%s"] * len(row))
            cur.execute(
                f"INSERT INTO staging.{table} ({cols}) VALUES ({vals})",
                tuple(row)
            )

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Data ingested into staging successfully")

if __name__ == "__main__":
    ingest()
