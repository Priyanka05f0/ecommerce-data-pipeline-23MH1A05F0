import psycopg2
import pandas as pd
import os
import yaml
from pathlib import Path

# -----------------------------
# Load config (CI + LOCAL SAFE)
# -----------------------------
def load_config():
    config_path = Path("config/config.yaml")

    if config_path.exists():
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)["database"]
    else:
        cfg = {}

    return {
        "host": cfg.get("host", os.getenv("DB_HOST", "localhost")),
        "port": int(cfg.get("port", os.getenv("DB_PORT", 5432))),
        "dbname": (
            cfg.get("dbname")
            or cfg.get("name")
            or os.getenv("DB_NAME", "ecommerce_db")
        ),
        "user": cfg.get("user", os.getenv("DB_USER", "admin")),
        "password": cfg.get("password", os.getenv("DB_PASSWORD", "password")),
    }

# -----------------------------
# Ingestion
# -----------------------------
def ingest():
    db = load_config()

    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"],
    )

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
