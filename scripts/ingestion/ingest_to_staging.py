import pandas as pd
import json
import time
from datetime import datetime
from sqlalchemy import create_engine, text
import yaml
import os

# -----------------------------
# Load configuration
# -----------------------------
def load_config():
    with open("/app/config/config.yaml", "r") as f:
        return yaml.safe_load(f)

# -----------------------------
# Main ingestion logic
# -----------------------------
def ingest():
    start_time = time.time()

    summary = {
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0
    }

    config = load_config()
    db = config["database"]

    engine = create_engine(
        f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )

    files = {
        "customers": "/app/data/raw/customers.csv",
        "products": "/app/data/raw/products.csv",
        "transactions": "/app/data/raw/transactions.csv",
        "transaction_items": "/app/data/raw/transaction_items.csv"
    }

    with engine.begin() as conn:

        # ✅ Ensure schema exists
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))

        # ✅ Truncate in correct order
        conn.execute(text("TRUNCATE staging.transaction_items CASCADE"))
        conn.execute(text("TRUNCATE staging.transactions CASCADE"))
        conn.execute(text("TRUNCATE staging.products CASCADE"))
        conn.execute(text("TRUNCATE staging.customers CASCADE"))

        for table, file_path in files.items():

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Missing file: {file_path}")

            df = pd.read_csv(file_path)

            df.to_sql(
                name=table,
                schema="staging",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=200
            )

            summary["tables_loaded"][table] = {
                "rows_loaded": len(df),
                "status": "success",
                "error_message": None
            }

    summary["total_execution_time_seconds"] = round(
        time.time() - start_time, 2
    )

    os.makedirs("/app/data/staging", exist_ok=True)
    with open("/app/data/staging/ingestion_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("✅ Data ingestion completed successfully")

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    ingest()
