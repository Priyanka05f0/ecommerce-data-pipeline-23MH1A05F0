import pandas as pd
import json
import time
from sqlalchemy import create_engine
from datetime import datetime
import os

DB_URL = "postgresql://admin:password@postgres:5432/ecommerce_db"
SQL_FILE = "/app/sql/queries/analytical_queries.sql"
OUTPUT_DIR = "/app/data/processed_analytics"


def execute_query(engine, sql):
    start = time.time()

    # ✅ FIX: use raw psycopg2 connection
    raw_conn = engine.raw_connection()
    try:
        df = pd.read_sql(sql, raw_conn)
    finally:
        raw_conn.close()

    exec_time = round((time.time() - start) * 1000, 2)
    return df, exec_time


def export_to_csv(df, filename):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(os.path.join(OUTPUT_DIR, filename), index=False)


def main():
    engine = create_engine(DB_URL)

    summary = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": 0,
        "query_results": {}
    }

    with open(SQL_FILE, "r") as f:
        sql_text = f.read()

    queries = [q.strip() for q in sql_text.split(";") if q.strip()]

    total_start = time.time()

    for i, query in enumerate(queries, start=1):
        query_name = f"query{i}"

        df, exec_time = execute_query(engine, query)
        export_to_csv(df, f"{query_name}.csv")

        summary["query_results"][query_name] = {
            "rows": len(df),
            "columns": len(df.columns),
            "execution_time_ms": exec_time
        }

    summary["queries_executed"] = len(queries)
    summary["total_execution_time_seconds"] = round(
        time.time() - total_start, 2
    )

    with open(os.path.join(OUTPUT_DIR, "analytics_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print("✅ Step 4.1 completed successfully: Analytics generated")


if __name__ == "__main__":
    main()
