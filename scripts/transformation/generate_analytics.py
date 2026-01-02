import time
import pandas as pd
from sqlalchemy import create_engine
import os

# -----------------------------
# Database connection
# -----------------------------
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_db")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# -----------------------------
# Helper function
# -----------------------------
def execute_query(engine, sql):
    start = time.time()
    df = pd.read_sql(sql, engine)
    duration = round(time.time() - start, 2)
    return df, duration

# -----------------------------
# Analytics
# -----------------------------
def main():
    print("📊 Generating analytics...")

    query = """
        SELECT
            f.product_id,
            SUM(f.quantity * f.price) AS total_revenue,
            SUM(f.quantity) AS units_sold,
            AVG(f.price) AS avg_price
        FROM warehouse.fact_sales f
        GROUP BY f.product_id
        ORDER BY total_revenue DESC
        LIMIT 10;
    """

    df, exec_time = execute_query(engine, query)

    print("✅ Analytics generated successfully")
    print(df)
    print(f"⏱ Execution time: {exec_time}s")

if __name__ == "__main__":
    main()
