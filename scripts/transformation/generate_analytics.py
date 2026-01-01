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

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

# -----------------------------
# Helper function
# -----------------------------
def execute_query(engine, sql):
    start = time.time()
    df = pd.read_sql(sql, engine)
    duration = round(time.time() - start, 2)
    return df, duration

# -----------------------------
# Main analytics logic
# -----------------------------
def main():
    print("📊 Generating analytics...")

    # ✅ FIXED: product_id instead of product_key
    query = """
    SELECT
        p.product_name,
        p.category,
        SUM(f.line_total) AS total_revenue,
        SUM(f.quantity) AS units_sold,
        AVG(f.unit_price) AS avg_price
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_products p
        ON f.product_id = p.product_id
    GROUP BY p.product_name, p.category
    ORDER BY total_revenue DESC
    LIMIT 10;
    """

    df, exec_time = execute_query(engine, query)

    output_path = "data/processed/top_products.csv"
    df.to_csv(output_path, index=False)

    print(f"✅ Analytics generated successfully in {exec_time}s")
    print(f"📁 Saved to {output_path}")

if __name__ == "__main__":
    main()
