import pandas as pd
import time
from sqlalchemy import create_engine
import os

# ---------------------------------
# Database connection (ENV SAFE)
# ---------------------------------
DB_URL = (
    f"postgresql://{os.getenv('DB_USER','admin')}:"
    f"{os.getenv('DB_PASSWORD','password')}@"
    f"{os.getenv('DB_HOST','localhost')}:"
    f"{os.getenv('DB_PORT','5432')}/"
    f"{os.getenv('DB_NAME','ecommerce_db')}"
)

# ---------------------------------
# Helper to execute query
# ---------------------------------
def execute_query(engine, sql):
    start = time.time()
    df = pd.read_sql(sql, engine)
    exec_time = round(time.time() - start, 2)
    return df, exec_time

# ---------------------------------
# Main analytics logic
# ---------------------------------
def main():
    engine = create_engine(DB_URL)

    # ✅ FIXED QUERY (schema-safe)
    query = """
        SELECT
            f.product_id,
            SUM(f.line_total) AS total_revenue,
            SUM(f.quantity) AS units_sold,
            AVG(f.line_total / NULLIF(f.quantity, 0)) AS avg_price
        FROM warehouse.fact_sales f
        GROUP BY f.product_id
        ORDER BY total_revenue DESC
        LIMIT 10;
    """

    df, exec_time = execute_query(engine, query)

    os.makedirs("data/processed", exist_ok=True)
    df.to_csv("data/processed/top_products.csv", index=False)

    print("📊 Analytics generated successfully")
    print(f"⏱ Query time: {exec_time}s")

# ---------------------------------
# Entry point
# ---------------------------------
if __name__ == "__main__":
    main()
