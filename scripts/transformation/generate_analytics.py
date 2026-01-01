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
# Helper
# ---------------------------------
def execute_query(engine, sql):
    start = time.time()
    df = pd.read_sql(sql, engine)
    return df, round(time.time() - start, 2)

# ---------------------------------
# Main analytics
# ---------------------------------
def main():
    engine = create_engine(DB_URL)

    # ✅ SCHEMA-SAFE QUERY (NO GUESSING)
    query = """
        SELECT
            product_id,
            SUM(quantity) AS units_sold
        FROM warehouse.fact_sales
        GROUP BY product_id
        ORDER BY units_sold DESC
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
