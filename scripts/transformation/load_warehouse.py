from sqlalchemy import create_engine, text
from datetime import timedelta
import os

DB_URL = (
    f"postgresql://{os.getenv('DB_USER','admin')}:"
    f"{os.getenv('DB_PASSWORD','password')}@"
    f"{os.getenv('DB_HOST','localhost')}:"
    f"{os.getenv('DB_PORT','5432')}/"
    f"{os.getenv('DB_NAME','ecommerce_db')}"
)

def load_dim_date(conn):
    result = conn.execute(text("""
        SELECT MIN(transaction_date), MAX(transaction_date)
        FROM production.transactions
    """)).fetchone()

    start_date, end_date = result
    if not start_date or not end_date:
        return

    conn.execute(text("TRUNCATE warehouse.dim_date CASCADE"))

    d = start_date
    while d <= end_date:
        conn.execute(text("""
            INSERT INTO warehouse.dim_date (
                date_key, full_date, year, quarter, month, day,
                month_name, day_name, week_of_year, is_weekend
            )
            VALUES (
                :dk, :fd, :y, :q, :m, :d,
                :mn, :dn, :w, :iw
            )
        """), {
            "dk": int(d.strftime("%Y%m%d")),
            "fd": d,
            "y": d.year,
            "q": (d.month - 1) // 3 + 1,
            "m": d.month,
            "d": d.day,
            "mn": d.strftime("%B"),
            "dn": d.strftime("%A"),
            "w": int(d.strftime("%U")),
            "iw": d.weekday() >= 5
        })
        d += timedelta(days=1)

def load_warehouse():
    engine = create_engine(DB_URL)

    with engine.begin() as conn:

        conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse"))
        conn.execute(text("TRUNCATE warehouse.fact_sales CASCADE"))

        load_dim_date(conn)

        conn.execute(text("""
            INSERT INTO warehouse.fact_sales (
                date_key, transaction_id, quantity,
                unit_price, line_total
            )
            SELECT
                TO_CHAR(t.transaction_date,'YYYYMMDD')::INT,
                ti.transaction_id,
                ti.quantity,
                ti.unit_price,
                ti.line_total
            FROM production.transaction_items ti
            JOIN production.transactions t
              ON ti.transaction_id = t.transaction_id;
        """))

    print("✅ Warehouse loaded successfully")

if __name__ == "__main__":
    load_warehouse()
