from sqlalchemy import create_engine, text
from datetime import timedelta

DB_URL = "postgresql://admin:password@postgres:5432/ecommerce_db"


# =========================
# DATE DIMENSION (DYNAMIC)
# =========================
def load_dim_date(conn):
    # Find min & max dates from production data
    result = conn.execute(text("""
        SELECT
            MIN(transaction_date),
            MAX(transaction_date)
        FROM production.transactions
    """)).fetchone()

    start_date, end_date = result

    if start_date is None or end_date is None:
        raise ValueError("No transaction dates found to build dim_date")

    # Clear dim_date safely
    conn.execute(text("TRUNCATE TABLE warehouse.dim_date CASCADE"))

    d = start_date
    while d <= end_date:
        conn.execute(
            text("""
                INSERT INTO warehouse.dim_date (
                    date_key, full_date, year, quarter, month, day,
                    month_name, day_name, week_of_year, is_weekend
                )
                VALUES (
                    :date_key, :full_date, :year, :quarter, :month, :day,
                    :month_name, :day_name, :week_of_year, :is_weekend
                )
            """),
            {
                "date_key": int(d.strftime("%Y%m%d")),
                "full_date": d,
                "year": d.year,
                "quarter": (d.month - 1) // 3 + 1,
                "month": d.month,
                "day": d.day,
                "month_name": d.strftime("%B"),
                "day_name": d.strftime("%A"),
                "week_of_year": int(d.strftime("%U")),
                "is_weekend": d.weekday() >= 5
            }
        )
        d += timedelta(days=1)


# =========================
# LOAD WAREHOUSE
# =========================
def load_warehouse():
    engine = create_engine(DB_URL)

    with engine.begin() as conn:

        # ==================================================
        # 0. CLEAR FACT FIRST
        # ==================================================
        conn.execute(text("TRUNCATE TABLE warehouse.fact_sales"))

        # ==================================================
        # 1. DIM DATE
        # ==================================================
        load_dim_date(conn)

        # ==================================================
        # 2. DIM PAYMENT METHOD
        # ==================================================
        conn.execute(text("TRUNCATE TABLE warehouse.dim_payment_method CASCADE"))
        conn.execute(text("""
            INSERT INTO warehouse.dim_payment_method (
                payment_method_name, payment_type
            )
            SELECT DISTINCT
                payment_method,
                CASE
                    WHEN payment_method IN (
                        'Credit Card','Debit Card','UPI','Net Banking'
                    ) THEN 'Online'
                    ELSE 'Offline'
                END
            FROM production.transactions
        """))

        # ==================================================
        # 3. DIM CUSTOMERS
        # ==================================================
        conn.execute(text("TRUNCATE TABLE warehouse.dim_customers CASCADE"))
        conn.execute(text("""
            INSERT INTO warehouse.dim_customers (
                customer_id, full_name, email,
                city, state, country, age_group,
                effective_date, end_date, is_current
            )
            SELECT
                customer_id,
                first_name || ' ' || last_name,
                email,
                city,
                state,
                country,
                age_group,
                CURRENT_DATE,
                NULL,
                TRUE
            FROM production.customers
        """))

        # ==================================================
        # 4. DIM PRODUCTS
        # ==================================================
        conn.execute(text("TRUNCATE TABLE warehouse.dim_products CASCADE"))
        conn.execute(text("""
            INSERT INTO warehouse.dim_products (
                product_id, product_name, category, sub_category,
                brand, price, cost, price_range,
                effective_date, end_date, is_current
            )
            SELECT
                product_id,
                product_name,
                category,
                sub_category,
                brand,
                price,
                cost,
                CASE
                    WHEN price < 50 THEN 'Budget'
                    WHEN price < 200 THEN 'Mid-range'
                    ELSE 'Premium'
                END,
                CURRENT_DATE,
                NULL,
                TRUE
            FROM production.products
        """))

        # ==================================================
        # 5. FACT SALES
        # ==================================================
        conn.execute(text("""
            INSERT INTO warehouse.fact_sales (
                date_key,
                customer_key,
                product_key,
                payment_method_key,
                transaction_id,
                quantity,
                unit_price,
                discount_amount,
                line_total,
                profit
            )
            SELECT
                TO_CHAR(t.transaction_date, 'YYYYMMDD')::INT,
                dc.customer_key,
                dp.product_key,
                pm.payment_method_key,
                ti.transaction_id,
                ti.quantity,
                ti.unit_price,
                (ti.unit_price * ti.quantity * ti.discount_percentage / 100),
                ti.line_total,
                ti.line_total - (dp.cost * ti.quantity)
            FROM production.transaction_items ti
            JOIN production.transactions t
                ON ti.transaction_id = t.transaction_id
            JOIN warehouse.dim_customers dc
                ON t.customer_id = dc.customer_id
               AND dc.is_current = TRUE
            JOIN warehouse.dim_products dp
                ON ti.product_id = dp.product_id
               AND dp.is_current = TRUE
            JOIN warehouse.dim_payment_method pm
                ON t.payment_method = pm.payment_method_name
        """))

    print("✅ Step 3.3 completed successfully: Warehouse loaded")


if __name__ == "__main__":
    load_warehouse()
