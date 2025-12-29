from sqlalchemy import create_engine, text

# -------------------------------------------------
# Database connection
# -------------------------------------------------
DB_URL = "postgresql://admin:password@postgres:5432/ecommerce_db"


def run_staging_to_production():
    engine = create_engine(DB_URL)

    with engine.begin() as conn:

        # ==================================================
        # 1. LOAD DIMENSION TABLES
        # ==================================================

        # -------------------------
        # Customers
        # -------------------------
        conn.execute(text("TRUNCATE TABLE production.customers CASCADE"))

        conn.execute(text("""
            INSERT INTO production.customers (
                customer_id,
                first_name,
                last_name,
                email,
                phone,
                registration_date,
                city,
                state,
                country,
                age_group
            )
            SELECT
                customer_id,
                INITCAP(first_name),
                INITCAP(last_name),
                LOWER(email),
                phone,
                registration_date,
                city,
                state,
                country,
                age_group
            FROM staging.customers
            WHERE email IS NOT NULL;
        """))

        # -------------------------
        # Products  (NULL-SAFE)
        # -------------------------
        conn.execute(text("TRUNCATE TABLE production.products CASCADE"))

        conn.execute(text("""
            INSERT INTO production.products (
                product_id,
                product_name,
                category,
                sub_category,
                price,
                cost,
                brand,
                stock_quantity,
                supplier_id
            )
            SELECT
                product_id,
                product_name,
                category,
                sub_category,
                ROUND(price, 2),
                ROUND(cost, 2),
                brand,
                stock_quantity,
                supplier_id
            FROM staging.products
            WHERE product_name IS NOT NULL
              AND price >= 0
              AND cost >= 0
              AND stock_quantity >= 0;
        """))

        # ==================================================
        # 2. LOAD FACT TABLES
        # ==================================================

        # -------------------------
        # Transactions
        # -------------------------
        conn.execute(text("""
            INSERT INTO production.transactions (
                transaction_id,
                customer_id,
                transaction_date,
                transaction_time,
                payment_method,
                shipping_address,
                total_amount
            )
            SELECT
                t.transaction_id,
                t.customer_id,
                t.transaction_date,
                t.transaction_time,
                t.payment_method,
                t.shipping_address,
                t.total_amount
            FROM staging.transactions t
            LEFT JOIN production.transactions p
                ON t.transaction_id = p.transaction_id
            WHERE p.transaction_id IS NULL
              AND t.total_amount > 0;
        """))

        # -------------------------
        # Transaction Items (FK SAFE)
        # -------------------------
        conn.execute(text("""
            INSERT INTO production.transaction_items (
                item_id,
                transaction_id,
                product_id,
                quantity,
                unit_price,
                discount_percentage,
                line_total
            )
            SELECT
                i.item_id,
                i.transaction_id,
                i.product_id,
                i.quantity,
                i.unit_price,
                i.discount_percentage,
                i.line_total
            FROM staging.transaction_items i
            JOIN production.products p
                ON i.product_id = p.product_id      -- FK SAFETY
            LEFT JOIN production.transaction_items t
                ON i.item_id = t.item_id
            WHERE t.item_id IS NULL
              AND i.quantity > 0
              AND i.unit_price >= 0
              AND i.line_total >= 0;
        """))

    print("✅ Step 3.2 completed successfully: Staging → Production ETL")


if __name__ == "__main__":
    run_staging_to_production()
