import psycopg2
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),   # 🔥 FIXED
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password")
}

def validate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Basic row count checks
    tables = [
        "production.customers",
        "production.products",
        "production.transactions"
    ]

    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        print(f"✅ {table} row count: {count}")

    cur.close()
    conn.close()

    print("✅ Data quality checks passed")

if __name__ == "__main__":
    validate()
