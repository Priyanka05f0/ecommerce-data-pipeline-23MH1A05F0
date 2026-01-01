import json
import logging
from pathlib import Path
from datetime import datetime
import psycopg2

# -----------------------------
# Config
# -----------------------------
DB_CONFIG = {
    "host": "postgres",
    "database": "ecommerce_db",
    "user": "admin",
    "password": "password",
    "port": 5432
}

REPORT_PATH = Path("data/processed/quality_checks_report.json")
LOG_PATH = Path("logs/quality_checks.log")

REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -----------------------------
# Quality checks
# -----------------------------
def run_quality_checks():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Check nulls
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE customer_id IS NULL),
            COUNT(*) FILTER (WHERE total_amount IS NULL)
        FROM production.transactions;
    """)
    null_customers, null_amounts = cur.fetchone()

    total_issues = null_customers + null_amounts
    quality_score = max(0, 100 - total_issues)

    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "quality_score": quality_score,
        "null_customer_ids": null_customers,
        "null_total_amounts": null_amounts,
        "status": "ok" if quality_score >= 95 else "degraded"
    }

    with open(REPORT_PATH, "w") as f:
        json.dump(report, f, indent=4)

    logging.info("Quality checks completed")
    print("✅ Data quality checks completed successfully")

    cur.close()
    conn.close()

# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    run_quality_checks()
