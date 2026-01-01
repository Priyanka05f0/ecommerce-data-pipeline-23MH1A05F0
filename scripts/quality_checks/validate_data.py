import json
import logging
import os
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2 import OperationalError, ProgrammingError

# -----------------------------
# Database config (LOCAL + CI SAFE)
# -----------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

# -----------------------------
# Paths
# -----------------------------
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
# Quality Checks
# -----------------------------
def run_quality_checks():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Check if production.transactions exists
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'production'
                AND table_name = 'transactions'
            );
        """)

        table_exists = cur.fetchone()[0]

        if not table_exists:
            raise RuntimeError(
                "production.transactions table does not exist. "
                "Run ingestion and transformation steps before quality checks."
            )

        # Run null checks
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

        logging.info("Quality checks completed successfully")
        print("✅ Data quality checks completed successfully")

        cur.close()
        conn.close()

    except OperationalError as e:
        logging.error(f"Database connection error: {e}")
        print("❌ Database connection failed during quality checks")
        raise

    except ProgrammingError as e:
        logging.error(f"SQL error: {e}")
        print("❌ SQL execution failed during quality checks")
        raise

    except Exception as e:
        logging.error(f"Quality check failed: {e}")
        print(f"❌ Quality checks failed: {e}")
        raise


# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    run_quality_checks()
