import json
import datetime
from sqlalchemy import create_engine, text
import os

DB_URL = "postgresql://admin:password@postgres:5432/ecommerce_db"

SQL_FILE = "/app/sql/queries/data_quality_checks.sql"
OUTPUT_FILE = "/app/logs/data_quality_report.json"


def run_quality_checks():
    engine = create_engine(DB_URL)
    checks = {}

    with engine.connect() as conn:
        with open(SQL_FILE, "r") as f:
            queries = f.read().split(";")

        for idx, q in enumerate(queries, start=1):
            q = q.strip()
            if not q:
                continue

            try:
                result = conn.execute(text(q))

                # ✅ FINAL FIX: Convert RowMapping → dict
                rows = [dict(row) for row in result.mappings().all()]

                checks[f"check_{idx}"] = {
                    "status": "passed" if len(rows) == 0 else "failed",
                    "violations": len(rows),
                    "details": rows
                }

            except Exception as e:
                checks[f"check_{idx}"] = {
                    "status": "error",
                    "error_message": str(e)
                }

    report = {
        "check_timestamp": datetime.datetime.utcnow().isoformat(),
        "checks_performed": checks
    }

    os.makedirs("/app/logs", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(report, f, indent=2)

    print("✅ Data quality checks completed successfully")


if __name__ == "__main__":
    run_quality_checks()
