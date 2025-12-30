import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import psycopg2
import statistics

# =============================
# Configuration
# =============================
DB_CONFIG = {
    "host": "postgres",          # Docker service name
    "database": "ecommerce_db",
    "user": "admin",
    "password": "password",
    "port": 5432
}

EXECUTION_REPORT = Path("data/processed/pipeline_execution_report.json")
MONITORING_REPORT = Path("data/processed/monitoring_report.json")
LOG_FILE = Path("logs/monitoring.log")

MONITORING_REPORT.parent.mkdir(parents=True, exist_ok=True)
LOG_FILE.parent.mkdir(exist_ok=True)

# =============================
# Logging
# =============================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# =============================
# Helpers
# =============================
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def load_pipeline_execution():
    if not EXECUTION_REPORT.exists():
        return None
    with open(EXECUTION_REPORT) as f:
        return json.load(f)

# =============================
# Monitoring Checks
# =============================
def check_last_execution(execution_report):
    if not execution_report:
        return {"status": "critical", "message": "No execution report found"}

    last_run = execution_report.get("end_time")
    last_run_dt = datetime.fromisoformat(last_run)

    if last_run_dt.tzinfo is None:
        last_run_dt = last_run_dt.replace(tzinfo=timezone.utc)

    hours_since = (datetime.now(timezone.utc) - last_run_dt).total_seconds() / 3600

    return {
        "status": "ok" if hours_since <= 24 else "critical",
        "last_run": last_run,
        "hours_since_last_run": round(hours_since, 2),
        "threshold_hours": 24
    }

def check_data_freshness(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(created_at)
        FROM production.transactions;
    """)
    latest = cur.fetchone()[0]

    if latest is None:
        return {"status": "critical", "message": "No production data found"}

    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)

    lag_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

    return {
        "status": "ok" if lag_hours <= 24 else "warning",
        "latest_record_time": latest.isoformat(),
        "lag_hours": round(lag_hours, 2)
    }

def check_volume_anomalies(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT transaction_date, COUNT(*)
        FROM production.transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY transaction_date
        ORDER BY transaction_date;
    """)
    rows = cur.fetchall()

    if len(rows) < 2:
        return {"status": "ok", "message": "Not enough data for anomaly detection"}

    counts = [r[1] for r in rows]
    mean = statistics.mean(counts)
    std = statistics.stdev(counts)

    today_count = counts[-1]
    anomaly = today_count > mean + 3*std or today_count < mean - 3*std

    return {
        "status": "anomaly_detected" if anomaly else "ok",
        "expected_range": f"{int(mean - 3*std)} - {int(mean + 3*std)}",
        "actual_count": today_count,
        "anomaly": anomaly
    }

def check_data_quality(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE customer_id IS NULL),
            COUNT(*) FILTER (WHERE total_amount <= 0)
        FROM production.transactions;
    """)
    null_customers, invalid_amounts = cur.fetchone()

    violations = null_customers + invalid_amounts
    score = max(0, 100 - violations)

    return {
        "status": "ok" if score >= 95 else "degraded",
        "quality_score": score,
        "null_customer_ids": null_customers,
        "invalid_amounts": invalid_amounts,
        "total_violations": violations
    }

def check_database_health(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    return {
        "status": "ok",
        "connectivity": "successful"
    }

# =============================
# Main
# =============================
def main():
    logging.info("Monitoring started")

    execution_report = load_pipeline_execution()
    conn = connect_db()

    report = {
        "monitoring_timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline_health": "healthy",
        "checks": {}
    }

    report["checks"]["last_execution"] = check_last_execution(execution_report)
    report["checks"]["data_freshness"] = check_data_freshness(conn)
    report["checks"]["volume_anomalies"] = check_volume_anomalies(conn)
    report["checks"]["data_quality"] = check_data_quality(conn)
    report["checks"]["database_health"] = check_database_health(conn)

    if any(c["status"] in ["critical", "anomaly_detected"]
           for c in report["checks"].values()):
        report["pipeline_health"] = "critical"
    elif any(c["status"] == "warning" for c in report["checks"].values()):
        report["pipeline_health"] = "degraded"

    with open(MONITORING_REPORT, "w") as f:
        json.dump(report, f, indent=4)

    logging.info("Monitoring completed successfully")
    print("✅ Step 5.3 completed: Pipeline monitoring successful")

if __name__ == "__main__":
    main()
