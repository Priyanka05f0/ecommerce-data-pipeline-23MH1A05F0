import subprocess
import time
import json
import logging
from datetime import datetime
from pathlib import Path
import os

# -----------------------------
# Paths
# -----------------------------
LOG_DIR = Path("logs")
REPORT_PATH = Path("data/processed/pipeline_execution_report.json")

LOG_DIR.mkdir(exist_ok=True)
REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

MAIN_LOG = LOG_DIR / f"pipeline_orchestrator_{timestamp}.log"
ERROR_LOG = LOG_DIR / "pipeline_errors.log"

# -----------------------------
# Logging configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(MAIN_LOG),
        logging.StreamHandler()
    ]
)

error_logger = logging.getLogger("error_logger")
error_handler = logging.FileHandler(ERROR_LOG)
error_handler.setLevel(logging.ERROR)
error_logger.addHandler(error_handler)

# -----------------------------
# ✅ ENSURE PYTHONPATH FOR DOCKER + PYTEST
# -----------------------------
env = os.environ.copy()
env["PYTHONPATH"] = "/app"

# -----------------------------
# ✅ FINAL CORRECT PIPELINE ORDER
# -----------------------------
PIPELINE_STEPS = [
    ("Data Generation", ["python", "scripts/data_generation/generate_data.py"]),
    ("Data Ingestion", ["python", "scripts/ingestion/ingest_to_staging.py"]),
    ("Staging to Production", ["python", "scripts/transformation/staging_to_production.py"]),
    ("Warehouse Load", ["python", "scripts/transformation/load_warehouse.py"]),
    ("Data Quality Checks", ["python", "scripts/quality_checks/validate_data.py"]),
    ("Analytics Generation", ["python", "scripts/transformation/generate_analytics.py"]),
]

# -----------------------------
# Helper: execute step with retry
# -----------------------------
def run_step(step_name, command, max_retries=3):
    attempts = 0
    start_time = time.time()

    while attempts < max_retries:
        try:
            logging.info(f"Starting step: {step_name}")
            subprocess.run(command, check=True, env=env)

            duration = round(time.time() - start_time, 2)
            logging.info(f"Completed step: {step_name} in {duration}s")

            return {
                "status": "success",
                "duration_seconds": duration,
                "retry_attempts": attempts
            }

        except subprocess.CalledProcessError as e:
            attempts += 1
            error_logger.error(
                f"{step_name} failed (attempt {attempts})",
                exc_info=True
            )

            if attempts >= max_retries:
                return {
                    "status": "failed",
                    "duration_seconds": round(time.time() - start_time, 2),
                    "retry_attempts": attempts,
                    "error_message": str(e)
                }

            time.sleep(2 ** (attempts - 1))

# -----------------------------
# Main execution
# -----------------------------
def main():
    execution_id = f"PIPE_{timestamp}"
    start_time = datetime.now().isoformat()

    report = {
        "pipeline_execution_id": execution_id,
        "start_time": start_time,
        "status": "success",
        "steps_executed": {}
    }

    for step_name, command in PIPELINE_STEPS:
        result = run_step(step_name, command)
        report["steps_executed"][step_name] = result

        if result["status"] == "failed":
            report["status"] = "failed"
            break

    report["end_time"] = datetime.now().isoformat()
    report["total_duration_seconds"] = round(
        (datetime.fromisoformat(report["end_time"]) -
         datetime.fromisoformat(start_time)).total_seconds(), 2
    )

    with open(REPORT_PATH, "w") as f:
        json.dump(report, f, indent=4)

    logging.info("Pipeline execution finished")
    logging.info(f"Execution report saved to {REPORT_PATH}")

if __name__ == "__main__":
    main()
