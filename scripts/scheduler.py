import schedule
import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path
import os

# -----------------------------
# Paths
# -----------------------------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

SCHEDULER_LOG = LOG_DIR / "scheduler_activity.log"
LOCK_FILE = LOG_DIR / "pipeline.lock"

PIPELINE_COMMAND = [
    "python",
    "scripts/pipeline_orchestrator.py"
]

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(SCHEDULER_LOG),
        logging.StreamHandler()
    ]
)

# -----------------------------
# Concurrency Guard
# -----------------------------
def is_pipeline_running():
    return LOCK_FILE.exists()

def create_lock():
    LOCK_FILE.write_text(str(os.getpid()))

def remove_lock():
    if LOCK_FILE.exists():
        LOCK_FILE.unlink()

# -----------------------------
# Job Function
# -----------------------------
def run_pipeline_job():
    if is_pipeline_running():
        logging.warning("Pipeline already running. Skipping this schedule.")
        return

    logging.info("Scheduled pipeline execution started")
    create_lock()

    try:
        result = subprocess.run(
            PIPELINE_COMMAND,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logging.info("Pipeline execution SUCCESS")
        else:
            logging.error("Pipeline execution FAILED")
            logging.error(result.stderr)

    except Exception as e:
        logging.exception("Scheduler error occurred")

    finally:
        remove_lock()
        logging.info("Scheduled pipeline execution finished")

# -----------------------------
# Schedule Configuration
# -----------------------------
# Daily at 02:00 AM (change time if needed)
schedule.every().day.at("02:00").do(run_pipeline_job)

logging.info("Scheduler started. Waiting for scheduled execution...")

# -----------------------------
# Loop
# -----------------------------
while True:
    schedule.run_pending()
    time.sleep(30)
