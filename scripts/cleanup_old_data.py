import os
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

# -----------------------------
# Config
# -----------------------------
RETENTION_DAYS = 7

TARGET_DIRS = [
    Path("data/raw"),
    Path("data/staging"),
    Path("logs")
]

PRESERVE_KEYWORDS = ["summary", "report"]
PRESERVE_FILES = [
    "pipeline_execution_report.json"
]

LOG_FILE = Path("logs/scheduler_activity.log")

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# -----------------------------
# Cleanup Logic
# -----------------------------
def should_preserve(file_path: Path):
    name = file_path.name.lower()

    if file_path.name in PRESERVE_FILES:
        return True

    for keyword in PRESERVE_KEYWORDS:
        if keyword in name:
            return True

    # Preserve today's files
    file_date = datetime.fromtimestamp(file_path.stat().st_mtime).date()
    if file_date == datetime.today().date():
        return True

    return False


def cleanup():
    cutoff_time = time.time() - (RETENTION_DAYS * 86400)

    logging.info("Cleanup job started")

    for directory in TARGET_DIRS:
        if not directory.exists():
            continue

        for file in directory.rglob("*"):
            if not file.is_file():
                continue

            if should_preserve(file):
                continue

            if file.stat().st_mtime < cutoff_time:
                try:
                    file.unlink()
                    logging.info(f"Deleted old file: {file}")
                except Exception as e:
                    logging.error(f"Failed to delete {file}: {e}")

    logging.info("Cleanup job completed")


if __name__ == "__main__":
    cleanup()
