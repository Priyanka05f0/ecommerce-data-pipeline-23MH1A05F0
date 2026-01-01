import os
import subprocess
import json

REPORT = "data/processed/quality_checks_report.json"

def test_quality_report_exists():
    subprocess.run(
        ["python", "scripts/quality_checks/validate_data.py"],
        check=True
    )
    assert os.path.exists(REPORT)

def test_quality_score_present():
    with open(REPORT) as f:
        report = json.load(f)

    assert "quality_score" in report
