import json
import subprocess

def test_quality_report_exists():
    subprocess.run(["python", "scripts/quality_checks/validate_data.py"], check=True)

def test_quality_score_present():
    with open("data/processed/data_quality_report.json") as f:
        data = json.load(f)
    assert "quality_score" in data
