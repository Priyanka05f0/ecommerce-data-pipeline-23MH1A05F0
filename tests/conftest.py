import sys
from pathlib import Path

# Add project root to PYTHONPATH so `scripts` can be imported
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))
