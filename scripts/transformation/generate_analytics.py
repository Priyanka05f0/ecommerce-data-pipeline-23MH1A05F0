import sys
from pathlib import Path

# Ensure project root is on PYTHONPATH
ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_DIR))

import psycopg2
import pandas as pd
from scripts.db import get_db_config


def main():
    conn = psycopg2.connect(**get_db_config())
    df = pd.read_sql("SELECT * FROM warehouse.fact_sales", conn)
    print(df.head())
    conn.close()


if __name__ == "__main__":
    main()
