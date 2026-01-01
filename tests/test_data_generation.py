import pandas as pd
import numpy as np
import os

DATA_DIR = "data/raw"

def test_files_exist():
    assert os.path.exists(f"{DATA_DIR}/customers.csv")
    assert os.path.exists(f"{DATA_DIR}/products.csv")
    assert os.path.exists(f"{DATA_DIR}/transactions.csv")
    assert os.path.exists(f"{DATA_DIR}/transaction_items.csv")

def test_transaction_items_columns():
    items = pd.read_csv(f"{DATA_DIR}/transaction_items.csv")
    required_cols = {
        "item_id",
        "transaction_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_percentage",
        "line_total",
    }
    assert required_cols.issubset(items.columns)

def test_line_total_calculation():
    items = pd.read_csv(f"{DATA_DIR}/transaction_items.csv")

    calculated = (
        items["quantity"]
        * items["unit_price"]
        * (1 - items["discount_percentage"] / 100)
    )

    # Floating point safe comparison
    assert np.isclose(
        items["line_total"],
        calculated,
        rtol=1e-02
    ).all()
