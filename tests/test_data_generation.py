import pandas as pd
import os

def test_files_exist():
    assert os.path.exists("data/raw/customers.csv")
    assert os.path.exists("data/raw/products.csv")
    assert os.path.exists("data/raw/transactions.csv")
    assert os.path.exists("data/raw/transaction_items.csv")

def test_transaction_items_columns():
    df = pd.read_csv("data/raw/transaction_items.csv")
    assert "quantity" in df.columns
    assert "unit_price" in df.columns
    assert "line_total" in df.columns

def test_line_total_calculation():
    df = pd.read_csv("data/raw/transaction_items.csv")
    row = df.iloc[0]
    assert round(row["quantity"] * row["unit_price"], 2) == round(row["line_total"], 2)
