# tests/test_data_generation.py
import os
import pandas as pd
import pytest

def test_files_exist():
    assert os.path.exists("data/raw/customers.csv")
    assert os.path.exists("data/raw/products.csv")
    assert os.path.exists("data/raw/transaction_items.csv")

def test_transaction_items_columns():
    df = pd.read_csv("data/raw/transaction_items.csv")
    expected_cols = ["item_id", "transaction_id", "product_id", "quantity", "unit_price", "discount_percentage", "line_total"]
    for col in expected_cols:
        assert col in df.columns

def test_line_total_calculation():
    df = pd.read_csv("data/raw/transaction_items.csv")
    row = df.iloc[0]
    
    # Fix: Calculate total considering the discount
    discount_factor = (1 - row["discount_percentage"] / 100)
    expected_total = (row["quantity"] * row["unit_price"]) * discount_factor
    
    # Assert with rounding
    assert round(expected_total, 2) == round(row["line_total"], 2)