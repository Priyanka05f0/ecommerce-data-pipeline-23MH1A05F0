# tests/test_transformation.py
import pytest
import psycopg2
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": "5432",
    "dbname": "ecommerce_db",
    "user": "admin",
    "password": "password"
}

def test_production_tables_populated():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    tables = ["customers", "products", "transactions"]
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM production.{table}")
        assert cur.fetchone()[0] > 0
        
    cur.close()
    conn.close()

def test_no_orphan_transaction_items():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Check that all transactions have a valid email (meaning the join worked)
    cur.execute("""
        SELECT COUNT(*) 
        FROM production.transactions 
        WHERE customer_email IS NULL
    """)
    assert cur.fetchone()[0] == 0
    
    cur.close()
    conn.close()