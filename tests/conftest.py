# tests/conftest.py
import pytest
import os
import psycopg2

# Get DB_HOST from environment, default to 'postgres' (Docker friendly)
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_CONFIG = {
    "host": DB_HOST,
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password")
}

@pytest.fixture
def db_connection():
    """Provides a database connection for tests."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
        conn.close()
    except psycopg2.OperationalError as e:
        pytest.fail(f"Could not connect to database at {DB_HOST}: {e}")