"""
Shared fixtures for all tests.
Provides reusable mock objects and sample DataFrames.
"""
import sys
import os

# add elt/ to path so tests can import extract_load_kaggle and silver_to_postgres
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'elt'))
# add project root to path so config.paths can be found
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch


# ── Sample DataFrame that mirrors what silver_ecommerce returns ──────────────
@pytest.fixture
def sample_df():
    """
    Minimal Silver DataFrame with all columns used across load functions.
    Mirrors the shape of ecommerce.silver.silver_ecommerce.
    """
    return pd.DataFrame({
        'invoice_no':   ['INV001', 'INV001', 'INV002', 'INV003'],
        'stock_code':   ['SKU001', 'SKU002', 'SKU001', 'SKU003'],
        'description':  ['Product A', 'Product B', 'Product A', 'Product C'],
        'quantity':     [2, 3, 1, 4],
        'invoice_date': pd.to_datetime(['2011-01-01', '2011-01-01', '2011-01-02', '2011-01-03']),
        'unit_price':   [10.0, 20.0, 10.0, 30.0],
        'customer_id':  [12345.0, 12345.0, 67890.0, None],   # None = anonymous transaction
        'country':      ['United Kingdom', 'United Kingdom', 'Germany', None],
        'total_price':  [20.0, 60.0, 10.0, 120.0],
    })


# ── Mock Postgres cursor ─────────────────────────────────────────────────────
@pytest.fixture
def mock_pg_cursor():
    """Mock psycopg2 cursor — tracks executemany calls."""
    cursor = MagicMock()
    cursor.fetchone.return_value = (100,)   # default row count response
    return cursor


# ── Mock Postgres connection ─────────────────────────────────────────────────
@pytest.fixture
def mock_pg_conn(mock_pg_cursor):
    """Mock psycopg2 connection — returns mock cursor."""
    conn = MagicMock()
    conn.cursor.return_value = mock_pg_cursor
    return conn


# ── Mock Snowflake connection ────────────────────────────────────────────────
@pytest.fixture
def mock_sf_conn():
    """Mock Snowflake connection."""
    return MagicMock()


# ── Temp RAW_DIR using pytest's tmp_path ─────────────────────────────────────
@pytest.fixture
def temp_raw_dir(tmp_path):
    """
    Creates a temporary RAW_DIR with a sample CSV file inside.
    Used to test file copy operations without touching the real filesystem.
    """
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    # create a dummy CSV file to simulate Kaggle download
    (raw_dir / "data.csv").write_text("InvoiceNo,StockCode\nINV001,SKU001\n")
    return raw_dir


# ── Temp download path simulating kagglehub cache ────────────────────────────
@pytest.fixture
def temp_download_dir(tmp_path):
    """
    Creates a temporary directory simulating kagglehub's cache path.
    Contains a sample CSV to simulate extracted dataset.
    """
    download_dir = tmp_path / "kagglehub" / "versions" / "1"
    download_dir.mkdir(parents=True)
    (download_dir / "data.csv").write_text("InvoiceNo,StockCode\nINV001,SKU001\n")
    return download_dir