"""
Unit tests for extract_load_kaggle.py

Tests cover:
  - kaggle_dataset()     → download, fallback paths, CSV copy
  - get_snowflake_connection() → env vars passed correctly
  - put_to_stage()       → correct PUT command executed
  - copy_to_bronze()     → correct COPY INTO executed
  - validate_load()      → failed rows logged, row count checked
  - load_to_snowflake()  → orchestration of put → copy → validate
"""

import pytest
import os
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pandas as pd


# ── kaggle_dataset() ─────────────────────────────────────────────────────────

class TestKaggleDataset:

    def test_successful_download_copies_csv(self, temp_raw_dir, temp_download_dir):
        """
        Happy path — kagglehub.dataset_download succeeds.
        CSV files should be copied from download_path to RAW_DIR.
        """
        with patch("extract_load_kaggle.kagglehub.dataset_download", return_value=str(temp_download_dir)), \
             patch("extract_load_kaggle.RAW_DIR", temp_raw_dir), \
             patch("extract_load_kaggle.shutil.copy") as mock_copy:

            from extract_load_kaggle import kaggle_dataset
            kaggle_dataset()

            # shutil.copy should have been called once for data.csv
            assert mock_copy.call_count == 1

    def test_fallback_v1_used_when_archive_missing(self, tmp_path, temp_raw_dir):
        """
        kagglehub raises FileNotFoundError on .archive cleanup.
        fallback_v1 (versions/1) exists → should be used.
        """
        # create fallback_v1 path with a CSV
        fallback_v1 = Path.home() / ".cache/kagglehub/datasets/carrie1/ecommerce-data/versions/1"
        
        with patch("extract_load_kaggle.kagglehub.dataset_download", side_effect=FileNotFoundError("archive gone")), \
             patch("extract_load_kaggle.RAW_DIR", temp_raw_dir), \
             patch("pathlib.Path.exists", side_effect=lambda p=None: str(fallback_v1) in str(p) if p else True), \
             patch("extract_load_kaggle.shutil.copy") as mock_copy, \
             patch.object(Path, "glob", return_value=[temp_raw_dir / "data.csv"]):

            from extract_load_kaggle import kaggle_dataset
            kaggle_dataset()

            assert mock_copy.call_count == 1

    def test_runtime_error_raised_when_both_fallbacks_missing(self, temp_raw_dir):
        """
        Both fallback paths don't exist → RuntimeError should be raised.
        """
        with patch("extract_load_kaggle.kagglehub.dataset_download", side_effect=FileNotFoundError("archive gone")), \
             patch("extract_load_kaggle.RAW_DIR", temp_raw_dir), \
             patch.object(Path, "exists", return_value=False):

            from extract_load_kaggle import kaggle_dataset
            with pytest.raises(RuntimeError, match="kagglehub fallback paths not found"):
                kaggle_dataset()

    def test_runtime_error_raised_when_no_csv_in_download_path(self, tmp_path, temp_raw_dir):
        """
        Download succeeds but no CSV files in download path → RuntimeError.
        """
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        with patch("extract_load_kaggle.kagglehub.dataset_download", return_value=str(empty_dir)), \
             patch("extract_load_kaggle.RAW_DIR", temp_raw_dir):

            from extract_load_kaggle import kaggle_dataset
            with pytest.raises(RuntimeError, match="No CSV files found"):
                kaggle_dataset()

    def test_raw_dir_created_if_not_exists(self, tmp_path, temp_download_dir):
        """
        RAW_DIR should be created if it doesn't exist yet.
        """
        new_raw_dir = tmp_path / "new_raw"
        assert not new_raw_dir.exists()

        with patch("extract_load_kaggle.kagglehub.dataset_download", return_value=str(temp_download_dir)), \
             patch("extract_load_kaggle.RAW_DIR", new_raw_dir), \
             patch("extract_load_kaggle.shutil.copy"):

            from extract_load_kaggle import kaggle_dataset
            kaggle_dataset()

            assert new_raw_dir.exists()


# ── get_snowflake_connection() ───────────────────────────────────────────────

class TestGetSnowflakeConnection:

    def test_connects_with_env_vars(self):
        """
        Snowflake connector should be called with values from environment variables.
        """
        env_vars = {
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_PASSWORD": "test_pass",
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_WAREHOUSE": "test_wh",
            "SNOWFLAKE_DATABASE": "test_db",
            "SNOWFLAKE_SCHEMA": "bronze",
            "SNOWFLAKE_ROLE": "test_role",
        }

        with patch.dict(os.environ, env_vars), \
             patch("extract_load_kaggle.snowflake.connector.connect") as mock_connect:

            mock_connect.return_value = MagicMock()
            from extract_load_kaggle import get_snowflake_connection
            get_snowflake_connection()

            mock_connect.assert_called_once_with(
                user="test_user",
                password="test_pass",
                account="test_account",
                warehouse="test_wh",
                database="test_db",
                schema="bronze",
                role="test_role"
            )


# ── put_to_stage() ───────────────────────────────────────────────────────────

class TestPutToStage:

    def test_put_command_contains_correct_stage(self, mock_pg_cursor):
        """
        PUT command should reference the correct Snowflake stage.
        """
        from extract_load_kaggle import put_to_stage
        csv_path = Path("/opt/data/raw/data.csv")
        put_to_stage(mock_pg_cursor, csv_path)

        call_args = mock_pg_cursor.execute.call_args[0][0]
        assert "@ecommerce.bronze.raw_stage" in call_args

    def test_put_command_contains_auto_compress(self, mock_pg_cursor):
        """
        PUT command should include AUTO_COMPRESS=TRUE.
        """
        from extract_load_kaggle import put_to_stage
        csv_path = Path("/opt/data/raw/data.csv")
        put_to_stage(mock_pg_cursor, csv_path)

        call_args = mock_pg_cursor.execute.call_args[0][0]
        assert "AUTO_COMPRESS=TRUE" in call_args


# ── copy_to_bronze() ─────────────────────────────────────────────────────────

class TestCopyToBronze:

    def test_copy_into_correct_table(self, mock_pg_cursor):
        """
        COPY INTO should target ecommerce.bronze.raw_ecommerce.
        """
        from extract_load_kaggle import copy_to_bronze
        copy_to_bronze(mock_pg_cursor)

        call_args = mock_pg_cursor.execute.call_args[0][0]
        assert "ecommerce.bronze.raw_ecommerce" in call_args

    def test_copy_uses_correct_file_format(self, mock_pg_cursor):
        """
        COPY INTO should use ecommerce.bronze.csv_format.
        """
        from extract_load_kaggle import copy_to_bronze
        copy_to_bronze(mock_pg_cursor)

        call_args = mock_pg_cursor.execute.call_args[0][0]
        assert "ecommerce.bronze.csv_format" in call_args

    def test_copy_uses_on_error_continue(self, mock_pg_cursor):
        """
        COPY INTO should use ON_ERROR = 'CONTINUE' to not fail on bad rows.
        """
        from extract_load_kaggle import copy_to_bronze
        copy_to_bronze(mock_pg_cursor)

        call_args = mock_pg_cursor.execute.call_args[0][0]
        assert "ON_ERROR" in call_args


# ── validate_load() ──────────────────────────────────────────────────────────

class TestValidateLoad:

    def test_logs_row_count(self, mock_pg_cursor):
        """
        validate_load should query row count from bronze table.
        """
        mock_pg_cursor.fetchall.return_value = []       # no failed rows
        mock_pg_cursor.fetchone.return_value = (5000,)  # 5000 rows loaded

        from extract_load_kaggle import validate_load
        validate_load(mock_pg_cursor)

        # two execute calls: VALIDATE + COUNT(*)
        assert mock_pg_cursor.execute.call_count == 2

    def test_no_failed_rows_logs_success(self, mock_pg_cursor, caplog):
        """
        When no failed rows, should log success message.
        """
        mock_pg_cursor.fetchall.return_value = []
        mock_pg_cursor.fetchone.return_value = (5000,)

        import logging
        from extract_load_kaggle import validate_load
        with caplog.at_level(logging.INFO):
            validate_load(mock_pg_cursor)

        assert "All rows loaded successfully" in caplog.text


# ── load_to_snowflake() ──────────────────────────────────────────────────────

class TestLoadToSnowflake:

    def test_orchestration_order(self):
        """
        load_to_snowflake should call put → copy → validate in order.
        """
        call_order = []

        with patch("extract_load_kaggle.get_snowflake_connection") as mock_conn, \
             patch("extract_load_kaggle.put_to_stage", side_effect=lambda *a: call_order.append("put")), \
             patch("extract_load_kaggle.copy_to_bronze", side_effect=lambda *a: call_order.append("copy")), \
             patch("extract_load_kaggle.validate_load", side_effect=lambda *a: call_order.append("validate")):

            mock_conn.return_value.cursor.return_value = MagicMock()

            from extract_load_kaggle import load_to_snowflake
            load_to_snowflake(Path("/opt/data/raw/data.csv"))

            assert call_order == ["put", "copy", "validate"]

    def test_connection_closed_on_success(self):
        """
        Snowflake connection should be closed after successful load.
        """
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MagicMock()

        with patch("extract_load_kaggle.get_snowflake_connection", return_value=mock_conn), \
             patch("extract_load_kaggle.put_to_stage"), \
             patch("extract_load_kaggle.copy_to_bronze"), \
             patch("extract_load_kaggle.validate_load"):

            from extract_load_kaggle import load_to_snowflake
            load_to_snowflake(Path("/opt/data/raw/data.csv"))

            mock_conn.close.assert_called_once()

    def test_connection_closed_on_failure(self):
        """
        Snowflake connection should be closed even if an error occurs.
        """
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MagicMock()

        with patch("extract_load_kaggle.get_snowflake_connection", return_value=mock_conn), \
             patch("extract_load_kaggle.put_to_stage", side_effect=Exception("PUT failed")):

            from extract_load_kaggle import load_to_snowflake
            with pytest.raises(Exception, match="PUT failed"):
                load_to_snowflake(Path("/opt/data/raw/data.csv"))

            mock_conn.close.assert_called_once()