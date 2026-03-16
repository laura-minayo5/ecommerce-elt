"""
Unit tests for silver_to_postgres.py

Tests cover:
  - get_snowflake_connection() → env vars passed correctly
  - get_postgres_connection()  → env vars passed correctly
  - fetch_silver_data()        → columns lowercased, correct query
  - load_customers()           → deduplication, null filtering, FK return
  - load_products()            → deduplication, null filtering, FK return
  - load_orders()              → FK guard against customers, deduplication
  - load_order_items()         → FK guard against orders + products
  - verify_counts()            → queries all 4 tables
  - load_silver_to_postgres()  → orchestration, commit, rollback on error
"""

import pytest
import os
import pandas as pd
from unittest.mock import MagicMock, patch, call


# ── get_snowflake_connection() ───────────────────────────────────────────────

class TestGetSnowflakeConnection:

    def test_connects_with_correct_schema(self):
        """
        silver_to_postgres should always connect to SILVER schema,
        not the default SNOWFLAKE_SCHEMA env var.
        """
        env_vars = {
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_PASSWORD": "test_pass",
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_WAREHOUSE": "test_wh",
            "SNOWFLAKE_DATABASE": "test_db",
            "SNOWFLAKE_ROLE": "test_role",
        }

        with patch.dict(os.environ, env_vars), \
             patch("silver_to_postgres.snowflake.connector.connect") as mock_connect:

            mock_connect.return_value = MagicMock()
            from silver_to_postgres import get_snowflake_connection
            get_snowflake_connection()

            # schema must be hardcoded SILVER, not from env
            call_kwargs = mock_connect.call_args[1]
            assert call_kwargs["schema"] == "SILVER"


# ── get_postgres_connection() ────────────────────────────────────────────────

class TestGetPostgresConnection:

    def test_connects_with_env_vars(self):
        """
        Postgres connection should use all ecommerce env vars.
        """
        env_vars = {
            "POSTGRES_ECOMMERCE_HOST": "ecommerce-postgres",
            "POSTGRES_ECOMMERCE_PORT": "5432",
            "POSTGRES_ECOMMERCE_DB": "ecommerce",
            "POSTGRES_ECOMMERCE_USER": "ecommerce_user",
            "POSTGRES_ECOMMERCE_PASSWORD": "ecommerce3NF",
        }

        with patch.dict(os.environ, env_vars), \
             patch("silver_to_postgres.psycopg2.connect") as mock_connect:

            mock_connect.return_value = MagicMock()
            from silver_to_postgres import get_postgres_connection
            get_postgres_connection()

            mock_connect.assert_called_once_with(
                host="ecommerce-postgres",
                port="5432",
                dbname="ecommerce",
                user="ecommerce_user",
                password="ecommerce3NF"
            )


# ── fetch_silver_data() ──────────────────────────────────────────────────────

class TestFetchSilverData:

    def test_columns_are_lowercased(self, mock_sf_conn, sample_df):
        """
        Snowflake returns uppercase columns — fetch_silver_data should
        lowercase all column names.
        """
        # simulate Snowflake returning uppercase columns
        uppercase_df = sample_df.copy()
        uppercase_df.columns = uppercase_df.columns.str.upper()

        with patch("silver_to_postgres.pd.read_sql", return_value=uppercase_df):
            from silver_to_postgres import fetch_silver_data
            result = fetch_silver_data(mock_sf_conn)

            # all columns should be lowercase
            assert all(col == col.lower() for col in result.columns)

    def test_returns_dataframe(self, mock_sf_conn, sample_df):
        """
        fetch_silver_data should return a pandas DataFrame.
        """
        with patch("silver_to_postgres.pd.read_sql", return_value=sample_df):
            from silver_to_postgres import fetch_silver_data
            result = fetch_silver_data(mock_sf_conn)

            assert isinstance(result, pd.DataFrame)

    def test_queries_correct_view(self, mock_sf_conn, sample_df):
        """
        Should query ecommerce.silver.silver_ecommerce view.
        """
        with patch("silver_to_postgres.pd.read_sql", return_value=sample_df) as mock_sql:
            from silver_to_postgres import fetch_silver_data
            fetch_silver_data(mock_sf_conn)

            query = mock_sql.call_args[0][0]
            assert "silver.silver_ecommerce" in query.lower()


# ── load_customers() ─────────────────────────────────────────────────────────

class TestLoadCustomers:

    def test_returns_dataframe(self, mock_pg_cursor, sample_df):
        """load_customers should return a DataFrame for FK filtering."""
        from silver_to_postgres import load_customers
        result = load_customers(mock_pg_cursor, sample_df)
        assert isinstance(result, pd.DataFrame)

    def test_null_customer_ids_excluded(self, mock_pg_cursor, sample_df):
        """
        Rows with null customer_id should be dropped —
        customer_id is NOT NULL in Postgres.
        """
        from silver_to_postgres import load_customers
        result = load_customers(mock_pg_cursor, sample_df)
        assert result['customer_id'].isna().sum() == 0

    def test_null_country_excluded(self, mock_pg_cursor, sample_df):
        """
        Rows with null country should be dropped —
        country is NOT NULL in Postgres.
        """
        from silver_to_postgres import load_customers
        result = load_customers(mock_pg_cursor, sample_df)
        assert result['country'].isna().sum() == 0

    def test_duplicate_customer_ids_removed(self, mock_pg_cursor, sample_df):
        """
        Each customer_id should appear only once — it's a PRIMARY KEY.
        """
        from silver_to_postgres import load_customers
        result = load_customers(mock_pg_cursor, sample_df)
        assert result['customer_id'].duplicated().sum() == 0

    def test_executemany_called(self, mock_pg_cursor, sample_df):
        """
        executemany should be called to insert customers.
        """
        from silver_to_postgres import load_customers
        load_customers(mock_pg_cursor, sample_df)
        assert mock_pg_cursor.executemany.called

    def test_insert_uses_on_conflict(self, mock_pg_cursor, sample_df):
        """
        INSERT should use ON CONFLICT DO NOTHING for idempotent reruns.
        """
        from silver_to_postgres import load_customers
        load_customers(mock_pg_cursor, sample_df)
        sql = mock_pg_cursor.executemany.call_args[0][0]
        assert "ON CONFLICT" in sql.upper()


# ── load_products() ──────────────────────────────────────────────────────────

class TestLoadProducts:

    def test_returns_dataframe(self, mock_pg_cursor, sample_df):
        """load_products should return a DataFrame for FK filtering."""
        from silver_to_postgres import load_products
        result = load_products(mock_pg_cursor, sample_df)
        assert isinstance(result, pd.DataFrame)

    def test_null_stock_codes_excluded(self, mock_pg_cursor, sample_df):
        """
        Rows with null stock_code should be dropped —
        stock_code is PRIMARY KEY.
        """
        from silver_to_postgres import load_products
        result = load_products(mock_pg_cursor, sample_df)
        assert result['stock_code'].isna().sum() == 0

    def test_duplicate_stock_codes_removed(self, mock_pg_cursor, sample_df):
        """
        Each stock_code should appear only once — it's a PRIMARY KEY.
        """
        from silver_to_postgres import load_products
        result = load_products(mock_pg_cursor, sample_df)
        assert result['stock_code'].duplicated().sum() == 0

    def test_description_can_be_null(self, mock_pg_cursor, sample_df):
        """
        description is nullable — nulls should not be dropped.
        """
        sample_df.loc[0, 'description'] = None
        from silver_to_postgres import load_products
        result = load_products(mock_pg_cursor, sample_df)
        # should not raise, description nulls are allowed
        assert isinstance(result, pd.DataFrame)


# ── load_orders() ────────────────────────────────────────────────────────────

class TestLoadOrders:

    def test_returns_dataframe(self, mock_pg_cursor, sample_df):
        """load_orders should return a DataFrame for FK filtering."""
        customers = pd.DataFrame({'customer_id': [12345.0, 67890.0]})
        from silver_to_postgres import load_orders
        result = load_orders(mock_pg_cursor, sample_df, customers)
        assert isinstance(result, pd.DataFrame)

    def test_fk_guard_filters_unknown_customers(self, mock_pg_cursor, sample_df):
        """
        Orders with customer_id not in customers table should be excluded.
        This is the FK guard — prevents FK constraint violations.
        """
        # only customer 12345 exists in customers
        customers = pd.DataFrame({'customer_id': [12345.0]})
        from silver_to_postgres import load_orders
        result = load_orders(mock_pg_cursor, sample_df, customers)

        # order INV002 has customer 67890 — should be excluded
        assert 'INV002' not in result['invoice_no'].values

    def test_null_invoice_no_excluded(self, mock_pg_cursor, sample_df):
        """
        Rows with null invoice_no should be dropped — it's NOT NULL + PK.
        """
        sample_df.loc[0, 'invoice_no'] = None
        customers = pd.DataFrame({'customer_id': [12345.0, 67890.0]})
        from silver_to_postgres import load_orders
        result = load_orders(mock_pg_cursor, sample_df, customers)
        assert result['invoice_no'].isna().sum() == 0

    def test_duplicate_invoice_nos_removed(self, mock_pg_cursor, sample_df):
        """
        Each invoice_no should appear only once — it's a PRIMARY KEY.
        """
        customers = pd.DataFrame({'customer_id': [12345.0, 67890.0]})
        from silver_to_postgres import load_orders
        result = load_orders(mock_pg_cursor, sample_df, customers)
        assert result['invoice_no'].duplicated().sum() == 0


# ── load_order_items() ───────────────────────────────────────────────────────

class TestLoadOrderItems:

    def test_fk_guard_filters_unknown_invoices(self, mock_pg_cursor, sample_df):
        """
        Items whose invoice_no is not in loaded orders should be excluded.
        """
        # only INV001 was loaded into orders
        orders = pd.DataFrame({'invoice_no': ['INV001']})
        products = pd.DataFrame({'stock_code': ['SKU001', 'SKU002', 'SKU003']})

        from silver_to_postgres import load_order_items
        load_order_items(mock_pg_cursor, sample_df, orders, products)

        rows_inserted = mock_pg_cursor.executemany.call_args[0][1]
        invoice_nos = [row[0] for row in rows_inserted]
        assert all(inv == 'INV001' for inv in invoice_nos)

    def test_fk_guard_filters_unknown_products(self, mock_pg_cursor, sample_df):
        """
        Items whose stock_code is not in loaded products should be excluded.
        """
        orders = pd.DataFrame({'invoice_no': ['INV001', 'INV002', 'INV003']})
        # only SKU001 exists in products
        products = pd.DataFrame({'stock_code': ['SKU001']})

        from silver_to_postgres import load_order_items
        load_order_items(mock_pg_cursor, sample_df, orders, products)

        rows_inserted = mock_pg_cursor.executemany.call_args[0][1]
        stock_codes = [row[1] for row in rows_inserted]
        assert all(sc == 'SKU001' for sc in stock_codes)

    def test_null_quantity_excluded(self, mock_pg_cursor, sample_df):
        """
        Rows with null quantity should be dropped — quantity is NOT NULL.
        """
        sample_df.loc[0, 'quantity'] = None
        orders = pd.DataFrame({'invoice_no': ['INV001', 'INV002', 'INV003']})
        products = pd.DataFrame({'stock_code': ['SKU001', 'SKU002', 'SKU003']})

        from silver_to_postgres import load_order_items
        load_order_items(mock_pg_cursor, sample_df, orders, products)

        rows_inserted = list(mock_pg_cursor.executemany.call_args[0][1])
        # row with null quantity should not appear
        quantities = [row[2] for row in rows_inserted]
        assert None not in quantities

    def test_insert_uses_on_conflict(self, mock_pg_cursor, sample_df):
        """
        INSERT should use ON CONFLICT DO NOTHING for idempotent reruns.
        """
        orders = pd.DataFrame({'invoice_no': ['INV001', 'INV002', 'INV003']})
        products = pd.DataFrame({'stock_code': ['SKU001', 'SKU002', 'SKU003']})

        from silver_to_postgres import load_order_items
        load_order_items(mock_pg_cursor, sample_df, orders, products)

        sql = mock_pg_cursor.executemany.call_args[0][0]
        assert "ON CONFLICT" in sql.upper()


# ── verify_counts() ──────────────────────────────────────────────────────────

class TestVerifyCounts:

    def test_queries_all_four_tables(self, mock_pg_cursor):
        """
        verify_counts should query all 4 tables: customers, products,
        orders, order_items.
        """
        mock_pg_cursor.fetchone.return_value = (100,)

        from silver_to_postgres import verify_counts
        verify_counts(mock_pg_cursor)

        # should have 4 execute calls — one per table
        assert mock_pg_cursor.execute.call_count == 4

        # extract all SQL queries called
        queries = [call[0][0] for call in mock_pg_cursor.execute.call_args_list]
        tables_queried = [q.split("FROM")[1].strip() for q in queries]

        assert "customers" in tables_queried
        assert "products" in tables_queried
        assert "orders" in tables_queried
        assert "order_items" in tables_queried


# ── load_silver_to_postgres() ────────────────────────────────────────────────

class TestLoadSilverToPostgres:

    def test_commits_on_success(self, sample_df):
        """
        pg_conn.commit() should be called after all tables are loaded.
        """
        mock_pg_conn = MagicMock()
        mock_pg_cursor = MagicMock()
        mock_pg_cursor.fetchone.return_value = (100,)
        mock_pg_conn.cursor.return_value = mock_pg_cursor

        with patch("silver_to_postgres.get_snowflake_connection"), \
             patch("silver_to_postgres.get_postgres_connection", return_value=mock_pg_conn), \
             patch("silver_to_postgres.fetch_silver_data", return_value=sample_df), \
             patch("silver_to_postgres.load_customers", return_value=sample_df[['customer_id']]), \
             patch("silver_to_postgres.load_products", return_value=sample_df[['stock_code']]), \
             patch("silver_to_postgres.load_orders", return_value=sample_df[['invoice_no']]), \
             patch("silver_to_postgres.load_order_items"), \
             patch("silver_to_postgres.verify_counts"):

            from silver_to_postgres import load_silver_to_postgres
            load_silver_to_postgres()

            mock_pg_conn.commit.assert_called_once()

    def test_rollback_on_failure(self, sample_df):
        """
        pg_conn.rollback() should be called if any step raises an exception.
        """
        mock_pg_conn = MagicMock()
        mock_pg_conn.cursor.return_value = MagicMock()

        with patch("silver_to_postgres.get_snowflake_connection"), \
             patch("silver_to_postgres.get_postgres_connection", return_value=mock_pg_conn), \
             patch("silver_to_postgres.fetch_silver_data", side_effect=Exception("Snowflake down")):

            from silver_to_postgres import load_silver_to_postgres
            with pytest.raises(Exception, match="Snowflake down"):
                load_silver_to_postgres()

            mock_pg_conn.rollback.assert_called_once()

    def test_connections_closed_on_success(self, sample_df):
        """
        Both Snowflake and Postgres connections should be closed after success.
        """
        mock_sf_conn = MagicMock()
        mock_pg_conn = MagicMock()
        mock_pg_cursor = MagicMock()
        mock_pg_cursor.fetchone.return_value = (100,)
        mock_pg_conn.cursor.return_value = mock_pg_cursor

        with patch("silver_to_postgres.get_snowflake_connection", return_value=mock_sf_conn), \
             patch("silver_to_postgres.get_postgres_connection", return_value=mock_pg_conn), \
             patch("silver_to_postgres.fetch_silver_data", return_value=sample_df), \
             patch("silver_to_postgres.load_customers", return_value=sample_df[['customer_id']]), \
             patch("silver_to_postgres.load_products", return_value=sample_df[['stock_code']]), \
             patch("silver_to_postgres.load_orders", return_value=sample_df[['invoice_no']]), \
             patch("silver_to_postgres.load_order_items"), \
             patch("silver_to_postgres.verify_counts"):

            from silver_to_postgres import load_silver_to_postgres
            load_silver_to_postgres()

            mock_sf_conn.close.assert_called_once()
            mock_pg_conn.close.assert_called_once()

    def test_connections_closed_on_failure(self):
        """
        Connections should be closed even if an error occurs — finally block.
        """
        mock_sf_conn = MagicMock()
        mock_pg_conn = MagicMock()
        mock_pg_conn.cursor.return_value = MagicMock()

        with patch("silver_to_postgres.get_snowflake_connection", return_value=mock_sf_conn), \
             patch("silver_to_postgres.get_postgres_connection", return_value=mock_pg_conn), \
             patch("silver_to_postgres.fetch_silver_data", side_effect=Exception("fetch failed")):

            from silver_to_postgres import load_silver_to_postgres
            with pytest.raises(Exception):
                load_silver_to_postgres()

            mock_sf_conn.close.assert_called_once()
            mock_pg_conn.close.assert_called_once()

    def test_load_order_respects_fk_dependencies(self, sample_df):
        """
        Load order must be: customers → products → orders → order_items.
        orders depends on customers, order_items depends on both.
        """
        call_order = []

        mock_pg_conn = MagicMock()
        mock_pg_cursor = MagicMock()
        mock_pg_cursor.fetchone.return_value = (100,)
        mock_pg_conn.cursor.return_value = mock_pg_cursor

        with patch("silver_to_postgres.get_snowflake_connection"), \
             patch("silver_to_postgres.get_postgres_connection", return_value=mock_pg_conn), \
             patch("silver_to_postgres.fetch_silver_data", return_value=sample_df), \
             patch("silver_to_postgres.load_customers",
                   side_effect=lambda *a: call_order.append("customers") or sample_df[['customer_id']]), \
             patch("silver_to_postgres.load_products",
                   side_effect=lambda *a: call_order.append("products") or sample_df[['stock_code']]), \
             patch("silver_to_postgres.load_orders",
                   side_effect=lambda *a: call_order.append("orders") or sample_df[['invoice_no']]), \
             patch("silver_to_postgres.load_order_items",
                   side_effect=lambda *a: call_order.append("order_items")), \
             patch("silver_to_postgres.verify_counts"):

            from silver_to_postgres import load_silver_to_postgres
            load_silver_to_postgres()

            assert call_order == ["customers", "products", "orders", "order_items"]