# LOAD cleaned data from Snowflake Silver layer into Postgres 3NF tables
# Reads from ecommerce.silver.silver_ecommerce view
# Loads into: customers, products, orders, order_items

import os
import logging
import psycopg2
import pandas as pd
import snowflake.connector

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("silver_to_postgres")

# Functions to connect to databases, fetch data, load data, and verify counts
# Function to connect to Snowflake and return connection
def get_snowflake_connection():
    logger.info("Connecting to Snowflake...")
    # Connect to Snowflake using credentials from environment variables
    sf_conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="SILVER",
        role=os.getenv("SNOWFLAKE_ROLE")
    )
    logger.info("Snowflake connection established.")
    return sf_conn

# Function to connect to Postgres ecommerce database and return connection
def get_postgres_connection():
    logger.info("Connecting to Postgres...")
    # Connect to Postgres using credentials from environment variables
    pg_conn = psycopg2.connect(
        host=os.getenv("POSTGRES_ECOMMERCE_HOST"),
        port=os.getenv("POSTGRES_ECOMMERCE_PORT"),
        dbname=os.getenv("POSTGRES_ECOMMERCE_DB"),
        user=os.getenv("POSTGRES_ECOMMERCE_USER"),
        password=os.getenv("POSTGRES_ECOMMERCE_PASSWORD")
    )
    logger.info("Postgres connection established.")
    return pg_conn

# Function to fetch all cleaned data from Snowflake Silver view into a DataFrame
def fetch_silver_data(sf_conn):
    logger.info("Fetching data from silver.silver_ecommerce...")

    df = pd.read_sql("""
        SELECT
            invoice_no,
            stock_code,
            description,
            quantity,
            invoice_date,
            unit_price,
            customer_id,
            country,
            total_price
        FROM ecommerce.silver.silver_ecommerce
    """, sf_conn)
    df.columns = df.columns.str.lower()
    logger.info(f"Fetched {len(df)} rows from Silver layer.")
    return df

# Functions to load data into Postgres tables — customers, products, orders, order_items
# 1. Load unique customers into Postgres customers table
def load_customers(pg_cursor, df):
    """
    Load unique customers into Postgres customers table.
    - customer_id: PRIMARY KEY → dropna + drop_duplicates
    - country: NOT NULL → dropna
    Returns customers DataFrame for FK filtering in load_orders.
    """
    logger.info("Loading customers...")
    # extract unique customers — customer_id + country
    customers = (
        df[['customer_id', 'country']]
        .dropna(subset=['customer_id', 'country'])
        .drop_duplicates(subset=['customer_id'])
    )
   
    # itertuples(index=False, name=None) converts DataFrame rows back to plain tuples for executemany
    pg_cursor.executemany("""
        INSERT INTO customers (customer_id, country)
        VALUES (%s, %s)
        ON CONFLICT (customer_id) DO NOTHING
    """, customers.itertuples(index=False, name=None)) # ON CONFLICT ensures safe re-run — won't fail if data already exists, Idempotent — same result every time

    logger.info(f"Loaded {len(customers)} unique customers.")
    return customers

# 2. Load unique products into Postgres products table
#  defines which stock_code exist
def load_products(pg_cursor, df):
    """
    Load unique products into Postgres products table.
    - stock_code: PRIMARY KEY → dropna + drop_duplicates
    - description, unit_price: nullable → no dropna needed
    Returns products DataFrame for FK filtering in load_order_items.
    """
    logger.info("Loading products...")

    products = (
        df[['stock_code', 'description', 'unit_price']]
        .dropna(subset=['stock_code'])
        .drop_duplicates(subset=['stock_code'])
    )
    pg_cursor.executemany("""
        INSERT INTO products (stock_code, description, unit_price)
        VALUES (%s, %s, %s)
        ON CONFLICT (stock_code) DO NOTHING
    """, products.itertuples(index=False, name=None))

    logger.info(f"Loaded {len(products)} unique products.")
    return products

# 3. Load unique orders into Postgres orders table
# can only load rows from invoices whose customer_id exists in customers
def load_orders(pg_cursor, df, customers):
    """
    Load unique orders into Postgres orders table.
    - invoice_no: PRIMARY KEY → dropna + drop_duplicates
    - customer_id: NOT NULL + FK to customers → dropna
    - invoice_date: NOT NULL → dropna
    Returns loaded orders DataFrame for FK filtering in load_order_items.
    """
    logger.info("Loading orders...")
    # extract unique orders — invoice_no + customer_id + invoice_date
    orders = (
        df[['invoice_no', 'customer_id', 'invoice_date']]
        .dropna(subset=['invoice_no', 'invoice_date'])
        .drop_duplicates(subset=['invoice_no'])
        .loc[lambda x: x['customer_id'].isin(customers['customer_id'])] # Fk guard
    )

    pg_cursor.executemany("""
        INSERT INTO orders (invoice_no, customer_id, invoice_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (invoice_no) DO NOTHING
    """, orders.itertuples(index=False, name=None))

    logger.info(f"Loaded {len(orders)} unique orders.")
    return orders


# 4. Load order items into Postgres order_items table
# can only load items whose invoice_no exists in orders
def load_order_items(pg_cursor, df, orders, products):
    """
    Load order items into Postgres order_items table.
    - invoice_no: NOT NULL + FK to orders → dropna + filter to loaded orders only
    - stock_code: NOT NULL + FK to products → dropna
    - quantity: NOT NULL → dropna
    - unit_price, total_price: nullable → no dropna needed
    Silver already guarantees uniqueness on invoice_no + stock_code.
    ON CONFLICT is a rerun safety net only.
    """
    logger.info("Loading order items...")
    order_items = (
        df[['invoice_no', 'stock_code', 'quantity', 'unit_price', 'total_price']]
        .dropna(subset=['quantity'])
        .loc[lambda x: x['invoice_no'].isin(orders['invoice_no'])]
        .loc[lambda x: x['stock_code'].isin(products['stock_code'])]

        # only include rows whose invoice_no exist in orders dataframe
        # and stock_code exist in products dataframe above
    )

      
    pg_cursor.executemany("""
        INSERT INTO order_items (invoice_no, stock_code, quantity, unit_price, total_price)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (invoice_no, stock_code) DO NOTHING
    """, order_items.itertuples(index=False, name=None))
    logger.info(f"Loaded {len(order_items)} order items.")


# Function to verify row counts in all Postgres tables after load
def verify_counts(pg_cursor):
    logger.info("Verifying row counts...")

    for table in ['customers', 'products', 'orders', 'order_items']:
        pg_cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = pg_cursor.fetchone()[0]
        logger.info(f"  {table}: {count} rows")


# Main function to orchestrate the entire load process from Snowflake Silver to Postgres 3NF tables
# Fetch from Snowflake Silver, load into Postgres 3NF tables.
def load_silver_to_postgres():

    # Initialize connections and cursors to None for proper cleanup in finally block
    sf_conn = None
    pg_conn = None
    pg_cursor = None

    try:
        sf_conn = get_snowflake_connection()
        pg_conn = get_postgres_connection()

        pg_cursor = pg_conn.cursor()

        # fetch silver data as a DataFrame
        df = fetch_silver_data(sf_conn)

        # load in order respecting FK constraints: customers → products → orders → order_items
        # 1. customers   — no dependencies
        # 2. products    — no dependencies
        # 3. orders      — depends on customers
        # 4. order_items — depends on orders + products
        customers = load_customers(pg_cursor, df)
        products = load_products(pg_cursor, df)
        orders = load_orders(pg_cursor, df, customers)
        load_order_items(pg_cursor, df, orders, products)

        # commit all 4 tables together — all or nothing
        pg_conn.commit()
        logger.info("All data committed to Postgres successfully.")

        verify_counts(pg_cursor)

    except Exception as e:
        logger.error(f"Load failed: {e}", exc_info=True)
        if pg_conn:
            pg_conn.rollback()
            logger.info("Postgres transaction rolled back.")
        raise

    finally:
        if sf_conn:
            sf_conn.close()
            logger.info("Snowflake connection closed.")
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            pg_conn.close()
            logger.info("Postgres connection closed.")

# Run the function if this script is executed directly
if __name__ == "__main__":
    load_silver_to_postgres()

