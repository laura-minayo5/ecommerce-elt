# EXTRACT data and store it in RAWDIR using kaggle_dataset() function,
# then LOAD it into Snowflake Bronze using load_to_snowflake() function. 

from pathlib import Path
import kagglehub
import shutil
import logging
# import pandas as pd
import os
import sys
import traceback
import snowflake.connector
from config.paths import RAW_DIR  # import from config

# Configure the basic logging settings
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# logger instance for this module 
logger = logging.getLogger("extract_load_kaggle")

# Function to download the dataset from Kaggle and copy it to the RAW_DIR
def kaggle_dataset():
    # Create the RAW_DIR if it doesn't exist and skips silently if it already exists.
    # parents=True allows it to create any necessary parent directories as well.
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Download dataset — wrap in try/except for kagglehub bug:
    # kagglehub creates a temp file to download into: ~/.cache/kagglehub/datasets/carrie1/ecommerce-data/1.archive
    # this is just an empty placeholder file first that kagglehub streams the download into it
    # kagglehub then extracts the .archive file reads its compressed content,creates the destination folder:
        #  ~/.cache/kagglehub/datasets/carrie1/ecommerce-data/versions/1/
        #  writes the CSV files into that folder:
        #  ~/.cache/kagglehub/datasets/carrie1/ecommerce-data/versions/1/data.csv 
    # kagglehub deletes the .archive temp file after extracting,
    # but sometimes the file is already gone → FileNotFoundError
    # but if it succeeds kagglehub returns the cache path:
        #  "/home/airflow/.cache/kagglehub/datasets/carrie1/ecommerce-data/versions/1" and code receives this as download_path
        #  then glob for *.csv and find data.csv 
    # The download itself succeeds so we fall back to the known cache path
    try:
        download_path = Path(kagglehub.dataset_download("carrie1/ecommerce-data"))

    except FileNotFoundError as e:
        logger.warning(f"kagglehub cleanup error (safe to ignore): {e}")

        # try both path structures — kagglehub changed folder layout between versions
        fallback_v1 = Path.home() / ".cache/kagglehub/datasets/carrie1/ecommerce-data/versions/1"
        fallback_v2 = Path.home() / ".cache/kagglehub/datasets/carrie1/ecommerce-data/1"

        if fallback_v1.exists():
            download_path = fallback_v1
        elif fallback_v2.exists():
            download_path = fallback_v2
        else:
            # neither path exists — something went wrong, fail loudly
            raise RuntimeError(f"kagglehub fallback paths not found: {fallback_v1} or {fallback_v2}")

        # log which fallback path was selected — outside if/elif/else so it
        # always logs regardless of which fallback was chosen
        logger.info(f"Using fallback path: {download_path}")

    # stores every file ending in .csv in a list
    csv_files = list(download_path.glob("*.csv"))
    if not csv_files:
        raise RuntimeError(f"No CSV files found in: {download_path}")

    # copy all CSV files to RAW_DIR
    for csv_file in csv_files:
        destination = RAW_DIR / csv_file.name   # define the destination path for the CSV file
        shutil.copy(csv_file, destination)      # copy the csv file to the destination directory
        logger.info(f"Copied {csv_file.name} → {destination}")

    logger.info("Successfully copied all CSV files to RAW_DIR.")

# Function to connect to Snowflake and return connection
def get_snowflake_connection():
    """Connect to Snowflake and return connection."""
    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )
    logger.info("Snowflake connection established.")
    return conn

# Function to PUT the CSV file into the Bronze raw_stage in Snowflake 
def put_to_stage(cursor, csv_path: Path):
    logger.info(f"Uploading {csv_path.name} to @raw_stage...")
    cursor.execute(
        f"PUT file://{csv_path} @ecommerce.bronze.raw_stage "
        f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )
    logger.info("File uploaded to stage successfully.")

# Function to COPY from stage into Bronze table
# COPY INTO database.schema.table, FROM database.schema.stage_name
def copy_to_bronze(cursor):
    logger.info("Loading data into bronze.raw_ecommerce...")
    cursor.execute("""
        COPY INTO ecommerce.bronze.raw_ecommerce
        FROM @ecommerce.bronze.raw_stage
        FILE_FORMAT = ecommerce.bronze.csv_format
        ON_ERROR = 'CONTINUE'
    """)
    logger.info("Data loaded into bronze.raw_ecommerce successfully.")


# Function to validate the load by checking for any errors and counting rows
def validate_load(cursor):

    cursor.execute("""
        SELECT * FROM TABLE(VALIDATE(ecommerce.bronze.raw_ecommerce, job_id => '_last'))
    """)
    failed_rows = cursor.fetchall()
    logger.info(f"Failed rows: {len(failed_rows)}")
    if failed_rows:
        logger.warning("Failed rows detected:")
        for row in failed_rows:
            logger.warning(row)
    else:
        logger.info("All rows loaded successfully without errors.")


    # Quick row count verify
    cursor.execute("SELECT COUNT(*) FROM ecommerce.bronze.raw_ecommerce")
    row_count = cursor.fetchone()[0] 
    logger.info(f"Verification — total rows in bronze.raw_ecommerce: {row_count}")

# Orchestrate the PUT → COPY INTO → verify for a single CSV file.
def load_to_snowflake(csv_path: Path):
    """Orchestrate PUT → COPY INTO → verify for a single CSV file."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        put_to_stage(cursor, csv_path)
        copy_to_bronze(cursor)
        validate_load(cursor)

    except Exception as e:
        logger.error(f"Snowflake load failed: {e}", exc_info=True)
        raise

    finally:
        cursor.close()
        conn.close()
        logger.info("Snowflake connection closed.")

# Main execution block to run the dataset download and load process
if __name__ == "__main__":
    try:
        # runs first, downloads & copies CSV to RAW_DIR
        kaggle_dataset()

        # Load each CSV in RAW_DIR into Snowflake Bronze
        #  PUT → COPY INTO → verify row count
        for csv_file in RAW_DIR.glob("*.csv"):
            load_to_snowflake(csv_file)

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        logging.error(traceback.format_exc())
        sys.exit(1) 