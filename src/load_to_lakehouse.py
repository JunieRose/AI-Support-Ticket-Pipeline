"""
Script Name: load_to_lakehouse.py

Description: 
    Performs a bulk insert of enriched support ticket data into 
    an Oracle Autonomous Database (Lakehouse).

    Features:
    - CSV validation
    - Batch database inserts
    - Safe resource cleanup
    - Airflow-friendly execution flow
"""

from pathlib import Path
import logging
import os
import re
import time

from dotenv import load_dotenv
import oracledb
import pandas as pd

from utils.oci_utils import (
    load_oci_config,
    create_storage_client,
    get_namespace,
    get_latest_object,
    download_object
)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

BUCKET_NAME = "bucket-tickets"
SILVER_PREFIX = "silver/"
FILENAME_PATTERN = "enriched_support_tickets_"

TMP_DIR = Path("data/tmp")

ENRICHED_FILE_PATTERN = r"enriched_support_tickets_(\d{8}_\d{6})\.csv"
TARGET_TABLE = "support_tickets"

REQUIRED_COLUMNS = [
  "ticket_id",
  "created_at",
  "customer_text",
  "region",
  "first_response_at",
  "sentiment",
  "category",
  "analysis_source"
]

# -------------------------------------------------------------------
# Environment Variables
# -------------------------------------------------------------------

load_dotenv()
DB_USER = os.getenv("OCI_DB_USER")
DB_PASSWORD = os.getenv("OCI_DB_PASSWORD")
DB_DSN = os.getenv("OCI_DB_DSN")

# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

if not all([DB_USER, DB_PASSWORD, DB_DSN]):
   raise ValueError(
      "Missing required Oracle database environment variables."
   )

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Pipeline Utility Functions
# -------------------------------------------------------------------

def extract_timestamp_from_filename(
    filename: str
) -> str:

    match = re.search(
        ENRICHED_FILE_PATTERN,
        filename
    )

    if not match:
        raise ValueError(
            f"Could not extract timestamp from: {filename}"
        )

    return match.group(1)

# -------------------------------------------------------------------
# Data Validation
# -------------------------------------------------------------------

def validate_input_data(dataframe: pd.DataFrame) -> None:
  # Validate required columns before database load.

  missing_columns = [
      column
      for column in REQUIRED_COLUMNS
      if column not in dataframe.columns
  ]

  if missing_columns:
    raise ValueError(
      f"Missing required columns: {missing_columns}"
    )

  if dataframe.empty:
      raise ValueError(
          "Input dataset is empty."
      )
    
# -------------------------------------------------------------------
# Database Functions
# -------------------------------------------------------------------

def get_database_connection() -> oracledb.Connection:
  # Create Oracle database connection.

  logger.info(
      "Connecting to Oracle Autonomous Database..."
  )

  return oracledb.connect(
      user=DB_USER,
      password=DB_PASSWORD,
      dsn=DB_DSN
  )

def prepare_records(dataframe: pd.DataFrame) -> list[tuple]:
  # Convert DataFrame into Oracle-compatible insert records.

  dataframe["created_at"] = pd.to_datetime(dataframe["created_at"])
  dataframe["first_response_at"] = pd.to_datetime(dataframe["first_response_at"])
  dataframe.replace({pd.NA: None}, inplace=True)
  
  return list(
    dataframe.itertuples(index=False, name=None)
  )

def execute_bulk_insert(cursor: oracledb.Cursor, records: list[tuple]) -> int:
  # Execute batch insert operation.

  sql = f"""
    INSERT INTO {TARGET_TABLE} (
      ticket_id,
      created_at,
      customer_text,
      region,
      first_response_at,
      sentiment,
      category,
      analysis_source
    )
    VALUES (
      :1,
      :2,
      :3,
      :4,
      :5,
      :6,
      :7,
      :8
    )
  """

  logger.info("Executing bulk insert for %s records...", len(records))

  cursor.executemany(sql, records)
  return cursor.rowcount

# -------------------------------------------------------------------
# Main Processing Logic
# -------------------------------------------------------------------

def load_data_to_lakehouse() -> None:
  # Main database loading workflow.

  # ---------------------------------------------------------------
  # OCI Initialization
  # ---------------------------------------------------------------

  config = load_oci_config()
  storage_client = create_storage_client(config)
  namespace = get_namespace(storage_client)

  # ---------------------------------------------------------------
  # Retrieve Latest Raw Dataset
  # ---------------------------------------------------------------

  latest_object = get_latest_object(
    storage_client=storage_client,
    namespace=namespace,
    bucket_name=BUCKET_NAME,
    prefix=SILVER_PREFIX,
    filename_pattern=FILENAME_PATTERN
  )

  silver_filename = Path(latest_object).name

  pipeline_timestamp = (
      extract_timestamp_from_filename(
          silver_filename
      )
  )

  local_enriched_file = TMP_DIR / silver_filename

  download_object(
      storage_client=storage_client,
      namespace=namespace,
      bucket_name=BUCKET_NAME,
      object_name=latest_object,
      download_path=local_enriched_file
  )


  logger.info("Loading enriched support ticket dataset...")

  start_time = time.time()

  df = pd.read_csv(local_enriched_file)
  validate_input_data(df)

  records = prepare_records(df)

  connection = None
  cursor = None

  try:
    connection = get_database_connection()
    cursor = connection.cursor()

    inserted_rows = execute_bulk_insert(
       cursor,
       records
    )

    connection.commit()

    elapsed_time = round(
        time.time() - start_time,
        2
    )

    logger.info(
        "Transaction committed successfully."
    )

    logger.info(
        "%s rows inserted into %s.",
        inserted_rows,
        TARGET_TABLE
    )

    logger.info(
        "Load completed in %s seconds.",
        elapsed_time
    )

  except oracledb.Error as database_error:

      if connection:
          connection.rollback()

      logger.exception(
          "Oracle database error occurred: %s",
          database_error
      )

      raise

  except Exception as error:

      if connection:
          connection.rollback()

      logger.exception(
          "Unexpected pipeline error: %s",
          error
      )

      raise

  finally:

      if cursor:
          cursor.close()

      if connection:
          connection.close()

      logger.info(
          "Database connection closed."
      )


def main() -> None:
  # Script entry point.

  try:
     load_data_to_lakehouse()

  except Exception as error:
    logger.exception(
       "Pipeline execution failed: %s",
       error
    )

    raise

if __name__ == "__main__":
   main()
