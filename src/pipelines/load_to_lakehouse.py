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
import time

from dotenv import load_dotenv
import oracledb
import pandas as pd

from src.utils.oci_utils import (
    get_database_connection,
    load_oci_config,
    create_storage_client,
    get_namespace,
    download_object
)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

BUCKET_NAME = "bucket-tickets"
SILVER_PREFIX = "silver/"
TMP_DIR = Path("data/tmp")

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
# Logging Configuration
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


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

def load_data_to_lakehouse(pipeline_timestamp: str) -> None:
  # Main database loading workflow.

  # ---------------------------------------------------------------
  # OCI Initialization
  # ---------------------------------------------------------------

  config = load_oci_config()
  storage_client = create_storage_client(config)
  namespace = get_namespace(storage_client)

  # ---------------------------------------------------------------
  # Retrieve Object from OCI
  # ---------------------------------------------------------------

  silver_object_name = (f"{SILVER_PREFIX}enriched_support_tickets_{pipeline_timestamp}.csv")

  local_enriched_file = Path(f"{TMP_DIR}/enriched_support_tickets_{pipeline_timestamp}.csv")

  download_object(
      storage_client=storage_client,
      namespace=namespace,
      bucket_name=BUCKET_NAME,
      object_name=silver_object_name,
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


def main(pipeline_timestamp: str) -> None:
  # Script entry point.

  try:
     load_data_to_lakehouse(pipeline_timestamp)

  except Exception as error:
    logger.exception(
       "Pipeline execution failed: %s",
       error
    )

    raise

if __name__ == "__main__":
   main()
