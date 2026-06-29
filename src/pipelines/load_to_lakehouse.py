"""
Script Name: load_to_lakehouse.py

Description: 
    Performs a bulk insert of enriched support ticket data into 
    an Oracle Autonomous Database (Lakehouse).

    Features:
    - Batch database inserts
    - Safe resource cleanup
"""

from pathlib import Path
import logging
import math
import time

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

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Database Functions
# -------------------------------------------------------------------


def prepare_records(dataframe: pd.DataFrame) -> list[tuple]:
    """Coerce types and convert DataFrame to Oracle-compatible row tuples."""
    dataframe = dataframe.copy()
    dataframe["created_at"] = pd.to_datetime(dataframe["created_at"])
    dataframe["first_response_at"] = pd.to_datetime(
        dataframe["first_response_at"], errors="coerce"
        )
    dataframe.replace({pd.NA: None}, inplace=True)

    return list(dataframe.itertuples(index=False, name=None))

def execute_bulk_insert(cursor: oracledb.Cursor, records: list[tuple]) -> int:
    """Execute batch insert operation."""

    sql = f"""
        INSERT INTO {TARGET_TABLE} (
        email_address,
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
    """
    Full load workflow for one pipeline run:
      1. Download enriched CSV from OCI silver layer.
      2. Validate and prepare records.
      3. Insert into Oracle Autonomous Database.
      4. Clean up temp file (always, even on failure).
    """
    config = load_oci_config()
    storage_client = create_storage_client(config)
    namespace = get_namespace(storage_client)

    silver_object_name = f"{SILVER_PREFIX}enriched_support_tickets_{pipeline_timestamp}.csv"
    local_enriched_file = TMP_DIR / f"enriched_support_tickets_{pipeline_timestamp}.csv"

    download_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=silver_object_name,
        download_path=local_enriched_file
    )

    try:
        logger.info("Loading enriched support ticket dataset...")
        start_time = time.time()
        
        df = pd.read_csv(local_enriched_file)
        records = prepare_records(df)

        connection = get_database_connection()
        cursor = connection.cursor()
        
        try:
            inserted_rows = execute_bulk_insert(cursor,records)
            connection.commit()

            elapsed_time = round(time.time() - start_time, 2)
            logger.info("Transaction committed successfully.")
            logger.info("%s rows inserted into %s.", inserted_rows, TARGET_TABLE)
            logger.info("Load completed in %s seconds.", elapsed_time)
            
        except oracledb.Error as database_error:
            connection.rollback()
            logger.exception("Database error during insert operation: %s", database_error)
            raise

        finally:
            cursor.close()
            connection.close()
            logger.info("Database connection closed.")

    finally:
        # Always remove the temp file, even if the DB step failed.
        if local_enriched_file.exists():
            local_enriched_file.unlink()
            logger.info("Clean up: Deleted local file %s", local_enriched_file)


def main(pipeline_timestamp: str) -> None:
    """Pipeline entry point for the lakehouse load task."""
    try:
        load_data_to_lakehouse(pipeline_timestamp)
    except Exception as error:
        logger.exception("Pipeline execution failed: %s", error)
        raise

if __name__ == "__main__":
   main()
