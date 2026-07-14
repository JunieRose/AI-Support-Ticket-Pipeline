"""
Script Name: load_to_lakehouse.py

Description: 
    Performs a bulk insert of enriched support ticket data into 
    an Oracle Autonomous Database (Lakehouse).

    Features:
    - Batch database inserts
    - Safe resource cleanup
"""

from datetime import datetime
from pathlib import Path
import logging

import oracledb
import pandas as pd

from src.utils.oci_utils import (
    load_oci_config,
    create_storage_client,
    get_namespace,
    download_object
)

from src.utils.db_utils import (
    get_database_connection,
    fetch_reference_mapping
)

from src.utils.pipeline_utils import (
    get_stage_id,
    start_pipeline_stage,
    complete_pipeline_stage,
    fail_pipeline_stage
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


def prepare_records(dataframe: pd.DataFrame, region_lookup: dict, category_lookup: dict) -> list[tuple]:
    """Coerce types and convert DataFrame to Oracle-compatible row tuples."""

    dataframe = dataframe.copy()
    dataframe["created_at"] = pd.to_datetime(dataframe["created_at"])
    dataframe["first_response_at"] = pd.to_datetime(dataframe["first_response_at"], errors="coerce")
    dataframe.replace({pd.NA: None}, inplace=True)
    dataframe["region_id"] = dataframe["region"].map(region_lookup)
    dataframe["category_id"] = dataframe["category"].map(category_lookup)
    dataframe.drop(columns=["region", "category"], inplace=True)

    return list(dataframe.itertuples(index=False, name=None))


def execute_bulk_insert(connection: oracledb.Connection, records: list[tuple]) -> int:
    """Execute batch insert operation."""

    sql = f"""
        INSERT INTO {TARGET_TABLE} (
        email_address,
        created_at,
        customer_text,
        first_response_at,
        sentiment_score,
        analysis_source,
        region_id,
        category_id
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

    try:
        logger.info("Starting bulk insert operation for %s records...", len(records))
        with connection.cursor() as cursor:
            cursor.executemany(sql, records)
            connection.commit()
            logger.info("Bulk insert operation completed successfully.")
            return cursor.rowcount
    except Exception as e:
        logger.exception("Failed to start pipeline stage: %s", e)
        connection.rollback()
        raise


# -------------------------------------------------------------------
# Main Processing Logic
# -------------------------------------------------------------------

def main(pipeline_timestamp: str) -> None:
    """
    Full load workflow for one pipeline run:
      1. Download enriched CSV from OCI silver layer.
      2. Validate and prepare records.
      3. Insert into Oracle Autonomous Database.
      4. Clean up temp file (always, even on failure).
    """
    start_time = datetime.now()
    connection = get_database_connection()
    stage_id = get_stage_id(conn=connection, pipeline_code="AI_SUPPORT", stage_name="Load to Lakehouse")
    run_id = start_pipeline_stage(conn=connection, start_time=start_time, execution_id=pipeline_timestamp, stage_id=stage_id)

    config = load_oci_config()
    storage_client = create_storage_client(config)
    namespace = get_namespace(storage_client)

    silver_object_name = f"{SILVER_PREFIX}enriched_support_tickets_{pipeline_timestamp}.csv"
    local_enriched_file = TMP_DIR / f"enriched_support_tickets_{pipeline_timestamp}.csv"

    summary = {
        "rows_loaded": 0,
        "target_table": TARGET_TABLE
    }

    download_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=silver_object_name,
        download_path=local_enriched_file
    )

    try:
        logger.info("Loading enriched support ticket dataset...")

        region_lookup = fetch_reference_mapping(conn=connection, table_name="DIM_REGIONS", key_column="REGION_NAME", value_column="REGION_ID")
        category_lookup = fetch_reference_mapping(conn=connection, table_name="DIM_CATEGORIES", key_column="CATEGORY_NAME", value_column="CATEGORY_ID")

        df = pd.read_csv(local_enriched_file)
        records = prepare_records(df, region_lookup, category_lookup)
        
        inserted_rows = execute_bulk_insert(connection, records)
        summary["rows_loaded"] = inserted_rows
        complete_pipeline_stage(conn=connection, run_id=run_id, metrics=summary)

    except Exception as error:
        logger.exception("Pipeline execution failed: %s", error)
        fail_pipeline_stage(conn=connection, run_id=run_id, error_message=str(error))
        raise

    finally:
        connection.close()
        logger.info("Database connection closed.")
        if local_enriched_file.exists():
            local_enriched_file.unlink()
            logger.info("Clean up: Deleted local file %s", local_enriched_file)


if __name__ == "__main__":
   main()
