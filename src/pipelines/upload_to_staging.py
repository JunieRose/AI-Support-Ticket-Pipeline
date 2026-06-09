"""
Script Name: upload_to_staging.py

Description:
        Authenticates with Oracle Cloud Infrastructure (OCI) 
        and uploads raw support ticket data to OCI Object Storage staging layer.

        Features:
        - Reads raw support ticket CSV from local file system
        - Authenticates using OCI SDK with config file
        - Uploads the CSV file to a specified OCI Object Storage bucket
        - Provides error handling for authentication and upload issues
"""

from pathlib import Path
import logging
import time

from src.utils.oci_utils import (
    load_oci_config,
    create_storage_client,
    get_namespace,
    upload_object
)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

BUCKET_NAME = "bucket-tickets"
RAW_PREFIX = "bronze/"

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Main Processing Logic
# -------------------------------------------------------------------

def load_to_staging(pipeline_timestamp: str) -> None:
    """
    Upload the bronze-layer CSV for a given run to OCI Object Storage.

    The local file is deleted after a successful upload to avoid
    accumulating files on the Airflow worker disk.
    """
    input_file = Path(f"data/bronze/raw_support_tickets_{pipeline_timestamp}.csv")

    start_time = time.time()

    config = load_oci_config()
    storage_client = create_storage_client(config)
    namespace = get_namespace(storage_client)

    logger.info("Target bucket: %s", BUCKET_NAME)

    raw_object_name = (f"{RAW_PREFIX}{input_file.name}")

    upload_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=raw_object_name,
        local_file=input_file
    )

    elapsed_time = round(time.time() - start_time, 2)
    logger.info("Staging upload completed in %s seconds.", elapsed_time)

    input_file.unlink()
    logger.info("Deleted local file: %s", input_file)


def main(pipeline_timestamp: str) -> None:
    """Pipeline entry point for the upload-to-staging task."""

    try:
        load_to_staging(pipeline_timestamp)
    except Exception as error:
        logger.exception("Pipeline execution failed: %s", error)
        raise


if __name__ == "__main__":
    main()

