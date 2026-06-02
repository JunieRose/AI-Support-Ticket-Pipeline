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

from datetime import datetime
from pathlib import Path
import logging
import time

from dotenv import load_dotenv
import oci

from src.utils.oci_utils import (
    load_oci_config,
    create_storage_client,
    get_namespace,
    upload_object
)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

RAW_DIR = Path("data/bronze")
RAW_FILE_PATTERN = "raw_support_tickets_*.csv"

BUCKET_NAME = "bucket-tickets"
RAW_PREFIX = "bronze/"
CONTENT_TYPE = "text/csv"


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

def get_latest_raw_file() -> Path:

    matching_files = sorted(
        RAW_DIR.glob(RAW_FILE_PATTERN)
    )

    if not matching_files:
        raise FileNotFoundError(
            "No raw support ticket files found."
        )

    return matching_files[-1]

# -------------------------------------------------------------------
# Main Processing Logic
# -------------------------------------------------------------------

def load_to_staging() -> None:
    # Main upload workflow.

    input_file = get_latest_raw_file()

    start_time = time.time()

    config = load_oci_config()
    storage_client = create_storage_client(config)
    namespace = get_namespace(storage_client)

    bucket = storage_client.get_bucket(
        namespace_name=namespace,
        bucket_name=BUCKET_NAME
    )

    logger.info("Validated bucket: %s",bucket.data.name)

    raw_object_name = (f"{RAW_PREFIX}{input_file.name}")

    upload_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=raw_object_name,
        local_file=input_file
    )

    elapsed_time = round(
        time.time() - start_time,
        2
    )

    logger.info(
        "Staging upload completed in %s seconds.",
        elapsed_time
    )

    input_file.unlink()
    logger.info("Deleted local file: %s", input_file)

def main() -> None:
    # Script entry point.

    try:
        load_to_staging()

    except oci.exceptions.ServiceError as service_error:

        logger.exception(
            "OCI service error occurred: %s",
            service_error
        )

        raise

    except Exception as error:

        logger.exception(
            "Pipeline execution failed: %s",
            error
        )

        raise


if __name__ == "__main__":
    main()

