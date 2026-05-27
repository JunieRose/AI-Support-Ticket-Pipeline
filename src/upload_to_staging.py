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
import os
import time

from dotenv import load_dotenv
import oci

from utils.oci_utils import (
    load_oci_config,
    create_storage_client,
    get_namespace,
    upload_object
)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

RAW_DIR = Path("data/raw")
RAW_FILE_PATTERN = "raw_support_tickets_*.csv"
INPUT_FILE = next(RAW_DIR.glob(RAW_FILE_PATTERN))

BUCKET_NAME = "bucket-tickets"
OBJECT_NAME = f"raw/{INPUT_FILE.name}"
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
# Main Processing Logic
# -------------------------------------------------------------------

def load_to_staging() -> None:
    # Main upload workflow.

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    start_time = time.time()

    config = load_oci_config()
    storage_client = create_storage_client(config)
    namespace = get_namespace(storage_client)

    bucket = storage_client.get_bucket(
        namespace_name=namespace,
        bucket_name=BUCKET_NAME
    )

    logger.info("Validated bucket: %s",bucket.data.name)

    upload_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=OBJECT_NAME,
        local_file=INPUT_FILE
    )

    elapsed_time = round(
        time.time() - start_time,
        2
    )

    logger.info(
        "Staging upload completed in %s seconds.",
        elapsed_time
    )

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

