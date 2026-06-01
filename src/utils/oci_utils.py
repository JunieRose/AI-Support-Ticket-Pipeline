"""
Module Name: oci_utils.py

Description:
    Shared OCI Object Storage utility functions
    used across the data pipeline.

Features:
    - OCI configuration loading
    - Object Storage client creation
    - File upload/download
    - Latest object retrieval
"""

from pathlib import Path
import logging
import os
import platform

from dotenv import load_dotenv
import oci


# -------------------------------------------------------------------
# Environment Variables
# -------------------------------------------------------------------

load_dotenv()

OCI_CONFIG_PROFILE = os.getenv("OCI_CONFIG_PROFILE")
OCI_CONFIG_PATH = os.getenv("OCI_CONFIG_PATH")

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# OCI Configuration
# -------------------------------------------------------------------

def load_oci_config() -> dict:

    logger.info("Loading OCI configuration...")

    return oci.config.from_file(
        OCI_CONFIG_PATH,
        OCI_CONFIG_PROFILE
    )


def create_storage_client(config: dict) -> oci.object_storage.ObjectStorageClient:

    logger.info("Initializing OCI Object Storage client...")

    return oci.object_storage.ObjectStorageClient(
        config
    )


def get_namespace(storage_client) -> str:

    namespace = (storage_client.get_namespace().data)

    logger.info("Connected to OCI namespace: %s", namespace)

    return namespace


# -------------------------------------------------------------------
# Object Storage Operations
# -------------------------------------------------------------------

def upload_object(
    storage_client,
    namespace: str,
    bucket_name: str,
    object_name: str,
    local_file: Path,
    content_type: str = "text/csv"
) -> None:
    # Upload local file to OCI Object Storage.

    logger.info("Uploading object: %s", object_name)

    with open(local_file, "rb") as file_data:

        storage_client.put_object(
            namespace_name=namespace,
            bucket_name=bucket_name,
            object_name=object_name,
            put_object_body=file_data,
            content_type=content_type
        )

    logger.info("Upload completed successfully.")


def download_object(
    storage_client,
    namespace: str,
    bucket_name: str,
    object_name: str,
    download_path: Path
) -> None:
    # Download OCI object locally.

    logger.info("Downloading object: %s", object_name)

    response = storage_client.get_object(
        namespace_name=namespace,
        bucket_name=bucket_name,
        object_name=object_name
    )

    download_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(download_path, "wb") as file:

        file.write(response.data.content)

    logger.info("Download completed: %s", download_path)


def get_latest_object(
    storage_client,
    namespace: str,
    bucket_name: str,
    prefix: str,
    filename_pattern: str
) -> str:
    # Retrieve latest matching object from OCI Object Storage.

    logger.info("Searching for latest object under prefix: %s", prefix)

    response = storage_client.list_objects(
        namespace_name=namespace,
        bucket_name=bucket_name,
        prefix=prefix
    )

    objects = response.data.objects

    matching_files = [
        obj.name
        for obj in objects
        if filename_pattern in obj.name
    ]

    if not matching_files:
        raise FileNotFoundError(
            f"No matching files found for pattern: {filename_pattern}"
        )

    latest_object = max(matching_files)

    logger.info("Latest object detected: %s", latest_object)

    return latest_object