"""
Module Name: oci_utils.py

Description:
    Interact with Oracle Cloud Infrastructure (OCI) services,
    including Object Storage.
"""

from pathlib import Path
import logging
import os

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
    """Load OCI configuration from file."""
    logger.info("Loading OCI configuration...")
    return oci.config.from_file(OCI_CONFIG_PATH, OCI_CONFIG_PROFILE)


def create_storage_client(config: dict) -> oci.object_storage.ObjectStorageClient:
    """Instantiate and return an OCI Object Storage client."""
    logger.info("Initializing OCI Object Storage client...")
    return oci.object_storage.ObjectStorageClient(config)


def get_namespace(storage_client) -> str:
    """Retrieve and return the OCI Object Storage namespace."""
    namespace = storage_client.get_namespace().data
    logger.info("Connected to OCI namespace: %s", namespace)
    return namespace


# -------------------------------------------------------------------
# Object Storage Operations
# -------------------------------------------------------------------

def upload_object(
    storage_client: oci.object_storage.ObjectStorageClient,
    namespace: str,
    bucket_name: str,
    object_name: str,
    local_file: Path,
    content_type: str = "text/csv"
) -> None:
    """Upload a local file to OCI Object Storage."""
    logger.info("Uploading %s → oci:%s/%s", local_file.name, bucket_name, object_name)

    with open(local_file, "rb") as file_data:
        storage_client.put_object(
            namespace_name=namespace,
            bucket_name=bucket_name,
            object_name=object_name,
            put_object_body=file_data,
            content_type=content_type
        )

    logger.info("Upload completed: %s", object_name)


def download_object(
    storage_client: oci.object_storage.ObjectStorageClient,
    namespace: str,
    bucket_name: str,
    object_name: str,
    download_path: Path
) -> None:
    """
    Download an OCI Object Storage object to a local path.
    Creates any missing parent directories before writing.
    """
    logger.info("Downloading oci:%s/%s → %s", bucket_name, object_name, download_path)

    response = storage_client.get_object(
        namespace_name=namespace,
        bucket_name=bucket_name,
        object_name=object_name
    )

    download_path.parent.mkdir(parents=True, exist_ok=True)

    with open(download_path, "wb") as file:
        file.write(response.data.content)

    logger.info("Download completed: %s", download_path)

