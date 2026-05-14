"""
Script Name: upload_to_staging.py
Description: Authenticates with Oracle Cloud Infrastructure (OCI) and uploads 
             a local file to a specified Object Storage bucket.
"""

import oci
import os

config = oci.config.from_file("/content/config", "DEFAULT")

BUCKET_NAME = "bucket-tickets"
OBJECT_NAME = "raw_support_tickets.csv"
LOCAL_FILE_PATH = "/content/raw_support_tickets.csv"

def upload_to_oci():
  # Verify local file existence before initializing heavy clients
  if not os.path.exists(LOCAL_FILE_PATH):
    print(f"Aborting: Local file '{LOCAL_FILE_PATH}' not found.")
    return
    
  try:
    # Initialize Object Storage Client
    print(f"Initializing OCI Client...")
    storage_client = oci.object_storage.ObjectStorageClient(config)
    
    # Retrieve the tenancy namespace
    namespace = storage_client.get_namespace().data
    print(f"Connected. Namespace: {namespace}")
    
    # Verify bucket accessibility
    print(f"Verifying bucket: {BUCKET_NAME}...")
    bucket_info = storage_client.get_bucket(namespace, BUCKET_NAME)
    print(f"Bucket validated. Created on: {bucket_info.data.time_created}")

    # Perform the upload
    print(f"Uploading {OBJECT_NAME} to {BUCKET_NAME}")
    with open(LOCAL_FILE_PATH, 'rb') as file_data:
      storage_client.put_object(
          namespace_name=namespace,
          bucket_name=BUCKET_NAME,
          object_name=OBJECT_NAME,
          put_object_body=file_data
      )
    
    print("Upload successful: File is now available on OCI Object Storage.")

  except oci.exceptions.ServiceError as se:
    print(f"OCI Service Error: {se.message}")

  except Exception as e:
    print(f"Unexpected error occurred: {e}")


if __name__ == "__main__":
    upload_to_oci()
