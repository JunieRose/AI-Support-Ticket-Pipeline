# Upload the file to OCI Object Storage
!pip install oci

import oci
import os

# SECURITY NOTE: Credential removed for GitHub portfolio
config = {
    "user": "REDACTED_USER",
    "key_file": "REDACTED_KEY_FILE",
    "fingerprint": "REDACTED_FINGERPRINT",
    "tenancy": "REDACTED_TENANCY",
    "region": "REDACTED_REGION"
}

object_storage = oci.object_storage.ObjectStorageClient(config)
namespace = object_storage.get_namespace().data
bucket_name = "bucket-incidents"
object_name = "raw_support_tickets.csv"
file_path = "/content/raw_support_tickets.csv"

with open(file_path, 'rb') as f:
  print(f"Uploading {object_name} to OCI...")
  object_storage.put_object(namespace, bucket_name, object_name, f)
  print("Upload complete! Check OCI Console.")
