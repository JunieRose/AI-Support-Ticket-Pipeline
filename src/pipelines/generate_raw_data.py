"""
Script Name: generate_raw_data.py

Description:
    Generates synthetic customer support ticket data for
    pipeline testing and data quality validation.

    The dataset intentionally includes a small percentage
    of invalid records to simulate real-world source system
    issues such as:

    - Missing required values
    - Duplicate records
    - Invalid email formats
    - Future timestamps
    - Invalid regions
    - Response timestamps occurring before ticket creation

    The generated dataset is saved locally and uploaded
    to OCI Object Storage Bronze layer.
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging
import time
import random

import pandas as pd

from src.utils.oci_utils import (
    load_oci_config,
    create_storage_client,
    get_namespace,
    upload_object
)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

RECORD_COUNT = 100
RANDOM_SEED = 37
RESPONSE_RATE = 0.8  # 80% of tickets will have a response, 20% will be unanswered
INVALID_VALUE_RATE = 0.95 # injecting 5% invalid value per column

BUCKET_NAME = "bucket-tickets"
RAW_PREFIX = "bronze/"

random.seed(RANDOM_SEED)

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Template Data
# -------------------------------------------------------------------

TEMPLATES = [
    "I was double charged for my {feature} subscription. When will I get my refund? It's been months now.",
    "I have terminated my subscription but I am continuosly getting invoices. This is unacceptable!",
    "Excellent service! Patrice was patient and demonstrated expertise in resolving my {feature} issue. Appreciate it!",
    "My account was disabled after multiple log in attempt. Please enable.",
    "How do I reset my Admin password for the {feature}?",
    "How do I enable {feature} feature? I can't find it in the settings. Please share the documentation link.",
    "Our customers is unable to reach us becase the {feature} is down. Give us a call and treat this with urgency.",
    "ALL USERS can't access {feature} because it keeps crashing. This is negatively impacting our productivity!",
    "Our system keeps is down, it keeps redirecting to Maintenance Page. We are losing customers because of this!",
    "My {feature} is unavailable for hours. I have a critical deadline and this is causing me a lot of stress."
]

FEATURES = [
    "Dashboard",
    "Portal",
    "Notification System",
    "Mobile App",
    "Admin Page"
]

ERRORS = [
    "503 Service Unavailable",
    "500 Internal Server",
    "504 Gateway Timeout",
    "401 Unauthorized"
]

VALID_REGIONS = [
    "CA-TORONTO",
    "AU-SYDNEY",
    "UK-LONDON",
    "SG-CENTRAL",
    "JP-TOKYO"
]

INVALID_REGIONS = [
    "U-CENTRAL",
    "MARS-1",
    "NEW-JERSEY",
    "JAPAN",
    "THAILAND",
    None
]

EMAIL_DOMAINS = [
    "gmail.com",
    "news70.hotmail.com",
    "sales.outlook.com",
    "yahoo.com",
    "icloud.com"
]

FIRST_NAMES = [
    "alice_", "bob2marly", "jason.de", "chris", "john_martin", "grace", "frank",
    "jasmine", "lili", "james", "d_j", "manuel", "sam", "austin"
]

INVALID_EMAILS = [
    "invalid-email",
    "missingatsign.com",
    "@gmail.com",
    "test@",
    "12345",
    None
]

# -------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------

def generate_pipeline_timestamp() -> str:
    """Return a timestamp string for use in filenames across the pipeline."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def build_output_file(pipeline_timestamp: str) -> Path:
    """Resolve the bronze-layer output path for a given run timestamp."""
    return Path(f"data/bronze/raw_support_tickets_{pipeline_timestamp}.csv")


def should_generate_invalid() -> bool:
    """Determine whether an invalid value should be generated."""
    return random.random() > INVALID_VALUE_RATE


def generate_email_address() -> str | None:
    """
    Generate a synthetic email address.

    Returns:
        Valid email address for most records.
        An incorrect format or null for intentionally invalid records.
    """
    if should_generate_invalid():
        return random.choice(INVALID_EMAILS)

    name = random.choice(FIRST_NAMES)
    number = random.randint(1, 99)
    domain = random.choice(EMAIL_DOMAINS)
    return f"{name}{number}@{domain}"


def generate_customer_text() -> str | None:
    """
    Generate customer ticket text.

    Returns:
        Valid ticket content for most records.
        Null or blank for intentionally invalid records.
    """
    if should_generate_invalid():
        if random.choice([True, False]):
            return None
        return "   "
    
    return random.choice(TEMPLATES).format(
      feature=random.choice(FEATURES),
      error=random.choice(ERRORS)
    )


def generate_region() -> str | None:
    """
    Simulate valid and invalid region based off a list.
    """
    if should_generate_invalid():
        return random.choice(INVALID_REGIONS)
    return random.choice(VALID_REGIONS)


def generate_created_at() -> datetime | None:
    """
    Return a random timestamp within the last 24 hours
    Or return invalid value: Null or future date.
    """
    if should_generate_invalid():
        if random.choice([True, False]):
            return None
        return datetime.now() + timedelta(hours=random.randint(1, 72))
    return datetime.now() - timedelta(seconds=random.randint(0, 86400))


def generate_first_response_at(created_at: datetime | None) -> str | None:
    """
    Simulate whether a ticket received a first response.

    Returns a formatted timestamp string for responded tickets,
    or Null for the ~20 % that remain unanswered.
    """
    if created_at is None:
        return None
    
    if random.random() > RESPONSE_RATE:
        return None

    if should_generate_invalid():
        invalid_first_response_at = created_at - timedelta(hours=random.randint(1, 24))
        return invalid_first_response_at.strftime("%Y-%m-%d %H:%M:%S")
    
    first_response_at = created_at + timedelta(seconds=random.randint(300, 86400))
    return first_response_at.strftime("%Y-%m-%d %H:%M:%S")


# -------------------------------------------------------------------
# Core Data Generation
# -------------------------------------------------------------------

def generate_support_data(record_count: int = RECORD_COUNT) -> pd.DataFrame:
    """Main function to generate synthetic support ticket records."""
    logger.info("Generating %s synthetic support ticket records...", record_count)

    ticket_data = []

    for i in range(record_count):
        # Store duplicate ticket record, change created_at
        if should_generate_invalid():
            previous_record = ticket_data[i-1]
            previous_record["created_at"] = generate_created_at()
            ticket_data.append(previous_record)
            pass

        created_at = generate_created_at()
      
        # Store unique ticket record
        ticket_record = {
            "email_address": generate_email_address(),
            "created_at": created_at and created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "customer_text": generate_customer_text(),
            "region": generate_region(),
            "first_response_at": generate_first_response_at(created_at)
        }
        ticket_data.append(ticket_record)
    
    logger.info("Successfully generated %s records", len(ticket_data))
    return pd.DataFrame(ticket_data)


def save_to_csv(dataframe: pd.DataFrame, output_file: Path) -> None:
    """Persist a DataFrame to CSV, creating parent directories as needed."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_file, index=False)
    logger.info("Dataframe saved to %s", output_file)


def load_to_staging(raw_file_path: Path) -> None:
    """
    Upload the bronze-layer CSV for a given run to OCI Object Storage.

    The local file is deleted after a successful upload to avoid
    accumulating files on the Airflow worker disk.
    """
    logger.info("Start uploading to OCI Object Storage...")
    start_time = time.time()

    config = load_oci_config()
    storage_client = create_storage_client(config)
    namespace = get_namespace(storage_client)

    logger.info("Target bucket: %s", BUCKET_NAME)

    raw_object_name = (f"{RAW_PREFIX}{raw_file_path.name}")

    upload_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=raw_object_name,
        local_file=raw_file_path
    )

    elapsed_time = round(time.time() - start_time, 2)
    logger.info("Staging upload completed in %s seconds.", elapsed_time)

    if raw_file_path.exists():
        raw_file_path.unlink()
    logger.info("Deleted local file: %s", raw_file_path)


# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------

def main() -> str:
    """
    Pipeline entry point — generates data and returns the run timestamp.

    The timestamp is pushed to Airflow XCom automatically when this
    function is used as a PythonOperator callable.
    """
    pipeline_timestamp = generate_pipeline_timestamp()
    output_file = build_output_file(pipeline_timestamp)

    try:
        support_data = generate_support_data()
        save_to_csv(dataframe=support_data, output_file=output_file)
        load_to_staging(raw_file_path=output_file)
    except Exception as error:
        logger.exception("Pipeline execution failed: %s", error)
        raise

    return pipeline_timestamp


if __name__ == "__main__":
    main()