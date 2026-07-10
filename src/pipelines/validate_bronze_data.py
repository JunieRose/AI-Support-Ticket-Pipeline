"""
Script Name: validate_raw_data.py

Description:
    Validates bronze-layer support ticket data prior to AI enrichment.

    Validation rules include:
    - Required field validation
    - Email format validation
    - Timestamp validation
    - Region reference validation
    - Duplicate detection

    Records passing validation are written to the validated dataset.
    Records failing validation are written to a quarantine dataset
    with failure reasons for troubleshooting and audit purposes.
"""

from datetime import datetime
from pathlib import Path
import logging
import pandas as pd
import re
import time


from src.utils.oci_utils import(
    get_database_connection,
    load_oci_config,
    create_storage_client,
    get_namespace,
    fetch_reference_values,
    download_object,
    upload_object
)

from src.utils.pipeline_utils import (
    get_stage_id,
    start_pipeline_stage,
    complete_pipeline_stage,
    fail_pipeline_stage
)

BUCKET_NAME = "bucket-tickets"
BRONZE_PREFIX = "bronze/"
VALID_PREFIX = "validated/"
QUARANTINE_PREFIX = "quarantine/"
TMP_DIR = Path("data/tmp/")

VALIDATED_DIR = Path("data/validated/")
QUARANTINE_DIR = Path("data/quarantine/")

REQUIRED_COLUMNS = [
  "email_address",
  "created_at",
  "customer_text",
  "region",
  "first_response_at"
]

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
DUPLICATE_KEY = ["email_address", "customer_text"]

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def validate_required_columns(dataframe: pd.DataFrame) -> None:
    """Check 1: Validate the DataFrame has all required columns and is not empty. """
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in dataframe.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    if dataframe.empty:
        raise ValueError("Input dataset is empty.")


# -------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------


def validate_email_address(row: pd.Series, failures: list) -> None:
    """
    Check 2: Validate email_address
    Rules:
        - Cannot be null or blank
        - Must match expected email format
    """
    if pd.isna(row["email_address"]) or str(row["email_address"]).strip() == "":
        failures.append(f"email_address is NULL or blank")
        return
    if not EMAIL_PATTERN.match(str(row["email_address"]).strip()):
        failures.append(f"Invalid email format: {row['email_address']}") 


def validate_created_at(row: pd.Series, failures: list) -> None:
    """
    Check 3: Validate created_at
    Rules:
        - Cannot be null or blank
        - Must be a valid datetime
        - Must not be in the future
    """
    if pd.isna(row["created_at"]) or str(row["created_at"]).strip() == "":
        failures.append("created_at is NULL or blank")
        return
    
    try:
        created_at = pd.to_datetime(row["created_at"])
    except Exception:
        failures.append(f"created_at is not a valid datetime: {row['created_at']}")
        return
    
    if created_at > datetime.now():
        failures.append("created_at is in future date")


def validate_customer_text(row: pd.Series, failures: list) -> None:
    """
    Check 4: Validate customer_text
    Rules:
        - Cannot be null or blank.
    """
    if pd.isna(row["customer_text"]) or str(row["customer_text"]).strip() == "":
        failures.append("customer_text is NULL or blank")


def validate_region(row: pd.Series, failures: list, valid_regions: set[str]) -> None:
    """
    Check 5: Validate region
    Rules:
        - Cannot be null or blank
        - Must exist in dim_regions reference table
    """
    if pd.isna(row["region"]) or str(row["region"]).strip() == "":
        failures.append("region is NULL or blank")
        return
    if row["region"] not in valid_regions:
        failures.append(f"Unrecognized region: {row['region']}")


def validate_first_response_at(row: pd.Series, failures: list) -> None:
    """
    Check 6: Validate first_response_at
    Rules:
        - When present, must be a valida datetime and must come after created_at
        - Null is allowed - it means the ticket has not yet have received an initial
            response which is a normal business state and not a data error
    """
    if pd.isna(row["first_response_at"]):
        return None # NULL is valid, nothing to check
    
    try:
        response_dt = pd.to_datetime(row["first_response_at"])
    except Exception:
        failures.append(f"first_response_at is not a valid datetime: {row['first_response_at']}")

    try:
        created_at = pd.to_datetime(row["created_at"])
        if response_dt < created_at:
            failures.append("first_response_at must be after created_at")
    except Exception:
        pass # created_at parse failure is already caught at validate_created_at

    
def validate_duplicates(valid_df: pd.DataFrame, quarantine_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Check 7: Identify duplicate support tickets
    Duplicate definition:
        email_address + customer_text
    Processing logic:
        - Sort by created_at descending
        - Keep the most recent record
        - Move older duplicates to quarantine
    """
    valid_df = valid_df.copy()
    valid_df["created_at"] = pd.to_datetime(valid_df["created_at"], format="mixed")
    valid_df = valid_df.sort_values(by="created_at", ascending=False)
    duplicate_mask = valid_df.duplicated(subset=DUPLICATE_KEY, keep="first")

    if duplicate_mask.any():
        duplicates = valid_df[duplicate_mask].copy()
        duplicates["failure_reason"] = "Duplicate email and text"

        quarantine_df = pd.concat([quarantine_df, duplicates]).sort_index()
        valid_df = valid_df[~duplicate_mask].sort_index()

    return valid_df, quarantine_df


# -------------------------------------------------------------------
# Save anbd Upload Outputs
# -------------------------------------------------------------------

def save_and_upload_valid_data(valid_df: pd.DataFrame, pipeline_timestamp: str, storage_client, namespace) -> str:
    """
    Save validated records to CSV file and uploads to OCI Object Storage.
    Returns the file name of the validated output file for metrics reporting
    """
    VALIDATED_DIR.mkdir(parents=True, exist_ok=True)

    validated_file = VALIDATED_DIR.joinpath(f"validated_support_tickets_{pipeline_timestamp}.csv")
    validated_object_name = f"{VALID_PREFIX}validated_support_tickets_{pipeline_timestamp}.csv"

    valid_df.to_csv(validated_file, index=False)
    logger.info("Validated records saved: %s (%s rows)", validated_file, len(valid_df))

    upload_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=validated_object_name,
        local_file=validated_file
    )

    return str(validated_file.name)


def save_and_upload_quarantined_data(quarantine_df: pd.DataFrame, pipeline_timestamp: str, storage_client, namespace) -> str:
    """
    Save quarantined records CSV file and uploads to OCI Object Storage.
    Returns the file name of the quarantined output file for metrics reporting
    """
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    quarantine_file = QUARANTINE_DIR.joinpath(f"quarantine_support_tickets_{pipeline_timestamp}.csv")
    quarantine_object_name = f"{QUARANTINE_PREFIX}quarantine_support_tickets_{pipeline_timestamp}.csv"

    quarantine_df.to_csv(quarantine_file, index=False)
    logger.info("Quarantine records saved: %s (%s rows)", quarantine_file, len(quarantine_df))

    upload_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=quarantine_object_name,
        local_file=quarantine_file
    )

    return str(quarantine_file.name)

# -------------------------------------------------------------------
# Main Orchestration
# -------------------------------------------------------------------


def validate_bronze_data(pipeline_timestamp: str, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Full validation run for one pipeline execution:
      1. Download the raw bronze CSV from OCI.
      2. Check required columns exist (fail the task if not).
      3. Fetch valid regions from Oracle dim_regions.
      4. Validate every row and split the DataFrame into two groups:
            valid_df:      rows that passes all checks
            quarantine_df: rows that failed at least one check
                           with a "failure_reasons" column explaining why
      5. Resolve duplicates from valid_df — keep latest, quarantine older copies.
      6. Save both output files and upload to OCI.
      7. Provide data quality summary.
      8. Clean up the temp file.
    """
    start_time = time.time()
    logger.info("Loading raw dataset...")

    try:
        validate_required_columns(df)
        valid_regions = fetch_reference_values("DIM_REGIONS", "REGION_NAME")
        df["failure_reason"] = ""

        for index, row in df.iterrows():
            failures = []

            validate_email_address(row, failures)
            validate_created_at(row, failures)
            validate_customer_text(row, failures)
            validate_region(row, failures, valid_regions)
            validate_first_response_at(row, failures)

            if failures:
                df.at[index, "failure_reason"] = "; ".join(failures)

        valid_df = df[df["failure_reason"] == ""]
        quarantine_df = df[df["failure_reason"] != ""]

        valid_df, quarantine_df = validate_duplicates(valid_df, quarantine_df)
        valid_df = valid_df.drop(columns=["failure_reason"])

        total = len(df)
        logger.info("Validation complete - %s/%s valid | %s/%s quarantined",
                    len(valid_df), total,
                    len(quarantine_df), total
        )

        if len(valid_df) == 0:
            logger.warning("All %s rows were quarantined "
                           "Check data/quarantine/ for failure reasons",
                           total
                           )
            
        if not quarantine_df.empty:
            logger.info("Data Quality Summary")
            logger.info("\n%s",
                        quarantine_df["failure_reason"]
                        .value_counts()
                        .to_string()
                        )
    except Exception as error:
        logger.exception("Validation failed: %s", error)
        raise

    elapsed = round(time.time() - start_time, 2)
    logger.info("Validation completed in %s seconds.", elapsed)
    return valid_df, quarantine_df


def main(pipeline_timestamp: str) -> None:
    """Pipeline entry point to the validation task"""
    start_time = datetime.now()
    connection = get_database_connection()
    stage_id = get_stage_id(conn=connection, pipeline_code="AI_SUPPORT", stage_name="Validate Bronze Data")
    run_id = start_pipeline_stage(conn=connection, start_time=start_time, execution_id=pipeline_timestamp, stage_id=stage_id)

    config = load_oci_config()
    storage_client = create_storage_client(config)
    namespace = get_namespace(storage_client)

    raw_object_name = f"{BRONZE_PREFIX}raw_support_tickets_{pipeline_timestamp}.csv"
    local_raw_file = TMP_DIR.joinpath(f"raw_support_tickets_{pipeline_timestamp}.csv")

    summary = {
        "rows_read": 0,
        "rows_valid": 0,
        "rows_quarantined": 0,
        "validation_pass_rate": 0.0,
        "valid_output_file": "",
        "quarantine_output_file": ""
    }

    try:
        download_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=raw_object_name,
        download_path=local_raw_file
        )

        logger.info("Loading bronze dataset...")
        df = pd.read_csv(local_raw_file)
        total = len(df)
        summary["rows_read"] = total
        logger.info("Loaded %s rows...", total)

        valid_df, quarantine_df = validate_bronze_data(pipeline_timestamp, df)
        summary["rows_valid"] = len(valid_df)
        summary["rows_quarantined"] = len(quarantine_df)
        summary["validation_pass_rate"] = round((len(valid_df) / total) * 100, 1)
        logger.info("Validation pass rate: %.1f%%", summary["validation_pass_rate"])

        if not valid_df.empty:
            valid_file = save_and_upload_valid_data(valid_df, pipeline_timestamp, storage_client, namespace)
            summary["valid_output_file"] = valid_file

        if not quarantine_df.empty:
            quarantine_file = save_and_upload_quarantined_data(quarantine_df, pipeline_timestamp, storage_client, namespace)
            summary["quarantine_output_file"] = quarantine_file

        complete_pipeline_stage(conn=connection, run_id=run_id, metrics=summary)

    except Exception as error:
        logger.exception("Pipeline execution failed: %s", error)
        fail_pipeline_stage(conn=connection, run_id=run_id, error_message=str(error))
        raise

    finally:
        if local_raw_file.exists():
            local_raw_file.unlink()
            logger.info("Clean up: Deleted local file %s", local_raw_file)
        

if __name__ == "__main__":
    main()

