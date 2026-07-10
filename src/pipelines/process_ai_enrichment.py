"""
Script Name: process_ai_enrichement.py

Description:
        Downloads validated dataset from OCI,
        performs AI enrichment using Gemini,
        applies local NLP fallback when needed,
        and uploads the enriched dataset back to OCI.

        Features:
            - OCI Object Storage integration
            - Gemini AI enrichment
            - Local NLP fallback
            - Structured logging
            - Timestamp lineage preservation
"""

from datetime import datetime
from pathlib import Path
import logging
import os
import time

from dotenv import load_dotenv
import pandas as pd
from google import genai
from google.genai import types
from textblob import TextBlob

from src.utils.oci_utils import (
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
    

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

BUCKET_NAME = "bucket-tickets"
VALIDATED_PREFIX = "validated/"
SILVER_PREFIX = "silver/"

TMP_DIR = Path("data/tmp/")
SILVER_DIR = Path("data/silver/")

MODEL_NAME = "gemini-2.5-flash"
API_DELAY_SECONDS = 3
MAX_RETRIES = 3

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Gemini Client
# -------------------------------------------------------------------

def get_gemini_client() -> genai.Client:
    """Initialise and return a Gemini client using the env API key."""
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("Missing GEMINI_API_KEY environment variable.")
    return genai.Client(api_key=api_key)


# -------------------------------------------------------------------
# Local NLP Fallback
# -------------------------------------------------------------------

CATEGORY_MAPPING = {
    "How-To Question": ["how do i", "how to", "help me with"],
    "Technical": ["error", "broken", "503", "500", "504", "401", "crash", "down", "maintenance", "unavailable"],
    "Account": ["password", "login", "account", "reset", "disable"],
    "Billing": ["bill", "charge", "refund", "invoice"]
    # Default category will be Feedback
}

def analyze_locally(text: str) -> tuple[float, str]:
    """
    Perform local TextBlob sentiment + keyword-based category analysis.
    Returns:
        A (sentiment_score, category) tuple.
        sentiment_score is rounded to 1 decimal place.
    """
    blob = TextBlob(str(text))
    sentiment = round(blob.sentiment.polarity, 1)
    text_lower = text.lower()

    for category, keywords in CATEGORY_MAPPING.items():
        if any(keyword in text_lower for keyword in keywords):
            return sentiment, category

    return sentiment, "Feedback"


# -------------------------------------------------------------------
# Gemini AI Processing
# -------------------------------------------------------------------

def build_prompt(text: str) -> str:
    """Construct the structured prompt sent to the Gemini model."""
    return f"""
    Analyze the sentiment and category of this support ticket.
    Format your response EXACTLY as: sentiment_score|category_name
    
    Rules:
    - sentiment_score: a number between -1.0 and 1.0
    - category_name: one of [Feedback, Technical, Account, Billing, How-To Question]
    
    Ticket: "{text}"
    """



def get_ai_analysis(client: genai.Client, text: str, categories: set) -> tuple[float, str]:
    """Call the Gemini model and return (sentiment_score, category)."""
    prompt = build_prompt(text)
    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=types.GenerateContentConfig(temperature=0.1)
    )

    try:
        score, category = response.text.strip().split("|", maxsplit=1)
        score = float(score)
        if category not in categories:
            category = "General"
        if -1.1 < score > 1.1:
            score = 0.0 
        return score, category
    except (ValueError, TypeError):
        logger.warning("Failed to parse AI response: %s", response.text)
        return 0.0, "General"


# -------------------------------------------------------------------
# Per-ticket Enrichment with Retry + Fallback
# -------------------------------------------------------------------

def enrich_ticket(client: genai.Client, text: str, categories: set) -> dict:
    """
    Enrich a single ticket with sentiment and category.

    Retries up to MAX_RETRIES times with exponential backoff.
    Falls back to local TextBlob NLP after retry exhaustion.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            sentiment, category = get_ai_analysis(client, text, categories)
            logger.info("AI Success: %s", category)
            time.sleep(API_DELAY_SECONDS)  # rate-limit courtesy delay
            return {
              "sentiment": sentiment,
              "category": category,
              "analysis_source": "gemini"
            }
        except Exception as error:
            logger.warning("AI attempt %s/%s failed: %s", attempt, MAX_RETRIES, str(error)[:22])
            time.sleep(2**attempt)  # Exponential backoff

    logger.warning("Using NLP fallback after retry exhaustion.")
    sentiment, category = analyze_locally(text)
    logger.info("NLP fallback Success: %s", category)
    return {
        "sentiment": sentiment,
        "category": category,
        "analysis_source": "textblob"
    }


# -------------------------------------------------------------------
# Dataframe-level Helpers
# -------------------------------------------------------------------

def enrich_dataframe(df: pd.DataFrame, client: genai.Client) -> pd.DataFrame:
    """Run enrich_ticket() on every row and return the enriched DataFrame."""
    enriched_results = []
    total = len(df)

    valid_categories = fetch_reference_values("DIM_CATEGORIES", "CATEGORY_NAME")

    for index, text in enumerate(df["customer_text"], start=1):
        logger.info("Processing ticket %s / %s", index, total)
        enriched_results.append(enrich_ticket(client, text, valid_categories))

    results_df = pd.DataFrame(enriched_results)
    enriched_df = pd.concat([df, results_df], axis=1)

    # Coerce any non-numeric sentiment values produced by fallback parsing
    enriched_df["sentiment"] = pd.to_numeric(
        enriched_df["sentiment"], errors="coerce"
    ).fillna(0)

    return enriched_df


def save_and_upload(enriched_df: pd.DataFrame, silver_filename: str, storage_client, namespace: str,) -> None:
    """Persist the enriched DataFrame locally and upload to OCI silver layer."""
    
    local_silver_file = SILVER_DIR.joinpath(silver_filename)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    enriched_df.to_csv(local_silver_file, index=False)
    logger.info("Enriched dataset saved locally: %s", local_silver_file)

    silver_object_name = f"{SILVER_PREFIX}{silver_filename}"
    upload_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=silver_object_name,
        local_file=local_silver_file,
    )


# -------------------------------------------------------------------
# Main Processing Logic
# -------------------------------------------------------------------

def main(pipeline_timestamp: str) -> None:
    """
    Full enrichment pipeline for one run:
      1. Download raw CSV from OCI bronze layer.
      2. Enrich every ticket via Gemini (with TextBlob fallback).
      3. Save enriched CSV locally and upload to OCI silver layer.
      4. Clean up temp files (always, even on failure).
    """
    start_time = datetime.now()
    connection = get_database_connection()
    stage_id = get_stage_id(conn=connection, pipeline_code="AI_SUPPORT", stage_name="AI Enrichment")
    run_id = start_pipeline_stage(conn=connection, start_time=start_time, execution_id=pipeline_timestamp, stage_id=stage_id)

    config = load_oci_config()
    storage_client = create_storage_client(config)
    namespace = get_namespace(storage_client)

    raw_object_name = f"{VALIDATED_PREFIX}validated_support_tickets_{pipeline_timestamp}.csv"
    local_raw_file = TMP_DIR.joinpath(f"validated_support_tickets_{pipeline_timestamp}.csv")

    summary = {
        "rows_read": 0,
        "rows_enriched": 0,
        "output_file": "",
        "gemini_enrichments": 0,
        "textblob_enrichments": 0,
        "gemini_percentage": 0.0
    }

    download_object(
        storage_client=storage_client,
        namespace=namespace,
        bucket_name=BUCKET_NAME,
        object_name=raw_object_name,
        download_path=local_raw_file
    )

    try:
        logger.info("Loading validated support ticket dataset...")
        df = pd.read_csv(local_raw_file)
        total = len(df)
        summary["rows_read"] = total
        logger.info("Loaded %s support tickets.", total)

        client = get_gemini_client()
        enriched_df = enrich_dataframe(df, client)

        if not enriched_df.empty:
            silver_filename = f"enriched_support_tickets_{pipeline_timestamp}.csv"
            save_and_upload(enriched_df, silver_filename, storage_client, namespace)
            summary["output_file"] = silver_filename
            summary["rows_enriched"] = len(enriched_df)

            gemini_count = (enriched_df["analysis_source"] == "gemini").sum().item()
            summary["gemini_enrichments"] = gemini_count
            summary["gemini_percentage"] = round((gemini_count / total) * 100, 1)

            textblob_count = (enriched_df["analysis_source"] == "textblob").sum().item()
            summary["textblob_enrichments"] = textblob_count

            logger.info("Gemini enrichments: %s | TextBlob enrichments: %s", gemini_count, textblob_count)
            logger.info("Gemini enrichment rate: %.1f%%", summary["gemini_percentage"])
            
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
