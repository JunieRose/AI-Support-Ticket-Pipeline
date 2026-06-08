"""
Script Name: process_ai_enrichement.py

Description:
        Downloads the latest raw support ticket dataset from
        OCI Object Storage, performs AI enrichment using Gemini,
        applies local NLP fallback when needed, and uploads the
        enriched dataset back to OCI Object Storage.

        Features:
            - OCI Object Storage integration
            - Gemini AI enrichment
            - Local NLP fallback
            - Structured logging
            - Timestamp lineage preservation
            - Airflow-friendly workflow
"""

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
    load_oci_config,
    create_storage_client,
    get_namespace,
    download_object,
    upload_object
)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

BUCKET_NAME = "bucket-tickets"
RAW_PREFIX = "bronze/"
SILVER_PREFIX = "silver/"

TMP_DIR = Path("data/tmp")
SILVER_DIR = Path("data/silver")

CONTENT_TYPE = "text/csv"

MODEL_NAME = "gemini-3-flash-preview"
API_DELAY_SECONDS = 3
MAX_RETRIES = 3


# -------------------------------------------------------------------
# Environment Variables
# -------------------------------------------------------------------

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


if not GEMINI_API_KEY:
  raise ValueError("Missing GEMINI_API_KEY environment variable.")


# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


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
  # Perform local fallback sentiment and category analysis.

  testimonial = TextBlob(str(text))
  sentiment = round(testimonial.sentiment.polarity,1)
  text_lower = text.lower()

  category = "Feedback"

  for mapped_category, keywords in CATEGORY_MAPPING.items():
    if any(keyword in text_lower for keyword in keywords):
      category = mapped_category
      break

  return sentiment, category


# -------------------------------------------------------------------
# Gemini AI Processing
# -------------------------------------------------------------------

def build_prompt(text: str) -> str:
  # Build AI enrichment prompt.

  return f"""
  Analyze the sentiment and category of this support ticket.
  Format your response EXACTLY as: sentiment_score|category_name
  
  Rules:
  - sentiment_score: a number between -1.0 and 1.0
  - category_name: one of [Feedback, Technical, Account, Billing, How-To Question]
  
  Ticket: "{text}"
  """


def parse_ai_response(response_text: str) -> tuple[float, str]:

  try:
    score, category = response_text.strip().split("|", maxsplit=1)
    return float(score), category.strip()
  
  except (ValueError, TypeError):
    logger.warning(
      "Failed to parse AI response: %s",
      response_text
    )
    return 0.0, "General"


def get_ai_analysis(client: genai.Client, text: str) -> tuple[float, str]:
  # Calls Gemini API to analyze ticket content.

  prompt = build_prompt(text)

  response = client.models.generate_content(
      model=MODEL_NAME,
      contents=prompt,
      config=types.GenerateContentConfig(
          temperature=0.1
          )
      )
  
  return parse_ai_response(response.text)


# -------------------------------------------------------------------
# Main Processing Logic
# -------------------------------------------------------------------

def enrich_ticket(client: genai.Client, text: str) -> dict:
  # Enrich a single support ticket record.

  for attempt in range(1, MAX_RETRIES + 1):

    try:
      sentiment, category = get_ai_analysis(client, text)

      logger.info("AI Success: %s", category)
      time.sleep(API_DELAY_SECONDS)  # Delay for free tier stability

      return {
        "sentiment": sentiment,
        "category": category,
        "analysis_source": "gemini"
      }
    
    except Exception as error:
      logger.warning("AI attempt %s failed: %s", attempt, str(error)[:22])
      time.sleep(2**attempt)  # Exponential backoff

  logger.warning(
    "Using NLP fallback after retry exhaustion."
  )

  sentiment, category = analyze_locally(text)
  logger.info("NLP Success: %s", category)

  return {
    "sentiment": sentiment,
    "category": category,
    "analysis_source": "textblob"
  }

def process_tickets(pipeline_timestamp: str) -> None:
  # Main execution logic to read, analyze, and save data.

  start_time = time.time()

  # ---------------------------------------------------------------
  # OCI Initialization
  # ---------------------------------------------------------------

  config = load_oci_config()
  storage_client = create_storage_client(config)
  namespace = get_namespace(storage_client)


  raw_object_name = (f"bronze/raw_support_tickets_{pipeline_timestamp}.csv")

  local_raw_file = Path(f"{TMP_DIR}/raw_support_tickets_{pipeline_timestamp}.csv")

  download_object(
      storage_client=storage_client,
      namespace=namespace,
      bucket_name=BUCKET_NAME,
      object_name=raw_object_name,
      download_path=local_raw_file
  )

  # ---------------------------------------------------------------
  # Load Raw Dataset
  # ---------------------------------------------------------------

  logger.info("Loading raw support ticket dataset...")

  df = pd.read_csv(local_raw_file)

  logger.info("Loaded %s support tickets.", len(df))


  # ---------------------------------------------------------------
  # Initialize Gemini Client
  # ---------------------------------------------------------------

  client = genai.Client(
      api_key=GEMINI_API_KEY
  )

  enriched_results = []

  for index, text in enumerate(df["customer_text"], start=1):

    logger.info(
      "Processing ticket %s / %s",
      index,
      len(df)
      )
    
    result = enrich_ticket(client, text)
    enriched_results.append(result)

  results_df = pd.DataFrame(enriched_results)

  enriched_df = pd.concat([df, results_df], axis=1)

  enriched_df["sentiment"] = pd.to_numeric(
    enriched_df["sentiment"], errors="coerce"
  ).fillna(0)


  # ---------------------------------------------------------------
  # Save Enriched Dataset
  # ---------------------------------------------------------------

  silver_filename = (f"enriched_support_tickets_{pipeline_timestamp}.csv")

  local_silver_file = (
      SILVER_DIR / silver_filename
  )

  SILVER_DIR.mkdir(
      parents=True,
      exist_ok=True
  )

  enriched_df.to_csv(
      local_silver_file,
      index=False
  )

  logger.info(
      "Enriched dataset saved locally: %s",
      local_silver_file
  )

  # ---------------------------------------------------------------
  # Upload Enriched Dataset to OCI
  # ---------------------------------------------------------------

  silver_object_name = (
      f"{SILVER_PREFIX}{silver_filename}"
  )


  upload_object(
    storage_client=storage_client,
    namespace=namespace,
    bucket_name=BUCKET_NAME,
    object_name=silver_object_name,
    local_file=local_silver_file
  )
  
  elapsed_time = round(
      time.time() - start_time,
      2
  )

  logger.info(
      "AI enrichment pipeline completed in %s seconds.",
      elapsed_time
  )

  logger.info(
    "Gemini enrichments: %s",
    (enriched_df["analysis_source"] == "gemini").sum()
  )

  logger.info(
    "Fallback enrichments: %s",
    (enriched_df["analysis_source"] == "textblob").sum()
  )

  local_raw_file.unlink()
  logger.info("Clean up: Deleted local file %s", local_raw_file)

def main(pipeline_timestamp: str) -> None:

  try:
    process_tickets(pipeline_timestamp)

  except Exception as error:
    logger.exception(
      "Pipeline execution failed: %s", str(error)
    )
    raise

if __name__ == "__main__":
  main()
