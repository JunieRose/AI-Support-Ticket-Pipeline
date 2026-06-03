"""
Script Name: generate_raw_data.py

Description: 
        Generate synthetic customer support ticket data for testing,
        analytics, and pipeline development. 

        The script creates a CSV file containing:
        - Ticket ID
        - Ticket creation timestamp
        - Customer feedback text
        - Region
        - First response timestamp
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging
import random

import pandas as pd

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

RECORD_COUNT = 100
BASE_TICKET_ID = 10001
RANDOM_SEED = 42
RESPONSE_RATE = 0.8  # 80% of tickets will have a response, 20% will be unanswered

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
    "I was charged twice for my {feature} subscription and I need a refund immediately.",
    "Excellent service! Agent helped me solving my {feature} issue. Kudos!",
    "How do I reset my password for the {feature}?",
    "My account was disabled after multiple log in attempt. Please enable.",
    "How do I enable {feature} feature? It is not available in the console.",
    "Some of the user's can't access {feature}. Getting {error} error.",
    "The {feature} so slow today and returning different errors. This is affecting our workflow!",
    "The {feature} is not responding. It is stuck in loading and we are getting so many complains. Please fix this ASAP.",
    "I can't get successful response. REST API is returning {error} error.",
    "ALL USERS can't access {feature} because it keeps crashing. The This is negatively impacting our productivity!"
]

FEATURES = [
    "Dashboard",
    "Portal",
    "API",
    "Mobile App",
    "Admin Page"
]

ERRORS = [
    "404",
    "429 Too Many Requests",
    "500 Internal Server",
    "Timeout",
    "Authentication Failed"
]

REGIONS = [
    "CA-TORONTO",
    "AU-SYDNEY",
    "UK-LONDON",
    "SG-CENTRAL",
    "JP-TOKYO"
]

def generate_pipeline_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def build_output_file(pipeline_timestamp: str) -> Path:
    return Path(f"data/bronze/raw_support_tickets_{pipeline_timestamp}.csv")


def generate_random_timestamp() -> datetime:
    # Random timestamp within the last 24 hours for created_at field.

    return datetime.now() - timedelta(
      seconds=random.randint(0, 86400)
    )


def simulate_first_response(created_at: datetime):
    # Simulate unresolved tickets with missing first response
    
    has_response = random.random() < RESPONSE_RATE

    if has_response:
      first_response_at = created_at + timedelta(
          seconds=random.randint(300, 86400)
      )

      first_response_value = first_response_at.strftime(
          "%Y-%m-%d %H:%M:%S"
      )
    else:
      first_response_value = None
    
    return first_response_value


def generate_customer_text() -> str:
    # Generate randomized customer feedback text using predefined templates and placeholder values.
    
    return random.choice(TEMPLATES).format(
      feature=random.choice(FEATURES),
      error=random.choice(ERRORS)
    )


def generate_support_data(record_count: int = RECORD_COUNT) -> pd.DataFrame:
    # Main function to generate synthetic support ticket data and save it as a CSV file.
    
    logger.info("Generating %s synthetic support ticket records...", RECORD_COUNT)

    ticket_data = []

    for index in range(record_count):
      ticket_id = f"INC-{BASE_TICKET_ID + index}"

      # Generate ticket creation timestamp
      created_at = generate_random_timestamp()
      
      # Store ticket record
      ticket_data.append(
        {
          "ticket_id": ticket_id,
          "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
          "customer_text": generate_customer_text(),
          "region": random.choice(REGIONS),
          "first_response_at": simulate_first_response(created_at)
        }
      )

    return pd.DataFrame(ticket_data)


def save_to_csv(dataframe: pd.DataFrame, output_file: Path) -> None:
  
    # Create parent directories if they do not exist
    output_file.parent.mkdir(parents=True, exist_ok=True)

    dataframe.to_csv(output_file, index=False)

    logger.info(
        "Successfully generated %s records and saved to %s",
        len(dataframe),
        output_file
    )


def main() -> str:
    # Main execution entry point.

    pipeline_timestamp = generate_pipeline_timestamp()
    output_file = build_output_file(pipeline_timestamp)

    try:

      # Set random seed for reproducibility
      random.seed(RANDOM_SEED)

      support_data = generate_support_data()

      save_to_csv(
        dataframe=support_data,
        output_file=output_file
      )

    except Exception as error:
      logger.exception("Pipeline execution failed: %s", error)
      raise

    return pipeline_timestamp

if __name__ == "__main__":
    main()