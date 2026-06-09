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
RANDOM_SEED = 37
RESPONSE_RATE = 0.8  # 80% of tickets will have a response, 20% will be unanswered

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

REGIONS = [
    "CA-TORONTO",
    "AU-SYDNEY",
    "UK-LONDON",
    "SG-CENTRAL",
    "JP-TOKYO"
]


# -------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------

def generate_pipeline_timestamp() -> str:
    """Return a timestamp string for use in filenames."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def build_output_file(pipeline_timestamp: str) -> Path:
    """Resolve the bronze-layer output path for a given run timestamp."""
    return Path(f"data/bronze/raw_support_tickets_{pipeline_timestamp}.csv")


def generate_random_timestamp() -> datetime:
    """Return a random timestamp within the last 24 hours."""
    return datetime.now() - timedelta(seconds=random.randint(0, 86400))


def simulate_first_response(created_at: datetime):
    """
    Simulate whether a ticket received a first response.

    Returns a formatted timestamp string for responded tickets,
    or None for the ~20 % that remain unanswered.
    """
    
    if random.random() < RESPONSE_RATE:
        first_response_at = created_at + timedelta(
            seconds=random.randint(300, 86400)
        )
        return first_response_at.strftime("%Y-%m-%d %H:%M:%S")
    return None


def generate_customer_text() -> str:
    """Generate randomized customer feedback text using predefined templates and placeholder values."""
    
    return random.choice(TEMPLATES).format(
      feature=random.choice(FEATURES),
      error=random.choice(ERRORS)
    )


# -------------------------------------------------------------------
# Core Data Generation
# -------------------------------------------------------------------

def generate_support_data(record_count: int = RECORD_COUNT) -> pd.DataFrame:
    """Main function to generate synthetic support ticket records."""
    logger.info("Generating %s synthetic support ticket records...", record_count)

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
    """Persist a DataFrame to CSV, creating parent directories as needed."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_file, index=False)
    logger.info(
        "Successfully generated %s records and saved to %s",
        len(dataframe),
        output_file
    )

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
    except Exception as error:
        logger.exception("Pipeline execution failed: %s", error)
        raise

    return pipeline_timestamp


if __name__ == "__main__":
    main()