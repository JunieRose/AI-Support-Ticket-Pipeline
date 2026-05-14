"""
Script Name: generate_raw_data.py
Description: Generates synthetic support ticket data for testing and analysis. 
             Outputs a CSV file containing ticket IDs, timestamps, customer 
             feedback, regions, and agent response times.
"""

import pandas as pd
import random
from datetime import datetime, timedelta

def generate_support_data(output_file="raw_support_tickets.csv", num_records=10):
  # Template library for simulating various customer support scenarios
  templates = [
      "Some of the user's can't access {feature}. Getting {error} error.",
      "Excellent service! Agent {agent} helped me a lot!",
      "Why is the {feature} so slow today? This is affecting our work!",
      "I was charged twice for my {feature} subscription. Refund please.",
      "How do I reset my password for the {feature}?",
      "The {feature} is not responding. It is stuck in loading and we are getting so many complains!",
      "How do I enable {feature} feature? It is not available in the console.",
      "I can't get successful response. REST API is returning {error} error.",
      "ALL USERS can't access {feature} because it keeps crashing. The This is negatively impacting our productivity!"]
  
  # Categorical data for randomization
  features = ["Dashboard", "Portal", "API", "Mobile App", "Admin Page"]
  errors = ["404", "429 Too Many Requests", "500 Internal Server", "Timeout", "Auth Failed"]
  regions = ["PH-NCR", "CA-TORONTO", "AU-SYDNEY", "UK-LONDON", "SG-CENTRAL"]
  agents = ["Charles", "Kimberly", "Diane", "Sam", "John"]
  
  ticket_data = []
  
  for i in range(num_records):
    ticket_id = f"INC-{1001 + i}"
    
    # Generate a random creation timestamp within April 2026
    created_at = datetime(
        2026, 4, random.randint(1, 30),
        random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
        )
    
    # Simulate a response time (between 5 minutes and 24 hours after creation)
    response_delay = random.randint(300, 86400)
    first_response_dt = created_at + timedelta(seconds=response_delay)
    
    # Construct the ticket description using random selection
    customer_text = random.choice(templates).format(
        feature = random.choice(features),
        error = random.choice(errors),
        agent = random.choice(agents),
        region = random.choice(regions)
        )
    
    # Store record as a dictionary for cleaner DataFrame ingestion
    ticket_data.append({
        "ticket_id": ticket_id,
        "created_at": created_at.strftime('%Y-%m-%d %H:%M:%S'),
        "customer_text": customer_text,
        "region": random.choice(regions),
        "first_response_at": first_response_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    
  # Convert to DataFrame and export
  try:
    df = pd.DataFrame(ticket_data)
    df.to_csv(output_file, index=False)
    print(f"Success: {len(df)} tickets generated and saved to {output_file}")
    
  except Exception as e:
    print(f"Error during file generation: {e}")

if __name__ == "__main__":
  generate_support_data()
