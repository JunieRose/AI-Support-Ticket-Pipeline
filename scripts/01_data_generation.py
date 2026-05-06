import pandas as pd
import random
from datetime import datetime, timedelta

# Sample templates to mimic support incidents
templates = [
    "I can't access my {feature}. Getting {error} error.",
    "Excellent service! {person} helped me a lot!",
    "Why is the {feature} so slow today? This is affecting our work.",
    "I was charged twice for my {feature} subscription. Refund please.",
    "How do I reset my password for the {feature}?",
    "The {feature} is not responding. Is there an outage in {region}?",
    "How do I enable {feature} feature?",
    "I can't get successful response. REST API is responding with {error} error.",
    "ALL USERS can't access {feature}"]

features = ["Dashboard", "Portal", "API", "Mobile App", "Admin Console"]
errors = ["404", "500 Internal Server", "Timeout", "Auth Failed"]
regions = ["PH-NCR", "PH-CL", "US-EAST", "UK-LON", "SG-CENTRAL"]

data = []
for i in range (1000):
  t_id = f"INC-{1000 + i}"

  # Random created date on April 2026
  created_date = datetime(2026, 4, random.randint(1,30), random.randint(0,23), random.randint(0,59), random.randint(0,59))
  first_response = created_date + timedelta(days=random.randint(0,1), seconds=random.randint(0,86400))
  resolved_at = first_response + timedelta(days=random.randint(0,6), seconds=random.randint(0,86400))

  text = random.choice(templates).format(
      feature = random.choice(features),
      error = random.choice(errors),
      person = "the agent",
      region = random.choice(regions)
  )

  data.append([t_id,
               created_date.strftime('%Y-%m-%d %H:%M:%S'),
               text,
               random.choice(regions),
               first_response.strftime('%Y-%m-%d %H:%M:%S'),
               resolved_at.strftime('%Y-%m-%d %H:%M:%S')
               ])

df = pd.DataFrame(data, columns=['ticket_id', 'created_date', 'customer_text', 'region', "first_response", "resolved_at"])
df.to_csv('raw_support_tickets.csv',index=False)

print(f"{len(df)} tickets generated! File saved as raw_support_tickets.csv")
