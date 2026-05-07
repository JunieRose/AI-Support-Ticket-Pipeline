# Analyze incident's sentiment and category through NLP library

# 1. Install the library
!pip install textblob

from textblob import TextBlob
import pandas as pd
from tqdm.notebook import tqdm

# 2. Load data

df = pd.read_csv('/content/raw_support_tickets.csv')

# 3. Local Analysis Function

def analyze_locally(text):
  # Get sentiment
  testimonial = TextBlob(str(text))
  sentiment = round(testimonial.sentiment.polarity,2)

  # Simple keyword category logic
  text_lower = str(text).lower()
  if any(word in text_lower for word in ['bill', 'charge', 'refund', 'money', 'invoice']):
    category = "Billing"
  elif any(word in text_lower for word in ['error', 'broken', 'bug', '404', '500', 'crash', 'timeout', 'outage']):
    category = "Technical"
  elif any(word in text_lower for word in ['password', 'login', 'account', 'access', 'reset', 'enable']):
    category = "Account"
  else:
    category = "Feedback"

  return sentiment, category

# 4. Process records

print(f"Processing {len(df)} records locally...")
results = []
for text in tqdm(df['customer_text']):
  results.append(analyze_locally(text))

# 4. Verification check and saving the file

if len(results) == len(df):
  df[['sentiment', 'category']] = pd.DataFrame(results, columns=['sentiment','category'])
  df.to_csv('enriched_tickets.csv', index=False)
  print(f"Success! {len(df)} records enriched and saved to 'enriched_tickets.csv'")

else:
  print(f"Error: Processed {len(results)} records but expected {len(df)}. Check if the loop finished.")

df.head()
