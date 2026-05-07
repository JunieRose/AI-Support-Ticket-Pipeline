# Analyze incident's sentiment and category through generative AI

# 1. Install the library
!pip install -q -U google-generativeai

import google.generativeai as genai
import pandas as pd
import time

# 2. Setup Gemini
# SECURITY NOTE: Credential removed for GitHub portfolio
genai.configure(api_key = "REDACTED_API_KEY")
model = genai.GenerativeModel('models/gemini-2.5-flash')

# 3. Load the data generated
# This is a new data that only contains 5 records to prevent Error 429 Resource Exhausted
df= pd.read_csv('/content/raw_support_tickets_5.csv')

def analyze_ticket(text):
  prompt = f"""Analyze this support ticket: "{text}"
  Provide a response in exactly this format: Sentiment|Category
  Sentiment: A number from -1 to (very angry) to 1 (very happy).
  Category: One of: Technical, Account, Billing, Outage, How-To Question or Feedback.
  """
  try:
    response = model.generate_content(prompt)
    content = response.text.strip()
    return content if '|' in content else "0|General"
  except Exception as e:
    return f"0|Error:{str(e)[:20]}"

# 4. Process the data
print("Starting AI enrichment...")
results = []
for i, text in enumerate(df['customer_text']):
  results.append(analyze_ticket(text))
  print(f"processed {i+1} of {len(df)}")
  # Rate limit management
  time.sleep(4)

# 5. Split result into new columns
df[['sentiment', 'category']] = [res.split('|') for res in results]

# 6. Save the enriched data
df.to_csv('enriched_tickets_genai.csv', index=False)
print("AI Enriched Complete: Enriched file saved!")
