"""
Script Name: process_ai_enrichement.py
Description: Processes support tickets to extract sentiment and category.
             Uses Gemini AI with a local TextBlob fallback for rate-limiting.
"""

import pandas as pd
import time
import os
from google import genai
from google.genai import types
import google.genai.errors as errors
from textblob import TextBlob
from google.colab import userdata

# Get GEMINI API Key in local or Colab environments
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
  try:
    from google.colab import userdata
    GEMINI_API_KEY = userdata.get('GEMINI_API_KEY')
  except ImportError:
    GEMINI_API_KEY = None

if not GEMINI_API_KEY:
    raise ValueError("API Key not found in Environment or Colab Secrets!")

INPUT_FILE = "raw_support_tickets.csv"
OUTPUT_FILE = "enriched_tickets.csv"

def analyze_locally(text):
  """Fallback NLP logic using TextBlob and Keyword Matching."""
  testimonial = TextBlob(str(text))
  sentiment = round(testimonial.sentiment.polarity,1)

  text_lower = str(text).lower()
  mapping = {
      "Billing": ['bill', 'charge', 'refund', 'money', 'invoice'],
      "Technical": ['error', 'broken', 'bug', '404', '429', '500', 'crash', 'timeout', 'outage'],
      "Account": ['password', 'login', 'account', 'reset', 'enable']
  }
  
  category = "Feedback" # Default
  for cat, keywords in mapping.items():
    if any(word in text_lower for word in keywords):
      category = cat
      break
      
  return sentiment, category


def get_ai_analysis(client, text):
  """Calls Gemini API to analyze ticket content."""
  prompt = f"""
  Analyze the sentiment and category of this support ticket.
  Format your response EXACTLY as: sentiment_score|category_name
  
  Rules:
  - sentiment_score: a number between -1.0 and 1.0
  - category_name: one of [Technical, Account, Billing, How-To Question, Feedback]
  
  Ticket: "{text}"
  """
  response = client.models.generate_content(
      model='gemini-3-flash-preview',
      contents=prompt,
      config=types.GenerateContentConfig(
          temperature=0.1
          )
      )
  
  content = response.text.strip()
  if '|' in content:
    score, cat = content.split('|')
    return float(score), cat
  return 0.0, "General"

def process_tickets():
  """Main execution logic to read, analyze, and save data."""
  if not os.path.exists(INPUT_FILE):
    print(f"Error: {INPUT_FILE} not found.")
    return

  df = pd.read_csv(INPUT_FILE)
  client = genai.Client(api_key=GEMINI_API_KEY)

  final_results = []

  print(f"Starting analysis for {len(df)} records...")

  for i, text in enumerate(df['customer_text']):
    try:
      # Attempt AI Analysis
      sentiment, category = get_ai_analysis(client, text)
      print(f"[{i+1}] AI Success: {category}")
      # Small delay for free tier stability
      time.sleep(8)

    except Exception as e:
      # If 429 (Rate Limit) or any other API error, use fallback
      print(f"[{i+1}] AI failed or throttled. Using NLP Fallback...")
      sentiment, category = analyze_locally(text)
      
    final_results.append({'sentiment': sentiment, 'category': category})
      
  # Merge results back to original DataFrame
  results_df = pd.DataFrame(final_results)
  df = pd.concat([df, results_df], axis=1)

  # Clean up and Save
  df['sentiment'] = pd.to_numeric(df['sentiment'], errors='coerce').fillna(0)
  df.to_csv(OUTPUT_FILE, index=False)
  print(f"✅ Processing complete. Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    process_tickets()
