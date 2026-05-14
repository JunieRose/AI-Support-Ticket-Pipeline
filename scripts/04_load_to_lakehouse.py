"""
Script Name: load_to_lakehouse.py
Description: Performs a bulk insert of enriched support ticket data into 
             an Oracle Autonomous Database (Lakehouse).
"""

import oracledb
import pandas as pd
import sys
from google.colab import userdata

# Configuration - Table Schema Mapping
TARGET_TABLE = "support_tickets"
INPUT_FILE = 'enriched_tickets.csv'

def load_data_to_lakehouse():
  """Reads local CSV and performs a batch insert into OCI Database."""

  # Load and validate data
  try:
    df = pd.read_csv(INPUT_FILE)
    # Convert DataFrame to a list of tuples for the database driver
    records = df[[
        'ticket_id', 'created_at', 'customer_text',
        'region', 'first_response_at', 'sentiment', 'category'
    ]].values.tolist()

  except FileNotFoundError:
    print(f"Error: {INPUT_FILE} not found. Please run the analyzer script first.")
    return

  # Database Authentication (using Colab Secrets)
  db_user = userdata.get('oci_db_user')
  db_password = userdata.get('oci_db_password')
  db_dsn = userdata.get('oci_db_dsn')

  conn = None
  try:
    # Establish connection
    print(f"Connecting to Oracle Autonomous Database...")
    conn = oracledb.connect(user=db_user, password=db_password, dsn=db_dsn)
    cursor = conn.cursor()

    # Define SQL with bind variables (Prevents SQL Injection)
    # Using TO_TIMESTAMP ensures date strings are handled correctly by Oracle
    sql = f"""
        INSERT INTO {TARGET_TABLE} (
          ticket_id, created_at, customer_text, region, 
          first_response_at, sentiment, category
        ) VALUES (
          :1, TO_TIMESTAMP(:2, 'YYYY-MM-DD HH24:MI:SS'), :3, :4, 
          TO_TIMESTAMP(:5, 'YYYY-MM-DD HH24:MI:SS'), :6, :7
        )
    """

    # Execute Bulk Load
    print(f"Executing bulk insert: {len(records)} records...")
    cursor.executemany(sql, records)

    # Commit the transaction
    conn.commit()
    print(f"Transaction committed. {cursor.rowcount} rows inserted successfully.")
    
  except oracledb.Error as db_err:
    print(f"Database Error: {db_err}")
  except Exception as e:
    print(f"Unexpected Error: {e}")

  finally:
    # Resource Cleanup
    if conn:
      cursor.close()
      conn.close()
      print("Database connection closed.")

if __name__ == "__main__":
  load_data_to_lakehouse()
