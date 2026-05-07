# Load the file to OCI Autonomous Database

# 1. Install the driver
!pip install oracledb

import oracledb
import pandas as pd

# 2. Load local file
df_final = pd.read_csv('enriched_tickets.csv')

# 3. Prepare data - Order of columns should match the table columns
# Convert dataframe to a list of tuples
data_to_load = df_final[['ticket_id', 'created_date', 'customer_text', 'region', 'first_response', 'resolved_at', 'sentiment', 'category']].values.tolist()

# 4. Bulk insert
# SECURITY NOTE: Credential removed for GitHub portfolio

user = "REDACTED_USER"
password = "REDACTED_PASSWORD"
dsn = "REDACTED_DSN"

try:
  conn = oracledb.connect(user=user, password=password, dsn=dsn, config_dir=None)
  cursor = conn.cursor()

  sql = """INSERT INTO silver_tickets(
    ticket_id, created_date, customer_text, region, first_response, resolved_at, sentiment, category)
    VALUES (:1, TO_TIMESTAMP(:2, 'YYYY-MM-DD HH24:MI:SS'), :3, :4, TO_TIMESTAMP(:5, 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(:6, 'YYYY-MM-DD HH24:MI:SS'), :7, :8)"""

  print(f"Starting bulk load of {len(data_to_load)} records...")
  cursor.executemany(sql, data_to_load)
  conn.commit()
  print(f"Success! {cursor.rowcount} rows are now in the Lakehouse.")

except Exception as e:
  print(f"Error during load: {e}")

# 5. Close connection
finally:
  if 'conn' in locals():
    cursor.close()
    conn.close()
