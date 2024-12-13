import sqlite3
import pandas as pd

# Connect to your SQLite database
conn = sqlite3.connect('airflow_retail.db')

# Query to select all data from a specific table
query = "SELECT * FROM CryptoAssets"

# Load the data into a pandas DataFrame
df = pd.read_sql(query, conn)

# Save the DataFrame to a CSV file
df.to_csv('airflow_data/airflow_retail.csv', index=False)

print(df)
# Close the connection
conn.close()
