from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import sqlite3
import json
import pandas as pd


# Functions for ETL pipeline
def extract(api_url, output_file):
    response = requests.get(api_url)
    response.raise_for_status()  # Ensure the request was successful
    with open(output_file, "w") as f:
        f.write(response.text)


def transform(input_file, output_file, transformations=None):
    with open(input_file, "r") as f:
        data = json.load(f)

    # Ensure the input data is in a list format
    records = data.get("data", data)

    transformed_data = []
    for record in records:
        transformed_record = {}
        for key, value in record.items():
            if transformations and key in transformations:
                # Apply transformation if defined
                transformed_record[key] = transformations[key](value)
            else:
                # Keep the value unchanged
                transformed_record[key] = value
        transformed_data.append(transformed_record)

    with open(output_file, "w") as f:
        json.dump(transformed_data, f, indent=4)


def load(input_file, db_file, table_name):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Open transformed data file
    with open(input_file, "r") as f:
        data = json.load(f)

    # Dynamically create the table
    if data:
        columns = data[0].keys()
        column_definitions = ", ".join([f"{col} TEXT" for col in columns])  # Use TEXT as default type
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions})")

        # Insert data dynamically
        placeholders = ", ".join(["?"] * len(columns))
        for record in data:
            cursor.execute(
                f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                tuple(record.values())
            )
    conn.commit()

    # Save data to CSV
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    df.to_csv(f'airflow_data/{table_name}.csv', index=False)

    conn.close()


# Define DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 10),
    'retries': 1
}

dag = DAG("airflow_generic_etl", default_args=default_args, schedule_interval="* * * * *")

# Configuration for each endpoint
ENDPOINTS = [
    {"name": "Assets", "url": "https://api.coincap.io/v2/assets"},
    {"name": "Rates", "url": "https://api.coincap.io/v2/rates"},
    {"name": "Exchanges", "url": "https://api.coincap.io/v2/exchanges"},
    {"name": "Markets", "url": "https://api.coincap.io/v2/markets"},
    {"name": "Candles", "url": "api.coincap.io/v2/candles?exchange=poloniex&interval=h8&baseId=ethereum&quoteId=bitcoin"}
]

for endpoint in ENDPOINTS:
    input_file = f"airflow_data/{endpoint['name'].lower()}_data.json"
    transformed_file = f"airflow_data/{endpoint['name'].lower()}_transformed_data.json"
    db_file = "airflow_retail.db"
    table_name = f"{endpoint['name']}"

    extract_task = PythonOperator(
        task_id=f"extract_{endpoint['name'].lower()}",
        python_callable=extract,
        op_kwargs={"api_url": endpoint["url"], "output_file": input_file},
        dag=dag
    )

    transform_task = PythonOperator(
        task_id=f"transform_{endpoint['name'].lower()}",
        python_callable=transform,
        op_kwargs={
            "input_file": input_file,
            "output_file": transformed_file,
            "transformations": None  # Add specific transformations if needed
        },
        dag=dag
    )

    load_task = PythonOperator(
        task_id=f"load_{endpoint['name'].lower()}",
        python_callable=load,
        op_kwargs={
            "input_file": transformed_file,
            "db_file": db_file,
            "table_name": table_name
        },
        dag=dag
    )

    extract_task >> transform_task >> load_task
