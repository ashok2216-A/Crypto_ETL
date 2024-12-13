from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import sqlite3
import json
import pandas as pd


# Functions for ETL pipeline
def extract():
    API_URL = "https://api.coincap.io/v2/assets" # Example of a live streaming API
    response = requests.get(API_URL)
    with open("airflow_data/cryptodata.json", "w") as f:
        f.write(response.text)


def transform():
    with open("airflow_data/cryptodata.json", "r") as f:
        data = json.load(f)["data"]
    transformed_data = [
        {
            "id": p["id"],"rank": p["rank"],"symbol": p["symbol"],"name": p["name"],
            "supply": p["supply"],"maxSupply": p["maxSupply"],"marketCapUsd": p["marketCapUsd"],
            "volumeUsd24Hr": p["volumeUsd24Hr"],
            "priceUsd": float(p["priceUsd"]) * 1.1,  # Example transformation: Add 10% markup
            "changePercent24Hr": p["changePercent24Hr"]
        } for p in data
    ]
    with open("airflow_data/transformed_cryptodata.json", "w") as f:
        json.dump(transformed_data, f)



def load():
    conn = sqlite3.connect("airflow_retail.db")
    cursor = conn.cursor()

    cursor.execute(""" 
        CREATE TABLE IF NOT EXISTS CryptoAssets (
            id TEXT PRIMARY KEY,
            rank INTEGER,
            symbol TEXT,
            name TEXT,
            supply REAL,
            maxSupply REAL,
            marketCapUsd REAL,
            volumeUsd24Hr REAL,
            priceUsd REAL,
            changePercent24Hr REAL
        )
    """)
    conn.commit()
    
    # Open transformed data file
    with open("airflow_data/transformed_cryptodata.json", "r") as f:
        data = json.load(f)
        
        for p in data:
            cursor.execute("""
                INSERT OR REPLACE INTO CryptoAssets (id, rank, symbol, name, supply, maxSupply, marketCapUsd, volumeUsd24Hr, priceUsd, changePercent24Hr)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                p["id"], p["rank"], p["symbol"], p["name"], p["supply"], p["maxSupply"], p["marketCapUsd"], p["volumeUsd24Hr"], p["priceUsd"], p["changePercent24Hr"]
            ))
    conn.commit()

    query = "SELECT * FROM CryptoAssets"
    df = pd.read_sql(query, conn)
    df.to_csv('airflow_data/airflow_retail.csv', index=False)

    conn.close()

# Define DAG
default_args = { 
    'owner': 'airflow', 
    'start_date': datetime(2024, 12, 10), 
    'retries': 1
}

dag = DAG("airflow_etl", default_args=default_args, schedule_interval="* * * * *")

extract_task = PythonOperator(task_id="extract", python_callable=extract, dag=dag)
transform_task = PythonOperator(task_id="transform", python_callable=transform, dag=dag)
load_task = PythonOperator(task_id="load", python_callable=load, dag=dag)

extract_task >> transform_task >> load_task