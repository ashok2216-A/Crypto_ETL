import sqlite3
import requests
import json

# API URL
API_URL = "https://api.coincap.io/v2/assets"  # Example of a live streaming API

# Fetch data from API
def fetch_data():
    response = requests.get(API_URL)
    return response.json()["data"]

# Create SQLite database and tables
def setup_database():
    conn = sqlite3.connect("etlenv/python_retail.db")
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
    return conn

# Insert data into the database
def insert_data(conn, assets):
    cursor = conn.cursor()
    for asset in assets:
        cursor.execute("""
            INSERT OR REPLACE INTO CryptoAssets (id, rank, symbol, name, supply, maxSupply, marketCapUsd, volumeUsd24Hr, priceUsd, changePercent24Hr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (asset["id"], int(asset["rank"]), asset["symbol"], asset["name"], float(asset["supply"]),
              float(asset["maxSupply"] or 0), float(asset["marketCapUsd"] or 0), float(asset["volumeUsd24Hr"] or 0),
              float(asset["priceUsd"] or 0), float(asset["changePercent24Hr"] or 0)))
    conn.commit()

# Main workflow
def main():
    assets = fetch_data()
    conn = setup_database()
    insert_data(conn, assets)
    conn.close()
    print("Data inserted successfully")

if __name__ == "__main__":
    main()