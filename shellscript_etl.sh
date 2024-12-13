#!/bin/bash
# Directory to store the data
DATA_DIR="etlenv/data"
mkdir -p $DATA_DIR
# API Endpoint
API_URL="https://api.coincap.io/v2/assets"
# Temporary file to store fetched data
temp_file="$DATA_DIR/temp_cryptodata.json"
# Fetch data and save it to a temporary file
curl -s $API_URL -o $temp_file
# Append the fetched data to the existing file
data_file="$DATA_DIR/cryptodata.json"
cat $temp_file >> $data_file
# Remove the temporary file
rm $temp_file
echo "Data fetched and appended to $data_file"