#!/bin/bash

# This script imports data from a JSON dump file into the Phalanx DB v4 REST server.
#
# Usage:
# 1. Make sure the phalanx_db_rest_v4.js server is running.
# 2. Run this script from your terminal: ./import_data.sh
#
# You can customize the API_URL and DUMP_FILE variables below if needed.

# Configuration
API_URL="http://localhost:8888/api/entries"
DUMP_FILE="phalanx_db_dump.json"

# Check if dump file exists
if [ ! -f "$DUMP_FILE" ]; then
    echo "Error: Dump file not found at $DUMP_FILE"
    exit 1
fi

echo "Importing data from $DUMP_FILE to $API_URL..."
echo "This may take a few moments for a large file..."

# Send data using curl
# The --data @"$DUMP_FILE" argument tells curl to read the data from the specified file.
curl -X POST -H "Content-Type: application/json" --data @"$DUMP_FILE" "$API_URL"

echo -e "\n\nImport process finished."
echo "Check the server logs for any potential errors." 