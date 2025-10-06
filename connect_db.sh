#!/bin/bash

# Check if a .env file exists and load its variables
if [ -f .env ]; then
  echo "--> Loading environment variables from .env file..."
  export $(cat .env | xargs)
fi

# Check if the variable was loaded successfully
if [ -z "$DB_CONNECTION_NAME" ]; then
    echo "Error: DB_CONNECTION_NAME is not set. Make sure it's in your .env file."
    exit 1
fi

echo "--> Authenticating with Google Cloud..."
gcloud auth application-default login

echo ""
echo "--> Starting Cloud SQL Auth Proxy..."
echo "Connecting to: $DB_CONNECTION_NAME"

# Start the proxy using the variable
./cloud-sql-proxy "$DB_CONNECTION_NAME"
