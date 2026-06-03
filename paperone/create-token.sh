#!/bin/sh
set -e

if [ -f /app/shared/grafana-token.txt ] && [ -s /app/shared/grafana-token.txt ]; then
  echo "Token already exists. Skipping creation."
  exit 0
fi

GRAFANA_URL="grafana:3000"
ADMIN_USER="admin"
ADMIN_PASSWORD="admin"

echo "Creating service account..."

SERVICE_ACCOUNT_ID=$(curl -s \
  -X POST \
"http://$ADMIN_USER:$ADMIN_PASSWORD@$GRAFANA_URL/api/serviceaccounts" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"external-service",
    "role":"Viewer"
  }' | jq -r '.id')

echo "Creating token for service account $SERVICE_ACCOUNT_ID..."

TOKEN=$(curl -s \
  -X POST \
"http://$ADMIN_USER:$ADMIN_PASSWORD@$GRAFANA_URL/api/serviceaccounts/$SERVICE_ACCOUNT_ID/tokens" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"external-service-token"
  }' | jq -r '.key')

if [ -z "$TOKEN" ]; then
  echo "Failed to create token. PNG generation might fail."
  exit 0
fi

if [ -d /app/shared ]; then
  echo "Token file already exists. Skipping folder creation."
else
  mkdir /app/shared
fi

echo "$TOKEN" > /app/shared/grafana-token.txt

echo "TOKEN CREATED:"
echo "$TOKEN"
