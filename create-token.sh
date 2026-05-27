#!/bin/sh

set -e

GRAFANA_URL="localhost:3000"
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

echo "$TOKEN" > /shared/grafana-token.txt

echo "TOKEN CREATED:"
echo "$TOKEN"
