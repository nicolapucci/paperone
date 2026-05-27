#!/bin/sh

set -e

GRAFANA_URL="http://localhost:3000"
ADMIN_USER="admin"
ADMIN_PASSWORD="admin"

echo "Creating service account..."

SERVICE_ACCOUNT_ID=$(curl -s \
  -X POST \
  -H "Content-Type: application/json" \
  -u "$ADMIN_USER:$ADMIN_PASSWORD" \
  "$GRAFANA_URL/api/serviceaccounts" \
  -d '{
    "name":"external-service",
    "role":"Admin"
  }' | jq -r '.id')

echo "Creating token..."

TOKEN=$(curl -s \
  -X POST \
  -H "Content-Type: application/json" \
  -u "$ADMIN_USER:$ADMIN_PASSWORD" \
  "$GRAFANA_URL/api/serviceaccounts/$SERVICE_ACCOUNT_ID/tokens" \
  -d '{
    "name":"external-service-token"
  }' | jq -r '.key')

echo "$TOKEN" > /shared/grafana-token.txt

echo "TOKEN CREATED:"
echo "$TOKEN"
