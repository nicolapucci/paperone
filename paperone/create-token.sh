#!/bin/sh
set -e

if [ -f /app/shared/grafana-token.txt ] && [ -s /app/shared/grafana-token.txt ]; then
  echo "Token already exists. Skipping creation."
  exit 0
fi

GRAFANA_URL="grafana:3000"
ADMIN_USER="admin"
ADMIN_PASSWORD="admin"


SERVICE_ACCOUNTS=$(curl -s \
  -X GET \
"http://$ADMIN_USER:$ADMIN_PASSWORD@$GRAFANA_URL/api/serviceaccounts/search" \
  -H "Content-Type: application/json" | jq -r '.')

echo "Checking for existing service accounts... {$SERVICE_ACCOUNTS}"

MESSAGE=$(echo "$SERVICE_ACCOUNTS" | jq -r '.message // empty')

if [ -n "$MESSAGE" ]; then
  echo "Error fetching service accounts: $SERVICE_ACCOUNTS $(echo "$SERVICE_ACCOUNTS" | jq -r '.message')"
else
  echo "Found existing service accounts."

  SERVICE_ACCOUNT_ID=$(echo "$SERVICE_ACCOUNTS" | jq -r '.serviceAccounts[] | select(.name=="external-service").id')
  echo "Service account ID for 'external-service': $SERVICE_ACCOUNT_ID"
  if [ -n "$SERVICE_ACCOUNT_ID" ]; then
    echo "Service account 'external-service' already exists. Using existing service account."
  fi
fi

if [ -z "$SERVICE_ACCOUNT_ID" ]; then
  echo "Creating new service account 'external-service'..."

  SERVICE_ACCOUNT_ID=$(curl -s \
    -X POST \
  "http://$ADMIN_USER:$ADMIN_PASSWORD@$GRAFANA_URL/api/serviceaccounts" \
    -H "Content-Type: application/json" \
    -d '{
      "name":"external-service",
      "role":"Viewer"
    }' | jq -r '.id')

  echo "Creating token for service account $SERVICE_ACCOUNT_ID..."

  if [ -z "$SERVICE_ACCOUNT_ID" ]; then
    echo "Failed to create service account. PNG generation might fail."
    exit 0
  fi
fi

EXISTING_TOKEN=$(curl -s \
  -X GET \
"http://$ADMIN_USER:$ADMIN_PASSWORD@$GRAFANA_URL/api/serviceaccounts/$SERVICE_ACCOUNT_ID/tokens" \
  -H "Content-Type: application/json" | jq -r '.[]')

if [ -n "$EXISTING_TOKEN" ]; then

  EXTERNAL_SERVICE_TOKEN=$(echo "$EXISTING_TOKEN" | jq -r 'select(.name=="external-service-token")')
  if [ -n "$EXTERNAL_SERVICE_TOKEN" ]; then
    echo "Token 'external-service-token' already exists. Deleting existing token to create a new one."
  
    curl -s \
      -X DELETE \
    "http://$ADMIN_USER:$ADMIN_PASSWORD@$GRAFANA_URL/api/serviceaccounts/$SERVICE_ACCOUNT_ID/tokens/$(echo "$EXTERNAL_SERVICE_TOKEN" | jq -r '.id')" \
      -H "Content-Type: application/json"
  
  else
    echo "Token 'external-service-token' not found. Creating new token...."
  fi
fi

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
