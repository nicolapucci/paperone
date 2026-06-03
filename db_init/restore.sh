#!/bin/bash
set -e

echo "Cleaning dump..."

  sed \
    -e '/OWNER TO/d' \
    -e '/SET SESSION AUTHORIZATION/d' \
    -e '/GRANT /d' \
    -e '/REVOKE /d' \
    /docker-entrypoint-initdb.d/bugia.sql.old \
    > /tmp/clean.sql

  echo "Importing cleaned dump..."

  psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -f /tmp/clean.sql

else
  echo "No dump file found. Skipping import."
fi