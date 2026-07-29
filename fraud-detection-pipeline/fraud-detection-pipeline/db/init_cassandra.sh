#!/usr/bin/env bash
# Applies the Cassandra schema to the running docker-compose 'cassandra' container.
# Usage: ./db/init_cassandra.sh
set -euo pipefail

CONTAINER_NAME="fraud-cassandra"
SCHEMA_FILE="$(dirname "$0")/cassandra_schema.cql"

echo "Waiting for Cassandra to accept connections..."
until docker exec "$CONTAINER_NAME" cqlsh -e "describe keyspaces" >/dev/null 2>&1; do
  sleep 3
  echo "  still waiting..."
done

echo "Applying schema from $SCHEMA_FILE ..."
docker exec -i "$CONTAINER_NAME" cqlsh < "$SCHEMA_FILE"
echo "Schema applied successfully."
