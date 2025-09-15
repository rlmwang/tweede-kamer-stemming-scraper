#!/usr/bin/env bash
set -euo pipefail

# Load variables from .env
if [ -f default.env ]; then
  export $(grep -v '^#' default.env | xargs)
else
  echo "default.env file not found!"
  exit 1
fi

# Required vars (with defaults if not set in .env)
DB_NAME="${POSTGRES_DB:-stemmingsuitslagen}"
DB_USER="${POSTGRES_USER:-postgres}"
DB_PASS="${POSTGRES_PASSWORD:-postgres}"
DB_PORT="${POSTGRES_PORT:-5432}"
CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-tweede-kamer-db}"

# Check if container already exists
if [ "$(docker ps -aq -f name=${CONTAINER_NAME})" ]; then
  echo "Container ${CONTAINER_NAME} already exists."
  echo "Starting it..."
  docker start "${CONTAINER_NAME}"
else
  echo "Creating and starting container ${CONTAINER_NAME}..."
  docker run --name "${CONTAINER_NAME}" \
    -e POSTGRES_DB="${DB_NAME}" \
    -e POSTGRES_USER="${DB_USER}" \
    -e POSTGRES_PASSWORD="${DB_PASS}" \
    -p "${DB_PORT}:5432" \
    -d postgres:16
fi
