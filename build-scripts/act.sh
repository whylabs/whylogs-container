#!/bin/bash

# Startup the artifact server
echo "Starting artifact server on 8080"
docker run --rm -p 127.0.0.1:8080:8080 --env AUTH_KEY=foo --name artifact-server ghcr.io/jefuller/artifact-server:latest &

# Run act
act --env ACTIONS_CACHE_URL=http://localhost:8080/ --env ACTIONS_RUNTIME_URL=http://localhost:8080/ --env ACTIONS_RUNTIME_TOKEN=foo --env GITHUB_RUN_ID=$(date '+%s') --secret-file="$HOME/.act-secrets" || true

# Remove the artifact server
echo "Shutting down artifact server"
docker stop artifact-server
