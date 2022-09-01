#!/bin/bash
trap 'kill $(jobs -p)' EXIT

# Inherited from github workflow env
DOC_DIR="${DOC_DIR:-build/dokka/html}"

mkdir -p $DOC_DIR

WHYLOGS_PERIOD=HOURS ORG_ID=0 CONTAINER_API_KEY=0 UPLOAD_DESTINATION=DEBUG_FILE_SYSTEM ./gradlew run &

while ! curl -s http://localhost:8080/swagger-docs >/dev/null ; do sleep 1; done ; echo "Server up"

echo "Getting swagger definition from local server."
curl http://localhost:8080/swagger-docs > $DOC_DIR/swagger.json
cat  $DOC_DIR/swagger.json

if [ "$1" = "redoc" ]; then
  # For running locally, supply "redoc" to the script as the first arg to also generate swagger docs
  # Can install redoc-cli with `npm i -g redoc-cli`
  redoc-cli build -o $DOC_DIR/whylogs-container.html $DOC_DIR/swagger.json
  echo "Generating swagger doc with redoc at $DOC_DIR/whylogs-container.html"
fi

echo "$(jobs -p)"
