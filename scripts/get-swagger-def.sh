#!/bin/bash
trap 'kill $(jobs -p)' EXIT

# Inherited from github workflow env
# DOC_DIR=build/dokka/html

mkdir -p $DOC_DIR

WHYLOGS_PERIOD=HOURS ORG_ID=0 CONTAINER_API_KEY=0 UPLOAD_DESTINATION=DEBUG_FILE_SYSTEM ./gradlew run &

while ! curl -s http://localhost:8080/swagger-docs >/dev/null ; do sleep 1; done ; echo "Server up"

echo "Getting swagger definition from local server."
DEF=$(curl http://localhost:8080/swagger-docs)
echo $DEF | tee $DOC_DIR/swagger.json

echo "$(jobs -p)"
