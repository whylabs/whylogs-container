FROM amd64/amazoncorretto:15-alpine

COPY build/install/whylogs-container /opt/whylogs
COPY supervisord.conf /opt/whylogs
COPY scripts/ /opt/whylogs/scripts/

# Sqlite will come in handy for debugging if something goes terribly wrong. We use
# a sqlite database to persist dataset profiles across server runs.
RUN apk update && apk add --no-cache python3 supervisor sqlite

EXPOSE 8080
WORKDIR /opt/whylogs

ENTRYPOINT ["supervisord", "-c", "/opt/whylogs/supervisord.conf"]
