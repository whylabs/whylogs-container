FROM amd64/amazoncorretto:15-alpine

COPY build/install/whylogs-container /opt/whylogs
COPY scripts/ /opt/whylogs/scripts/

# Sqlite will come in handy for debugging if something goes terribly wrong. We use
# a sqlite database to persist dataset profiles across server runs.
RUN apk update && apk add --no-cache sqlite=3.32.1-r1 ngrep

EXPOSE 8080
WORKDIR /opt/whylogs

CMD ["/opt/whylogs/bin/whylogs-container"]
