FROM amd64/amazoncorretto:15-alpine

COPY build/install/whylogs-container /opt/whylogs
COPY supervisord.conf /opt/whylogs
COPY scripts/ /opt/whylogs/scripts/

RUN apk update && apk add --no-cache python3 supervisor

EXPOSE 8080
WORKDIR /opt/whylogs

ENTRYPOINT ["supervisord", "-c", "/opt/whylogs/supervisord.conf"]
