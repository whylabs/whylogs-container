FROM amd64/amazoncorretto:15
COPY build/install/whylogs-container /opt/whylogs
EXPOSE 8080
WORKDIR /opt/whylogs
ENTRYPOINT ["/opt/whylogs/bin/whylogs-container"]
