This is the whylogs container, a container that hosts
[whylogs](https://github.com/whylabs/whylogs) behind a REST interface that can
be used to generate whylogs profiles for data monitoring. This container is a
good choice for any whylogs user who doesn't want to embed the whylogs library
into their code base to generate profiles, or prefers to keep it separate from
their main application for any reason. It will handle the logic of profiling and
periodically uploading data to WhyLabs or S3 for you.

# Links
- [General Documentation][whylabs.ai docs]
- [Docker Hub Image](https://hub.docker.com/repository/docker/whylabs/whylogs)
- [Dokka Docs](https://whylabs.github.io/whylogs-container-docs)
- [Redoc API Overview](https://whylabs.github.io/whylogs-container-docs/whylogs-container)


# Configuration and Running

For most general purpose documentation on running and configuring the container,
see [general documentation][whylabs.ai docs].

# Developing

Most useful commands are targets in the Makefile. While doing local development,
you can run `make run` to build and run the server locally (without Docker).
Configuration is done via environment variables and you'll need to set some of
them to get the service working. This is a good minimal set of config.

```bash
UPLOAD_DESTINATION=DEBUG_FILE_SYSTEM
FILE_SYSTEM_WRITER_ROOT=my-profiles
WHYLOGS_PERIOD=DAYS
CONTAINER_API_KEY=password
PORT=8080
```

If you're running this from bash/zsh then you can add this to your environment
by running.

```bash
set -a
source local.env
set +a
```

Run `make help` for other useful targets. Mostly, you can depend on GitHub CI to
run the right tests and checks for each PR.

[whylabs.ai docs]: https://docs.whylabs.ai/docs/integrations-whylogs-container
