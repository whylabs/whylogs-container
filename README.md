Image on [Docker Hub](https://hub.docker.com/repository/docker/whylabs/whylogs)
Docs on [Github Pages](https://whylabs.github.io/whylogs-container-docs)

## Before Pushing

You can test the Github CI by running `./build-scripts/act.sh` after you install [act](https://github.com/nektos/act). This will set up the artifact server that you need locally along side the act run, since the build depends on having one present.


### act Setup

- Create `~/.act-events.json` with this content: `{"act": true}` to enable skipping certain steps locally.

## Building Docker image

- To build the docker image (locally):

```
make build-docker
```

## Running the Docker image

- You should prepare a `local.env` file in your workspace with the following content. Make sure you configure the heap to the appropriate size of your
  container. The heap size also determines how many concurrent dataset profiles you can track at the same time (i.e. number of tag key-value combination). You can omit it for quick tests is the JVM defaults are fine of course.

```
WHYLOGS_PERIOD=HOURS (default is HOURS if unspecified. Supported values: MINUTES, HOURS, DAYS)
JAVA_OPTS=-XX:+UseZGC -XX:+UnlockExperimentalVMOptions -XX:-ZUncommit -Xmx4G

# Optional destination. Defaults to WHYLABS.
UPLOAD_DESTINATION=WHYLABS # WHYLABS | S3

# Required for uploading to S3
S3_BUCKET=my-s3-bucket
# Optional for uploading to S3. Defaults to the empty string. Prefix name without a `/`.
S3_PREFIX=whylogs_profiles

# Optional alternate api endpoint
WHYLABS_API_ENDPOINT=http://localhost:8080
# Required for whylabs uploads. Hard coded api key for WhyLabs that the container always uses when uploading profiles.
WHYLABS_API_KEY=xxxxxx

# Each request to the container will have to provide an X-API-Key header with the
# following value.
CONTAINER_API_KEY=secret-key

# Required for uploading to whylabs. Specify an organization ID that is accessible with your WHYLABS_API_KEY.
ORG_ID=org-10

# Optional for forcing the container to send empty dataset profiles even when no data has been sent to it.
# Without this, you won't actually get any profiles uploaded if you don't send data which can make it difficult
# to tell if there is a problem or just no data flowing through by chance.
EMPTY_PROFILE_DATASET_IDS = ["dataset-id-1", "dataset-id-2"]

# OPTIONAL additional set of strings considered to be null values.
# Do not include spaces or quotes around the strings. This value is picked up in the env
# within whylogs-java at runtime.
# NULL_STRINGS=nil,NaN,nan,null

# Optional request queueing mode, selecting for perf or reliablity in incoming  requests.
REQUEST_QUEUEING_MODE=SQLITE #default
REQUEST_QUEUEING_MODE=IN_MEMORY
```

- Run the Docker image with the following command:

```
docker run -it --rm -p 127.0.0.1:8080:8080 --env-file local.env --name whycontainer whycontainer
```

- Or run the service directly without docker

```
make run

# which just runs
./gradlew run
```

You can see a bunch of canned commands with `make help` as well. The makefile is essentially a list of canned commands for this repository.

If you run it directly then you'll need to make sure the env variables are in your shell environment since docker isn't there to load them for you anymore.

## Writing dataset profiles

The container can be configured to send data to WhyLabs or S3. There are some configuration options that only apply to one of those cases.

When writing to S3, the `EMPTY_PROFILE_DATASET_IDS` can optionally include a list of dataset ids that you want to ensure the container always uploads data for,
regardless of whether any data has been sent to the container for a given time period. We include various metadata as s3 metadata.

- `whylogs-dataset-epoch-millis` - The dataset timestamp as a milli time. This is the time that the dataset profile was created, and it's assumed to be the time
  that the oldest data it contains was created too. This will be the start of a time period (minute/hour/day), not the actual first time data is sent.
- `whylogs-session-id` - A unique UUID generated when the profile is created.
- `whylogs-dataset-id` - The dataset id that was specified either in the API or as part of the `EMPTY_PROFILE_DATASET_IDS` config. It's effectively a string for
  you to tie back to a model or dataset that this data is a part of.
- `whylogs-session-epoch-millis` - The milli time that the container was started.
- `whylogs-segment-tags` - The tags used in this profile.

## Controlling the live REST service

The container runs supervisord as its main command so it won't ever exit on its own. You can manipulate the rest server from within the container without
shutting down the container by using supervisord as follows.

```
# Connect to the running container, assuming you used `--name whycontainer` to run it.
docker exec -it whycontainer sh

# Restart the server
./scripts/restart-server.sh

# The script is a covenience around supervisorctl. You can manually run
supervisorctl -c /opt/whylogs/supervisord.conf restart app
supervisorctl -c /opt/whylogs/supervisord.conf start app
supervisorctl -c /opt/whylogs/supervisord.conf stop app
```

The rest server does make use of temporary files on its file system so restarting the rest server without terminating the container has the advantage of not
wiping out the ephemeral storage if it would have resulted in the container getting destroyed.

## Inspecting the persistent storage

The persistence is accomplished through a map abstraction that eventually calls sqlite. You can directly interface with sqlite in the container to verify its
contents.

```
# Connect to the running container, assuming you used `--name whycontainer` to run it.
docker exec -it whycontainer sh

# Dump the content of the sqlite databse
./scripts/query-profiles.sh

# The script just calls sqlite with the locaiton of the db and a query
sqlite3 /tmp/profile-entries-map.sqlite 'select * from items;'
```

The db is pretty simple. Just a key and value column where the values are serialized dataset profiles. The dataset profiles are base64 encoded strings based on
the output of the protobuf serialization. Everything else is human readable.

## Monitoring traffic

Sometimes it's useful to monitor network traffic if you're having issues actually connecting to songbird or some request is failing for a mysterious reason. You
can use `ngrep` to show the headers, url, etc.

```
# Connect to the container
docker exec -it whycontainer sh

ngrep -q -W byline
```
