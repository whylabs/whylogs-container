## Building Docker image
* To build the docker image (locally):

```
$ MAVEN_TOKEN=xxxxx gw installDist
$ docker build . -t whycontainer
```

* Or run a single command:
```
MAVEN_TOKEN=xxxxx gw installDist && docker build . -t whycontainer
```

You need to create an api token in Gitlab that has API permissions in order to authenticate with our private Gitlab
repo that contains dependencies we consume here.

## Running the Docker image

* You should prepare a `local.env` file in your workspace with the following content. Make sure you configure
the heap to the appropriate size of your container. The heap size also determines how many concurrent dataset profiles
you can track at the same time (i.e. number of tag key-value combination)

```
WHYLOGS_PERIOD=HOURS (default is HOURS if unspecified. Supported values: MINUTES, HOURS, DAYS)
JAVA_OPTS=-XX:+UseZGC -XX:+UnlockExperimentalVMOptions -XX:-ZUncommit -Xmx4G

# Optional alternate api endpoint
WHYLABS_API_ENDPOINT=http://localhost:8080
WHYLABS_API_KEY=xxxxxx
WHYLOGS_PERIOD=HOURS

# Specify the api key that will be checked on each request. The header API key
# must match this value.
CONTAINER_API_KEY=secret-key

# Specify an organization ID that is accessible with your WHYLABS_API_KEY.
ORG_ID=org-10

# OPTIONAL additional set of strings considered to be null values.
# Do not include spaces or quotes around the strings.
# NULL_STRINGS=nil,NaN,nan,null
```

* Run the Docker image with the following command:

```
docker run -it --rm -p 127.0.0.1:8080:8080 --env-file local.env --name whycontainer whycontainer
```

## Controlling the live REST service

The container runs supervisord as its main command so it won't ever exit on its
own. You can manipulate the rest server from within the container without
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

The rest server does make use of temporary files on its file system so restarting
the rest server without terminating the container has the advantage of not
wiping out the ephemeral storage if it would have resulted in the container
getting destroyed.

## Inspecting the persistent storage

The persistence is accomplished through a map abstraction that eventually calls
sqlite. You can directly interface with sqlite in the container to verify its
contents.

```
# Connect to the running container, assuming you used `--name whycontainer` to run it.
docker exec -it whycontainer sh

# Dump the content of the sqlite databse
./scripts/query-profiles.sh

# The script just calls sqlite with the locaiton of the db and a query
sqlite3 /tmp/profile-entries-map.sqlite 'select * from items;'
```

The db is pretty simple. Just a key and value column where the values are
serialized dataset profiles. The dataset profiles are base64 encoded strings
based on the output of the protobuf serialization. Everything else is human
readable.


## Monitoring traffic

Sometimes it's useful to monitor network traffic if you're having issues
actually connecting to songbird or some request is failing for a mysterious
reason. You can use `ngrep` to show the headers, url, etc.

```
# Connect to the container
docker exec -it whycontainer sh

ngrep -q -W byline
```

## Publishing the image

You can use the `./publish_scripts/ecr-publish.mk` script to publish the
container to our prod ECR. You'll have to make sure the IAM user that you use
with the AWS CLI is allowed to push images by adding yourself to
`https://console.aws.amazon.com/ecr/repositories/private/003872937983/whylogs-container/permissions/?region=us-east-1`.

```
./publish_scripts/ecr-publish.mk authenticate
MAVEN_TOKEN=xxxxx ./publish_scripts/ecr-publish.mk publish
```

This will force a build at publish time to make sure that you're pushing the
most recent image so you'll need to supply your `MAVEN_TOKEN` again. Images are
tagged with the current date and time and they're visible at
`https://console.aws.amazon.com/ecr/repositories/private/003872937983/whylogs-container?region=us-east-1`.
We don't use a `latest` tag because AWS ECR's immutable tag feature doesn't
permit it. The value of having immutable tags was higher than the convenience of
being able to type `latest`, though it isn't clear why they don't make an
exception for that popular convention.
