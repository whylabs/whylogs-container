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
OUTPUT_PATH=s3://<your-bucket>/<prefix>
WHYLOGS_PERIOD=HOURS (default is HOURS if unspecified. Supported values: MINUTES, HOURS, DAYS)
JAVA_OPTS=-XX:+UseZGC -XX:+UnlockExperimentalVMOptions -XX:-ZUncommit -Xmx4G

# Optional alternate api endpoint
WHYLABS_API_ENDPOINT=http://localhost:8080
WHYLABS_API_KEY=xxxxxx
WHYLOGS_PERIOD=HOURS
# Specify the api key that will be checked on each request. The header API key
# must match this value.
CONTAINER_API_KEY=secret-key
```

* Run the Docker image with the following command:
```
docker run -it --rm -p 127.0.0.1:8080:8080 --env-file local.env --name whycontainer whycontainer
```

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
the rest server without terminating the container does have the advantage of
definitely not wiping out the ephemeral storage if it would have resulted in the
container getting destroyed.
