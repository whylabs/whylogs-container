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
```

* Run the Docker image with the following command:
```
docker run -it --rm -p 127.0.0.1:8080:8080 --env-file local.env whycontainer
```
