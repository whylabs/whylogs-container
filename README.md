## Building Docker image
* To build the docker image (locally):

```
$ gw installDist
$ docker build . -t whycontainer
```

* Or run a single command:
```
gw installDist && docker build . -t whycontainer
```

## Running the Docker image

* You should prepare a `local.env` file in your workspace with the following content. Make sure you configure
the heap to the appropriate size of your container. The heap size also determines how many concurrent dataset profiles
you can track at the same time (i.e. number of tag key-value combination)
```
OUTPUT_PATH=s3://<your-bucket>/<prefix>
WHYLOGS_PERIOD=HOURS (default is HOURS if unspecified. Supported values: MINUTES, HOURS, DAYS)
AWS_REGION=<your bucket's AWS region>~~~~
AWS_ACCESS_KEY_ID=<AWS access Key ID>
AWS_SECRET_ACCESS_KEY=<AWS-ACCESS-KEY>
AWS_SESSION_TOKEN=<your session token if you use something else but IAM user>
AWS_KMS_KEY_ID=<your kms key - must be in the same region as your S3>
JAVA_OPTS=-XX:+UseZGC -XX:+UnlockExperimentalVMOptions -XX:-ZUncommit -Xmx4G
```

* Run the Docker image with the following command:
```
docker run -it --rm -p 127.0.0.1:8080:8080 --env-file local.env whycontainer
```