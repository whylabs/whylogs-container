.PHONY: build build-docker run debug run-docker

TAG := whylabs/whylogs:latest
NAME := whylogs

build:
	./gradlew installDist

test:
	./gradlew test

build-docker:
	docker build . -t $(TAG)

run:
	./gradlew run

debug:
	./gradlew run --debug-java

run-docker:
	docker run -it --rm -p 127.0.0.1:5005:5005  -p 127.0.0.1:8080:8080 --env-file dev.env --name $(NAME) $(TAG)

poll-sqlite-size:
	ls -1  /tmp/ | grep -e 'sqlite$$' | xargs -I{} bash -c "echo -n '{}:' && sqlite3 /tmp/{} 'select count(1) from items;'" && echo;
