.PHONY: build run debug run-docker

NAME := whycontainer

build:
	gw installDist
	docker build . -t $(NAME)

run:
	gw run

debug:
	gw run --debug-java

run-docker:
	docker run -it --rm -p 127.0.0.1:5005:5005  -p 127.0.0.1:8080:8080 --env-file local.env --name $(NAME) $(NAME)
