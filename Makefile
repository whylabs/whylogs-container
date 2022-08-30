.PHONY: build build-docker run debug run-docker poll-sqlite-size help test

TAG := whylabs/whylogs:dev
NAME := whylogs

build: ## Build the service code locally.
	./gradlew installDist

test: ## Run unit tests.
	./gradlew test

build-docker:build ## Build the service code into a docker container as whylabs/whylogs:dev.
	docker build . -t $(TAG)

run: ## Run the service code locally without Docker.
	./gradlew run

docs: ## Generate dakka documentation
	./gradlew dokkaHtml

debug: ## Run the service in debug mode so you can connect to it via remote JVM debugger.
	./gradlew run --debug-java

run-docker: ## Run the Docker container using a local.env file.
	docker run -p 127.0.0.1:8080:8080 -it --rm --env-file local.env $(TAG)

poll-sqlite-size: ## If you run the service locally then this will echo info about the sqlite storage.
	ls -1  /tmp/ | grep -e 'sqlite$$' | xargs -I{} bash -c "echo -n '{}:' && sqlite3 /tmp/{} 'select count(1) from items;'" && echo;

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@cat ${MAKEFILE_LIST} | sed -n 's/^\(.\+\)\:.\+##\s\+\(.\+\)/\1 # \2/p' | sort | column -t -c 2 -s '#'

