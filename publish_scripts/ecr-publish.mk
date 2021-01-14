#!/usr/bin/make -f

CONTAINER_NAME := whylogs-container
ECR_ENDPOINT := 003872937983.dkr.ecr.us-east-1.amazonaws.com
ECR_REGION := us-east-1
ECR_REPO := $(ECR_ENDPOINT)/$(CONTAINER_NAME)
DATE_TAG := $(shell date +%Y-%m-%d_%H-%M-%S)

define i
echo
echo "[INFO]$(1)"
echo
endef

.PHONY: publish build authenticate help build

default: help

help: ## Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

publish: build ## Publish your most recently built whylogs-container to ECR. This triggers a build.
	@$(call i, Publishing $(CONTAINER_NAME):$(DATE_TAG) to $(ECR_REPO))
	docker tag $(CONTAINER_NAME):$(DATE_TAG) $(ECR_REPO):$(DATE_TAG)
	docker push $(ECR_REPO):$(DATE_TAG)

build: ## Build the whylogs-container locally
	@$(call i, Building $(CONTAINER_NAME))
	@if [[ -z "$$MAVEN_TOKEN" ]]; then \
		echo "MAVEN_TOKEN not set. Can't download dependencies for the build." 1>&2 ;\
		exit 1 ;\
	fi
	MAVEN_TOKEN=$(MAVEN_TOKEN) gw installDist 
	docker build . -t $(CONTAINER_NAME):$(DATE_TAG)

authenticate: ## Authenticate with ECR so you can push and pull
	@$(call i, Authenticating local docker with ECR. You may need to add yourself to https://console.aws.amazon.com/ecr/repositories/private/003872937983/whylogs-container/permissions/?region=us-east-1)
	aws ecr get-login-password --region $(ECR_REGION) | docker login --username AWS --password-stdin $(ECR_ENDPOINT)
