# Makefile for building and pushing the DIDComm Mediator image to ECR.

# AWS profile: pass via `make deploy-stage ECR_REPO=my-ecr-repo PROFILE=myprofile`.
# When PROFILE is omitted, the AWS CLI uses its normally configured profile.
PROFILE ?=
AWS_PROFILE_FLAG := $(if $(PROFILE),--profile $(PROFILE),)

# The region is read from the selected AWS CLI profile/configuration; it is
# deliberately not set or accepted as an override in this Makefile.
override AWS_CONFIG_REGION = $(shell aws configure get region $(AWS_PROFILE_FLAG))

# ECR_REPO must be supplied explicitly to `make deploy-stage`.
ECR_REPO ?=

# Short git hash of HEAD (7 chars).
GIT_HASH := $(shell git rev-parse --short=7 HEAD)
GIT_SHA  := $(shell git rev-parse HEAD)
IMAGE_TAG := didcomm-mediator-$(GIT_HASH)

# Account id resolved from the selected profile (or env credentials).
ACCOUNT_ID = $(shell aws sts get-caller-identity $(AWS_PROFILE_FLAG) --query Account --output text)
REGISTRY   = $(ACCOUNT_ID).dkr.ecr.$(AWS_CONFIG_REGION).amazonaws.com
REMOTE_IMAGE = $(REGISTRY)/$(ECR_REPO):$(IMAGE_TAG)
LOCAL_IMAGE  = $(ECR_REPO):$(IMAGE_TAG)

.PHONY: deploy-stage
deploy-stage:
	@test "$(origin ECR_REPO)" = "command line" && test -n "$(ECR_REPO)" \
		|| (echo "ERROR: pass the ECR repository explicitly, e.g. make deploy-stage ECR_REPO=stage-services" && exit 1)
	@test -n "$(AWS_CONFIG_REGION)" \
		|| (echo "ERROR: no AWS region is configured for the selected AWS CLI profile" && exit 1)
	@echo ">> Resolving AWS account..."
	$(eval RESOLVED_ACCOUNT := $(ACCOUNT_ID))
	@test -n "$(RESOLVED_ACCOUNT)" || (echo "ERROR: could not resolve AWS account id (check --profile or AWS_* env vars)" && exit 1)
	@echo ">> Account: $(RESOLVED_ACCOUNT)  Image: $(REMOTE_IMAGE)"

	@echo ">> Logging in to ECR..."
	aws ecr get-login-password $(AWS_PROFILE_FLAG) --region $(AWS_CONFIG_REGION) \
		| docker login --username AWS --password-stdin $(REGISTRY)

	@echo ">> Building image..."
	docker build -f apps/mediator/Dockerfile -t $(LOCAL_IMAGE) .

	@echo ">> Tagging image..."
	docker tag $(LOCAL_IMAGE) $(REMOTE_IMAGE)

	@echo ">> Pushing image..."
	docker push $(REMOTE_IMAGE)

	@echo ">> Image pushed: $(REMOTE_IMAGE)"
	@$(MAKE) gh-release-stage

# owner/repo from origin remote (handles https and ssh URLs).
GH_REPO = $(shell git config --get remote.origin.url | sed -E 's,git@github.com:,,; s,https://github.com/,,; s,\.git$$,,')

.PHONY: gh-release-stage
gh-release-stage:
	@command -v gh >/dev/null || (echo "ERROR: gh CLI is required" && exit 1)
	@echo ">> Creating GitHub release stage-$(GIT_HASH) on $(GH_REPO)..."
	gh release create stage-$(GIT_HASH) \
		--repo $(GH_REPO) \
		--title "stage-$(GIT_HASH)" \
		--target $(GIT_SHA) \
		--generate-notes
