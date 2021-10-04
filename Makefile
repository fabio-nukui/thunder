.PHONY: build
.DEFAULT_GOAL := help
ifeq (${STRAT},)
ENV_FILE=env/.env-dev
else
ENV_FILE=env/.env-strat-${STRAT}
include $(ENV_FILE)
endif

###################################################################################################
## SCRIPTS
###################################################################################################

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([\w-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		line = '{: <20} {}'.format(target, help)
		line = re.sub(r'^({})'.format(target), '\033[96m\\1\033[m', line)
		print(line)
endef

export PRINT_HELP_PYSCRIPT

###################################################################################################
## VARIABLES
###################################################################################################

IMAGE_NAME = thunder
CONTAINER_USER = thunder
DEV_IMAGE_NAME = thunder-dev
LAB_IMAGE_NAME = thunder-lab
DEV_CONTAINER_NAME = thunder-dev
LAB_CONTAINER_NAME = thunder-lab
ARBITRAGE_CONTAINER_NAME = thunder-strat-${STRATEGY}
JUPYTER_PORT=8888
DATA_SOURCE = s3://crypto-thunder
PYTHON = python3
GIT_BRANCH = $(shell git rev-parse --verify --short=12 HEAD)

###################################################################################################
## GENERAL COMMANDS
###################################################################################################

help: ## Show this message
	@$(PYTHON) -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

build-dev: ## Build docker dev image
	echo $(GIT_BRANCH) > docker/git_commit
	docker build --target dev -t $(DEV_IMAGE_NAME) -f docker/Dockerfile .

start-dev: ## Start docker container for development
ifeq ($(shell docker ps -a --format "{{.Names}}" | grep ^$(DEV_CONTAINER_NAME)$$),)
	docker run -it \
		--net=host \
		-v $(PWD):/home/$(CONTAINER_USER)/work \
		--name $(DEV_CONTAINER_NAME) \
		--env-file $(ENV_FILE) \
		$(DEV_IMAGE_NAME)
else
	docker start -i $(DEV_CONTAINER_NAME)
endif

start-dev-local: ## Start docker container for development, passing local AWS credentials
ifeq ($(shell docker ps -a --format "{{.Names}}" | grep ^$(DEV_CONTAINER_NAME)$$),)
	docker run -it \
		--net=host \
		-v $(PWD):/home/$(CONTAINER_USER)/work \
		-v $(HOME)/.aws/credentials:/home/$(CONTAINER_USER)/.aws/credentials \
		--name $(DEV_CONTAINER_NAME) \
		--env-file $(ENV_FILE) \
		$(DEV_IMAGE_NAME)
else
	docker start -i $(DEV_CONTAINER_NAME)
endif

build-lab: ## Build docker lab image
	echo $(GIT_BRANCH) > docker/git_commit
	docker build --target lab -t $(LAB_IMAGE_NAME) -f docker/Dockerfile .

start-lab: ## Start docker container running jupyterlab
ifeq ($(shell docker ps -a --format "{{.Names}}" | grep ^$(LAB_CONTAINER_NAME)$$),)
	docker run -it \
		--net=host \
		-v $(PWD):/home/$(CONTAINER_USER)/work \
        -p $(JUPYTER_PORT):$(JUPYTER_PORT) \
		--name $(LAB_CONTAINER_NAME) \
		--env-file $(ENV_FILE) \
		$(LAB_IMAGE_NAME) \
		jupyter lab --port=$(JUPYTER_PORT)
else
	docker start -i $(LAB_CONTAINER_NAME)
endif

rm-dev: ## Remove stopped dev container
	docker rm $(DEV_CONTAINER_NAME)

build: check-all ## Build docker prod image
	echo $(GIT_BRANCH) > docker/git_commit
	docker build --target prod -t $(IMAGE_NAME) -f docker/Dockerfile .

start: ## Start docker container running arbitrage strategy "$STRAT" (e.g.: make start STRAT=1)
	docker run --rm -d \
		--net=host \
		-v $(PWD)/logs:/home/$(CONTAINER_USER)/work/logs \
		--name $(ARBITRAGE_CONTAINER_NAME) \
		--env-file $(ENV_FILE) \
		$(IMAGE_NAME)

stop:  ## Stop docker conteiner running strategy "$STRAT" (e.g.: make stop STRAT=1)
	docker stop $(ARBITRAGE_CONTAINER_NAME)

restart: build stop start  ## Restart running strategy "$STRAT" with updated code

upload-notebooks: ## Upload jupyter notebooks
	aws s3 sync \
		--exclude='.gitkeep' \
		--exclude='*.ipynb_checkpoints*' \
		notebooks $(DATA_SOURCE)/notebooks

download-notebooks: ## Download jupyter notebooks
	aws s3 sync $(DATA_SOURCE)/notebooks notebooks

get-env: ## Download .env files
	aws s3 sync $(DATA_SOURCE)/env env

check-all: isort lint types test check-clean-tree ## Run all checks and tests

check-clean-tree: ## Fail if git tree has unstaged/uncommited changes
ifneq ($(shell git status -s),)
	exit 1
endif
	@exit 0

isort: ## Check import sorts
	docker exec $(DEV_CONTAINER_NAME) isort -c src tests app.py

lint: ## Run code style checker
	docker exec $(DEV_CONTAINER_NAME) flake8 src tests app.py

types: ## Run type checker
	docker exec $(DEV_CONTAINER_NAME) pyright

test: ## Run test cases in tests directory
	docker exec $(DEV_CONTAINER_NAME) pytest -v tests
