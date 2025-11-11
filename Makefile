project_name = mini-scan

.DEFAULT_GOAL := help

.PHONY: help
help: ## Print this help message
	@echo "List of available make commands";
	@echo "";
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}';

.PHONY: dev
dev: ## Run docker compose and all services
	docker compose --env-file .env.local up -d

.PHONY: demo
demo: ## starts up every service including the ingester. Will not detach, will build all compilable services from scratch
	docker compose --env-file .env.demo --profile demo up --build

.PHONY: down
down: ## Stop all docker compose services
	docker compose down

.PHONY: tools
tools: ## show a list of tools used for this project
	@echo "List of tools used"
	@echo "golang v1.23 \n\t https://go.dev/dl/"
	@echo "golang-migrate\n\t https://github.com/golang-migrate/migrate/tree/master/cmd/migrate"
	@echo "docker and docker compose \n\t https://docs.docker.com/compose/"
	

.PHONY: test
test: ## test the project
	@echo running tests for miniscan
	go test ./...

# using phony here because relying on make to determine if we should run coverage or not
# is a bit weird to me. We're using it as a task running, not a build tool
.PHONY: cover
cover: ## Run tests with coverage file provided to coverage.out
	@echo running coverage tests
	go test -coverprofile=coverage.out ./...

.PHONY: cover-html
cover-html: cover ## Run tests with coverage file and html output for viewing
	@echo Generating html cover report
	go tool cover -html=coverage.out