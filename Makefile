
.PHONY: install-dev
install-dev: ## Install development dependencies
	uv sync --locked --all-extras

.PHONY: upgrade
upgrade: ## Upgrade dependencies
	uv sync --upgrade

.PHONY: test
test: ## Run tests
	uv run pytest

.PHONY: build
build: test ## Run tests and build the package
	uv build

.PHONY: help
help: ## Show all available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
