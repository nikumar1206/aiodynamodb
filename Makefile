.PHONY: install lint format test build clean help

help:
	@echo "Available commands:"
	@echo "  make install    - Install dependencies"
	@echo "  make lint       - Run ruff linter"
	@echo "  make format     - Format code with ruff"
	@echo "  make test       - Run tests"
	@echo "  make build      - Build distribution"
	@echo "  make clean      - Clean build artifacts"

install:
	uv sync --all-extras

lint:
	uv run ruff check src tests

format:
	uv run ruff format src tests

test:
	uv run pytest tests -v

build: clean
	uv build

clean:
	rm -rf dist/ build/ *.egg-info .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
