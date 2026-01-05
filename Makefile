.PHONY: run format lint help

help:
	@echo "Available targets:"
	@echo "  make run     - Start the fetcher"
	@echo "  make format  - Format and fix all linting issues"
	@echo "  make lint    - Check code (no fixes, like CI)"

run:
	uv run python -m driveslow.fetcher

format:
	uv run ruff check --fix .
	uv run ruff format .

lint:
	uv run ruff check .
	uv run ruff format --check .
