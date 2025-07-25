.PHONY : install install-ci format lint test test-cov clean security docs
.DEFAULT_GOAL := help

help:
	@echo "Available commands:"
	@echo "  install     : Install all dependencies"
	@echo "  install-ci  : Install all dependencies for CI"
	@echo "  format      : Run pre-commit hooks (black, isort)"
	@echo "  lint        : Run all linting tools"
	@echo "  test        : Run tests"
	@echo "  test-cov    : Run tests with coverage report"
	@echo "  security    : Run security checks with bandit"
	@echo "  type-check  : Run mypy type checking"
	@echo "  clean       : Clean up cache and build artifacts"
	@echo "  all-checks  : Run format, lint, security, type-check, and test"

install:
	pip install poetry
	poetry install --with dev --with test --all-extras
	pre-commit install

install-ci:
	pip install poetry
	poetry install --with test --all-extras

format:
	pre-commit run --all-files

lint:
	poetry run flake8 bizon/
	poetry run pydocstyle bizon/

test:
	poetry run pytest tests/ -v

test-cov:
	poetry run pytest tests/ --cov=bizon --cov-report=term-missing --cov-report=html

security:
	poetry run bandit -r bizon/

type-check:
	poetry run mypy bizon/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .coverage htmlcov/ .mypy_cache/ .pytest_cache/
	rm -rf dist/ build/ *.egg-info/

all-checks: format lint security type-check test