PACKAGE_NAME := can-recompiler
VENV_PATH := $(HOME)/venvs/can_recompiler_dataweb_env
PYTHON := $(VENV_PATH)/bin/python
PIP := $(VENV_PATH)/bin/pip
ACTIVATE := source $(VENV_PATH)/bin/activate

.PHONY: all setup-env build clean install uninstall test lint format deploy help

all: build

setup-env:
	@echo "Creating virtual environment at $(VENV_PATH)..."
	python3 -m venv $(VENV_PATH)
	$(PIP) install --upgrade pip
	$(PIP) install build
	@echo "To activate the environment, run:"
	@echo "  source $(VENV_PATH)/bin/activate"

build: setup-env
	@echo "Building package using modern build tools..."
	$(PYTHON) -m build
	@echo "✅ Build complete. Artifacts in dist/"

clean:
	@echo "Cleaning build artifacts..."
	rm -rf build dist *.egg-info
	rm -rf __pycache__ */__pycache__ */*/__pycache__
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete

install: setup-env build
	@echo "Installing $(PACKAGE_NAME) from wheel (minimal deps for Databricks)..."
	$(PIP) install dist/can_recompiler-*.whl

install-standalone: setup-env build
	@echo "Installing $(PACKAGE_NAME) from wheel with pandas for standalone usage..."
	$(PIP) install dist/can_recompiler-*.whl[standalone]

install-dev: setup-env
	@echo "Installing $(PACKAGE_NAME) in development mode..."
	$(PIP) install -e ".[dev,standalone]"

uninstall: setup-env
	@echo "Uninstalling $(PACKAGE_NAME)..."
	$(PIP) uninstall -y $(PACKAGE_NAME)

test: setup-env install-dev
	@echo "Running tests..."
	$(PYTHON) -m pytest tests/ -v

lint: setup-env install-dev
	@echo "Running linting..."
	$(PYTHON) -m flake8 .
	$(PYTHON) -m mypy .

format: setup-env install-dev
	@echo "Formatting code..."
	$(PYTHON) -m black .

check: lint test
	@echo "✅ All checks passed!"

example: setup-env install-standalone
	@echo "Running example..."
	$(PYTHON) example.py

deploy: build
	@echo "Deploying $(PACKAGE_NAME) to AWS S3..."
	./deploy.sh

help:
	@echo "Makefile targets:"
	@echo "  setup-env    - Create and set up the virtual environment"
	@echo "  build        - Build the package using modern build tools"
	@echo "  clean        - Remove build artifacts"
	@echo "  install      - Install the built wheel in the venv"
	@echo "  install-dev  - Install in development mode with dev dependencies"
	@echo "  uninstall    - Uninstall the package from the venv"
	@echo "  test         - Run pytest tests"
	@echo "  lint         - Run flake8 and mypy"
	@echo "  format       - Format code with black"
	@echo "  check        - Run all linting and tests"
	@echo "  example      - Run the example script"
	@echo "  deploy       - Build and deploy to AWS S3 (US & EU regions)"
	@echo "  help         - Show this help message"