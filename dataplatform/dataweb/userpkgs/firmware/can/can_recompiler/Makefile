# CAN Recompiler Package Makefile
# Provides automated testing and development workflows

# Configuration
VENV_DIR = test_env
PYTHON = python3
PIP = $(VENV_DIR)/bin/pip
PYTHON_VENV = $(VENV_DIR)/bin/python
TEST_FILE = tests/

# Test directories and files
TEST_DIR = tests
ALL_TEST_FILES = $(wildcard $(TEST_DIR)/test_*.py)
MAIN_TEST_FILES = $(filter-out %_stress.py %_final.py, $(ALL_TEST_FILES))
STRESS_TEST_FILE = $(TEST_DIR)/test_stress_cases.py
FINAL_TEST_FILE = $(TEST_DIR)/test_final_breaking.py

# Colors for output
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m # No Color

.PHONY: help venv install-deps test test-verbose test-all test-stress test-final test-main clean setup all lint coverage coverage-all coverage-html coverage-report clean-coverage

# Default target
all: setup test

help:
	@echo "$(GREEN)CAN Recompiler Package - Available Commands:$(NC)"
	@echo ""
	@echo "$(YELLOW)Setup Commands:$(NC)"
	@echo "  setup          - Complete setup (venv + deps + test)"
	@echo "  venv           - Create virtual environment"
	@echo "  install-deps   - Install testing dependencies"
	@echo ""
	@echo "$(YELLOW)Testing Commands:$(NC)"
	@echo "  test           - Run main comprehensive test suite"
	@echo "  test-all       - Run ALL test files (main + stress + final)"
	@echo "  test-main      - Run main test file only"
	@echo "  test-stress    - Run stress test cases only"
	@echo "  test-final     - Run final breaking test cases only"
	@echo "  test-verbose   - Run tests with verbose output"
	@echo "  test-quick     - Run quick smoke tests"
	@echo "  coverage       - Run tests with coverage analysis"
	@echo "  coverage-all   - Run ALL tests with coverage analysis"
	@echo "  coverage-html  - Generate HTML coverage report"
	@echo "  coverage-report- Show detailed coverage report"
	@echo ""
	@echo "$(YELLOW)Maintenance Commands:$(NC)"
	@echo "  clean          - Remove virtual environment and cache"
	@echo "  lint           - Check code style (if available)"
	@echo "  deps-list      - Show installed dependencies"
	@echo ""
	@echo "$(YELLOW)Examples:$(NC)"
	@echo "  make setup         # First time setup"
	@echo "  make test          # Run main tests"
	@echo "  make test-all      # Run all test suites"
	@echo "  make coverage-all  # Full coverage analysis"
	@echo "  make clean         # Clean up"

# Create virtual environment
venv:
	@echo "$(GREEN)Creating virtual environment...$(NC)"
	$(PYTHON) -m venv $(VENV_DIR)
	@echo "$(GREEN)✅ Virtual environment created in $(VENV_DIR)$(NC)"

# Install testing dependencies
install-deps: venv
	@echo "$(GREEN)Installing testing dependencies...$(NC)"
	$(PIP) install --upgrade pip
	$(PIP) install pytest pandas numpy coverage pyspark pytest-cov
	@echo "$(GREEN)✅ Dependencies installed successfully$(NC)"

# Complete setup
setup: venv install-deps
	@echo "$(GREEN)🚀 Setup complete! Ready to run tests.$(NC)"
	@echo ""
	@echo "$(YELLOW)Available commands:$(NC)"
	@echo "  make test           - Run comprehensive test suite"
	@echo "  make test-verbose   - Run tests with detailed output"
	@echo "  make clean          - Clean up environment"

# Run comprehensive test suite
test: $(VENV_DIR)
	@echo "$(GREEN)🧪 Running CAN Recompiler Test Suite...$(NC)"
	@echo "$(YELLOW)Tests: $(TEST_FILE)$(NC)"
	@echo "----------------------------------------"
	$(PYTHON_VENV) -m pytest $(TEST_FILE) -v
	@echo "----------------------------------------"
	@echo "$(GREEN)✅ Test execution completed$(NC)"

# Run tests with verbose output and timing
test-verbose: $(VENV_DIR)
	@echo "$(GREEN)🧪 Running Verbose Test Suite...$(NC)"
	@echo "$(YELLOW)Environment: $(VENV_DIR)$(NC)"
	@echo "$(YELLOW)Python: $(shell $(PYTHON_VENV) --version)$(NC)"
	@echo "$(YELLOW)Test file: $(TEST_FILE)$(NC)"
	@echo "========================================"
	time $(PYTHON_VENV) $(TEST_FILE)
	@echo "========================================"
	@echo "$(GREEN)✅ Verbose test execution completed$(NC)"

# Quick smoke tests
test-quick: $(VENV_DIR)
	@echo "$(GREEN)⚡ Running Quick Smoke Tests...$(NC)"
	@echo "$(YELLOW)Testing virtual environment...$(NC)"
	$(PYTHON_VENV) --version
	@echo "$(YELLOW)Testing pandas import...$(NC)"
	$(PYTHON_VENV) -c "import pandas; print(f'✅ pandas {pandas.__version__} working')"
	@echo "$(YELLOW)Testing numpy import...$(NC)"
	$(PYTHON_VENV) -c "import numpy; print(f'✅ numpy {numpy.__version__} working')"
	@echo "$(YELLOW)Testing test file syntax...$(NC)"
	$(PYTHON_VENV) -m py_compile $(TEST_FILE)
	@echo "✅ Test file syntax valid"
	@echo "$(GREEN)✅ Environment ready for testing!$(NC)"

# Run all test files
test-all: $(VENV_DIR)
	@echo "$(GREEN)🧪 Running ALL Test Suites...$(NC)"
	@echo "$(YELLOW)Test files: $(ALL_TEST_FILES)$(NC)"
	@echo "========================================"
	@for test_file in $(ALL_TEST_FILES); do \
		echo "$(YELLOW)Running $$test_file...$(NC)"; \
		$(PYTHON_VENV) $$test_file || exit 1; \
		echo "$(GREEN)✅ $$test_file completed$(NC)"; \
		echo "----------------------------------------"; \
	done
	@echo "$(GREEN)🎉 All test suites completed successfully!$(NC)"

# Run main test file only
test-main: $(VENV_DIR)
	@echo "$(GREEN)🧪 Running Main Test Suite...$(NC)"
	@echo "$(YELLOW)Test file: $(MAIN_TEST_FILE)$(NC)"
	@echo "----------------------------------------"
	$(PYTHON_VENV) $(MAIN_TEST_FILE)
	@echo "----------------------------------------"
	@echo "$(GREEN)✅ Main test suite completed$(NC)"

# Run stress test cases only
test-stress: $(VENV_DIR)
	@echo "$(GREEN)💪 Running Stress Test Cases...$(NC)"
	@echo "$(YELLOW)Test file: $(STRESS_TEST_FILE)$(NC)"
	@echo "----------------------------------------"
	$(PYTHON_VENV) $(STRESS_TEST_FILE)
	@echo "----------------------------------------"
	@echo "$(GREEN)✅ Stress tests completed$(NC)"

# Run final breaking test cases only
test-final: $(VENV_DIR)
	@echo "$(GREEN)🔍 Running Final Breaking Test Cases...$(NC)"
	@echo "$(YELLOW)Test file: $(FINAL_TEST_FILE)$(NC)"
	@echo "----------------------------------------"
	$(PYTHON_VENV) $(FINAL_TEST_FILE)
	@echo "----------------------------------------"
	@echo "$(GREEN)✅ Final breaking tests completed$(NC)"

# Alternative test runner using pytest (if preferred)
test-pytest: $(VENV_DIR)
	@echo "$(GREEN)🧪 Running tests with pytest...$(NC)"
	@echo "$(YELLOW)Note: This may have config conflicts, prefer 'make test'$(NC)"
	cd /tmp && $(PYTHON_VENV) -m pytest $(shell pwd)/$(TEST_FILE) -v --tb=short

# Run tests with coverage analysis
coverage: $(VENV_DIR)
	@echo "$(GREEN)📊 Running Test Coverage Analysis...$(NC)"
	@echo "$(YELLOW)Analyzing code coverage for source files (excluding tests)...$(NC)"
	$(PYTHON_VENV) -m pytest --cov=. --cov-report=term-missing $(TEST_FILE)
	@echo ""
	@echo "$(GREEN)📈 Coverage Summary:$(NC)"
	$(PYTHON_VENV) -m coverage report --show-missing --omit="test_*.py,*test*.py"
	@echo ""
	@echo "$(YELLOW)💡 Use 'make coverage-html' for detailed HTML report$(NC)"

# Run ALL tests with coverage analysis
coverage-all: $(VENV_DIR)
	@echo "$(GREEN)📊 Running Coverage Analysis for ALL Test Suites...$(NC)"
	@echo "$(YELLOW)Test files: $(ALL_TEST_FILES)$(NC)"
	@echo "$(YELLOW)Analyzing code coverage for all Python files...$(NC)"
	@echo "----------------------------------------"
	@# Run first test file to initialize coverage
	COVERAGE_FILE=.coverage $(PYTHON_VENV) -m coverage run --source=. --omit="test_*.py,*test*.py" -m pytest $(MAIN_TEST_FILE)
	@# Append coverage from additional test files
	@for test_file in $(STRESS_TEST_FILE) $(FINAL_TEST_FILE); do \
		echo "$(YELLOW)Adding coverage from $$test_file...$(NC)"; \
		COVERAGE_FILE=.coverage $(PYTHON_VENV) -m coverage run --source=. --omit="test_*.py,*test*.py" -m pytest --append $$test_file; \
	done
	@echo "----------------------------------------"
	@echo "$(GREEN)📈 Combined Coverage Summary:$(NC)"
	$(PYTHON_VENV) -m coverage report --show-missing --omit="test_*.py,*test*.py"
	@echo ""
	@echo "$(YELLOW)💡 Use 'make coverage-html' for detailed HTML report$(NC)"

# Generate HTML coverage report
coverage-html: $(VENV_DIR)
	@echo "$(GREEN)🌐 Generating HTML Coverage Report...$(NC)"
	$(PYTHON_VENV) -m pytest --cov=. --cov-report=html $(TEST_FILE) >/dev/null 2>&1
	$(PYTHON_VENV) -m coverage html --omit="test_*.py,*test*.py"
	@echo "$(GREEN)✅ HTML report generated in htmlcov/$(NC)"
	@echo "$(YELLOW)Open htmlcov/index.html in your browser to view detailed coverage$(NC)"

# Show detailed coverage report
coverage-report: $(VENV_DIR)
	@echo "$(GREEN)📋 Detailed Coverage Report:$(NC)"
	@echo "========================================"
	$(PYTHON_VENV) -m pytest --cov=. --cov-report=term-missing $(TEST_FILE) >/dev/null 2>&1
	$(PYTHON_VENV) -m coverage report --show-missing --skip-covered --omit="test_*.py,*test*.py"
	@echo "========================================"
	@echo "$(YELLOW)Lines marked with '!' are missing coverage$(NC)"

# Clean coverage data
clean-coverage:
	@echo "$(GREEN)🧹 Cleaning coverage data...$(NC)"
	rm -rf .coverage htmlcov/
	@echo "$(GREEN)✅ Coverage data cleaned$(NC)"

# Show installed dependencies
deps-list: $(VENV_DIR)
	@echo "$(GREEN)📦 Installed Dependencies:$(NC)"
	$(PIP) list

# Check if virtual environment exists
$(VENV_DIR):
	@if [ ! -d "$(VENV_DIR)" ]; then \
		echo "$(RED)❌ Virtual environment not found. Run 'make setup' first.$(NC)"; \
		exit 1; \
	fi

# Lint code (if tools are available)
lint: $(VENV_DIR)
	@echo "$(GREEN)🔍 Checking code style...$(NC)"
	@if $(PYTHON_VENV) -c "import flake8" 2>/dev/null; then \
		$(PYTHON_VENV) -m flake8 *.py --max-line-length=120 --ignore=E501,W503; \
	else \
		echo "$(YELLOW)⚠️  flake8 not installed, skipping lint check$(NC)"; \
	fi

# Clean up virtual environment and cache
clean:
	@echo "$(GREEN)🧹 Cleaning up...$(NC)"
	rm -rf $(VENV_DIR)
	rm -rf __pycache__
	rm -rf *.pyc
	rm -rf .pytest_cache
	rm -rf .coverage htmlcov/
	@echo "$(GREEN)✅ Cleanup completed$(NC)"

# Development helpers
dev-shell: $(VENV_DIR)
	@echo "$(GREEN)🐚 Starting development shell...$(NC)"
	@echo "$(YELLOW)Virtual environment: $(VENV_DIR)$(NC)"
	@echo "$(YELLOW)To exit: type 'exit'$(NC)"
	@echo ""
	bash --init-file <(echo "source $(VENV_DIR)/bin/activate; echo 'CAN Recompiler dev environment ready!'")

# Check environment
check-env: $(VENV_DIR)
	@echo "$(GREEN)🔍 Environment Check:$(NC)"
	@echo "Python version: $(shell $(PYTHON_VENV) --version)"
	@echo "Pip version: $(shell $(PIP) --version)"
	@echo "Virtual env: $(shell pwd)/$(VENV_DIR)"
	@echo "Test file: $(TEST_FILE) $(shell [ -f $(TEST_FILE) ] && echo '✅' || echo '❌')"
	@echo ""
	@echo "$(GREEN)📦 Key Dependencies:$(NC)"
	@$(PYTHON_VENV) -c "import pandas; print(f'pandas: {pandas.__version__}')" 2>/dev/null || echo "pandas: ❌"
	@$(PYTHON_VENV) -c "import numpy; print(f'numpy: {numpy.__version__}')" 2>/dev/null || echo "numpy: ❌"

# Show test coverage info
test-info:
	@echo "$(GREEN)📊 Test Suite Information:$(NC)"
	@echo ""
	@echo "$(YELLOW)Test Structure:$(NC)"
	@grep -n "class.*Test" $(TEST_FILE) || echo "No test classes found"
	@echo ""
	@echo "$(YELLOW)Test Count:$(NC)"
	@grep -c "def test_" $(TEST_FILE) | sed 's/^/  Total test methods: /' || echo "  No test methods found"
	@echo ""
	@echo "$(YELLOW)Test File Size:$(NC)"
	@wc -l $(TEST_FILE) | sed 's/^/  /' || echo "  Test file not found"

# Force clean rebuild
rebuild: clean setup
	@echo "$(GREEN)🔄 Complete rebuild finished$(NC)"

# Emergency test run (minimal dependencies)
test-minimal:
	@echo "$(GREEN)🆘 Emergency test run (no venv)...$(NC)"
	@echo "$(YELLOW)Warning: Using system Python, may have import issues$(NC)"
	$(PYTHON) $(TEST_FILE)