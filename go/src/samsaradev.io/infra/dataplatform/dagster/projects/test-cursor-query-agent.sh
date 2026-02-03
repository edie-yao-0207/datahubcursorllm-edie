#!/bin/bash
set -euo pipefail

# Test script for Cursor Query Agent Setup
# This script simulates different setup modes to ensure no regressions are introduced.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Find backend root: use environment variable if set, otherwise search upward for marker files
find_backend_root() {
    # If BACKEND_ROOT is explicitly set, use it
    if [ -n "${BACKEND_ROOT:-}" ]; then
        echo "$BACKEND_ROOT"
        return 0
    fi

    # Otherwise, search upward from script location for marker files
    local current_dir="$SCRIPT_DIR"
    while [ "$current_dir" != "/" ]; do
        # Check for common backend root markers
        if [ -f "$current_dir/go.mod" ] || \
           [ -f "$current_dir/bin/cursor-query-agent-setup.sh" ] || \
           [ -d "$current_dir/.git" ] && [ -d "$current_dir/go/src/samsaradev.io" ]; then
            echo "$current_dir"
            return 0
        fi
        current_dir=$(dirname "$current_dir")
    done

    # Fallback: return empty if not found
    echo ""
    return 1
}

BACKEND_ROOT=$(find_backend_root)
if [ -z "$BACKEND_ROOT" ]; then
    echo "❌ Error: Could not find backend root. Please set BACKEND_ROOT environment variable."
    exit 1
fi

SETUP_SCRIPT="$BACKEND_ROOT/bin/cursor-query-agent-setup.sh"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

FAILED_TESTS=()

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔍 Comprehensive Cursor Query Agent Test Suite"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Sandbox directory for testing
TEST_SANDBOX="/tmp/cqa_test_sandbox"
rm -rf "$TEST_SANDBOX"
mkdir -p "$TEST_SANDBOX/mock_backend"

# Mock the directory structure required by the script
create_mock_repo() {
    local root="$1"
    mkdir -p "$root/.cursor/rules"
    mkdir -p "$root/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/common/llm"
    mkdir -p "$root/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub"
    mkdir -p "$root/go/src/samsaradev.io/infra/dataplatform/datahub/llm"

    touch "$root/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/common/llm/requirements.cursordatahub.txt"
    touch "$root/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/common/llm/requirements.cursordatahub.mcp.txt"
    touch "$root/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/common/llm/gen_datahub_schema.py"
    touch "$root/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub/analytics_patch.txt"
    touch "$root/go/src/samsaradev.io/infra/dataplatform/datahub/llm/.python_startup.py"
    touch "$root/.cursor/rules/query_agent.mdc"
}

run_test() {
    local mode_name="$1"
    local flags="$2"
    shift 2
    local expected_strings=("$@")

    echo -n "🧪 Testing mode: $mode_name... "

    # Use specified repo path or default
    local br_env="${MOCK_REPO_PATH:-$TEST_SANDBOX/mock_backend}"

    # For Lite mode, simulate NO BACKEND_ROOT by providing a non-existent path
    if [[ "$flags" == *"--lite"* ]]; then
        br_env="/tmp/non_existent_path_$(date +%s)"
    fi

    # In CI, use OAuth mode to skip token validation unless explicitly testing token mode
    local test_flags="$flags"
    if [ "${CI:-false}" = "true" ] || [ "${BUILDKITE:-false}" = "true" ]; then
        # Only skip token validation if not explicitly testing the default token mode
        if [[ "$mode_name" != "Default (Token)" ]]; then
            # Add --oauth if not already present to skip token checks
            if [[ "$flags" != *"--oauth"* ]] && [[ "$flags" != *"-oauth"* ]]; then
                test_flags="$flags --oauth"
            fi
        fi
    fi

    local output
    # shellcheck disable=SC2086
    # Pass CI environment variables to setup script so it skips token validation
    if [ "${CI:-false}" = "true" ] || [ "${BUILDKITE:-false}" = "true" ]; then
        if output=$(CI=true BUILDKITE=true BACKEND_ROOT="$br_env" bash "$SETUP_SCRIPT" $test_flags --check-env 2>&1); then
            # Check if expected success markers are in output
            # Support OR logic: strings starting with "OR:" are alternatives to the previous string
            local missing=()
            local i=0
            local skip_next=false
            while [ $i -lt ${#expected_strings[@]} ]; do
                if [ "$skip_next" = true ]; then
                    skip_next=false
                    i=$((i + 1))
                    continue
                fi

                local s="${expected_strings[$i]}"
                # Check if next item is an OR condition
                if [ $((i + 1)) -lt ${#expected_strings[@]} ] && [[ "${expected_strings[$((i + 1))]}" == OR:* ]]; then
                    local or_s="${expected_strings[$((i + 1))]#OR:}"
                    if ! echo "$output" | grep -q "$s" && ! echo "$output" | grep -q "$or_s"; then
                        missing+=("($s OR $or_s)")
                    fi
                    skip_next=true
                else
                    # Regular check
                    if ! echo "$output" | grep -q "$s"; then
                        missing+=("$s")
                    fi
                fi
                i=$((i + 1))
            done

            if [ ${#missing[@]} -eq 0 ]; then
                echo -e "${GREEN}PASSED${NC}"
            else
                echo -e "${RED}FAILED${NC} (missing output: ${missing[*]})"
                FAILED_TESTS+=("$mode_name")
                return 1
            fi
        else
            local exit_code=$?
            echo -e "${RED}FAILED${NC} (exit code $exit_code)"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "Setup script output:"
            echo "$output"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            FAILED_TESTS+=("$mode_name")
            return 1
        fi
    else
        if output=$(BACKEND_ROOT="$br_env" bash "$SETUP_SCRIPT" $test_flags --check-env 2>&1); then
            # Check if expected success markers are in output
            # Support OR logic: strings starting with "OR:" are alternatives to the previous string
            local missing=()
            local i=0
            local skip_next=false
            while [ $i -lt ${#expected_strings[@]} ]; do
                if [ "$skip_next" = true ]; then
                    skip_next=false
                    i=$((i + 1))
                    continue
                fi

                local s="${expected_strings[$i]}"
                # Check if next item is an OR condition
                if [ $((i + 1)) -lt ${#expected_strings[@]} ] && [[ "${expected_strings[$((i + 1))]}" == OR:* ]]; then
                    local or_s="${expected_strings[$((i + 1))]#OR:}"
                    if ! echo "$output" | grep -q "$s" && ! echo "$output" | grep -q "$or_s"; then
                        missing+=("($s OR $or_s)")
                    fi
                    skip_next=true
                else
                    # Regular check
                    if ! echo "$output" | grep -q "$s"; then
                        missing+=("$s")
                    fi
                fi
                i=$((i + 1))
            done

            if [ ${#missing[@]} -eq 0 ]; then
                echo -e "${GREEN}PASSED${NC}"
            else
                echo -e "${RED}FAILED${NC} (missing output: ${missing[*]})"
                FAILED_TESTS+=("$mode_name")
                return 1
            fi
        else
            local exit_code=$?
            echo -e "${RED}FAILED${NC} (exit code $exit_code)"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "Setup script output:"
            echo "$output"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            FAILED_TESTS+=("$mode_name")
            return 1
        fi
    fi
}

# --- 1. Infrastructure Checks ---
echo "⚙️  Verifying Script Infrastructure..."

# Check if Lite file list is in sync
# Extract all quoted strings inside the LITE_FILES=( ... ) block
LITE_FILES_IN_SETUP=$(sed -n '/LITE_FILES=(/,/)/p' "$SETUP_SCRIPT" | grep -o '"[^"]*"' | tr -d '"' | sort)
# Expected core files that must be present in Lite mode
EXPECTED_CORE_FILES="sql_runner.py
.init_agent.py
.close_agent.py
lineage.py
_s3_logger.py
mcp_app.py
.python_startup.py
requirements.cursordatahub.txt
requirements.cursordatahub.mcp.txt
gen_datahub_schema.py"

echo -n "  - File mapping consistency... "
MISSING_FROM_SETUP=$(comm -23 <(echo "$EXPECTED_CORE_FILES" | sort) <(echo "$LITE_FILES_IN_SETUP"))
if [ -z "$MISSING_FROM_SETUP" ]; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}STALE${NC} (Missing from setup: $MISSING_FROM_SETUP)"
    FAILED_TESTS+=("File Mapping Sync")
fi

# --- 2. Setup Mock Repo ---
create_mock_repo "$TEST_SANDBOX/mock_backend"

# --- 3. Run Functional Tests ---
echo "🚀 Running Setup Mode Validations..."

# In CI, skip token validation tests and use OAuth for all tests
if [ "${CI:-false}" = "true" ] || [ "${BUILDKITE:-false}" = "true" ]; then
    echo "ℹ️  CI environment detected - skipping token validation tests"
    # In CI (Docker), databricks CLI may or may not be installed
    # Accept either "databricks CLI found" OR "will use Python for OAuth login"
    run_test "OAuth" "--oauth" "Using OAuth mode - skipping DATABRICKS_TOKEN check" "databricks CLI found" "OR:will use Python for OAuth login"
    run_test "Backend + OAuth" "--backend --oauth" "Running environment preflight checks" "Using OAuth mode - skipping DATABRICKS_TOKEN check"
    run_test "Lite + OAuth" "--lite --oauth" "Lite mode enabled" "Using OAuth mode - skipping DATABRICKS_TOKEN check"
else
    run_test "Default (Token)" "" "Running environment preflight checks" "All required files found"
    run_test "OAuth" "--oauth" "Using OAuth mode - skipping DATABRICKS_TOKEN check" "databricks CLI found"
    run_test "Backend" "--backend" "Running environment preflight checks"
    run_test "Backend + OAuth" "--backend --oauth" "Running environment preflight checks" "Using OAuth mode - skipping DATABRICKS_TOKEN check"
    run_test "Lite" "--lite" "Lite mode enabled" "Lite mode: skipping backend repo file checks" "Lite mode: skipping BACKEND_ROOT check"
    run_test "Lite + OAuth" "--lite --oauth" "Lite mode enabled" "Using OAuth mode - skipping DATABRICKS_TOKEN check"
fi

# --- 4. Cleanup ---
rm -rf "$TEST_SANDBOX"

echo ""
if [ ${#FAILED_TESTS[@]} -eq 0 ]; then
    echo -e "${GREEN}✅ All setup modes and infrastructure validated successfully!${NC}"
    exit 0
else
    echo -e "${RED}❌ Validation failed!${NC}"
    echo -e "${RED}Issues found in:${NC}"
    for ft in "${FAILED_TESTS[@]}"; do
        echo -e "  - ${RED}$ft${NC}"
    done
    exit 1
fi
