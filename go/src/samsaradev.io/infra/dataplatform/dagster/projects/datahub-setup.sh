#!/bin/bash

# DataHub Setup Script with UV
# This script sets up DataHub with custom patches for local development
# This is a temporary measure until we do a full fork of DataHub

set -e  # Exit on error
set -u  # Exit on undefined variable

# ============================================================================
# Configuration
# ============================================================================

DATAHUB_VERSION="v0.15.0.1"
PYTHON_VERSION="3.9"
VENV_NAME="datamodel"
VENV_UNITY_NAME="datamodel-unity"

# Set defaults
BACKEND_ROOT="${BACKEND_ROOT:-$HOME/co/backend}"
DATAHUB_ROOT="${BACKEND_ROOT}/../datahub"
PROJECTS_DIR="${BACKEND_ROOT}/go/src/samsaradev.io/infra/dataplatform/dagster/projects"
VENV_DIR="${BACKEND_ROOT}/../venvs/${VENV_NAME}"
VENV_UNITY_DIR="${BACKEND_ROOT}/../venvs/${VENV_UNITY_NAME}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "$1 is not installed. Please install it first."
        if [ "$1" = "uv" ]; then
            echo "   Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
        fi
        exit 1
    fi
}

# Get site-packages directory dynamically
get_site_packages_dir() {
    python -c "import site; print(site.getsitepackages()[0])"
}

# ============================================================================
# Main Setup Functions
# ============================================================================

setup_python_env() {
    log_info "Setting up Python environments with UV..."

    cd "$PROJECTS_DIR"

    # Create core DataHub venv
    if [ ! -d "$VENV_DIR" ]; then
        log_info "Creating core DataHub virtual environment at $VENV_DIR..."
        uv venv "$VENV_DIR" --python "$PYTHON_VERSION"
    else
        log_success "Core virtual environment already exists at $VENV_DIR"
    fi

    # Create Unity Catalog venv
    if [ ! -d "$VENV_UNITY_DIR" ]; then
        log_info "Creating Unity Catalog virtual environment at $VENV_UNITY_DIR..."
        uv venv "$VENV_UNITY_DIR" --python "$PYTHON_VERSION"
    else
        log_success "Unity virtual environment already exists at $VENV_UNITY_DIR"
    fi

    # Install dependencies in core venv
    log_info "Installing core DataHub dependencies (this may take a minute)..."
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
    uv pip install -r datamodel/requirements.txt
    deactivate
    log_success "Core DataHub dependencies installed"

    # Install dependencies in Unity venv (with unity-catalog extras)
    log_info "Installing Unity Catalog dependencies (this may take a minute)..."
    # shellcheck disable=SC1091
    source "$VENV_UNITY_DIR/bin/activate"
    uv pip install -r datamodel/requirements.txt
    # Install DataHub with Unity Catalog extras from requirements file
    uv pip install -r datamodel/requirements.datahub.unity.txt
    deactivate
    log_success "Unity Catalog dependencies installed"
}

apply_python_patches() {
    log_info "Applying patches to installed Python packages..."

    # Define patches
    local patches=(
        "patches/source_common_subtypes_patch.txt:datahub/ingestion/source/common/subtypes.py"
        "unity_recipe_updates_patch.txt:datahub/ingestion/source/unity/source.py"
        "dbt_common_patch.txt:datahub/ingestion/source/dbt/dbt_common.py"
    )

    # Apply patches to core venv
    log_info "Applying patches to core DataHub environment..."
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
    local site_packages
    site_packages=$(python -c "import site; print(site.getsitepackages()[0])")

    for patch_mapping in "${patches[@]}"; do
        IFS=':' read -r patch_file target_file <<< "$patch_mapping"
        local src="${PROJECTS_DIR}/datamodel/datamodel/resources/datahub/${patch_file}"
        local dst="${site_packages}/${target_file}"

        if [ -f "$src" ]; then
            cp "$src" "$dst"
            log_success "Applied to core: $patch_file"
        else
            log_warning "Patch file not found: $src"
        fi
    done
    deactivate

    # Apply patches to Unity venv
    log_info "Applying patches to Unity Catalog environment..."
    # shellcheck disable=SC1091
    source "$VENV_UNITY_DIR/bin/activate"
    site_packages=$(python -c "import site; print(site.getsitepackages()[0])")

    for patch_mapping in "${patches[@]}"; do
        IFS=':' read -r patch_file target_file <<< "$patch_mapping"
        local src="${PROJECTS_DIR}/datamodel/datamodel/resources/datahub/${patch_file}"
        local dst="${site_packages}/${target_file}"

        if [ -f "$src" ]; then
            cp "$src" "$dst"
            log_success "Applied to Unity: $patch_file"
        else
            log_warning "Patch file not found: $src"
        fi
    done
    deactivate
}

setup_datahub_repo() {
    if [ -d "$DATAHUB_ROOT" ]; then
        log_info "DataHub repository already exists, updating..."
        cd "$DATAHUB_ROOT"
        git fetch --tags
        git checkout "tags/${DATAHUB_VERSION}" 2>/dev/null || git checkout "$DATAHUB_VERSION"
        log_success "DataHub repository updated to ${DATAHUB_VERSION}"
    else
        log_info "Cloning DataHub repository..."
        cd "${BACKEND_ROOT}/.."
        git clone https://github.com/datahub-project/datahub.git
        cd datahub
        git checkout "tags/${DATAHUB_VERSION}"
        log_success "DataHub repository cloned"
    fi
}

apply_datahub_patches() {
    log_info "Applying DataHub frontend and configuration patches..."

    local datahub_resources="${PROJECTS_DIR}/datamodel/datamodel/resources/datahub"
    local patches_dir="${datahub_resources}/patches"

    # Apply text patches with sed
    sed -i '' '/mixpanel: isThirdPartyLoggingEnabled/s/isThirdPartyLoggingEnabled/true/' \
        "$DATAHUB_ROOT/datahub-web-react/src/app/analytics/analytics.ts"

    sed -i '' 's/const k = 1000; \/\/ /const k = 1024; \/\/ /' \
        "$DATAHUB_ROOT/datahub-web-react/src/app/shared/formatNumber.ts"

    sed -i '' "s/showSeparateSiblings: \${SHOW_SEPARATE_SIBLINGS:false}/showSeparateSiblings: \${SHOW_SEPARATE_SIBLINGS:true}/" \
        "$DATAHUB_ROOT/metadata-service/configuration/src/main/resources/application.yaml"

    # Apply file patches - format is "source_path:target_path"
    # Some patches are in the parent dir, some in patches/ subdirectory
    local file_patches=(
        "${datahub_resources}/analytics_patch.txt:datahub-web-react/src/conf/analytics.ts"
        "${patches_dir}/quickstart_docker_compose_patch.txt:docker/docker-compose-patched.yml"
        "${patches_dir}/column_stats_patch.txt:datahub-web-react/src/app/entity/shared/tabs/Dataset/Stats/snapshot/ColumnStats.tsx"
        "${patches_dir}/EmbedTab_patch.txt:datahub-web-react/src/app/entity/shared/tabs/Embed/EmbedTab.tsx"
        "${patches_dir}/DatasetEntity_patch.txt:datahub-web-react/src/app/entity/dataset/DatasetEntity.tsx"
        "${patches_dir}/DatasetStatsSummary_patch.txt:datahub-web-react/src/app/entity/dataset/shared/DatasetStatsSummary.tsx"
        "${patches_dir}/DatasetStatsSummarySubHeader_patch.txt:datahub-web-react/src/app/entity/dataset/profile/stats/stats/DatasetStatsSummarySubHeader.tsx"
        "${patches_dir}/SearchPage_patch.txt:datahub-web-react/src/app/search/SearchPage.tsx"
        "${patches_dir}/EmptySearchResults_patch.txt:datahub-web-react/src/app/search/EmptySearchResults.tsx"
        "${patches_dir}/event_patch.txt:datahub-web-react/src/app/analytics/event.ts"
    )

    for patch_mapping in "${file_patches[@]}"; do
        IFS=':' read -r src target_file <<< "$patch_mapping"
        local dst="${DATAHUB_ROOT}/${target_file}"

        if [ -f "$src" ]; then
            mkdir -p "$(dirname "$dst")"
            cp "$src" "$dst"
            log_success "Applied: $(basename "$src")"
        else
            log_warning "Patch not found: $src"
        fi
    done

    log_success "All patches applied"
}

setup_java() {
    log_info "Setting up Java environment..."

    if ! brew list openjdk@17 &>/dev/null; then
        log_info "Installing OpenJDK 17..."
        brew install openjdk@17
    else
        log_success "OpenJDK 17 already installed"
    fi

    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
    export PATH=$JAVA_HOME/bin:$PATH

    log_success "Java environment configured"
}

build_datahub() {
    log_info "Building DataHub (this will take several minutes)..."

    cd "$DATAHUB_ROOT"

    # Activate core venv to get access to datahub CLI
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"

    log_info "Running DataHub quickstart..."
    METADATA_SERVICE_AUTH_ENABLED=false datahub docker quickstart \
        --version "$DATAHUB_VERSION" \
        -f docker/docker-compose-patched.yml

    deactivate

    log_info "Building frontend distribution..."
    ./gradlew :datahub-frontend:dist -x yarnTest -x yarnLint --stacktrace

    log_info "Starting DataHub frontend container..."
    (cd docker && \
        COMPOSE_DOCKER_CLI_BUILD=1 \
        DOCKER_BUILDKIT=1 \
        docker compose -p datahub \
            -f docker-compose-without-neo4j.yml \
            -f docker-compose-without-neo4j.override.yml \
            -f docker-compose.dev.yml \
            up -d --no-deps --force-recreate --build datahub-frontend-react)

    log_success "DataHub build complete!"
}

# ============================================================================
# Main Execution
# ============================================================================

main() {
    echo ""
    log_info "🚀 Starting DataHub Setup with UV"
    echo ""

    # Check prerequisites
    check_command uv
    check_command git
    check_command docker
    check_command brew

    # Setup Python environment and dependencies
    setup_python_env
    apply_python_patches

    # Check if we should stop here (Python-only mode)
    if [[ "${1:-}" == "-python-only" ]]; then
        echo ""
        log_success "Python-only setup complete!"
        echo ""
        echo "📋 Virtual environments created:"
        echo "   Core DataHub:    $VENV_DIR"
        echo "   Unity Catalog:   $VENV_UNITY_DIR"
        echo ""
        echo "To activate environments:"
        echo "   Core:  source $VENV_DIR/bin/activate"
        echo "   Unity: source $VENV_UNITY_DIR/bin/activate"
        echo ""
        exit 0
    fi

    # Full setup: DataHub repository, patches, and build
    setup_datahub_repo
    apply_datahub_patches
    setup_java
    build_datahub

    echo ""
    log_success "🎉 DataHub setup complete!"
    echo ""
    echo "📋 Next steps:"
    echo "   - DataHub is running at: http://localhost:9002"
    echo "   - Core DataHub venv:    $VENV_DIR"
    echo "   - Unity Catalog venv:   $VENV_UNITY_DIR"
    echo "   - DataHub source:       $DATAHUB_ROOT"
    echo ""
    echo "To activate virtual environments:"
    echo "   Core:  source $VENV_DIR/bin/activate"
    echo "   Unity: source $VENV_UNITY_DIR/bin/activate"
    echo ""
}

# Run main function with all arguments
main "$@"
