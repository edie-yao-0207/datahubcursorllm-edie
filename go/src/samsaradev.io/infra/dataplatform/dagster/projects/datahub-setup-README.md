# DataHub Setup Script

This script sets up a local DataHub instance with custom patches for development.

## Prerequisites

1. **Install UV** (fast Python package manager):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. **Required tools**:
   - Git
   - Docker & Docker Compose
   - Homebrew (macOS)

## Usage

### Full Setup (Python + DataHub build)

```bash
./datahub-setup.sh
```

This will:
- Create a Python 3.9 virtual environment using UV
- Install all required Python dependencies
- Apply patches to DataHub Python packages
- Clone/update DataHub repository (v0.15.0.1)
- Apply frontend and configuration patches
- Install OpenJDK 17
- Build and start DataHub

**Time:** ~10-15 minutes on first run

### Python-Only Setup

If you only need to set up the Python environment and patches:

```bash
./datahub-setup.sh -python-only
```

**Time:** ~2-3 minutes

## What's Different?

Compared to the old script, this version:

✅ **Uses UV** for 10-100x faster Python package installation
✅ **Two virtual environments** - mirrors production setup with separate core and Unity Catalog environments
✅ **Unity Catalog support** - includes `great_expectations` and other required extras
✅ **Idempotent** - safe to run multiple times, skips existing setup
✅ **Better error handling** - fails fast with clear error messages
✅ **Colored output** - easier to track progress
✅ **Smart caching** - reuses existing DataHub clone instead of re-cloning
✅ **Modular** - organized into clear functions for each step
✅ **Dynamic paths** - automatically routes to correct venv based on ingestion source

## Environment Variables

- `BACKEND_ROOT` - Path to backend directory (default: `$HOME/co/backend`)

## Locations

After setup:
- **Core DataHub venv**: `$BACKEND_ROOT/../venvs/datamodel`
- **Unity Catalog venv**: `$BACKEND_ROOT/../venvs/datamodel-unity`
- **DataHub source**: `$BACKEND_ROOT/../datahub`
- **DataHub UI**: http://localhost:9002
- **DataHub GMS API**: http://localhost:8080

## Activating Virtual Environments

**Core DataHub** (for most development):
```bash
source $BACKEND_ROOT/../venvs/datamodel/bin/activate
```

**Unity Catalog** (if you need to test Unity-specific features):
```bash
source $BACKEND_ROOT/../venvs/datamodel-unity/bin/activate
```

Or if using the default paths:
```bash
# Core
source ~/co/venvs/datamodel/bin/activate

# Unity
source ~/co/venvs/datamodel-unity/bin/activate
```

**Note:** When running Dagster jobs, the system automatically uses the correct virtual environment based on the ingestion source, so you typically only need to activate the core environment.

## Troubleshooting

### "uv: command not found"

Install UV:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc  # or ~/.zshrc
```

### SSL/Certificate errors

The new script uses UV which handles certificate issues better than pip. If you still encounter issues, try:
```bash
export UV_NO_VERIFY_SSL=1
./datahub-setup.sh
```

### Docker build failures

Ensure Docker is running:
```bash
docker ps
```

### DataHub already cloned with wrong version

Remove the directory and re-run:
```bash
rm -rf $BACKEND_ROOT/../datahub
./datahub-setup.sh
```

## Development Workflow

### 1. Initial Setup (First Time Only)

Run the setup script:
```bash
./datahub-setup.sh
```

### 2. Running Dagster Jobs Locally

#### Option A: Using the Helper Script (Recommended)

The easiest way to run Dagster jobs locally is using the provided helper script:

```bash
# Run a specific Dagster job
./run-local-dagster.sh dagster job execute -j metadata_datahub_graph_glue -m datamodel

# Launch Dagster UI for local development
./run-local-dagster.sh dagster dev -m datamodel

# Start an interactive shell with environment configured
./run-local-dagster.sh bash
```

The helper script automatically sets all required environment variables and activates the virtual environment.

#### Option B: Manual Setup

If you prefer to set up the environment manually:

```bash
# Set environment variables for local development
export MODE=LOCAL_DATAHUB_RUN
export BACKEND_ROOT=~/co/backend  # Or your actual path

# Activate the virtual environment
source ~/co/venvs/datamodel/bin/activate

# Run your Dagster job (example)
dagster job execute -j metadata_datahub_graph_glue -m datamodel
```

**Important:** The `MODE=LOCAL_DATAHUB_RUN` environment variable tells the Dagster jobs to:
- Use local UV virtual environments instead of container paths
- Route Unity Catalog/DBT ingestion to `~/co/venvs/datamodel-unity/bin/datahub`
- Route other sources (glue, tableau) to `~/co/venvs/datamodel/bin/datahub`
- Connect to DataHub at `http://localhost:8080` instead of the production server

### 3. Making Changes to DataHub

If you need to modify DataHub source code:

```bash
cd $BACKEND_ROOT/../datahub

# Make your changes...

# Rebuild frontend if needed:
./gradlew :datahub-frontend:dist -x yarnTest -x yarnLint

# Restart containers:
cd docker
docker compose -p datahub restart datahub-frontend-react
```

### 4. Environment Variables Reference

For local development, set these in your shell or `.envrc`:

```bash
export MODE=LOCAL_DATAHUB_RUN          # Tells jobs to use local paths
export BACKEND_ROOT=~/co/backend        # Path to your backend directory
export DATAHUB_GMS_URL=http://localhost:8080  # Local DataHub GMS server
```

