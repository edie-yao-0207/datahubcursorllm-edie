#!/bin/bash

# Setup script for Marimo notebook task
# This script sets up the environment and runs the Marimo notebook

set -e

echo "🚀 Setting up Marimo notebook for notebook_task_table compute logic..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv is not installed. Please install uv first:"
    echo "   curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    echo "❌ Please run this script from the notebook_task directory"
    exit 1
fi

echo "📦 Installing dependencies with uv..."
uv sync

echo "✅ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Set up Databricks credentials (optional):"
echo "   cp env.example .env"
echo "   # Edit .env with your DATABRICKS_TOKEN"
echo ""
echo "2. Test Databricks connection (detailed):"
echo "   uv run marimo edit marimo_notebook.py --watch"
echo ""
echo "This will:"
echo "  - Start Marimo development server"
echo "  - Open your browser to the notebook interface"
echo "  - Enable real-time sync between web UI and code editor"
echo "  - Connect to Databricks if credentials are provided"
