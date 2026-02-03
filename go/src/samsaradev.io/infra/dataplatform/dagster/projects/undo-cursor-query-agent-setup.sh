#!/bin/bash

# This script is used to revert the Cursor environment to the original state
# by removing specific changes made by the cursor-query-agent-setup.sh script

echo "Reverting Cursor Query Agent setup changes..."

if [ -z "$BACKEND_ROOT" ]; then
  BACKEND_ROOT="$HOME/co/backend"
fi

# Deactivate the virtual environment if it's active
if [[ -n "$VIRTUAL_ENV" && "$VIRTUAL_ENV" == *"cursordatahub"* ]]; then
    echo "Deactivating virtual environment..."
    deactivate 2>/dev/null || true
fi

# Remove the virtual environment directory if it exists
if [ -d "$BACKEND_ROOT"/../venvs/cursordatahub ]; then
    echo "Removing virtual environment directory..."
    rm -rf "$BACKEND_ROOT"/../venvs/cursordatahub
fi

echo "Removing symlinks..."
rm -f "$BACKEND_ROOT"/../datahubcursorllm/prompt.md
rm -f "$BACKEND_ROOT"/../datahubcursorllm/datahub.json
rm -f "$BACKEND_ROOT"/../datahubcursorllm/semantic_map.json
rm -f "$BACKEND_ROOT"/../datahubcursorllm/sql_runner.py
# Remove the .envrc file if it exists
if [ -f "$BACKEND_ROOT"/../datahubcursorllm/.envrc ]; then
    echo "Removing .envrc file..."
    rm -f "$BACKEND_ROOT"/../datahubcursorllm/.envrc
fi

# Remove the datahubcursorllm directory if even if it's not empty
echo "Removing datahubcursorllm directory..."
rm -rf "$BACKEND_ROOT"/../datahubcursorllm

echo "DataHub Cursorsetup has been successfully reverted."

