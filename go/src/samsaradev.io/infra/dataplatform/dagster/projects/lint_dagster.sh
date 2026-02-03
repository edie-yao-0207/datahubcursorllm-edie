#!/bin/bash

function check_pyenv_version {
    local version=$1
    pyenv versions | grep -q "$version"
}

echo "Current Python version: $(python --version 2>&1)"

# Create virtual environment if it doesn't exist
version_to_check="3.7.13"
check_pyenv_version "$version_to_check"

if ! check_pyenv_version "$version_to_check"; then
    command="pyenv install 3.7.13"
    eval "$command"
fi

function get_current_pyenv_version {
    if ! pyenv_version=$(pyenv version-name 2>/dev/null); then
        echo "pyenv is not installed or no Python version is set."
    else
        echo "$pyenv_version"
    fi
}

initial_pyenv_version=$(get_current_pyenv_version)

echo "initial pyenv version: $initial_pyenv_version"

eval "$(pyenv init -)"
pyenv virtualenv 3.7.13 dagster-linter-env
echo "datamodel" > .python-version
pyenv activate dagster-linter-env

echo "pyenv version before pip install: $(get_current_pyenv_version)"

# Install dependencies
(
    cd "$BACKEND_ROOT/go/src/samsaradev.io/infra/dataplatform/dagster/projects" || exit
    pip install -r linter_requirements.txt \
    && "$BACKEND_ROOT/python3/tools/python-format-dagster"
)

command="pyenv activate $initial_pyenv_version"
eval "$command"

echo "pyenv version at end of script: $(get_current_pyenv_version)"
