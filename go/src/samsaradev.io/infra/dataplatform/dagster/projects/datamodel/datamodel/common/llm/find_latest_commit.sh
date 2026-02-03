#!/bin/bash
if [ -z "$BACKEND_ROOT" ]; then
  BACKEND_ROOT="$HOME/co/backend"
fi


LATEST_SQL_RUNNER_HASH=$(git -C "$BACKEND_ROOT" show origin/master:go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/common/llm/sql_runner.py | shasum | awk '{print $1}')
if [ -z "$LATEST_SQL_RUNNER_HASH" ]; then
    echo "Failed to retrieve the latest SQL Runner hash."
    exit 1
fi

echo "$LATEST_SQL_RUNNER_HASH"
