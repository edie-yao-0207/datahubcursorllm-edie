FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/python:3.9-slim__latest

ENV PYTHONPATH=$PYTHONPATH:/dataweb

RUN apt update && apt install -y openjdk-17-jdk

COPY go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel /datamodel
COPY go/src/samsaradev.io/infra/dataplatform /dataplatform
COPY go/src/samsaradev.io/team /team
COPY dataplatform/workflows /dataplatform/workflows
COPY dataplatform/notebooks /dataplatform/notebooks
COPY .cursor/rules/query_agent.mdc /datamodel/query_agent.mdc
COPY bin/cursor-query-agent-setup.sh /datamodel/bin/cursor-query-agent-setup.sh
COPY go/src/samsaradev.io/infra/dataplatform/dagster/projects/test-cursor-query-agent.sh /datamodel/test-cursor-query-agent.sh

COPY go/src/samsaradev.io/infra/dataplatform/dagster/projects/dataweb /dataweb
COPY dataplatform/dataweb/assets /dataweb/dataweb/assets
COPY dataplatform/dataweb/jobs /dataweb/dataweb/jobs
COPY dataplatform/dataweb/userpkgs /dataweb/dataweb/userpkgs

RUN pip install --upgrade pip
RUN pip install --upgrade -r /datamodel/requirements.txt

# create virtual environment for datahub
RUN python -m venv /datahub_env
RUN /datahub_env/bin/pip install --upgrade pip
RUN /datahub_env/bin/pip install --upgrade -r /datamodel/requirements.datahub.txt
RUN /datahub_env/bin/python -m pip list

# create virtual environment for datahub unity catalog ingestion
RUN python -m venv /datahub_env_unity
RUN /datahub_env_unity/bin/pip install --upgrade pip
RUN /datahub_env_unity/bin/pip install --upgrade -r /datamodel/requirements.datahub.unity.txt
RUN /datahub_env_unity/bin/python -m pip list

RUN mkdir -p /opt/dagster/dagster_home

# Patch the DataHub glue ingestion script with our small changes
RUN cp /datamodel/datamodel/resources/datahub/patches/source_common_subtypes_patch.txt /usr/local/lib/python3.9/site-packages/datahub/ingestion/source/common/subtypes.py
RUN cp /datamodel/datamodel/resources/datahub/patches/source_common_subtypes_patch.txt /datahub_env/lib/python3.9/site-packages/datahub/ingestion/source/common/subtypes.py
RUN mv /datamodel/datamodel/resources/datahub/patches/source_common_subtypes_patch.txt /datahub_env_unity/lib/python3.9/site-packages/datahub/ingestion/source/common/subtypes.py

# Patch DataHub Unity Catalog ingestion
RUN cp /datamodel/datamodel/resources/datahub/unity_recipe_updates_patch.txt /usr/local/lib/python3.9/site-packages/datahub/ingestion/source/unity/source.py
RUN cp /datamodel/datamodel/resources/datahub/unity_recipe_updates_patch.txt /datahub_env/lib/python3.9/site-packages/datahub/ingestion/source/unity/source.py
RUN mv /datamodel/datamodel/resources/datahub/unity_recipe_updates_patch.txt /datahub_env_unity/lib/python3.9/site-packages/datahub/ingestion/source/unity/source.py

# Patch DataHub dbt ingestion
RUN cp /datamodel/datamodel/resources/datahub/dbt_common_patch.txt /usr/local/lib/python3.9/site-packages/datahub/ingestion/source/dbt/dbt_common.py
RUN mv /datamodel/datamodel/resources/datahub/dbt_common_patch.txt /datahub_env/lib/python3.9/site-packages/datahub/ingestion/source/dbt/dbt_common.py

RUN DATABRICKS_UNITY_TOKEN="databricks_token" AWS_SECRET_ACCESS_KEY="aws_secret_key"  AWS_ACCESS_KEY_ID="aws_access_key" SPARK_VERSION=3.3 MODE=PYTEST_RUN pytest /datamodel/datamodel_tests/
RUN DATABRICKS_UNITY_TOKEN="databricks_token" AWS_SECRET_ACCESS_KEY="aws_secret_key"  AWS_ACCESS_KEY_ID="aws_access_key" SPARK_VERSION=3.3 MODE=PYTEST_RUN pytest /dataweb/dataweb_tests/

# Run cursor query agent setup tests
RUN chmod +x /datamodel/test-cursor-query-agent.sh /datamodel/bin/cursor-query-agent-setup.sh
RUN CI=true BUILDKITE=true BACKEND_ROOT=/datamodel bash /datamodel/test-cursor-query-agent.sh
