FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/python:3.9-slim__latest

ENV PYTHONPATH=$PYTHONPATH:/dataweb

RUN apt update && apt install -y openjdk-17-jdk

COPY go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel /datamodel

COPY go/src/samsaradev.io/infra/dataplatform/dagster/projects/dataweb /dataweb
COPY dataplatform/dataweb/assets /dataweb/dataweb/assets
COPY dataplatform/dataweb/jobs /dataweb/dataweb/jobs
COPY dataplatform/dataweb/userpkgs /dataweb/dataweb/userpkgs
COPY python3/samsaradev /dataweb/samsaradev
COPY python3/pkgs/firmware /dataweb/firmware

RUN pip install --upgrade pip
RUN pip install --upgrade -r /dataweb/requirements.txt
RUN mkdir -p /opt/dagster/dagster_home

RUN DATABRICKS_UNITY_TOKEN="databricks_token" AWS_SECRET_ACCESS_KEY="aws_secret_key"  AWS_ACCESS_KEY_ID="aws_access_key" SPARK_VERSION=3.3 MODE=PYTEST_RUN pytest /datamodel/datamodel_tests/test_certified_tables.py
RUN DATABRICKS_UNITY_TOKEN="databricks_token" AWS_SECRET_ACCESS_KEY="aws_secret_key"  AWS_ACCESS_KEY_ID="aws_access_key" SPARK_VERSION=3.3 MODE=PYTEST_RUN pytest /dataweb/dataweb_tests/
