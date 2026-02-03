FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/python:3.10-slim__latest

ENV PYTHONPATH=$PYTHONPATH:/ml

RUN apt update && apt install -y openjdk-17-jdk

COPY go/src/samsaradev.io/infra/dataplatform/dagster/projects/ml /ml
COPY go/src/samsaradev.io/infra/dataplatform /dataplatform
COPY go/src/samsaradev.io/team /team

RUN mkdir -p /opt/dagster/dagster_home

RUN pip install --upgrade pip
RUN pip install --upgrade -r /ml/requirements.txt

# Install test dependencies and run tests during build
RUN pip install pytest pyspark==3.3.0
RUN SPARK_VERSION=3.3 MODE=PYTEST_RUN pytest /ml/ml_tests/
