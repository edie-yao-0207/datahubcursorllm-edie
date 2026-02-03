# https://github.com/dagster-io/dagster/blob/master/python_modules/automation/automation/docker/images/dagster-celery-k8s/Dockerfile
FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/python:3.10-slim__latest

ENV DAGSTER_VERSION=1.9.1
ARG DAGSTER_VERSION

# All packages are hard-pinned to `dagster`, so setting the version on just `DAGSTER` will ensure
# compatible versions.
RUN pip install -U uv
RUN python -m uv pip install \
    dagster==${DAGSTER_VERSION} \
    dagster-azure \
    dagster-postgres \
    dagster-k8s \
    dagster-aws \
    dagster-celery[flower,redis,kubernetes] \
    dagster-celery-k8s \
    dagster-gcp \
    dagster-graphql \
    dagster-webserver
