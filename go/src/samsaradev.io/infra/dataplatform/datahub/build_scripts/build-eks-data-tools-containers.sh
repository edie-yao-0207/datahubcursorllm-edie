#!/bin/bash

set -euo pipefail
# this script should be run manually from master when changes are made to the datahub helm charts

cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*  Building images for dagster and      *
*  datahub applications.                *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF

export DOCKER_DEFAULT_PLATFORM=linux/amd64

# buildkite
apt-get update
apt-get install openjdk-17-jdk -y

# without setting the locale, datahub-upgrade images fail when doing helm deployments
apt-get -y install locales
locale-gen en_US.UTF-8
export LANG=en_US.UTF-8
export LANGUAGE=en_US.UTF-8
export LC_ALL=en_US.UTF-8

update-alternatives --list java | grep java-17
update-alternatives --list javac | grep java-17

# shellcheck disable=SC2046
update-alternatives --set java $(update-alternatives --list java | grep java-17) && update-alternatives --set javac $(update-alternatives --list javac | grep java-17)

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# if running locally, run this code
# brew install openjdk@17
# export JAVA_HOME=/opt/homebrew/opt/openjdk@17
# export PATH=$JAVA_HOME/bin:$PATH

DAGSTER_VERSION="1.9.1"
DATAHUB_VERSION="v0.15.0.1"

temp_dir=$(mktemp -d datahub-repo-XXXXXXXXXX)
pushd "${temp_dir}" >> /dev/null
git clone https://github.com/datahub-project/datahub.git
pushd datahub/ >> /dev/null
git checkout tags/${DATAHUB_VERSION}

echo "updating spring boot version to 3.3.6"
sed -i'' -e 's|ext.springBootVersion = '\''3.2.9'\''|ext.springBootVersion = '\''3.3.6'\''|g' build.gradle

./gradlew :metadata-service:war:build -x test --parallel
mv ./metadata-service/war/build/libs/war.war .

./gradlew :metadata-jobs:mae-consumer-job:build -x test --parallel
mv ./metadata-jobs/mae-consumer-job/build/libs/mae-consumer-job.jar .

./gradlew :metadata-jobs:mce-consumer-job:build -x test --parallel
mv ./metadata-jobs/mce-consumer-job/build/libs/mce-consumer-job.jar .

./gradlew :datahub-upgrade:build -x test --parallel
mv ./datahub-upgrade/build/libs/datahub-upgrade.jar .

registry="492164655156.dkr.ecr.us-west-2.amazonaws.com"
awsregion="us-west-2"

echo "logging into AWS"
aws ecr get-login-password --region "${awsregion}" | docker login --username AWS --password-stdin "https://$registry"

# CI registry
ci_registry="228413409843.dkr.ecr.us-west-2.amazonaws.com"
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "https://$ci_registry"

pushd ../../ >> /dev/null

process_directory() {
  local image_path=$1
  local registry=$2
  local temp_dir=$3

  pushd "${temp_dir}" >> /dev/null
  pushd datahub/ >> /dev/null

  if [[ "$image_path" == *"acryl-datahub-actions"* ]]; then
    pushd ../../ >> /dev/null
    temp_dir_actions=$(mktemp -d datahub-actions-repo-XXXXXXXXXX)
    pushd "${temp_dir_actions}" >> /dev/null
    git clone https://github.com/acryldata/datahub-actions.git
    pushd datahub-actions/ >> /dev/null
  elif [[ "$image_path" == *"docker_vendored_images/datahub"* ]]; then
    container_name=$(echo "$image_path" | awk -F'/' '{print $(NF-1)}')

    # if 'setup' is in container name, set container_prefix to "", else set it to "datahub-"
    if [[ "$container_name" == *"-setup" ]]; then
      container_prefix=""
    else
      container_prefix="datahub-"
    fi

    cp "docker/${container_prefix}${container_name}/Dockerfile" "../../${image_path}/Dockerfile"
  fi

  if [[ "$image_path" == *"docker_vendored_images/datahub"* ]]; then
    app_name="datahub"
    tag="${DATAHUB_VERSION}"
    sed -i'' -e 's|FROM alpine:3.18|FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/alpine:3.18__latest|g' "../../${image_path}/Dockerfile"
    sed -i'' -e 's|FROM alpine:3.20|FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/alpine:3.20__latest|g' "../../${image_path}/Dockerfile"
    sed -i'' -e 's|FROM golang:1-alpine3.20|FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/golang-alpine:alpine3.20__latest|g' "../../${image_path}/Dockerfile"
    sed -i'' -e 's|FROM golang:1-alpine3.18|FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/golang-alpine:alpine3.18__latest|g' "../../${image_path}/Dockerfile"
    sed -i'' -e 's|FROM python:3.10|FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/python:3.10__latest|g' "../../${image_path}/Dockerfile"
    sed -i'' -e 's|FROM python:3-alpine|FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/python:3-alpine__latest|g' "../../${image_path}/Dockerfile"
    sed -i'' -e "s|FROM confluentinc/cp-base-new:\$KAFKA_DOCKER_VERSION|FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/confluentinc/cp-base-new:7.4.4__latest|g" "../../${image_path}/Dockerfile"
  elif [[ "$image_path" == *"docker_vendored_images/dagster"* ]]; then
    app_name="dagster"
    tag="${DAGSTER_VERSION}"
  else
    return  # skip processing if none match
  fi

  repository=$(basename "$image_path")

  uri="${registry}/${app_name}-${repository}"
  latest_tag="${uri}:latest"
  prod_tag="${uri}:prod"
  version_tag="${uri}:${tag}"

  echo "Building ${version_tag} & ${latest_tag} & ${prod_tag}"
  docker build . -f "../../${image_path}/Dockerfile" -t "${version_tag}" -t "${latest_tag}" -t "${prod_tag}"

  echo "Pushing ${version_tag} & ${latest_tag} & ${prod_tag}"
  docker push "${version_tag}"
  docker push "${latest_tag}"
  docker push "${prod_tag}"

  echo "✅ Image pushed: ${version_tag}"
  pushd ../../ >> /dev/null
}

# these images are copied from the dagster and datahub projects so that we can build them and upload to ECR
# where they can be scanned by Wiz for vulnerabilities

# Process the specific directory first, since other images depend on it
specific_dir="go/src/samsaradev.io/infra/dataplatform/datatools/docker_vendored_images/datahub/ingestion-base/"
if [[ -d "$specific_dir" ]]; then
  process_directory "$specific_dir" "$registry" "$temp_dir"
fi

  # List of image paths to skip
  skip_image_paths=()

# Process all other directories
for image_path in go/src/samsaradev.io/infra/dataplatform/datatools/docker_vendored_images/*/*/; do
  echo "Processing $image_path"

  skip=false
  for skip_path in "${skip_image_paths[@]}"; do
    if [[ "$image_path" == "$skip_path" ]]; then
      skip=true
      break
    fi
  done

  if $skip; then
    echo "Skipping $image_path"
    continue
  fi

  if [[ "$image_path" != "$specific_dir" ]]; then
    process_directory "$image_path" "$registry" "$temp_dir"
  fi
done

cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                          *
*   Data tools images built.               *
*                                          *
***** ***** ***** ***** ***** ***** *****
EOF
