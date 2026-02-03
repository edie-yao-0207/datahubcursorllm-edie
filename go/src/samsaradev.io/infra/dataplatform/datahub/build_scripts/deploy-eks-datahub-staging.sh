#!/bin/bash

set -euo pipefail
# this script should be run manually from master when changes are made to the datahub helm charts

cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*  Installing helm charts for datahub   *
*  core services.                       *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF

COMMIT_SHA_SHORT=$(git rev-parse --short HEAD)

export DOCKER_DEFAULT_PLATFORM=linux/amd64

export NODE_OPTIONS='--max-old-space-size=4096'

apt-get update
apt-get install openjdk-17-jdk -y
update-alternatives --list java | grep java-17
update-alternatives --list javac | grep java-17

# shellcheck disable=SC2046
update-alternatives --set java $(update-alternatives --list java | grep java-17) && update-alternatives --set javac $(update-alternatives --list javac | grep java-17)

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

DATAHUB_VERSION="v0.15.0.1"

temp_dir=$(mktemp -d datahub-repo-XXXXXXXXXX)
pushd "${temp_dir}" >> /dev/null
git clone https://github.com/datahub-project/datahub.git
pushd datahub/ >> /dev/null
git checkout tags/${DATAHUB_VERSION}

echo "patching datahub frontend dockerfile to use internal registry"
sed -i'' -e 's|FROM alpine:3.18 AS base|FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/alpine:3.18__latest AS base|g' docker/datahub-frontend/Dockerfile

echo "patching datahub frontend play.server.akka.max-header-value-length"
sed -i'' -e 's|play.server.akka.max-header-size|play.server.akka.max-header-value-length|g' datahub-frontend/conf/application.conf

echo "patching analytics file with mixpanel token"
cp /code/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub/analytics_patch.txt datahub-web-react/src/conf/analytics.ts
sed -i'' -e '/mixpanel: isThirdPartyLoggingEnabled/s/isThirdPartyLoggingEnabled/true/' datahub-web-react/src/app/analytics/analytics.ts

echo "patching file size formatter to use 1024 instead of 1000"
sed -i'' -e 's/const k = 1000; \/\/ /const k = 1024; \/\/ /' datahub-web-react/src/app/shared/formatNumber.ts

echo "patching frontend files"
cp /code/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub/patches/column_stats_patch.txt datahub-web-react/src/app/entity/shared/tabs/Dataset/Stats/snapshot/ColumnStats.tsx
cp /code/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub/patches/EmbedTab_patch.txt datahub-web-react/src/app/entity/shared/tabs/Embed/EmbedTab.tsx
cp /code/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub/patches/DatasetEntity_patch.txt datahub-web-react/src/app/entity/dataset/DatasetEntity.tsx
cp /code/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub/patches/DatasetStatsSummary_patch.txt datahub-web-react/src/app/entity/dataset/shared/DatasetStatsSummary.tsx
cp /code/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub/patches/DatasetStatsSummarySubHeader_patch.txt datahub-web-react/src/app/entity/dataset/profile/stats/stats/DatasetStatsSummarySubHeader.tsx
cp /code/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub/patches/SearchPage_patch.txt datahub-web-react/src/app/search/SearchPage.tsx
cp /code/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub/patches/EmptySearchResults_patch.txt datahub-web-react/src/app/search/EmptySearchResults.tsx
cp /code/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub/patches/event_patch.txt datahub-web-react/src/app/analytics/event.ts


cd datahub-web-react
yarn cache clean
yarn add postcss-import --dev
yarn add postcss-preset-env --dev
yarn install
yarn list --pattern postcss

echo "module.exports = {
  plugins: [
    require('postcss-import'),
    require('postcss-preset-env')({}),
  ],
};" > postcss.config.js


cat postcss.config.js

cd ..

apt-get update && apt-get install -y qemu-user-static

./gradlew :datahub-frontend:dist -x test -x yarnTest -x yarnLint --parallel --stacktrace

mv ./datahub-frontend/build/distributions/datahub-frontend-*.zip datahub-frontend.zip

registry="492164655156.dkr.ecr.us-west-2.amazonaws.com"
awsregion="us-west-2"
uri="${registry}/datahub-frontend-staging"
tag="${uri}"

image_tag="${COMMIT_SHA_SHORT}"
tag="${uri}:${image_tag}"
version_tag="${uri}:${DATAHUB_VERSION}"

echo "logging into AWS"
aws ecr get-login-password --region "${awsregion}" | docker login --username AWS --password-stdin "https://$registry"

# CI registry
ci_registry="228413409843.dkr.ecr.us-west-2.amazonaws.com"
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "https://$ci_registry"

echo "Building ${tag}"
DOCKER_BUILDKIT=1 docker build --platform=linux/amd64 -t "${tag}" -t "${version_tag}" -f ./docker/datahub-frontend/Dockerfile .

docker inspect --format='{{.Architecture}}' "${tag}"

docker push "${tag}"
docker push "${version_tag}"

if ! command -v kubectl > /dev/null; then
  echo "***** installing kubectl... *****"
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
fi

if ! command -v helm > /dev/null; then
  echo "***** installing helm *****"
  curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  chmod 700 get_helm.sh
  ./get_helm.sh
fi

echo "***** creating kubeconfig... *****"
aws eks --region us-west-2 update-kubeconfig --name datahub --alias datahub
kubectl config use-context datahub
current_context=$(kubectl config current-context)
if [[ ! "$current_context" == "datahub" ]]; then
  echo "**** kubectl current context not set to datahub, exiting... *****"
  exit 1
fi

echo "***** installing datahub prerequisites helm values... *****"
pushd /code/go/src/samsaradev.io/infra/dataplatform/datahub/helm/datahub/charts/prerequisites > /dev/null
helm upgrade --install prerequisites . --namespace=datahub-staging  --values values_staging.yaml
popd > /dev/null

echo "***** installing datahub core helm values... *****"
pushd /code/go/src/samsaradev.io/infra/dataplatform/datahub/helm/datahub/charts/datahub > /dev/null

cp -f "values_staging.yaml" "values_staging_with_tag.yaml"
sed -i "s/\$COMMIT_SHA_SHORT_frontend/${COMMIT_SHA_SHORT}/g" "values_staging_with_tag.yaml"

helm upgrade --install datahub . --namespace=datahub-staging  --values values_staging_with_tag.yaml --timeout 45m --debug
popd > /dev/null

cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                          *
*   Datahub services updated.              *
*   It can take a few minutes to start     *
*   up the new pod.                        *
*   View progress with:                    *
*   $ kubectl get pods -n datahub-staging  *
*                                          *
***** ***** ***** ***** ***** ***** *****
EOF
