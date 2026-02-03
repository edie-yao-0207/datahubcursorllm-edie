#!/bin/bash

set -euo pipefail
source .buildkite/utils/dagster-changes.sh

cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*  Building Dagster code server images. *
*  This can take a bit of time with     *
*  no cache.                            *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF

DAGSTER_VERSION="1.9.1"

COMMIT_SHA_SHORT=$(git rev-parse --short HEAD)

if [[ "$REGION" == "US" ]]; then
  registry="492164655156.dkr.ecr.us-west-2.amazonaws.com"
  awsregion="us-west-2"
elif [[ "$REGION" == "EU" ]]; then
  registry="353964698255.dkr.ecr.eu-west-1.amazonaws.com"
  awsregion="eu-west-1"
else
  echo "$REGION"
  echo "Error: unknown region"
  exit 1
fi

ci_registry="228413409843.dkr.ecr.us-west-2.amazonaws.com"

if [ "${LIVE_RUN_IN_BRANCH_BUILD:-}" == "" ]; then
    LIVE_RUN_IN_BRANCH_BUILD=false
fi

if [ "${FORCE_DEPLOY_REFRESH:-}" == "" ]; then
    FORCE_DEPLOY_REFRESH=false
fi


branch="$BUILDKITE_BRANCH"


# Build docker images and push to ECR
echo "Now authenticating for host name: ${registry}"

pushd go/src/samsaradev.io/infra/dataplatform/dagster/helm/ > /dev/null
cp -f "dagster-user-deployments.values.yaml" "codeserver-values.yaml"
popd > /dev/null

for image_path in go/src/samsaradev.io/infra/dataplatform/dagster/docker/*/; do

  code_path=${image_path//dagster\/docker/dagster\/projects}

  if [ "$image_path" == "go/src/samsaradev.io/infra/dataplatform/dagster/docker/dataweb/" ]; then
    code_path2="dataplatform/dataweb"
  else
    code_path2=$code_path  # datamodel has no 2nd code path, so we will set it to the first code path
  fi

  image_path_without_slash=${image_path%/}
  model_path=${image_path_without_slash##*/}

  if [ "$model_path" == "" ]; then
    continue
  fi

  if ! has_dagster_changes "$image_path" && ! has_dagster_changes "$code_path"  && ! has_dagster_changes "$code_path2" && [ ! "$FORCE_DEPLOY_REFRESH" == true ]; then
    echo "no changes to project, skipping container upload and using latest tag for: $image_path}"
    pushd go/src/samsaradev.io/infra/dataplatform/dagster/helm/ > /dev/null
    # need to replace tag with latest uploaded tag in the temp codeserver-values.yaml file, or helm update will error
    latest_uploaded_tag=$(aws ecr describe-images --repository-name dagster-"$model_path"-codeserver --image-ids imageTag=prod | python3 -c "import sys, json; data=json.load(sys.stdin); tags=data['imageDetails'][0]['imageTags']; print(next(tag for tag in tags if tag not in ['prod', 'latest']))")
    echo "latest discovered tag:"
    echo "$latest_uploaded_tag"
    sed -i "s/\$COMMIT_SHA_SHORT_$model_path/${latest_uploaded_tag}/g" "codeserver-values.yaml"
    popd > /dev/null
    continue
  fi

  pushd go/src/samsaradev.io/infra/dataplatform/dagster/helm/ > /dev/null
  sed -i "s/\$COMMIT_SHA_SHORT_$model_path/${COMMIT_SHA_SHORT}/g" "codeserver-values.yaml"
  popd > /dev/null

  aws ecr get-login-password --region "${awsregion}" | docker login --username AWS --password-stdin "https://$registry"

  # CI registry
  aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "https://$ci_registry"

  image_tag="${COMMIT_SHA_SHORT}"
  repository=$(basename "$image_path")

  uri="${registry}/dagster-${repository}-codeserver"
  tag="${uri}:${image_tag}"
  latest_tag="${uri}:latest"
  prod_tag="${uri}:prod"

  echo "Building ${tag} & ${latest_tag} & ${prod_tag}"
  docker build . -f "${image_path}/Dockerfile" -t "${tag}" -t "${latest_tag}" -t "${prod_tag}"

  if [ "$branch" != "master" ] && [ ! "$LIVE_RUN_IN_BRANCH_BUILD" == true ]  && [ ! "$FORCE_DEPLOY_REFRESH" == true ]; then
    echo "branch build: dry run, skipping container upload (after container build)"
    continue
  fi

  echo "Pushing ${tag} & ${latest_tag} & ${prod_tag}"
  docker push "${tag}"
  docker push "${latest_tag}"
  docker push "${prod_tag}"

  echo "✅ Image pushed: ${COMMIT_SHA_SHORT}"
  echo "☸️ When deploying, please make sure image tag is updated to ${COMMIT_SHA_SHORT} in Dagster UI"

done

if [ "$branch" != "master" ] && [ ! "$LIVE_RUN_IN_BRANCH_BUILD" == true ]  && [ ! "$FORCE_DEPLOY_REFRESH" == true ]; then
  echo "branch build: dry run, images not pushed"
fi

cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*   Codeservers images pushed to        *
*   containerregistry.                  *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF



cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*  Installing helm charts for dagster   *
*  core services.                       *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF

if ! command -v kubectl > /dev/null; then
  echo "***** installing kubectl... *****"
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
fi

if ! command -v helm > /dev/null; then
  echo "***** installing helm *****"
  export DESIRED_VERSION="v3.18.4"
  curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  chmod 700 get_helm.sh
  ./get_helm.sh
fi

echo "***** creating kubeconfig... *****"
aws eks --region "${awsregion}" update-kubeconfig --name dagster --alias dagster
kubectl config use-context dagster
current_context=$(kubectl config current-context)
if [[ ! "$current_context" == "dagster" ]]; then
  echo "**** kubectl current context not set to dagster, exiting... *****"
  exit 1
fi

aws eks --region "${awsregion}" update-kubeconfig --name dagster --alias dagster --role arn:aws:iam::492164655156:role/OrganizationAccountAccessRole

if [ "$branch" != "master" ] && [ ! "$LIVE_RUN_IN_BRANCH_BUILD" == true ]  && [ ! "$FORCE_DEPLOY_REFRESH" == true ]; then
  echo "branch build: dry run, exiting"
  exit 0
fi

pushd go/src/samsaradev.io/infra/dataplatform/dagster/helm/ > /dev/null
echo "***** installing dagster core helm values... *****"
# --skip-schema-validation added due to kubernetesjsonschema.dev being down
# See: https://github.com/dagster-io/dagster/issues/33134
helm upgrade --install dagster dagster --repo https://dagster-io.github.io/helm --version "$DAGSTER_VERSION" --values dagster.values.yaml --namespace dagster-prod --skip-schema-validation

echo "***** installing user deployments helm values... *****"
# --skip-schema-validation added due to kubernetesjsonschema.dev being down
# See: https://github.com/dagster-io/dagster/issues/33134
helm upgrade --force --install dagster-user-deployments dagster-user-deployments  --repo https://dagster-io.github.io/helm --version "$DAGSTER_VERSION" --values codeserver-values.yaml --namespace dagster-prod --skip-schema-validation
echo "***** installed user deployments helm values, deleting temporary codeserver-values.yaml... *****"
rm codeserver-values.yaml
popd > /dev/null


cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*   Dagster services updated.           *
*   It can take a few minutes to start  *
*   up the new pods.                    *
*   View progress with:                 *
*   $ kubectl get pods -n default       *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF
