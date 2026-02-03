#!/usr/bin/env bash

# This script deploys the Samsara helm chart to the amundsen k8s cluster

set -eu
cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*   Deploying Amundsen helm chart...    *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF

echo "***** Deploying Amundsen helm chart... *****"

if ! which kubectl > /dev/null; then
  echo "***** installing kubectl... *****"
  brew install kubectl
fi

if ! which helm > /dev/null; then
  echo "***** installing helm *****"
  brew install helm
fi

pushd $BACKEND_ROOT >> /dev/null
if [[ ! -d "../amundsen" ]]; then
  echo "***** samsara amundsen repo does not exist, cloning... *****"
  cd ..
  git clone --recursive git@github.com:samsara-dev/amundsen.git
fi

pushd ../amundsen/
if [[ "$(git branch --show-current)" != "main" ]]; then
  cat << EOF
***** ***** ***** ***** ***** ***** ***** ***** ***** *
*                                                     *
*  ** ERROR **                                        *
*  Not on main branch in amundsen repo.               *
*  We only want to deploy the main branch             *
*  to protect against deploying dev branches          *
*  Go to Amundsen repo and switch to main and retry.  *
*                                                     *
***** ***** ***** ***** ***** ***** ***** ***** ***** *
EOF
  exit 1
fi
popd >> /dev/null

echo "***** creating kubeconfig... *****"
AWS_PROFILE=databricks-superadmin aws eks --region us-west-2 update-kubeconfig --name amundsen --alias amundsen
AWS_PROFILE=databricks-superadmin awsenv kubectl config use-context amundsen
current_context=$(kubectl config current-context)
if [[ ! "$current_context" == "amundsen" ]]; then
  echo "**** kubectl current context not set to amundsen, exiting... *****"
  exit 1
fi

# Install chart dependencies & deploy to eks cluster
pushd $BACKEND_ROOT >> /dev/null
pushd ../amundsen/amundsen-kube-helm/templates/helm >> /dev/null
echo "***** updating helm dependencies... *****"
helm dependency update
echo "***** upgrading helm chart... *****"
helm upgrade --install amundsen . --values values.yaml
popd >> /dev/null
popd >> /dev/null

cat << EOF
***** ***** ***** ***** ***** ***** ***** ***** *****
*                                                   *
*   The helm chart has been successfully upgraded   *
*   Wait ~30 seconds and view new pods with:        *
*   $ kubectl get pods -n default                   *
*                                                   *
***** ***** ***** ***** ***** ***** ***** ***** *****
EOF
