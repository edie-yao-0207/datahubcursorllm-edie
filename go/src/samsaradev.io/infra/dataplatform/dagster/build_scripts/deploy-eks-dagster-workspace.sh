#!/bin/bash

set -eu


cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*  Apply ConfigMap for dagster          *
*  workspace.                           *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF

if ! command -v kubectl > /dev/null; then
  echo "***** installing kubectl... *****"
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
fi

if [[ "$REGION" == "US" ]]; then
  awsregion="us-west-2"
elif [[ "$REGION" == "EU" ]]; then
  awsregion="eu-west-1"
else
  echo "$REGION"
  echo "Error: unknown region"
  exit 1
fi

echo "***** creating kubeconfig... *****"
aws eks --region "${awsregion}" update-kubeconfig --name dagster --alias dagster
echo "***** created kubeconfig... *****"
kubectl config use-context dagster
echo "***** ran config use-context dagster... *****"

branch="$BUILDKITE_BRANCH"

if [ "$branch" != "master" ]; then
echo "branch build: dry run, exiting"
exit 0
fi

aws eks --region us-west-2 update-kubeconfig --name dagster --alias dagster --role arn:aws:iam::492164655156:role/OrganizationAccountAccessRole

echo "***** updated kubeconfig... *****"

current_context=$(kubectl config current-context)
if [[ ! "$current_context" == "dagster" ]]; then
  echo "**** kubectl current context not set to dagster, exiting... *****"
  exit 1
fi

pushd go/src/samsaradev.io/infra/dataplatform/dagster/eksaddons/ >> /dev/null
kubectl apply --namespace=dagster-prod -f configmap-dagster-prod-workspace.yaml
popd >> /dev/null

cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*   Code servers updated.              *
*   It can take a few minutes to start  *
*   up the new pod.                     *
*   View progress with:                 *
*   $ kubectl get pods -n default       *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF

echo "***** Removing unused containers... *****"

kubectl delete pod --namespace=dagster-prod  --field-selector=status.phase==Failed
kubectl delete pod --namespace=dagster-prod  --field-selector=status.phase==Succeeded

cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*   Removed unused containers.          *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF
