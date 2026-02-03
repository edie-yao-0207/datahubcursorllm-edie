#!/bin/bash

set -eu

cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*  Building Amundsen frontend image...  *
*  This can take a bit of time with     *
*  no cache. The image is large.        *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF

temp_dir=$(mktemp -d amundsen-repo-XXXXXXXXXX)
pushd ${temp_dir} >> /dev/null
git clone --recursive git@github.com:samsara-dev/amundsen.git
pushd amundsen/ >> /dev/null

date=`date '+%Y%m%dT%H%M%S'`
image_version="${date}"
registry="492164655156.dkr.ecr.us-west-2.amazonaws.com"
tag="$registry/amundsen-frontend:${image_version}"
latest_tag="$registry/amundsen-frontend:latest"
echo "Building ${tag} & ${latest_tag}"
docker build . -f Dockerfile.frontend.public --no-cache -t "${tag}" -t "${latest_tag}"

echo "Pushing ${tag} & ${latest_tag}"
AWS_PROFILE=databricks-superadmin aws ecr get-login-password --region=us-west-2  | docker login --username AWS --password-stdin "https://$registry"
AWS_PROFILE=databricks-superadmin docker push "${tag}"
AWS_PROFILE=databricks-superadmin docker push "${latest_tag}"
popd >> /dev/null
popd >> /dev/null

if ! which kubectl > /dev/null; then
  echo "***** installing kubectl... *****"
  brew install kubectl
fi

echo "***** creating kubeconfig... *****"
AWS_PROFILE=databricks-superadmin aws eks --region us-west-2 update-kubeconfig --name amundsen --alias amundsen
AWS_PROFILE=databricks-superadmin awsenv kubectl config use-context amundsen
current_context=$(kubectl config current-context)
if [[ ! "$current_context" == "amundsen" ]]; then
  echo "**** kubectl current context not set to amundsen, exiting... *****"
  exit 1
fi

echo "***** restarting frontend service... *****"
AWS_PROFILE=databricks-superadmin awsenv kubectl rollout restart deployment amundsen-frontend

cat << EOF
***** ***** ***** ***** ***** ***** *****
*                                       *
*   Frontend service restarted.         *
*   It can take a few minutes to start  *
*   up the new pod.                     *
*   View progress with:                 *
*   $ kubectl get pods -n default       *
*                                       *
***** ***** ***** ***** ***** ***** *****
EOF


rm -rf ${temp_dir}
