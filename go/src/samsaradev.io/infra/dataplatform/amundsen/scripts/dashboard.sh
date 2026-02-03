#!/bin/bash

set -eu

secret=$(AWS_PROFILE=databricks-superadmin awsenv kubectl -n kube-system get secret | grep eks-admin | awk '{print $1}')
token=$(AWS_PROFILE=databricks-superadmin awsenv kubectl -n kube-system describe secret "${secret}" | grep 'token:      ' | sed 's/^.*: //' | xargs)
AWS_PROFILE=databricks-superadmin awsenv kubectl proxy &
python -m webbrowser http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/#!/login

cat << EOF
***** ***** ***** ***** ***** ***** ***** ***** ***** ***** *****

K8s Dashboard Token:
${token}

Paste the token in the login prompt

THe kubectl proxy is running the background to serve the dashboard
locally. Use '$ killall kubectl' to stop.

***** ***** ***** ***** ***** ***** *****
EOF
