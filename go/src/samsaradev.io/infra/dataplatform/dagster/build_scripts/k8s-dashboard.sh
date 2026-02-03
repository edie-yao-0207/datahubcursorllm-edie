#!/bin/bash

set -eu

# Dashboard launch script requires that 'kubectl' version 1.24.0+.


token=$(AWS_PROFILE=databricks-superadmin kubectl -n kubernetes-dashboard create token admin-user)
kubectl -n kubernetes-dashboard port-forward svc/kubernetes-dashboard-nginx-controller 8443:443 &>/dev/null &



cat << EOF
***** ***** ***** ***** ***** ***** ***** ***** ***** ***** *****

K8s Dashboard Token:
${token}

Paste the token in the login prompt:
https://localhost:8443

THe kubectl proxy is running the background to serve the dashboard
locally. Use '$ killall kubectl' to stop.

***** ***** ***** ***** ***** ***** *****
EOF
