#!/bin/bash

# adapted from a blog post on Travis about ci with minikube:
# https://blog.travis-ci.com/2017-10-26-running-kubernetes-on-travis-ci-with-minikube

set -e

export MINIKUBE_WANTUPDATENOTIFICATION=false
export MINIKUBE_WANTREPORTERRORPROMPT=false
export CHANGE_MINIKUBE_NONE_USER=true

maybe_install_nsenter() {
    type nsenter || {
        echo 'Installing nsenter ...'
        mkdir -p $HOME/.local/bin
        docker run --rm -v $HOME/.local/bin:/target jpetazzo/nsenter
        type nsenter
    }
}

maybe_install() {
    local util_name=${1:?}
    local util_url=${2:?}
    type $util_name || {
        echo "Installing ${util_name} ..."
        curl -Lo ./$util_name $util_url
        install -Dv ./$util_name $HOME/.local/bin/
        rm ./$util_name
        type $util_name
    }
}

wait_for_server() {
    local service_name=${1:?}
    local ping_command=${2:?}
    echo "Waiting for ${service_name} ..."
    for ((_i=0; _i<20; _i++)); do
        sleep 5
        eval $ping_command && break
    done || {
        echo "ERROR: Failed to start ${service_name}."
        exit 1
    }
    echo "Successfully started  ${service_name}!"
}

# Install minikube dependencies
maybe_install_nsenter
maybe_install kubectl https://storage.googleapis.com/kubernetes-release/release/v1.9.0/bin/linux/amd64/kubectl
maybe_install minikube https://storage.googleapis.com/minikube/releases/v0.25.0/minikube-linux-amd64

# Start minikube
sudo -E $(which minikube) start --vm-driver=none --extra-config=apiserver.Authorization.Mode=RBAC

# Update the kubectl context settings to target the minikube cluster
minikube update-context

# Wait until the minikube cluster is ready
JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'
wait_for_server 'minikube cluster' "kubectl get nodes -o jsonpath='${JSONPATH}' 2>&1 | grep -q 'Ready=True'"

# Echo some info about the k8s setup
kubectl cluster-info
kubectl version

# Create test user's namespace
kubectl create -f - <<EOF
{
  "kind": "Namespace",
  "apiVersion": "v1",
  "metadata": {
    "name": "${USER}",
    "labels": {
      "name": "${USER}"
    }
  }
}
EOF

# Start Kubernetes proxy for access to API server
kubectl proxy --port=8001 &
wait_for_server 'k8s proxy' 'curl -sf http://localhost:8001/ >/dev/null'
