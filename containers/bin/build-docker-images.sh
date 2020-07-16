#!/usr/bin/env bash
# Usage: build-docker-images.sh [OPTION]
#
# Builds docker images that are used with our default configurations to run and test Waiter
# in both the Marathon (minimesos) and Kubernetes (minikube) environments.
#
# Mutually exclusive options:
#   --minikube    Build images in the minikube docker context
#   --all         Build images in both the local and the minikube docker contexts

set -ex

if [ "$1" == --minikube ]; then
    echo 'Using minikube docker environment'
    eval $(minikube docker-env)
elif [ "$1" == --all ]; then
    eval "$0 --minikube"
    echo 'Continuing using local docker environment'
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
cd "$DIR"

# the custom-env container uses test-apps as a base, so this order is important
./test-apps/bin/build-docker-image.sh
./custom-env/bin/build-docker-image.sh

# the fileserver container is independent
./fileserver/bin/build-docker-image.sh

# the envoy reverse-proxy container is independent
./reverse-proxy/bin/build-docker-image.sh
