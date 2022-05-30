#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
cd "$DIR"

CUSTOM_IMAGE=twosigma/waiter-raven
echo "Building docker image for raven sidecar"
docker build -t ${CUSTOM_IMAGE} .

type kind &>/dev/null && kind load docker-image ${CUSTOM_IMAGE}
