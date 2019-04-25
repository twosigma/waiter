#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
cd "$DIR"

CUSTOM_IMAGE=twosigma/integration
echo "Building docker image for ${CUSTOM_IMAGE}"
docker build -t ${CUSTOM_IMAGE} .
