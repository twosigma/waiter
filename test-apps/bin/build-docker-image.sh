#!/usr/bin/env bash
# Usage: build-docker-image.sh
#
# Builds a docker image for the kitchen test app that can be used as a minimesos agent.

set -ex

TEST_APPS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=twosigma/kitchen

cd ${TEST_APPS_DIR}
echo "Building docker images for ${NAME}"
docker build -t ${NAME} .
