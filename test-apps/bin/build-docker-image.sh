#!/usr/bin/env bash
# Usage: build-docker-image.sh
#
# Builds a docker image that includes the test apps and can also be used as a minimesos agent.

set -ex

TEST_APPS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=twosigma/kitchen

cd ${TEST_APPS_DIR}
echo "Building docker images for ${NAME}"
docker build -t ${NAME} .
