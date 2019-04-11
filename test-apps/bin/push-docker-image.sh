#!/usr/bin/env bash
# Usage: push-docker-image.sh
#
# Builds and pushes a docker image that includes the test apps and can also be used as a minimesos agent.

set -eux

TEST_APPS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=twosigma/waiter-test-apps

LABEL=$(date +%Y%m%d)

echo "Removing existing images for ${LABEL} and latest"
docker image rm -f ${NAME}:${LABEL}
docker image rm -f ${NAME}:latest

echo "Building docker image for ${NAME}"
${TEST_APPS_DIR}/bin/build-docker-image.sh

echo "Pushing docker image for ${NAME}:${LABEL}"
docker tag ${NAME} ${NAME}:${LABEL}
docker push ${NAME}:${LABEL}

echo "Pushing docker image for ${NAME}:latest"
docker tag ${NAME} ${NAME}:latest
docker push ${NAME}:latest
