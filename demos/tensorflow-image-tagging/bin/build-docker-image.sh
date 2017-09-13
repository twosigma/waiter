#!/usr/bin/env bash
# Usage: build-docker-image.sh
#
# Builds a docker image for the kitchen test app that can be used as a minimesos agent.

set -ev

KITCHEN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=twosigma/kitchen
JAR=${KITCHEN_DIR}/target/uberjar/kitchen-0.1.0-SNAPSHOT-standalone.jar

if [ ! -f ${JAR} ]; then
    echo "The kitchen jar file was not found! Attempting to build it now."
    ${KITCHEN_DIR}/bin/build-uberjar.sh
fi

echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${KITCHEN_DIR}
