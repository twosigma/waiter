#!/usr/bin/env bash
# Usage: build-docker-image.sh
#
# Builds a docker image for the tensorflow-image-tagging test app that can be used as a minimesos agent.

set -ev

DEMO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=twosigma/tensorflow-image-tagging
JAR=${DEMO_DIR}/target/uberjar/tensorflow-image-tagging-0.1.0-SNAPSHOT-standalone.jar

if [ ! -f ${JAR} ]; then
    echo "The tensorflow-image-tagging jar file was not found! Attempting to build it now."
    ${DEMO_DIR}/bin/build-uberjar.sh
fi

echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${DEMO_DIR}
