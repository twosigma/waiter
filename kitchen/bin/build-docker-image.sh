#!/usr/bin/env bash

KITCHEN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=twosigma/kitchen
JAR=${KITCHEN_DIR}/target/uberjar/kitchen-0.1.0-SNAPSHOT-standalone.jar

if [ ! -f ${JAR} ]; then
    echo "The kitchen jar file was not found! Attempting to build it now."
    pushd ${KITCHEN_DIR}
    lein uberjar
    popd
fi

echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${KITCHEN_DIR}
