#!/usr/bin/env bash
# Usage: build-docker-image.sh
#
# Builds a docker image for the kitchen test app that can be used as a minimesos agent.

set -ex

KITCHEN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=twosigma/kitchen

cd ${KITCHEN_DIR}
echo "Copying over nginx-server files"
mkdir -p ${KITCHEN_DIR}/.temp/nginx-server
cp -rf ${KITCHEN_DIR}/../nginx-server/* ${KITCHEN_DIR}/.temp/nginx-server

echo "Building docker images for ${NAME}"
docker build -t ${NAME} .
