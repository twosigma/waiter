#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
cd "$DIR"

FILESERVER_IMAGE=twosigma/waiter-fileserver
echo "Building docker image for ${FILESERVER_IMAGE}"
docker build -t ${FILESERVER_IMAGE} .
