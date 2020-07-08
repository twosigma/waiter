#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
cd "$DIR"

echo "Building docker image for envoy sidecar"
docker build -t envoy:v1 --build-arg SERVICE_PORT --build-arg PORT0 .
#docker run --rm -d --name envoy --network host envoy:v1