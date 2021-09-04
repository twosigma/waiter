#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
cd "$DIR"

echo "Building docker image for raven sidecar"
docker build -t twosigma/waiter-raven .
