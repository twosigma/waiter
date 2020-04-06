#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
cd "$DIR"

TEST_APPS_IMAGE=twosigma/waiter-test-apps

pushd ./courier
mvn clean package
popd

pushd ./sediment
mvn clean package
popd

echo "Building docker image for ${TEST_APPS_IMAGE}"
docker build -t ${TEST_APPS_IMAGE} .
