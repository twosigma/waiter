#!/bin/bash

set -e

pushd kitchen
echo "\n"
pwd
ls bin
./bin/ci/run.sh
popd

pushd waiter-init
pwd
ls bin
./bin/ci/run.sh
popd
