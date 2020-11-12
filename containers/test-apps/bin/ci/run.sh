#!/bin/bash

pushd kitchen
pwd
tree
./bin/ci/run.sh
popd

pushd waiter-init
pwd
tree
./bin/ci/run.sh
popd
