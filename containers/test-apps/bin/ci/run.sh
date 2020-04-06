#!/bin/bash

pushd kitchen
./bin/ci/run.sh
popd

pushd waiter-init
./bin/ci/run.sh
popd

