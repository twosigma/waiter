#!/usr/bin/env bash
# Usage: build-uberjar.sh
#
# Builds the kitchen uberjar.

set -ev

DEMO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

pushd ${DEMO_DIR}

lein voom build-deps
lein uberjar

popd
