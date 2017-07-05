#!/usr/bin/env bash
# Usage: build-uberjar.sh
#
# Builds the kitchen uberjar.

set -ev

KITCHEN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export PATH=${KITCHEN_DIR}/..:$PATH

pushd ${KITCHEN_DIR}

lein voom build-deps
lein uberjar

popd
