#!/usr/bin/env bash
# Usage: build-uberjar.sh
#
# Builds the tensorflow-image-tagging uberjar.

set -ev

DEMO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

pushd ${DEMO_DIR}

lein do clean, uberjar

popd
