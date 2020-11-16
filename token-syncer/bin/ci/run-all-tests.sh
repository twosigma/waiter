#!/usr/bin/env bash
# Usage: run-all-tests.sh
#
# Runs the Token-Syncer unit and integration tests, and dumps log files if the tests fail.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$CI" ]; then
    # Don't exceed the Travis container 4GB max
    export JVM_OPTS="-Xmx700m"
    export LEIN_JVM_OPTS="-Xmx200m"
fi

${DIR}/run-unit-tests.sh && ${DIR}/run-integration-tests.sh test integration
