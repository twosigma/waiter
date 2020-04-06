#!/usr/bin/env bash
# Usage: run-all-tests.sh
#
# Runs the Token-Syncer unit and integration tests, and dumps log files if the tests fail.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$TRAVIS" ]; then
    # Don't run for PRs that didn't modify the Token-Syncer
    if [ "${TRAVIS_PULL_REQUEST}" != false ]; then
        PR_URL="https://api.github.com/repos/${TRAVIS_REPO_SLUG}/pulls/${TRAVIS_PULL_REQUEST}/files"
        if !(curl -s $PR_URL | jq '.[].filename' | grep -qE '^"(\.travis|token-syncer)'); then
            echo "Skipping Token-Syncer tests on this build (project unmodified)."
            exit 0
        fi
    fi

    # Don't exceed the Travis container 4GB max
    export JVM_OPTS="-Xmx700m"
    export LEIN_JVM_OPTS="-Xmx200m"
fi

${DIR}/run-unit-tests.sh && ${DIR}/run-integration-tests.sh test integration
