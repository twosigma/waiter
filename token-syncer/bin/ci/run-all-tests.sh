#!/usr/bin/env bash
# Usage: run-all-tests.sh
#
# Runs the Token-Syncer unit and integration tests, and dumps log files if the tests fail.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/run-unit-tests.sh && ${DIR}/run-integration-tests.sh test integration
