#!/usr/bin/env bash
# Usage: run-unit-tests.sh
#
# Runs the Token-Syncer unit tests, and dumps log files if the tests fail.

lein with-profiles +test-repl test || { echo "unit tests failed -- dumping logs"; tail -n +1 -- log/*.log; exit 1; }
