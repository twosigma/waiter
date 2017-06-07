#!/usr/bin/env bash

export WAITER_PORT=${1:-9091}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Starting waiter..."
cd ${DIR}/..
WAITER_AUTH_RUN_AS_USER=$(id -un) lein run config-shell.edn
