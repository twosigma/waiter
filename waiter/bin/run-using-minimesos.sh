#!/usr/bin/env bash

export WAITER_PORT=${1:-9091}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$(minimesos info | grep MINIMESOS)
EXIT_CODE=$?
if [ ${EXIT_CODE} -eq 0 ]
then
    export WAITER_MARATHON=${MINIMESOS_MARATHON%;}
    echo "WAITER_MARATHON = ${WAITER_MARATHON}"
else
    echo "Could not get Marathon URI from minimesos; you may need to restart minimesos"
    exit ${EXIT_CODE}
fi

echo "Starting waiter..."
cd ${DIR}/..
WAITER_AUTH_RUN_AS_USER=$(id -un) lein run config-minimesos.edn
