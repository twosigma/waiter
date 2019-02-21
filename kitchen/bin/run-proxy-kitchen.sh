#!/usr/bin/env bash
# Usage: run-proxy-kitchen.sh http
# Usage: run-proxy-kitchen.sh h2c
#
# Launches kitchen fronted by a nginx instance to handle
# incoming requests using the specified protocol.

set -eux
echo "run-proxy-kitchen.sh: starting..."

KITCHEN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
echo "run-proxy-kitchen.sh: kitchen directory is ${KITCHEN_DIR}"

PROTO_VERSION=${1:-http}
echo "run-proxy-kitchen.sh: proxy requested to handle ${PROTO_VERSION} protocol"

PROXY_CMD=$(which nginx)
echo "run-proxy-kitchen.sh: nginx proxy command is ${PROXY_CMD}"

cd ${KITCHEN_DIR}

echo "run-proxy-kitchen.sh: launching kitchen on port ${PORT1}"
${KITCHEN_DIR}/bin/kitchen -p ${PORT1} &

echo "run-proxy-kitchen.sh: generating proxy config file at ${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf"
envsubst '${PORT0} ${PORT1}' < "${KITCHEN_DIR}/bin/nginx-${PROTO_VERSION}-server.conf" > "${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf"

echo "run-proxy-kitchen.sh: launching the proxy listening on port ${PORT0} forwarding to port ${PORT1}"
${PROXY_CMD} -c "${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf" -p ${MESOS_SANDBOX}

PROXY_PID=$(cat ${MESOS_SANDBOX}/nginx.pid)
echo "run-proxy-kitchen.sh: proxy pid is ${PROXY_PID}"

echo "run-proxy-kitchen.sh: waiting for proxy to terminate"
wait ${PROXY_PID}

echo "run-proxy-kitchen.sh: exiting."