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

NGINX_HTTP2=""
if [[ ( "${PROTO_VERSION}" = "h2c" ) || ( "${PROTO_VERSION}" = "h2" ) ]]; then
  NGINX_HTTP2="http2"
fi

NGINX_SSL=""
if [[ ( "${PROTO_VERSION}" = "https" ) || ( "${PROTO_VERSION}" = "h2" ) ]]; then
  NGINX_SSL="ssl"
fi

PROXY_CMD=$(which nginx)
echo "run-proxy-kitchen.sh: nginx proxy command is ${PROXY_CMD}"

cd ${KITCHEN_DIR}

echo "run-proxy-kitchen.sh: generating SSL certificates"
openssl req -x509 -nodes -days 30 -newkey rsa:2048 \
  -keyout ${MESOS_SANDBOX}/nginx-kitchen.key \
  -out ${MESOS_SANDBOX}/nginx-kitchen.crt \
  -subj "/C=US/ST=TX/L=HOU/O=TS/OU=PS/CN=kitchen.test.org"

echo "run-proxy-kitchen.sh: launching kitchen on port ${PORT1}"
${KITCHEN_DIR}/bin/kitchen -p ${PORT1} &
KITCHEN_PID=$!
echo "run-proxy-kitchen.sh: kitchen pid is ${KITCHEN_PID}"

echo "run-proxy-kitchen.sh: waiting for kitchen to launch on port ${PORT1}"
while ! nc -z localhost ${PORT1}; do
  sleep 0.2
done
echo "run-proxy-kitchen.sh: kitchen launched"

echo "run-proxy-kitchen.sh: generating proxy config file at ${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf"
export NGINX_HTTP2="${NGINX_HTTP2}"
export NGINX_SSL="${NGINX_SSL}"
envsubst '${NGINX_HTTP2} ${NGINX_SSL} ${PORT0} ${PORT1}' \
  < "${KITCHEN_DIR}/bin/nginx-template.conf" > "${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf"

echo "run-proxy-kitchen.sh: launching the proxy listening on port ${PORT0} forwarding to port ${PORT1}"
${PROXY_CMD} -c "${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf" -p ${MESOS_SANDBOX}

echo "run-proxy-kitchen.sh: waiting for nginx pid file ${MESOS_SANDBOX}/nginx.pid to be created"
while [[ ! -f ${MESOS_SANDBOX}/nginx.pid ]]; do
  sleep 0.1
done

PROXY_PID=$(cat ${MESOS_SANDBOX}/nginx.pid)
echo "run-proxy-kitchen.sh: proxy pid is ${PROXY_PID}"

echo "run-proxy-kitchen.sh: waiting for kitchen to terminate"
wait ${KITCHEN_PID}

echo "run-proxy-kitchen.sh: exiting."