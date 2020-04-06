#!/usr/bin/env bash
# Usage: run-nginx-server.sh http
# Usage: run-nginx-server.sh h2c
#
# Launches kitchen fronted by a nginx instance to handle
# incoming requests using the specified protocol.
# Supported protocols are: http, https, h2c, and h2.
# Expects the following environment variables to be set:
# - KITCHEN_CMD the command to run kitchen, port will be specified with the -p flag
# - MESOS_SANDBOX the sandbox directory location
# - PORT0 the port nginx server will listen on for configured protocol requests
# - PORT1 the port nginx server will listen on for http/1.1 requests
# - PORT2 the port the kitchen server will listen

set -ux

# Log a message to stdout
function nginx_log() {
  printf '%s run-nginx-server.sh: %s\n' "$(date +'%Y-%m-%dT%H:%M:%S%z')" "$1"
}

function wait_for_kitchen {
    URI=${1}
    while ! curl -s ${URI} >/dev/null;
    do
        nginx_log "kitchen is not listening on ${URI} yet"
        sleep 2.0
    done
    nginx_log "connected to kitchen on ${URI}!"
}

nginx_log "starting..."

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
nginx_log "base directory is ${BASE_DIR}"

PROTO_VERSION=${1:-http}
nginx_log "nginx requested to handle ${PROTO_VERSION} protocol"

NGINX_CMD=${2:-}
if [[ -z "${NGINX_CMD}" ]]; then
  if [[ -e "${BASE_DIR}/bin/nginx" ]]; then
    NGINX_CMD="${BASE_DIR}/bin/nginx"
  else
    NGINX_CMD="$(which nginx)"
  fi
fi
if [[ -z "${NGINX_CMD}" ]]; then
  nginx_log "nginx command could not be resolved!"
  exit 1
fi
if [[ ! -e "${NGINX_CMD}" ]]; then
  nginx_log "nginx command (${NGINX_CMD}) does not exist!"
  exit 1
fi
nginx_log "nginx command is ${NGINX_CMD}"
nginx_log "nginx version:"
${NGINX_CMD} -v

NGINX_HTTP2=""
if [[ ( "${PROTO_VERSION}" = "h2c" ) || ( "${PROTO_VERSION}" = "h2" ) ]]; then
  NGINX_HTTP2="http2"
fi

NGINX_SSL=""
if [[ ( "${PROTO_VERSION}" = "https" ) || ( "${PROTO_VERSION}" = "h2" ) ]]; then
  NGINX_SSL="ssl"
fi

cd ${BASE_DIR}

nginx_log "generating SSL certificates"
openssl req -x509 -nodes -days 30 -newkey rsa:2048 \
  -keyout ${MESOS_SANDBOX}/nginx-server.key \
  -out ${MESOS_SANDBOX}/nginx-server.crt \
  -subj "/C=US/ST=TX/L=HOU/O=TS/OU=PS/CN=server.test.org"

nginx_log "generating config file at ${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf"
export NGINX_HTTP2="${NGINX_HTTP2}"
export NGINX_SSL="${NGINX_SSL}"
envsubst '${NGINX_HTTP2} ${NGINX_SSL} ${PORT0} ${PORT1} ${PORT2}' \
  < "${BASE_DIR}/data/nginx-template.conf" > "${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf"

KITCHEN_CMD=$(echo "${KITCHEN_CMD}" | tr -d '"')
nginx_log "kitchen executable is ${KITCHEN_CMD}"
nginx_log "launching kitchen listening on port ${PORT2}"
${KITCHEN_CMD} -p ${PORT2} &
# Wait for waiter to be listening
wait_for_kitchen "http://127.0.0.1:${PORT2}"

nginx_log "launching nginx listening on port ${PORT0} and ${PORT1} and forwarding to ${PORT2}"
${NGINX_CMD} -c "${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf" -p ${MESOS_SANDBOX}

nginx_log "exiting."