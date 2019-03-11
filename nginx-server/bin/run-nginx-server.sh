#!/usr/bin/env bash
# Usage: run-nginx-server.sh http
# Usage: run-nginx-server.sh h2c
#
# Launches kitchen fronted by a nginx instance to handle
# incoming requests using the specified protocol.

set -ux

# Log a message to stdout
nginx_log() {
  printf '%s run-nginx-server.sh: %s\n' "$(date +'%Y-%m-%dT%H:%M:%S%z')" "$1"
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
envsubst '${NGINX_HTTP2} ${NGINX_SSL} ${PORT0} ${PORT1}' \
  < "${BASE_DIR}/data/nginx-template.conf" > "${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf"

nginx_log "launching nginx listening on ports ${PORT0} and ${PORT1}"
${NGINX_CMD} -c "${MESOS_SANDBOX}/nginx-${PROTO_VERSION}-server.conf" -p ${MESOS_SANDBOX}

nginx_log "exiting."