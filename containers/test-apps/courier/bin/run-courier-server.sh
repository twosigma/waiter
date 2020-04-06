#!/usr/bin/env bash
# Usage: run-courier-server.sh PORT0 PORT1
#
# Launches a courier instance to handle incoming requests mainly to test gRPC calls.
# PORT0 is the port on which courier server will listen on for incoming grpc requests.
# PORT1 is the port on which courier server will listen on for incoming http health check requests.

set -ux

GRPC_SERVER_PORT=${1:-${PORT0:-8080}}
HEALTH_CHECK_SERVER_PORT=${2:-${PORT1:-8081}}

# Log a message to stdout
function courier_log() {
  printf '%s run-courier-server.sh: %s\n' "$(date +'%Y-%m-%dT%H:%M:%S%z')" "$1"
}

courier_log "starting..."

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
courier_log "base directory is ${BASE_DIR}"

COURIER_JAR="${BASE_DIR}/data/courier-uberjar.jar"
if [[ ! -f ${COURIER_JAR} ]]; then
  courier_log "${COURIER_JAR} file not found!"
  exit 1
fi

JAVA_CMD=$(which java)
courier_log "java command is ${JAVA_CMD}"
if [[ -z "${JAVA_CMD}" ]]; then
  courier_log "java command could not be resolved!"
  exit 1
fi

courier_log "launching courier grpc and health checks servers at ports ${GRPC_SERVER_PORT} and ${HEALTH_CHECK_SERVER_PORT}"
${JAVA_CMD} -jar ${COURIER_JAR} ${GRPC_SERVER_PORT} ${HEALTH_CHECK_SERVER_PORT}

courier_log "exiting."
