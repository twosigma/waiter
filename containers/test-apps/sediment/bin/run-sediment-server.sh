#!/usr/bin/env bash
# Usage: run-sediment-server.sh PORT
#
# Launches a sediment instance to handle incoming requests mainly to test trailers.
# Supported protocols are: http and h2c.
# PORT is the port on which sediment server will listen on for incoming requests.

set -ux

SERVER_PORT=${1:-8080}

# Log a message to stdout
function sediment_log() {
  printf '%s run-sediment-server.sh: %s\n' "$(date +'%Y-%m-%dT%H:%M:%S%z')" "$1"
}

sediment_log "starting..."

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
sediment_log "base directory is ${BASE_DIR}"

SEDIMENT_JAR="${BASE_DIR}/data/sediment-uberjar.jar"
if [[ ! -f ${SEDIMENT_JAR} ]]; then
  sediment_log "${SEDIMENT_JAR} file not found!"
  exit 1
fi

JAVA_CMD=$(which java)
sediment_log "java command is ${JAVA_CMD}"
if [[ -z "${JAVA_CMD}" ]]; then
  sediment_log "java command could not be resolved!"
  exit 1
fi

sediment_log "launching sediment server at http://127.0.0.1:${SERVER_PORT}"
${JAVA_CMD} -jar ${SEDIMENT_JAR} ${SERVER_PORT}

sediment_log "exiting."
