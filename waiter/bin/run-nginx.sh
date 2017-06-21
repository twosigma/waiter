#!/usr/bin/env bash
# Usage: run-nginx.sh [WAITER_SERVERS] [NGINX_HTTP_SERVER_PORT] [NGINX_DAEMON]
#
# Examples:
#   run-nginx.sh 172.17.0.1:9091;172.17.0.1:9092 9300 off
#   run-nginx.sh 172.17.0.1:9091;172.17.0.1:9092 9300
#   run-nginx.sh 172.17.0.1:9091;172.17.0.1:9092
#   run-nginx.sh
#
# Runs nginx, configured to proxy the provided Waiters and to listen on the provided port.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/..

WAITER_SERVERS=${1:-172.17.0.1:9091;172.17.0.1:9092}
NGINX_HTTP_SERVER_PORT=${2:-9300}
NGINX_DAEMON=${3:-off}

while IFS=';' read -ra SERVERS; do
      for server in "${SERVERS[@]}"; do
          NGINX_HTTP_UPSTREAM="${NGINX_HTTP_UPSTREAM}server ${server}; "
      done
done <<< "${WAITER_SERVERS}"

echo "NGINX_HTTP_UPSTREAM = ${NGINX_HTTP_UPSTREAM}"
echo "NGINX_HTTP_SERVER_PORT = ${NGINX_HTTP_SERVER_PORT}"
echo "NGINX_DAEMON = ${NGINX_DAEMON}"

function generate_from_template {
    sed \
        -e "s|{NGINX_HTTP_UPSTREAM}|${NGINX_HTTP_UPSTREAM}|g" \
        -e "s|{NGINX_HTTP_SERVER_PORT}|${NGINX_HTTP_SERVER_PORT}|g" \
        -e "s|{NGINX_DAEMON}|${NGINX_DAEMON}|g" \
        < $1 > $2
}

NGINX_GENERATED_CONF=${DIR}/generated-nginx.conf
generate_from_template "${DIR}/nginx.conf" "${NGINX_GENERATED_CONF}"
nginx -c ${NGINX_GENERATED_CONF} -p ${WAITER_DIR}
