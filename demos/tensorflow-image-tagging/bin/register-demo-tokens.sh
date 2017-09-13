#!/usr/bin/env bash
# Usage: register-demo-tokens.sh
#
# Registers the tensorflow-image-tagging demo tokens on Waiter.
# Checks if WAITER_URI is set, and sets it to a reasonable default if not.

set -ev

if [ -z ${WAITER_URI+x} ]; then
    export WAITER_URI=127.0.0.1:9091
    echo "WAITER_URI is unset, defaulting to ${WAITER_URI}"
else
    echo "WAITER_URI is set to ${WAITER_URI}"
fi

RUN_AS_USER="$(id -un)"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function generate_from_template {
  sed -e "s|RUN_AS_USER|${RUN_AS_USER}|g" < $1 > $2
}

function register_token {
  curl -X POST -H"content-type: application/json" "${WAITER_URI}/token" -d @$1
}

temp_file=$(mktemp)

echo "Registering image-search token"
generate_from_template "${DIR}/image-search.json" "${temp_file}"
register_token "${temp_file}"

echo "Registering image-tagging token"
generate_from_template "${DIR}/image-tagging.json" "${temp_file}"
register_token "${temp_file}"

rm ${temp_file}

