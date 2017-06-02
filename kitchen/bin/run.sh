#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

java -Xmx128M -jar ${DIR}/../target/uberjar/kitchen-0.1.0-SNAPSHOT-standalone.jar "$@"
