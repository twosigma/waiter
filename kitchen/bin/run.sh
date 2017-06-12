#!/usr/bin/env bash
# Usage: run.sh [options]
#
# Examples:
#   run.sh --port 8080
#   run.sh
#
# Runs Kitchen, passing through any and all arguments provided.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JAR=${DIR}/../target/uberjar/kitchen-0.1.0-SNAPSHOT-standalone.jar

if [ ! -f ${JAR} ]; then
    KITCHEN=$(realpath ${DIR}/..)
    echo "The kitchen jar file was not found! You may need to build it by doing:"
    echo "$ cd ${KITCHEN} && lein uberjar"
    exit 1
fi

java -Xmx128M -jar ${JAR} "$@"
