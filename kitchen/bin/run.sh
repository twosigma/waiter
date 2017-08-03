#!/usr/bin/env bash
# Usage: run.sh [options]
#
# Examples:
#   run.sh --port 8080
#   run.sh
#
# Runs Kitchen, passing through any and all arguments provided.
# This script has special handling for an argument if it is `--mem`.
# When the argument is `--mem`, that value is extracted and used as
# the Xmx flag for the java command while launching kitchen.
# The default value for memory allocated to kitchen is 128M.

KITCHEN_MEM="128M"

i=0
until [ "$((i=$i+1))" -gt "$#" ]
do
  case "$1" in
    --mem)
      KITCHEN_MEM="$2"; $((i=$i-1)); shift ;;
    *)
      set -- "$@" "$1"  ;;
  esac;
  shift;
done

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JAR=${DIR}/../target/uberjar/kitchen-0.1.0-SNAPSHOT-standalone.jar

if [ ! -f ${JAR} ]; then
    KITCHEN=$(realpath ${DIR}/..)
    echo "The kitchen jar file was not found! You may need to build it by doing:"
    echo "$ cd ${KITCHEN} && lein uberjar"
    exit 1
fi

JAVA_CMD="java -Xmx${KITCHEN_MEM} -jar ${JAR} "$@""
echo "Running ${JAVA_CMD}"
${JAVA_CMD}
