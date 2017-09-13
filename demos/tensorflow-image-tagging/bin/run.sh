#!/usr/bin/env bash
# Usage: run.sh [options]
#
# Examples:
#   run.sh --port 8080
#   run.sh
#
# Runs tensorflow-image-tagging, passing through any and all arguments provided.
# This script has special handling for an argument if it is `--mem`.
# When the argument is `--mem`, that value is extracted and used as
# the Xmx flag for the java command while launching tensorflow-image-tagging.
# The default value for memory allocated to tensorflow-image-tagging is 128M.

DEMO_MEM="256M"

i=0
until [ "$((i=$i+1))" -gt "$#" ]
do
  case "$1" in
    --mem)
      DEMO_MEM="$2"; $((i=$i-1)); shift ;;
    *)
      set -- "$@" "$1"  ;;
  esac;
  shift;
done

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JAR=${DIR}/../target/uberjar/tensorflow-image-tagging-0.1.0-SNAPSHOT-standalone.jar

if [ ! -f ${JAR} ]; then
    DEMO=$(realpath ${DIR}/..)
    echo "The tensorflow-image-tagging jar file was not found! You may need to build it by doing:"
    echo "$ cd ${DEMO} && lein uberjar"
    exit 1
fi

JAVA_CMD="java -Xmx${DEMO_MEM} -jar ${JAR} "$@""
echo "Running ${JAVA_CMD}"
${JAVA_CMD}
