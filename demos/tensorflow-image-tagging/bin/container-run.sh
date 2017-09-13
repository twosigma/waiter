#!/usr/bin/env bash
# Usage: container-run.sh [options]
#
# Examples:
#   container-run.sh --port 8080
#   container-run.sh
#
# Runs demo app as deployed in a docker image (under /opt/tensorflow-image-tagging), passing through any and all arguments provided.

java -Xmx256M -jar /opt/tensorflow-image-tagging/tensorflow-image-tagging-0.1.0-SNAPSHOT-standalone.jar "$@"
