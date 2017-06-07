#!/usr/bin/env bash
# Usage: container-run.sh [options]
#
# Examples:
#   container-run.sh --port 8080
#   container-run.sh
#
# Runs Kitchen as deployed in a docker image (under /opt/kitchen), passing through any and all arguments provided.

java -Xmx128M -jar /opt/kitchen/kitchen-0.1.0-SNAPSHOT-standalone.jar "$@"
