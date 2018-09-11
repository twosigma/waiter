#!/usr/bin/env bash
# Usage: run-integration-tests-k8s-scheduler.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests-k8s-scheduler.sh parallel-test integration-fast
#   run-integration-tests-k8s-scheduler.sh parallel-test integration-slow
#   run-integration-tests-k8s-scheduler.sh parallel-test
#   run-integration-tests-k8s-scheduler.sh
#
# Runs the Waiter integration tests using the (local) k8s scheduler, and dumps log files if the tests fail.

set -e

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
KITCHEN_DIR=${WAITER_DIR}/../kitchen

# Start minikube
${DIR}/minikube-setup.sh

# Ensure we have the docker image for the pods
${KITCHEN_DIR}/bin/build-docker-image.sh

# Start waiter
WAITER_PORT=9091
${WAITER_DIR}/bin/run-using-k8s.sh ${WAITER_PORT} &

# Start monitoring state of Kubernetes pods
bash +x ${DIR}/monitor-pods.sh &

# Run the integration tests
LEIN_TEST_THREADS=4 \
    WAITER_TEST_KITCHEN_CMD=/opt/kitchen/kitchen \
    WAITER_AUTH_RUN_AS_USER=${USER} \
    WAITER_URI=127.0.0.1:${WAITER_PORT} \
    ${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR} || test_failures=true

# If there were failures, dump the logs
if [ "$test_failures" = true ]; then
    echo "Uploading logs..."
    ${WAITER_DIR}/bin/ci/upload_logs.sh
    exit 1
fi
