#!/usr/bin/env bash
# Usage: run-integration-tests-k8s-scheduler.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests-k8s-scheduler.sh parallel-test integration-heavy
#   run-integration-tests-k8s-scheduler.sh parallel-test integration-lite
#   run-integration-tests-k8s-scheduler.sh parallel-test
#   run-integration-tests-k8s-scheduler.sh
#
# Runs the Waiter integration tests using the (local) k8s scheduler.

set -e

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
KITCHEN_DIR=${WAITER_DIR}/../kitchen

# Start minikube
${DIR}/minikube-setup.sh

# Start S3 test server
if [[ $TEST_SELECTOR =~ heavy$ ]]; then
    ${DIR}/s3-server-setup.sh
    S3SERVER_IP=$(docker inspect s3server | jq -r '.[0].NetworkSettings.Networks.bridge.IPAddress')
    export WAITER_S3_BUCKET=http://$S3SERVER_IP:8000/waiter-service-logs
fi

# Ensure we have the docker image for the pods
${KITCHEN_DIR}/bin/build-docker-image.sh

# Start waiter
: ${WAITER_PORT:=9091}
${WAITER_DIR}/bin/run-using-k8s.sh ${WAITER_PORT} &

# Start monitoring state of Kubernetes pods
bash +x ${DIR}/monitor-pods.sh &

# Run the integration tests
export LEIN_TEST_THREADS=4
export WAITER_TEST_KITCHEN_CMD=/opt/kitchen/kitchen
export WAITER_AUTH_RUN_AS_USER=${USER}
export WAITER_URI=127.0.0.1:${WAITER_PORT}
${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR}

# If there were failures, dump the logs
if [ "$test_failures" = true ]; then
    echo "Uploading logs..."
    ${WAITER_DIR}/bin/ci/upload_logs.sh
    exit 1
fi
