#!/usr/bin/env bash
# export GRAPHITE_SERVER_PORT=5555
# nc -kl localhost $GRAPHITE_SERVER_PORT > /dev/null &
# ncat_pid=$!
# echo "PID: ${ncat_pid}"
# ss -tulpn | grep $GRAPHITE_SERVER_PORT

# export GITHUB_WORKFLOW=integration-fast
# export GITHUB_RUN_ID=1234
# repo=twosigma/waiter
# pr_number=5
# dump_name="${repo//\//-}-PR${pr_number}-$GITHUB_WORKFLOW-$GITHUB_RUN_ID"
# echo $dump_name

# if lsof -i:5555; then
#   echo testing
# fi
