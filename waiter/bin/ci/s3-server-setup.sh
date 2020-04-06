#!/bin/bash

set -e

# Install AWS CLI (for s3api commands) via pip
# We use this to handle S3 authentication for bucket creation
# https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html
type aws || pip install awscli --upgrade --user

# Run cloudserver (S3 compatible test server) via docker
# The API server endpoint is accessible via localhost:8888
# https://hub.docker.com/r/scality/s3server
echo Starting S3 server docker container
docker run --name s3server --detach --rm --env=REMOTE_MANAGEMENT_DISABLE=1 --publish=8888:8000 zenko/cloudserver:8.1.15
echo -n Waiting for S3 server
while ! curl localhost:8888 &>/dev/null; do
    echo -n .
    sleep 1
done
echo

# Create a public RW bucket for waiter app logs, using the default cloudserver credentials
# https://github.com/scality/cloudserver#run-it-with-a-file-backend
AWS_ACCESS_KEY_ID=accessKey1 AWS_SECRET_ACCESS_KEY=verySecretKey1 \
    aws s3api create-bucket --endpoint-url=http://localhost:8888 \
    --acl=public-read-write --bucket=waiter-service-logs --output=table
