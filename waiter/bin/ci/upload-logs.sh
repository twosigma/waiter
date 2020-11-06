#!/usr/bin/env bash

set -e

cd ${GITHUB_WORKSPACE}

tarball=./dump.txz
log_dirs=./waiter/log
repo=${GITHUB_REPOSITORY}
pr_number=$(jq -r ".pull_request.number" "$GITHUB_EVENT_PATH")
dump_name="${repo//\//-}-PR${pr_number}-${GITHUB_WORKFLOW// /-}-$GITHUB_RUN_ID"
echo $dump_name

# Grab Mesos logs
if [ -d ./waiter/.minimesos ]; then
    # List the last 10 containers
    docker ps --all --last 10

    # Extract the Mesos master logs from docker
    mesos_master_container=$(docker ps --all --latest --filter 'name=minimesos-master-' --format '{{.ID}}')
    mkdir -p ./mesos/master-logs
    docker cp --follow-link $mesos_master_container:/var/log/mesos-master.INFO ./mesos/master-logs/
    docker cp --follow-link $mesos_master_container:/var/log/mesos-master.WARNING ./mesos/master-logs/

    # Extract the Mesos agent logs from docker
    mesos_agent_containers=$(docker ps --all --last 4 --filter 'name=minimesos-agent-' --format '{{.ID}}')
    for container in $mesos_agent_containers; do
        destination=./mesos/agent-logs/$container
        mkdir -p $destination
        docker cp --follow-link $container:/var/log/mesos-slave.INFO $destination
        docker cp --follow-link $container:/var/log/mesos-slave.WARNING $destination
        docker cp --follow-link $container:/var/log/mesos-slave.ERROR $destination
        docker cp --follow-link $container:/var/log/mesos-fetcher.INFO $destination || echo "Container $container does not have mesos-fetcher.INFO"
    done

    log_dirs+=' ./waiter/.minimesos ./mesos/master-logs ./mesos/agent-logs'
fi

# Grab Kubernetes pod logs from S3 server
if [ "$(docker ps -qf name=s3server)" ]; then
    mkdir -p ./s3-bucket/pod-logs
    # Get all files in the bucket, using the default cloudserver credentials
    # https://github.com/scality/cloudserver#run-it-with-a-file-backend
    AWS_ACCESS_KEY_ID=accessKey1 AWS_SECRET_ACCESS_KEY=verySecretKey1 \
        aws s3 --endpoint http://localhost:8888 ls --recursive s3://waiter-service-logs/ | \
        awk '{ print $4 }' | \
        while read log_file_path; do
            mkdir -p ./s3-bucket/pod-logs/$(dirname $log_file_path)
            curl -s http://localhost:8888/waiter-service-logs/$log_file_path > ./s3-bucket/pod-logs/$log_file_path
        done
    log_dirs+=' ./s3-bucket/pod-logs'
fi

# Grab shell scheduler logs
if [ -d ./waiter/scheduler ]; then
    log_dirs+=' ./waiter/scheduler'
fi

# Tarball-up all of our Waiter/Mesos/etc logs
tar --ignore-failed-read -cJf $tarball --transform="s|\\./[^/]*/\\.*|${dump_name}/|" --warning=no-file-changed $log_dirs || exitcode=$?

# GNU tar always exits with 0, 1 or 2 (https://www.gnu.org/software/tar/manual/html_section/tar_19.html)
# 0 = Successful termination
# 1 = Some files differ (we're OK with this)
# 2 = Fatal error
if [ "$exitcode" == 2 ]; then
  echo "The tar command exited with a fatal error (exit code $exitcode), exiting..."
  exit $exitcode
fi
./waiter/bin/ci/gdrive-upload "github-actions-${dump_name}" $tarball
