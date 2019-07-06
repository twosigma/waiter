#!/usr/bin/env bash

set -e

cd ${TRAVIS_BUILD_DIR}

tarball=./dump.txz
log_dirs=./waiter/log
repo=${TRAVIS_PULL_REQUEST_SLUG:-${TRAVIS_REPO_SLUG}}
dump_name="${repo//\//-}-${TRAVIS_JOB_NUMBER:-dump}"

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

echo zzzxxx
echo $(ls -l ./containers/test-apps/saml/idpserver/idp-server-out.txt)
cat ./containers/test-apps/saml/idpserver/idp-server-out.txt

echo zzzxxx
./waiter/bin/ci/gdrive-upload "travis-${dump_name}" $tarball
