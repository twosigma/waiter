<img src="./waiter.svg" align="right" width="250px" height="250px">

# Waiter

[![Build Status](https://travis-ci.org/twosigma/waiter.svg)](https://travis-ci.org/twosigma/waiter)

Welcome to Two Sigma's Waiter project!

Waiter is a web service platform that runs, manages, and automatically scales services without requiring human intervention.

[Waiter Design](waiter/docs/waiter-design-docs.md) is a good place to start to learn more.

## Subproject Summary

In this repository, you'll find two subprojects, each with its own documentation.

* [`waiter`](waiter) - This is the actual web service platform, Waiter. It comes with a [JSON REST API](waiter/docs/rest-api.md).
* [`kitchen`](kitchen) - This is the kitchen application, a test app used by the Waiter integration tests.

Please visit the `waiter` subproject first to get started.

## Quickstart

The quickest way to get Mesos, Marathon, and Waiter running locally is with [docker](https://www.docker.com/) and [minimesos](https://minimesos.org/). 

1. Install `docker`
1. Install `minimesos`
1. Clone down this repo
1. Run `kitchen/bin/build-docker-image.sh` to build the minimesos agent image with kitchen baked in
1. `cd waiter`
1. Run `minimesos up` to start Mesos, ZooKeeper, and Marathon
1. Run `lein voom build-deps` to fetch dependencies
1. Run `bin/run-using-minimesos.sh` to start Waiter
1. Waiter should now be listening locally on port 9091

## Quickstart (local shell scheduling)

Waiter can also be run without Mesos and Marathon, using the "shell scheduler". Note that this scheduler should only be used for testing purposes, not in production. 

1. Clone down this repo
1. `cd waiter`
1. Run `lein voom build-deps` to fetch dependencies
1. Run `bin/run-using-shell-scheduler.sh` to start Waiter
1. Waiter should now be listening locally on port 9091

## Contributing

In order to accept your code contributions, please fill out the appropriate Contributor License Agreement in the [`cla`](cla) folder and submit it to tsos@twosigma.com.

## Disclaimer

Apache Mesos is a trademark of The Apache Software Foundation. The Apache Software Foundation is not affiliated, endorsed, connected, sponsored or otherwise associated in any way to Two Sigma, Waiter, or this website in any manner.

© Two Sigma Open Source, LLC
