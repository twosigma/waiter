# waiter

[![Build Status](https://travis-ci.org/twosigma/waiter.svg)](https://travis-ci.org/twosigma/waiter)

Welcome to Two Sigma's waiter project!

Waiter is a web service platform that runs, manages and automatically scales services without requiring human intervention.

[Waiter Design](waiter/docs/waiter-design-docs.md) is a good place to start to learn more.

## Subproject Summary

In this repository, you'll find two subprojects, each with its own documentation.

* [`waiter`](waiter) - This is the actual web service platform, waiter. It comes with a [JSON REST API](waiter/docs/rest-api.md).
* [`kitchen`](kitchen) - This is the kitchen application, a test app used by the waiter integration tests.

Please visit the `waiter` subproject first to get started.

## Quickstart

The quickest way to get Mesos, Marathon, and waiter running locally is with [docker](https://www.docker.com/) and [minimesos](https://minimesos.org/). 

1. Install `docker`
1. Install `minimesos`
1. Clone down this repo
1. `cd waiter`
1. Run `minimesos up` to start Mesos, ZooKeeper, and Marathon
1. Run `bin/run-using-minimesos.sh` to start waiter
1. Waiter should now be listening locally on port 9091

## Contributing

In order to accept your code contributions, please fill out the appropriate Contributor License Agreement in the [`cla`](cla) folder and submit it to tsos@twosigma.com.

## Disclaimer

Apache Mesos is a trademark of The Apache Software Foundation. The Apache Software Foundation is not affiliated, endorsed, connected, sponsored or otherwise associated in any way to Two Sigma, waiter, or this website in any manner.

© Two Sigma Open Source, LLC
