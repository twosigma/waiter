Waiter is a distributed autoscaler and load balancer for managing web services at scale.
Waiter particularly excels at running services with unpredictable loads or multiple co-existing versions.
Waiter uses [Marathon](https://mesosphere.github.io/marathon/) to schedule services on a [Mesos](http://mesos.apache.org/) cluster.

## Building

Prerequisites:

* Java 8
* [Leiningen](http://leiningen.org/) (at least v2.8.1)

Waiter uses [voom](https://github.com/LonoCloud/lein-voom) to pull in dependencies that are not available in clojars:

```bash
$ lein voom build-deps
```

If you want to override the default directories for local maven and voom repos:

```bash
$ WAITER_MAVEN_LOCAL_REPO=... VOOM_REPOS=... lein with-profile +override-maven voom build-deps
```

Once you have fetched dependencies:

```bash
$ lein uberjar
```

## Running

The quickest way to get Mesos, Marathon, and Waiter running locally is with [docker](https://www.docker.com/) and [minimesos](https://minimesos.org/).
Check out the [Quickstart](../README.md#quickstart) for details.

Read the [config-minimal.edn](config-minimal.edn) or [config-full.edn](config-full.edn) files for descriptions of the Waiter config structure.
Waiter logs are in `/log`, and `waiter.log` should contain info on what went wrong if Waiter doesn't start.

## Tests

To run all unit tests, simply run `lein test`. The unit tests run very fast, and they do not require Waiter to be up and running.

The Waiter integration tests require Waiter to be up and running. The integration tests rely heavily on the [kitchen test app](../kitchen).
They therefore need to know where kitchen is installed on your Mesos agent(s), so that they can send the appropriate command to Waiter.
You can customize this path by setting the `WAITER_TEST_KITCHEN_CMD` environment variable.

Once Waiter has started:

```bash
# Assuming you're using the provided minimesos support:
$ export WAITER_TEST_KITCHEN_CMD=/opt/kitchen/container-run.sh

# Run a very basic integration test:
$ lein test :only waiter.basic-test/test-basic-shell-command

# Run all "fast" integration tests:
$ lein test :integration-fast

# Run all "slow" integration tests:
$ lein test :integration-slow

# Run all "perf" integration tests:
# - WAITER_TEST_REQUEST_LATENCY_MAX_INSTANCES sets the target number of instances
# - APACHE_BENCH_DIR sets the directory where the ab tool is installed
$ WAITER_TEST_REQUEST_LATENCY_MAX_INSTANCES=30 APACHE_BENCH_DIR=/usr/bin lein test :perf
```

## What is Waiter

Waiter is a web service platform that runs, manages and automatically scales services without human intervention.
Waiter particularly excels at running services with unpredictable loads or requiring intensive computing resources.

Developers register services on Waiter by simply supplying their service startup command via a token or directly in a HTTP request header.
Waiter takes care of managing the entire lifecycle of services from that point on, including running the service when and only when there is traffic, scaling the service up when there is more traffic, and tearing down the service if there is no traffic.

Managing service lifecycle is the key differentiator of Waiter that empowers developers to build more and faster.

## What can you do with Waiter

Waiter was designed with simplicity in mind - your existing web services can run on Waiter without any modification as long as they meet the following two conditions:

* Speak HTTP
* Client requests can be sent to any backend of the same service

### Handling Unpredictable Traffic with Optimal Resource Utilization

In many (arguably most) situations, it is hard to anticipate how much traffic your service will receive and when it will come.
At Two Sigma, we run massive batch workloads to simulate real trading environments.
The demand fluctuates and is highly unpredictable. The services that serve those workloads may see zero requests for several days in a row and then suddenly see thousands of requests per second. Capacity planning becomes infeasible. If we underestimate the traffic, the services can be easily overwhelmed, become unresponsive, or even crash, resulting in constant human intervention and poor developer productivity. If we provision sufficient capacity, then the allocated resources are completely wasted when there is no traffic.

Waiter solves this problem. When demand increases, Waiter launches more instances to meet demand and scales down when those instances are no longer needed.

### Machine Learning at Scale

Many machine learning libraries and algorithms are memory hungry or CPU hungry.
By running machine learning services on Waiter, you can easily scale these services on demand.
At Two Sigma, Waiter serves and scales machines learning services critical to our business.
We have services running on Waiter handling thousands of requests per second.

## Documentation

* [How to Waiter](docs/how-to-waiter.md)
* [REST API](docs/rest-api.md)
* [Waiter Design](docs/waiter-design-docs.md)
* [Service Descriptions](docs/service-description.md)
* [Service Checklist](docs/service-checklist.md)
* [How Waiter Autoscaling Works](docs/autoscaling.md)
* [Service Parameters](docs/parameters.md)