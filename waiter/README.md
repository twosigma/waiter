Waiter is a distributed autoscaler and load balancer for managing web services at scale. Waiter particularly excels at running services with unpredictable loads or multiple co-existing versions. Waiter uses [Marathon](https://mesosphere.github.io/marathon/) to schedule services on a [Mesos](http://mesos.apache.org/) cluster.

## Running Waiter

Prerequisites:

* Java 8
* [Leiningen](http://leiningen.org/)
* A running [Marathon](https://mesosphere.github.io/marathon/)

To run all unit tests:

`lein test`

To start Waiter:

`lein run <path-to-config.edn>`

Please check the [config-minimal.edn](config-minimal.edn) or [config-full.edn](config-full.edn) files for description of the config file.
Logs are in `/log`, and `waiter.log` should contain info on what went wrong if Waiter doesn't start.

Once Waiter has started, to run a very basic integration test:

`WAITER_TEST_KITCHEN_CMD=<kitchen-cmd> lein test :only waiter.basic-test/test-basic-shell-command`

where `<kitchen-cmd>` tells the test what command to use for starting up Waiter's "kitchen" test service (e.g. `java -Xmx128M -jar kitchen.jar`). To run all "fast" integration tests:

`WAITER_TEST_KITCHEN_CMD=<kitchen-cmd> lein test :integration-fast`

To run all "slow" integration tests:

`WAITER_TEST_KITCHEN_CMD=<kitchen-cmd> lein test :integration-slow`

## What is Waiter

Waiter is a web service platform that runs, manages and automatically scales services without human intervention. Waiter particularly excels at running services with unpredictable loads or requiring intensive computing resources.

Developers register services on Waiter by simply supplying their service startup command via a token or directly in a HTTP request header. Waiter takes care of managing the entire lifecycle of services from that point on, including running the service when and only when there is traffic, scaling the service up when there is more traffic, and tearing down the service if there is no traffic.

Managing service lifecycle is the key differentiator of Waiter that empowers developers to build more and faster.

## What can you do with Waiter

Waiter was designed with simplicity in mind - your existing web services can run on Waiter without any modification as long as they meet the following two conditions:

* Speak HTTP
* Client requests can be sent to any backend of the same service

### Handling Unpredictable Traffic with Optimal Resource Utilization

In many (arguably most) situations, it is hard to anticipate how much traffic your service will receive and when it will come. At Two Sigma, we run massive batch workloads to simulate real trading environments. The demand fluctuates and is highly unpredictable. The services that serve those workloads may see zero requests for several days in a row and then suddenly see thousands of requests per second. Capacity planning becomes infeasible. If we underestimate the traffic, the services can be easily overwhelmed, become unresponsive, or even crash, resulting in constant human intervention and poor developer productivity. If we provision sufficient capacity, then the allocated resources are completely wasted when there is no traffic.

Waiter solves this problem. When demand increases, Waiter launches more instances to meet demand and scales down when those instances are no longer needed.

### Machine Learning at Scale

Many machine learning libraries and algorithms are memory hungry or CPU hungry. By running machine learning services on Waiter, you can easily scale these services on demand. At Two Sigma, Waiter serves and scales machines learning services critical to our business. We have services running on Waiter handling thousands of requests per second.

## Documentation

* [How to Waiter](docs/how-to-waiter.md)
* [REST API](docs/rest-api.md)
* [Waiter Design](docs/waiter-design-docs.md)
* [Service Descriptions](docs/service-description.md)
* [Service Checklist](docs/service-checklist.md)
* [How Waiter Autoscaling Works](docs/autoscaling.md)
* [Service Parameters](docs/parameters.md)