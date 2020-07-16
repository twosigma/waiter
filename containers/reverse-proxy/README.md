# Waiter-Kubernetes Reverse-Proxy Sidecar Container

The sidecar container starts an envoy reverse-proxy that
is serving on `SERVICE_PORT` and forwards all traffic to `PORT0` on localhost.

## Settings

No default container ports are present.
User must define what ports they want to use by
supplying `SERVICE_PORT` and `PORT0` to the replicaset

## Building the Docker image

    docker build -t twosigma/waiter-envoy .
