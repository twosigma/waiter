# Waiter-Kubernetes Reverse-Proxy Sidecar Container

The sidecar container starts an envoy reverse-proxy that
is serving on `SERVICE_PORT` and forwards all traffic to `PORT0` on localhost.
If a non-zero `HEALTH_CHECK_PORT_INDEX` is provided,
then a second port with that offset from `SERVICE_PORT` is also set up
forwarding to the corresponding port offset from `PORT0`.

## Settings

No default container ports are present.
User must define what ports they want to use by
supplying `SERVICE_PORT` and `PORT0` to the replicaset.
`HEALTH_CHECK_PORT_INDEX` is optional,
and no additional health check port is proxied if absent.

## Building the Docker image

    docker build -t twosigma/waiter-envoy .

There is also a helper script included in `./bin`
for building the image in both the host docker
and the minikube docker contexts.
