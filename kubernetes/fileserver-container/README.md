# Waiter-Kubernetes Fileserver Sidecar Container

Waiter on Marathon+Mesos uses the Mesos file-browsing REST API
to give users access to their service instances' local files (e.g., logs).
To provide a similar experience for Waiter on Kubernetes users,
we add a fileserver as a sidecar-container in each Waiter-managed pod.
The sidecar-container runs an nginx server,
mounting the same home directory volume used in the main app container,
and serving that mounted directory using automatic JSON directory indexing.

## Settings

By default, the nginx server binds to port 9090 in its container.
The default port can be overridden by setting `WAITER_FILESERVER_PORT`
in the container's environment to the desired port number.

## Building the Docker image

    docker build -t "twosigma/waiter-fileserver:$(date +%Y%m%d)" .
