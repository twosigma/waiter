#!/bin/bash

set -eu

echo 'Checking k8s-in-docker (kind) status...'
if ! kind get clusters | grep -q kind; then
    echo 'INFO: k8s-in-docker is not running' >&2
    echo 'INFO: Running `kind create cluster` to create a new cluster' >&2
    kind create cluster --config - <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
- role: worker
networking:
  podSubnet: "10.244.0.0/16"
EOF
fi

echo 'Checking if kind pods are reachable from host machine...'

# Create test pod to populate kind control-plane routes
pod_name=kind-test-$(date +%F%T%N | tr -d '.:-')
kubectl --context=kind-kind create -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod_name}
spec:
  containers:
  - name: app
    image: alpine
    command:
    - sleep
    - infinity
EOF
sleep 0.25
kubectl --context=kind-kind delete pod ${pod_name} --wait=false

kind_subnet=10.244.0.0/16
kind_route_pattern='^10\.244\.0\.[0-9]\+ dev'

export WAITER_DOCKER_NETWORK=kind

# Check kind container subnet
kind_ip="$(docker inspect kind-control-plane -f "{{.NetworkSettings.Networks.${WAITER_DOCKER_NETWORK}.IPAddress}}")"
if ! docker exec kind-control-plane ip route | grep -q "${kind_route_pattern}"; then
    echo "ERROR: kind pods not using expected subnet ${kind_subnet}" >&2
    exit 1
fi

# Check routing from host to kind worker nodes
route_check_fail=false
route_check_fixes=
kind_nodes=( $(kind get nodes | sort) )
for ((i=0; i<${#kind_nodes[@]}; i++)); do
    kind_node=${kind_nodes[$i]}
    kind_node_ip=$(docker inspect $kind_node | jq -r ".[0].NetworkSettings.Networks.${WAITER_DOCKER_NETWORK}.IPAddress")
    if [[ $kind_node == kind-control-plane ]]; then
        kind_node_subnet=10.244.0.0/24
    else
        for ((j=0; j<5; j++)); do
            kind_node_subnet=$(docker exec kind-control-plane ip route | fgrep "via $kind_node_ip " | cut -d' ' -f1)
            [[ "$kind_node_subnet" ]] && break
            sleep 1
        done
        if (( j == 5 )); then
            echo "ERROR: failed to get subnet for node $kind_node" >&2
            route_check_fail=true
        fi
    fi
    if ! ip route | fgrep -q "${kind_node_subnet} via ${kind_node_ip} dev"; then
        echo "ERROR: route to kind containers is not correctly configured for node $kind_node" >&2
        route_check_fixes+="sudo ip route replace ${kind_node_subnet} via ${kind_node_ip}"$'\n'
        route_check_fail=true
    fi
done
if [[ $route_check_fail == true ]]; then
    echo "$route_check_fixes" >&2
    read -p 'Fix routes now (requires sudo)? [y/N] ' yn
    if [[ $yn == [yY]* ]]; then
        sudo bash -xc "$route_check_fixes"
    else
        exit 1
    fi
fi

# Start local S3 server
[ "$(docker ps -q --filter=name=s3server)" ] || ./bin/ci/s3-server-setup.sh
S3SERVER_IP=$(docker inspect s3server | jq -r ".[0].NetworkSettings.Networks.${WAITER_DOCKER_NETWORK}.IPAddress")
if [[ $S3SERVER_IP == null ]]; then
    echo 'ERROR: Could not determine S3 server ip.' >&2
    exit 1
fi
export WAITER_S3_BUCKET=http://${S3SERVER_IP}:8000/waiter-service-logs

ns='{"kind":"Namespace","apiVersion":"v1","metadata":{"name":"USER","labels":{"name":"USER"}}}'
for u in $USER waiter; do
    if ! kubectl get ns $u &>/dev/null; then
        echo "Adding k8s namespace $u"
        kubectl create -f <(sed "s/USER/$u/g" <<< "$ns")
    fi
done

# Stop local k8s api server proxy and mock graphite server on exit
trap 'kill %1 %2' EXIT

# Start local k8s api server proxy
echo 'Starting K8s proxy...'
kubectl proxy &
sleep 0.1

# Start netcat to listen to a port. The Codahale Graphite reporter will be able to report without failing and spamming logs.
echo 'Starting mock graphite server (nc)...'
export GRAPHITE_SERVER_PORT=5555
nc -kl localhost $GRAPHITE_SERVER_PORT > /dev/null &

# Clean up old resources

echo 'Cleaning up old k8s objects...'
for resource in pod rs; do
    targets=$(kubectl --namespace=waiter get $resource --selector=waiter/cluster=waiter --no-headers -o custom-columns=:metadata.name)
    for target in $targets; do
        kubectl --namespace=waiter delete --wait=false --cascade=orphan $resource $target
    done
done
echo 'Done deleting old k8s objects.'

echo 'Cleaning up old logs...'
rm -f log/{request.log,scheduler.log,waiter-error.log,waiter.log,waiter-tests.log}

# Squelch reflective access warning from javassist package
# https://www.javassist.org/html/javassist/util/proxy/DefineClassHelper.html#toClass(java.lang.Class,byte%5B%5D)
export JVM_OPTS='--add-opens java.base/java.lang=ALL-UNNAMED'

# Start waiter
echo 'Starting Waiter...'
WAITER_PORT=9091 WAITER_AUTH_RUN_AS_USER=$USER lein run ${WAITER_CONFIG:-config-k8s.edn}
echo "DONE: Waiter exited with code $?"
