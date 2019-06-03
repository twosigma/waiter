FROM twosigma/mesos-agent:1.0.0-1.0.1604

# we need nginx for our http/2 tests
# gettext-base is needed for envsubst in the nginx server
# python is needed to launch kitchen
RUN apt-get update && apt-get install -y gettext-base nginx python3

COPY courier/bin/run-courier-server.sh /opt/courier/bin/run-courier-server.sh
COPY courier/data/courier-uberjar.jar /opt/courier/data/courier-uberjar.jar
COPY kitchen/bin/kitchen /opt/kitchen/kitchen
COPY nginx/bin/run-nginx-server.sh /opt/nginx/bin/run-nginx-server.sh
COPY nginx/data/nginx-template.conf /opt/nginx/data/nginx-template.conf
COPY sediment/bin/run-sediment-server.sh /opt/sediment/bin/run-sediment-server.sh
COPY sediment/data/sediment-uberjar.jar /opt/sediment/data/sediment-uberjar.jar
COPY waiter-init/bin/waiter-k8s-init /usr/bin/waiter-k8s-init
COPY waiter-init/bin/waiter-mesos-init /usr/bin/waiter-mesos-init
