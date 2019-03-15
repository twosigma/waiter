FROM twosigma/mesos-agent:1.0.0-1.0.1604

# we need nginx for our http/2 tests
# gettext-base is needed for envsubst in the nginx server
# python is needed to launch kitchen
RUN apt-get update \
  && apt-get install -y gettext-base \
  && apt-get install -y nginx \
  && apt-get install -y python3

COPY kitchen/bin/waiter-init /usr/bin/waiter-init
COPY kitchen/bin/kitchen /opt/kitchen/kitchen
COPY nginx-server/bin/run-nginx-server.sh /opt/nginx-server/bin/run-nginx-server.sh
COPY nginx-server/data/nginx-template.conf /opt/nginx-server/data/nginx-template.conf
