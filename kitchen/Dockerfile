FROM twosigma/mesos-agent:1.0.0-1.0.1604

RUN apt-get update \
  && apt-get install -y gettext-base \
  && apt-get install -y nginx \
  && apt-get install -y python3

RUN mkdir -p /opt/kitchen
COPY bin/kitchen /opt/kitchen
COPY bin/waiter-init /usr/bin/waiter-init
COPY .temp/nginx-server/bin/run-nginx-server.sh /opt/nginx-server/bin/run-nginx-server.sh
COPY .temp/nginx-server/data/nginx-template.conf /opt/nginx-server/data/nginx-template.conf
