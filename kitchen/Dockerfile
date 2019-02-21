FROM twosigma/mesos-agent:1.0.0-1.0.1604

RUN apt-get update && apt-get install -y python3

RUN mkdir -p /opt/kitchen
COPY bin/kitchen /opt/kitchen
COPY bin/waiter-init /usr/bin/waiter-init
