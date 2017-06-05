FROM containersol/mesos-agent:1.0.0-0.1.0

RUN mkdir -p /opt/kitchen
COPY target/uberjar/kitchen-0.1.0-SNAPSHOT-standalone.jar /opt/kitchen
COPY bin/container-run.sh /opt/kitchen
RUN chmod ugo=r /opt/kitchen/kitchen-0.1.0-SNAPSHOT-standalone.jar
