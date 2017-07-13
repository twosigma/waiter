FROM containersol/mesos-agent:1.0.0-0.1.0

RUN mkdir -p /opt/token-syncer
COPY target/uberjar/token-syncer-0.1.0-SNAPSHOT-standalone.jar /opt/token-syncer
COPY bin/container-run.sh /opt/token-syncer
RUN chmod ugo=r /opt/token-syncer/token-syncer-0.1.0-SNAPSHOT-standalone.jar
