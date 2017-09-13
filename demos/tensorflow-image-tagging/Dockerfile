FROM containersol/mesos-agent:1.0.0-0.1.0

RUN apt-get update
RUN apt-get install -y python3 python3-pip python3-dev
RUN pip3 install --upgrade https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-1.3.0-cp34-cp34m-linux_x86_64.whl

RUN mkdir -p /opt/kitchen
COPY target/uberjar/kitchen-0.1.0-SNAPSHOT-standalone.jar /opt/kitchen
COPY bin/container-run.sh /opt/kitchen
RUN chmod ugo=r /opt/kitchen/kitchen-0.1.0-SNAPSHOT-standalone.jar
