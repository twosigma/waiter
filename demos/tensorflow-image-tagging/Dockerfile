FROM korekontrol/ubuntu-java-python3

RUN pip3 install --upgrade https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-1.3.0-cp34-cp34m-linux_x86_64.whl

RUN apt-get install -y wget
RUN mkdir -p /tmp/imagenet && wget -O /tmp/imagenet/inception-2015-12-05.tgz http://download.tensorflow.org/models/image/imagenet/inception-2015-12-05.tgz

COPY target/uberjar/tensorflow-image-tagging-0.1.0-SNAPSHOT-standalone.jar /opt/tensorflow-image-tagging/
COPY bin/container-run.sh /opt/tensorflow-image-tagging/

RUN chmod ugo=r /opt/tensorflow-image-tagging/tensorflow-image-tagging-0.1.0-SNAPSHOT-standalone.jar

ENTRYPOINT ["/opt/tensorflow-image-tagging/container-run.sh"]
CMD ["--port" "8080"]
