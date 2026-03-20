FROM flink:1.18.1-scala_2.12-java11

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    build-essential \
    python3-dev \
    zlib1g-dev \
    openjdk-11-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

RUN pip3 install --no-cache-dir apache-flink==1.18.1 kafka-python pandas pyarrow numpy

COPY connectors/* /opt/flink/lib/
