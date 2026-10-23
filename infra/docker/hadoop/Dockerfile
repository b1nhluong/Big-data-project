# [TV1] Image Hadoop 3.4.1 dùng chung cho HDFS (NameNode, DataNode, job init) và initContainer của replayer.
#   docker build -f infra/docker/hadoop/Dockerfile -t shoppulse/hadoop:3.4.1 .
#
# Vì sao không dùng thẳng apache/hadoop:3.4.1: image chính thức chỉ có bản amd64, không chạy được
# trên laptop chip Apple (arm64) hay máy ARM khác. Image này dựng từ eclipse-temurin (có cả amd64 và
# arm64) + bản phân phối Hadoop thuần Java, nên `docker build` trên máy nào sẽ ra đúng kiến trúc máy đó.
# Đường dẫn, user (uid 1000) và biến môi trường giữ giống apache/hadoop để manifest không phải đổi.
FROM eclipse-temurin:11-jre-jammy

ARG HADOOP_VERSION=3.4.1
ARG HADOOP_MIRROR=https://archive.apache.org/dist/hadoop/common

USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 procps curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fsSL "${HADOOP_MIRROR}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}-lean.tar.gz" -o /tmp/hadoop.tgz; \
    tar -xzf /tmp/hadoop.tgz -C /opt; \
    mv "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop; \
    rm -rf /tmp/hadoop.tgz /opt/hadoop/share/doc; \
    groupadd -g 1000 hadoop; \
    useradd -u 1000 -g hadoop -m -s /bin/bash hadoop; \
    mkdir -p /data; chown -R hadoop:hadoop /opt/hadoop /data

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:${PATH}

USER hadoop
WORKDIR /opt/hadoop
