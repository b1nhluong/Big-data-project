# [TV1] Image Spark của ShopPulse: Spark 3.5.7 + Python deps + connector JAR + mã nguồn.
#   docker build -f infra/docker/spark/Dockerfile -t shoppulse/spark:1.0 .
# JAR được tải sẵn lúc build (không dùng --packages lúc chạy -> khởi động nhanh, không phụ thuộc mạng).
FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu
USER root
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/spark/jars; for j in \
    org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.7/spark-sql-kafka-0-10_2.12-3.5.7.jar \
    org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.7/spark-token-provider-kafka-0-10_2.12-3.5.7.jar \
    org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    org/apache/spark/spark-avro_2.12/3.5.7/spark-avro_2.12-3.5.7.jar \
    com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.1/spark-cassandra-connector-assembly_2.12-3.5.1.jar \
    io/graphframes/graphframes-spark3_2.12/0.12.2/graphframes-spark3_2.12-0.12.2.jar \
    org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar ; do \
      curl -fsSLO "$MAVEN/$j"; done
COPY requirements.txt /tmp/requirements.txt
# pyspark đã có sẵn trong image -> bỏ khỏi danh sách cài
RUN grep -v '^pyspark' /tmp/requirements.txt > /tmp/req.txt && pip install --no-cache-dir -r /tmp/req.txt
WORKDIR /opt/shoppulse
COPY conf/ conf/
COPY shoppulse/ shoppulse/
COPY ingestion/ ingestion/
COPY batch/ batch/
COPY streaming/ streaming/
COPY ml/ ml/
COPY mapreduce/ mapreduce/
COPY serving/__init__.py serving/export_serving.py serving/
COPY benchmarks/__init__.py benchmarks/report.py benchmarks/
ENV PYTHONPATH=/opt/shoppulse SHOPPULSE_PROFILE=k8s
USER spark
