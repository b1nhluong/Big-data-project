# [TV1] Replayer: đọc CSV REES46, phát lại vào Kafka
#   docker build -f infra/docker/replayer/Dockerfile -t shoppulse/replayer:1.0 .
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir "confluent-kafka>=2.5" "fastavro>=1.9" "pandas==2.2.3" "pyyaml>=6" "pyspark==3.5.7"
COPY conf/ conf/
COPY shoppulse/ shoppulse/
COPY ingestion/ ingestion/
ENV PYTHONPATH=/app SHOPPULSE_PROFILE=k8s PYTHONUNBUFFERED=1
RUN useradd -r -u 10001 app
USER 10001
ENTRYPOINT ["python", "-m", "ingestion.replayer"]
