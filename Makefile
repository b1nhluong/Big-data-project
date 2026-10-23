# ShopPulse — lệnh thường dùng. `make help` để xem danh sách.
SHELL := /bin/bash
PY ?= python
K := kubectl
export PYTHONPATH := $(CURDIR)

help:  ## Liệt kê lệnh
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n",$$1,$$2}'

# ---------------------------------------------------------------- phát triển local
venv:  ## Tạo môi trường Python (cần Java 17)
	$(PY) -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt

lint:  ## Kiểm tra mã nguồn
	ruff check --select F,E9 --line-length 130 .

test:  ## Chạy toàn bộ test (unit + integration, ~1 phút)
	$(PY) -m pytest

sample-data:  ## Sinh 300k sự kiện mẫu định dạng REES46
	$(PY) -m ingestion.generate_sample --events 300000 --days 28 --out data/raw

local-pipeline:  ## Chạy toàn bộ pipeline trên 1 máy (không cần K8s) + sinh docs/results.md
	bash scripts/run_local_pipeline.sh

api-local:  ## Chạy API trên dữ liệu local: http://localhost:8000/docs
	uvicorn serving.api.app:app --reload --port 8000

download-data:  ## Tải REES46 từ Kaggle (cần ~/.kaggle/kaggle.json)
	bash scripts/download_rees46.sh

# ---------------------------------------------------------------- image
images:  ## Build image Docker (đúng kiến trúc của máy đang chạy: amd64 hoặc arm64)
	docker build -f infra/docker/hadoop/Dockerfile -t shoppulse/hadoop:3.4.1 .
	docker build -f infra/docker/spark/Dockerfile -t shoppulse/spark:1.0 .
	docker build -f serving/api/Dockerfile -t shoppulse/api:1.0 .
	docker build -f infra/docker/replayer/Dockerfile -t shoppulse/replayer:1.0 .
	docker build -f infra/docker/hive-metastore/Dockerfile -t shoppulse/hive-metastore:4.0.1 .
	docker build -f infra/docker/hadoop-python/Dockerfile -t shoppulse/hadoop-python:3.4.1 .

images-k3s: images  ## Nạp image vào containerd của k3s — chạy trên TỪNG node (không cần registry)
	for i in hadoop:3.4.1 spark:1.0 api:1.0 replayer:1.0 hive-metastore:4.0.1 hadoop-python:3.4.1; do \
	  docker save shoppulse/$$i | sudo k3s ctr images import -; done

# ---------------------------------------------------------------- Kubernetes
helm-repos:
	helm repo add strimzi https://strimzi.io/charts/
	helm repo add spark-operator https://kubeflow.github.io/spark-operator
	helm repo add trino https://trinodb.github.io/charts
	helm repo add apache-airflow https://airflow.apache.org
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo add superset https://apache.github.io/superset
	helm repo update

k8s-secrets:  ## Tạo Secret từ infra/k8s/security/secrets.yaml (tự điền từ bản mẫu)
	test -f infra/k8s/security/secrets.yaml || (echo "Hãy tạo infra/k8s/security/secrets.yaml từ secrets.example.yaml" && exit 1)
	$(K) apply -f infra/k8s/security/secrets.yaml

k8s-core:  ## Tầng 1: namespace, bảo mật, Kafka, HDFS, Cassandra, Redis, PostgreSQL
	$(K) apply -f infra/k8s/00-namespaces.yaml
	$(MAKE) k8s-secrets
	$(K) apply -f infra/k8s/security/rbac.yaml -f infra/k8s/security/network-policies.yaml
	helm upgrade --install strimzi strimzi/strimzi-kafka-operator -n kafka -f infra/k8s/kafka/strimzi-values.yaml --wait
	$(K) apply -f infra/k8s/kafka/
	$(K) apply -f infra/k8s/hdfs/hdfs.yaml
	$(K) -n storage create configmap cassandra-schema --from-file=serving/cassandra/schema.cql --dry-run=client -o yaml | $(K) apply -f -
	$(PY) -m ingestion.promotions generate --days 213
	$(K) -n storage create configmap postgres-init --from-file=infra/k8s/postgres/init/ --dry-run=client -o yaml | $(K) apply -f -
	$(K) apply -f infra/k8s/cassandra/cassandra.yaml -f infra/k8s/redis/redis.yaml -f infra/k8s/postgres/postgres.yaml
	$(K) -n kafka wait kafka/shoppulse --for=condition=Ready --timeout=600s
	$(K) -n storage rollout status statefulset/cassandra --timeout=900s
	$(K) apply -f infra/k8s/cassandra/init-schema-job.yaml

k8s-compute:  ## Tầng 2: Spark Operator, Hive Metastore, Trino, Airflow
	helm upgrade --install spark-operator spark-operator/spark-operator -n compute -f infra/k8s/spark/spark-operator-values.yaml --wait
	$(K) apply -f infra/k8s/hive-metastore/hive-metastore.yaml -f infra/k8s/trino/hadoop-conf.yaml
	helm upgrade --install trino trino/trino -n serving -f infra/k8s/trino/values.yaml
	helm upgrade --install airflow apache-airflow/airflow -n compute -f infra/k8s/airflow/values.yaml
	$(K) apply -f infra/k8s/airflow/rbac.yaml

k8s-monitoring:  ## Tầng 3: Prometheus, Grafana, cảnh báo, dashboard
	helm upgrade --install monitoring prometheus-community/kube-prometheus-stack -n monitoring -f infra/k8s/monitoring/kube-prometheus-stack-values.yaml
	$(K) apply -f infra/k8s/monitoring/podmonitors.yaml -f infra/k8s/monitoring/prometheus-rules.yaml
	$(PY) dashboards/grafana/build.py
	for f in dashboards/grafana/*.json; do n=$$(basename $$f .json); \
	  $(K) -n monitoring create configmap grafana-$$n --from-file=$$f --dry-run=client -o yaml | \
	  $(K) label --local -f - grafana_dashboard=1 -o yaml | $(K) apply -f -; done

k8s-serving:  ## Tầng 4: API + Superset
	$(K) apply -f infra/k8s/api/api.yaml
	helm upgrade --install superset superset/superset -n serving -f infra/k8s/superset/values.yaml

load-history:  ## Đưa CSV REES46 lên HDFS rồi chạy CSV -> bronze
	$(K) -n storage exec -i hdfs-namenode-0 -- hdfs dfs -mkdir -p /shoppulse/raw
	for f in data/raw/*.csv; do $(K) -n storage exec -i hdfs-namenode-0 -- bash -c "HADOOP_USER_NAME=spark hdfs dfs -put -f - /shoppulse/raw/$$(basename $$f)" < $$f; done
	$(MAKE) spark-batch JOB=csv-to-parquet SCRIPT=ingestion/csv_to_parquet.py ARGS=

spark-batch:  ## Chạy 1 job batch: make spark-batch JOB=silver SCRIPT=batch/silver.py ARGS="--dates 2019-11-15"
	$(PY) scripts/render_spark_app.py --job $(JOB) --script $(SCRIPT) --args "$(ARGS)" | $(K) apply -f -

streaming-up:  ## Bật 4 job streaming
	$(K) apply -f infra/k8s/spark/apps/

streaming-down:
	$(K) delete -f infra/k8s/spark/apps/ --ignore-not-found

replay:  ## Phát lại tháng 11/2019 vào Kafka (x60, có bơm lỗi + flash sale)
	$(K) -n compute delete job replayer --ignore-not-found
	$(K) -n kafka get secret replayer -o jsonpath='{.data.password}' | base64 -d | \
	  xargs -I{} $(K) -n compute create secret generic replayer-kafka --from-literal=password={} --dry-run=client -o yaml | $(K) apply -f -
	$(K) apply -f infra/k8s/jobs/replayer.yaml

trino-tables:  ## Đăng ký bảng + view Lambda trong Trino
	$(K) -n serving exec -i deploy/trino-coordinator -- trino < serving/trino/tables.sql
	$(K) -n serving exec -i deploy/trino-coordinator -- trino < serving/trino/views.sql

# ---------------------------------------------------------------- thí nghiệm (tuần 9–10)
bench:  ## Thí nghiệm hiệu năng batch trên cụm
	$(MAKE) spark-batch JOB=optimization SCRIPT=batch/optimization.py ARGS="--repeat 3"
	$(MAKE) spark-batch JOB=format-bench SCRIPT=batch/format_benchmark.py ARGS=

chaos-kafka:  ## Tắt 1 broker Kafka, kiểm tra không mất dữ liệu
	bash tests/chaos/kill_kafka_broker.sh
chaos-driver:  ## Kill driver streaming, kiểm tra exactly-once
	bash tests/chaos/kill_streaming_driver.sh
chaos-datanode:  ## Tắt 1 DataNode, quan sát tái nhân bản
	bash tests/chaos/kill_datanode.sh
chaos-cassandra:  ## ONE vs QUORUM khi 2/3 node Cassandra bất khả dụng (CAP)
	bash tests/chaos/cassandra_cap.sh

.PHONY: help venv lint test sample-data local-pipeline api-local download-data images images-k3s helm-repos \
	k8s-secrets k8s-core k8s-compute k8s-monitoring k8s-serving load-history spark-batch streaming-up \
	streaming-down replay trino-tables bench chaos-kafka chaos-driver chaos-datanode chaos-cassandra
