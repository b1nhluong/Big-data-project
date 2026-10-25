# Danh mục file: tuần nào, ai đưa vào repo chính

Sinh tự động từ `docs/weekly/`. Mỗi file trong repo tham chiếu có đúng một người đưa vào repo chính, ở tuần sớm nhất cần đến nó.
Cột *Commit gợi ý* là commit của việc chứa file đó (có thể gộp nhiều file vào một commit, hoặc tách nhỏ hơn).

Hai loại file được đánh dấu riêng:

- **tự chạy job để sinh ra**: file kết quả trong `benchmarks/results/`. Số trong repo tham chiếu đo trên dữ liệu mẫu 300 nghìn sự kiện; nhóm chạy job trên dữ liệu và cụm của mình rồi commit kết quả thật.
- **viết lại bằng số đo/trải nghiệm của nhóm**: báo cáo, lessons learned, EDA, benchmark, chaos. Bản tham chiếu chỉ là khung; nội dung phải là điều nhóm thực sự đo và gặp.

## Tuần 1 (28/9–4/10) — Khởi động, đề xuất, khung repo

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `.github/CODEOWNERS` | `chore: init repo structure and branch protection` |
| TV1 | `.github/ISSUE_TEMPLATE/task.md` | `chore: init repo structure and branch protection` |
| TV1 | `.github/PULL_REQUEST_TEMPLATE.md` | `chore: init repo structure and branch protection` |
| TV1 | `.github/workflows/ci.yml` | `chore: init repo structure and branch protection` |
| TV1 | `.gitignore` | (đi kèm) |
| TV1 | `README.md` | `chore: init repo structure and branch protection` |
| TV1 | `docs/adr/0002-kubernetes.md` | `docs(adr): 0002 kubernetes over yarn and compose` |
| TV1 | `infra/k3s/install-server.sh` | `feat(infra): k3s server and agent install scripts` |
| TV1 | `infra/k3s/join-agent.sh` | `feat(infra): k3s server and agent install scripts` |
| TV1 | `infra/k3s/kind-config.yaml` | (đi kèm) |
| TV2 | `conf/shoppulse.yaml` | `feat(core): shared config, spark session and event schema` |
| TV2 | `ingestion/__init__.py` | (đi kèm) |
| TV2 | `ingestion/generate_sample.py` | `feat(ingestion): rees46 download script and sample generator` |
| TV2 | `pytest.ini` | (đi kèm) |
| TV2 | `requirements-dev.txt` | (đi kèm) |
| TV2 | `requirements.txt` | (đi kèm) |
| TV2 | `scripts/download_rees46.sh` | `feat(ingestion): rees46 download script and sample generator` |
| TV2 | `shoppulse/__init__.py` | (đi kèm) |
| TV2 | `shoppulse/config.py` | `feat(core): shared config, spark session and event schema` |
| TV2 | `shoppulse/io.py` | (đi kèm) |
| TV2 | `shoppulse/schemas.py` | `feat(core): shared config, spark session and event schema` |
| TV2 | `shoppulse/spark.py` | `feat(core): shared config, spark session and event schema` |
| TV3 | `docs/adr/0001-lambda-vs-kappa.md` | `docs(adr): 0001 lambda vs kappa` |
| TV3 | `docs/report/bao-cao.md` | `docs(report): project proposal` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV4 | `batch/eda.py` | (đi kèm) |
| TV4 | `docs/adr/0005-file-formats.md` | `docs(adr): 0005 file formats` |
| TV4 | `docs/eda.md` | `docs(eda): null, duplicate and funnel stats on 1% sample` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV5 | `docs/adr/0003-cassandra.md` | `docs(adr): 0003 cassandra and 0004 hdfs` |
| TV5 | `docs/adr/0004-hdfs.md` | `docs(adr): 0003 cassandra and 0004 hdfs` |
| TV5 | `docs/plan.md` | (đi kèm) |
| TV5 | `docs/weekly/week-01.md` | (đi kèm) |
| TV5 | `docs/weekly/week-02.md` | (đi kèm) |
| TV5 | `docs/weekly/week-03.md` | (đi kèm) |
| TV5 | `docs/weekly/week-04.md` | (đi kèm) |
| TV5 | `docs/weekly/week-05.md` | (đi kèm) |
| TV5 | `docs/weekly/week-06.md` | (đi kèm) |
| TV5 | `docs/weekly/week-07.md` | (đi kèm) |
| TV5 | `docs/weekly/week-08.md` | (đi kèm) |
| TV5 | `docs/weekly/week-09.md` | (đi kèm) |
| TV5 | `docs/weekly/week-10.md` | (đi kèm) |
| TV5 | `docs/weekly/week-11.md` | (đi kèm) |
| TV5 | `docs/weekly/week-12.md` | (đi kèm) |
| TV5 | `docs/ke-hoach/01-Tong-quan-du-an.pdf` | (đi kèm) |
| TV5 | `docs/ke-hoach/02-Ke-hoach-tung-tuan.pdf` | (đi kèm) |
| TV5 | `docs/ke-hoach/03-Viec-theo-tung-nguoi.pdf` | (đi kèm) |
| TV5 | `docs/ke-hoach/README.md` | (đi kèm) |
| TV5 | `docs/ke-hoach/file-index.md` | (đi kèm) |
| TV5 | `docs/ke-hoach/porting-guide.md` | (đi kèm) |
| TV5 | `docs/ke-hoach/so-do-kien-truc.png` | (đi kèm) |
| TV5 | `docs/ke-hoach/so-do-luong-du-lieu.png` | (đi kèm) |
| TV5 | `docs/ke-hoach/timeline-12-tuan.png` | (đi kèm) |

## Tuần 2 (5/10–11/10) — Hạ tầng lõi trên Kubernetes

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `Makefile` | `chore: add k8s-core make target` |
| TV1 | `infra/k8s/00-namespaces.yaml` | `feat(infra): namespaces, quotas and secret template` |
| TV1 | `infra/k8s/kafka/kafka-cluster.yaml` | `feat(kafka): strimzi kraft cluster and events topic` |
| TV1 | `infra/k8s/kafka/strimzi-values.yaml` | `feat(kafka): strimzi kraft cluster and events topic` |
| TV1 | `infra/k8s/kafka/topics.yaml` | `feat(kafka): strimzi kraft cluster and events topic` |
| TV1 | `infra/k8s/kafka/users.yaml` | `feat(kafka): strimzi kraft cluster and events topic` |
| TV1 | `infra/k8s/security/secrets.example.yaml` | `feat(infra): namespaces, quotas and secret template` |
| TV2 | `benchmarks/results/csv_to_parquet.json` | **tự chạy job để sinh ra**, không chép |
| TV2 | `ingestion/csv_to_parquet.py` | `feat(ingestion): csv to date-partitioned parquet job` |
| TV3 | `docs/adr/0006-spark-version.md` | (đi kèm) |
| TV3 | `infra/docker/spark/Dockerfile` | `feat(infra): spark 3.5 image with connectors` |
| TV3 | `infra/k8s/security/rbac.yaml` | `feat(infra): spark operator with sparkpi smoke test` |
| TV3 | `infra/k8s/spark/spark-operator-values.yaml` | `feat(infra): spark operator with sparkpi smoke test` |
| TV3 | `scripts/render_spark_app.py` | `feat(infra): spark operator with sparkpi smoke test` |
| TV4 | `infra/docker/hadoop/Dockerfile` | (đi kèm) |
| TV4 | `infra/k8s/hdfs/hdfs.yaml` | `feat(hdfs): namenode and three datanodes` |
| TV4 | `ingestion/avro/event.avsc` | `feat(ingestion): avro event schema` |
| TV5 | `infra/k8s/cassandra/cassandra.yaml` | `feat(serving): three-node cassandra and redis` |
| TV5 | `infra/k8s/cassandra/init-schema-job.yaml` | `feat(serving): query-first cassandra schema` |
| TV5 | `infra/k8s/redis/redis.yaml` | `feat(serving): three-node cassandra and redis` |
| TV5 | `serving/cassandra/schema.cql` | `feat(serving): query-first cassandra schema` |
| TV2 | `batch/bronze_layout_benchmark.py` | `perf(batch): bronze layout experiment (sort, binary id, zstd)` |

## Tuần 3 (12/10–18/10) — Luồng dữ liệu đầu–cuối đầu tiên

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `infra/docker/replayer/Dockerfile` | (đi kèm) |
| TV1 | `infra/k8s/jobs/replayer.yaml` | (đi kèm) |
| TV1 | `infra/k8s/postgres/init/00_metastore.sh` | `feat(ingestion): promotions in postgres with parallel jdbc load` |
| TV1 | `infra/k8s/postgres/init/01_promotions.sql` | `feat(ingestion): promotions in postgres with parallel jdbc load` |
| TV1 | `infra/k8s/postgres/postgres.yaml` | `feat(ingestion): promotions in postgres with parallel jdbc load` |
| TV1 | `ingestion/chaos.py` | (đi kèm) |
| TV1 | `ingestion/promotions.py` | `feat(ingestion): promotions in postgres with parallel jdbc load` |
| TV1 | `ingestion/replayer.py` | `feat(ingestion): time-ordered kafka replayer` |
| TV2 | `batch/__init__.py` | (đi kèm) |
| TV2 | `batch/silver.py` | `feat(batch): product and user dimensions, promo broadcast join` |
| TV2 | `benchmarks/results/silver_dq.json` | **tự chạy job để sinh ra**, không chép |
| TV2 | `shoppulse/quality.py` | `feat(batch): silver layer with dq rules and pii hashing` |
| TV2 | `shoppulse/udfs.py` | `feat(batch): silver layer with dq rules and pii hashing` |
| TV3 | `infra/k8s/spark/apps/stream-bronze-sink.yaml` | (đi kèm) |
| TV3 | `streaming/__init__.py` | (đi kèm) |
| TV3 | `streaming/bronze_sink.py` | `feat(streaming): kafka to hdfs bronze sink` |
| TV3 | `streaming/common.py` | `feat(streaming): kafka to hdfs bronze sink` |
| TV3 | `streaming/metrics.py` | `feat(streaming): prometheus query listener` |
| TV3 | `streaming/sinks.py` | (đi kèm) |
| TV3 | `tests/integration/test_streaming_exactly_once.py` | `test(streaming): exactly-once count after restart` |
| TV5 | `tests/__init__.py` | (đi kèm) |
| TV5 | `tests/conftest.py` | `test: spark fixture and core unit tests` |
| TV5 | `tests/integration/__init__.py` | (đi kèm) |
| TV5 | `tests/unit/__init__.py` | (đi kèm) |
| TV5 | `tests/unit/test_config.py` | `test: spark fixture and core unit tests` |
| TV5 | `tests/unit/test_schemas_udfs.py` | `test: spark fixture and core unit tests` |

## Tuần 4 (19/10–25/10) — Batch layer v1 + Speed layer v1

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `infra/docker/hive-metastore/Dockerfile` | `feat(infra): hive metastore and trino with hive and cassandra catalogs` |
| TV1 | `infra/k8s/airflow/rbac.yaml` | `feat(infra): airflow with kubernetes executor` |
| TV1 | `infra/k8s/airflow/values.yaml` | `feat(infra): airflow with kubernetes executor` |
| TV1 | `infra/k8s/hive-metastore/hive-metastore.yaml` | `feat(infra): hive metastore and trino with hive and cassandra catalogs` |
| TV1 | `infra/k8s/trino/hadoop-conf.yaml` | `feat(infra): hive metastore and trino with hive and cassandra catalogs` |
| TV1 | `infra/k8s/trino/values.yaml` | `feat(infra): hive metastore and trino with hive and cassandra catalogs` |
| TV2 | `batch/gold_customers.py` | `feat(batch): batch sessionization and rfm with window functions` |
| TV2 | `batch/gold_kpis.py` | `feat(batch): gold kpis with rollup and funnel pivot` |
| TV2 | `batch/gold_products.py` | `feat(batch): top-n products, gini udaf and co-view pairs` |
| TV2 | `benchmarks/__init__.py` | (đi kèm) |
| TV2 | `scripts/run_local_pipeline.sh` | (đi kèm) |
| TV3 | `infra/k8s/spark/apps/stream-kpi-minute.yaml` | (đi kèm) |
| TV3 | `streaming/kpi_minute.py` | `feat(streaming): per-minute kpis with 10-minute watermark` |
| TV4 | `benchmarks/results/ml_als.json` | **tự chạy job để sinh ra**, không chép |
| TV4 | `docs/lessons/08-scaling.md` | `docs(ml): als tuning notes` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV4 | `ml/als_recommender.py` | `feat(ml): als recommender with time split and ndcg@10` |
| TV5 | `docs/cassandra-data-model.md` | `docs(serving): cassandra data model and consistency levels` |
| TV5 | `serving/__init__.py` | (đi kèm) |
| TV5 | `serving/trino/__init__.py` | (đi kèm) |
| TV5 | `serving/trino/catalog/cassandra.properties` | (đi kèm) |
| TV5 | `serving/trino/catalog/hive.properties` | (đi kèm) |
| TV5 | `serving/trino/generate_ddl.py` | `feat(serving): generate trino ddl from parquet schema` |
| TV5 | `serving/trino/tables.sql` | `feat(serving): generate trino ddl from parquet schema` |

## Tuần 5 (26/10–1/11) — Tối ưu batch, streaming nâng cao, bơm lỗi

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `benchmarks/results/mapreduce_vs_spark.json` | **tự chạy job để sinh ra**, không chép |
| TV1 | `infra/docker/hadoop-python/Dockerfile` | (đi kèm) |
| TV1 | `infra/k8s/jobs/mapreduce-coview.yaml` | (đi kèm) |
| TV1 | `mapreduce/__init__.py` | `feat(mapreduce): co-view pair count with combiner` |
| TV1 | `mapreduce/compare.py` | `feat(mapreduce): co-view pair count with combiner` |
| TV1 | `mapreduce/export_input.py` | `feat(mapreduce): co-view pair count with combiner` |
| TV1 | `mapreduce/mapper_sessions.py` | `feat(mapreduce): co-view pair count with combiner` |
| TV1 | `mapreduce/reducer_pairs.py` | `feat(mapreduce): co-view pair count with combiner` |
| TV1 | `mapreduce/reducer_sum.py` | `feat(mapreduce): co-view pair count with combiner` |
| TV1 | `mapreduce/run_hadoop.sh` | `feat(mapreduce): co-view pair count with combiner` |
| TV1 | `mapreduce/run_local.sh` | `feat(mapreduce): co-view pair count with combiner` |
| TV1 | `tests/unit/test_chaos.py` | `feat(ingestion): fault injection modes for replayer` |
| TV1 | `tests/unit/test_mapreduce.py` | `feat(mapreduce): co-view pair count with combiner` |
| TV2 | `batch/optimization.py` | `perf(batch): join, pruning, cache and skew experiments` |
| TV2 | `batch/rdd_vs_dataframe.py` | (đi kèm) |
| TV2 | `docs/lessons/02-spark-processing.md` | `docs(lessons): draft 02 spark and 06 performance` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV2 | `docs/lessons/06-performance.md` | `docs(lessons): draft 02 spark and 06 performance` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV2 | `docs/plans/join_broadcast.txt` | `perf(batch): join, pruning, cache and skew experiments` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV2 | `docs/plans/join_bucketed.txt` | `perf(batch): join, pruning, cache and skew experiments` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV2 | `docs/plans/join_sort_merge.txt` | `perf(batch): join, pruning, cache and skew experiments` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV2 | `docs/plans/skew_salting.txt` | `perf(batch): join, pruning, cache and skew experiments` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV3 | `docs/lessons/03-stream-processing.md` | `feat(streaming): track late events dropped by watermark` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV3 | `infra/k8s/spark/apps/stream-trending.yaml` | (đi kèm) |
| TV3 | `streaming/trending.py` | `feat(streaming): sliding-window trending to redis zset` |
| TV4 | `benchmarks/results/ml_propensity.json` | **tự chạy job để sinh ra**, không chép |
| TV4 | `ml/features.py` | `feat(ml): leakage-safe session features` |
| TV4 | `ml/purchase_propensity.py` | `feat(ml): purchase propensity pipeline, gbt vs logistic regression` |
| TV5 | `infra/k8s/monitoring/kube-prometheus-stack-values.yaml` | `feat(monitoring): prometheus stack, podmonitors and alert rules` |
| TV5 | `infra/k8s/monitoring/podmonitors.yaml` | `feat(monitoring): prometheus stack, podmonitors and alert rules` |
| TV5 | `infra/k8s/monitoring/prometheus-rules.yaml` | `feat(monitoring): prometheus stack, podmonitors and alert rules` |

## Tuần 6 (2/11–8/11) — Stateful streaming, đồ thị, dashboard — DEMO GIỮA KỲ

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `docs/demo-script.md` | `docs: midterm demo script` |
| TV2 | `airflow/dags/shoppulse_daily.py` | `feat(batch): nightly airflow dag silver to gold` |
| TV2 | `airflow/dags/templates/spark_batch.yaml` | `feat(batch): nightly airflow dag silver to gold` |
| TV3 | `infra/k8s/spark/apps/stream-sessions.yaml` | (đi kèm) |
| TV3 | `streaming/sessions.py` | `feat(streaming): stateful sessions with cart-abandon and bot alerts` |
| TV3 | `tests/unit/test_sessions_state.py` | `feat(streaming): stateful sessions with cart-abandon and bot alerts` |
| TV4 | `ml/product_graph.py` | `feat(ml): co-view product graph with graphframes` |
| TV5 | `dashboards/grafana/build.py` | `feat(monitoring): grafana dashboards as code` |
| TV5 | `dashboards/grafana/streaming-ops.json` | (đi kèm) |

## Tuần 7 (9/11–15/11) — Serving layer Lambda + phân tích nâng cao

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `infra/k8s/superset/values.yaml` | `feat(infra): superset connected to trino` |
| TV2 | `batch/compaction.py` | `feat(batch): safe small-file compaction` |
| TV2 | `batch/reconciliation.py` | `feat(batch): speed vs batch reconciliation job` |
| TV2 | `benchmarks/results/reconciliation.json` | **tự chạy job để sinh ra**, không chép |
| TV4 | `ml/segmentation.py` | `feat(ml): kmeans customer segmentation` |
| TV4 | `ml/stats.py` | `feat(ml): correlation, quantiles and chi-square tests` |
| TV4 | `ml/timeseries.py` | `feat(ml): category revenue anomalies and forecast` |
| TV5 | `serving/export_serving.py` | `feat(serving): export gold tables to cassandra with ttl` |
| TV5 | `serving/trino/demo_queries.sql` | `feat(serving): unified lambda views on trino` |
| TV5 | `serving/trino/views.sql` | `feat(serving): unified lambda views on trino` |

## Tuần 8 (16/11–22/11) — Bảo mật, API, kiểm thử

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `docs/lessons/10-security-governance.md` | `docs(lessons): security verification evidence` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV1 | `infra/k8s/security/network-policies.yaml` | `feat(infra): network policies, rbac and hdfs acls` |
| TV2 | `docs/data-dictionary.md` | `test(batch): unit tests for gold transforms` |
| TV2 | `tests/integration/test_batch_pipeline.py` | `test(batch): csv to gold integration test` |
| TV2 | `tests/unit/test_batch.py` | `test(batch): unit tests for gold transforms` |
| TV3 | `dashboards/grafana/realtime-business.json` | `feat(streaming): realtime business dashboard` |
| TV3 | `tests/unit/test_api.py` | `test(streaming): window and trending unit tests` |
| TV3 | `tests/unit/test_features_reconcile.py` | `test(streaming): window and trending unit tests` |
| TV4 | `dashboards/superset/queries.sql` | `feat(analytics): superset dashboards for business questions` |
| TV4 | `docs/results.md` | `feat(ml): versioned model storage on hdfs` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV4 | `ml/__init__.py` | `feat(ml): versioned model storage on hdfs` |
| TV5 | `docs/lessons/09-data-quality-testing.md` | `docs(lessons): draft 09 data quality and testing` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV5 | `infra/k8s/api/api.yaml` | `feat(api): recommendations api with hpa and metrics` |
| TV5 | `serving/api/Dockerfile` | `feat(api): recommendations api with hpa and metrics` |
| TV5 | `serving/api/__init__.py` | `feat(api): recommendations api with hpa and metrics` |
| TV5 | `serving/api/app.py` | `feat(api): recommendations api with hpa and metrics` |
| TV5 | `serving/api/requirements.txt` | `feat(api): recommendations api with hpa and metrics` |
| TV5 | `serving/api/store.py` | `feat(api): recommendations api with hpa and metrics` |

## Tuần 9 (23/11–29/11) — Thí nghiệm hiệu năng & mở rộng

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `batch/format_benchmark.py` | `perf(hdfs): file format size and scan benchmark` |
| TV1 | `docs/benchmarks.md` | `perf(kafka): acks and partition count benchmarks` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV2 | `benchmarks/results/optimization.json` | **tự chạy job để sinh ra**, không chép |
| TV4 | `benchmarks/report.py` | `feat(benchmarks): report generator for all results` |
| TV5 | `benchmarks/locustfile.py` | `perf(api): locust load test with hpa` |

## Tuần 10 (30/11–6/12) — Chaos test, chứng minh chịu lỗi

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `tests/chaos/kill_kafka_broker.sh` | `test(chaos): kafka broker loss and node partition` |
| TV2 | `tests/chaos/kill_datanode.sh` | `test(chaos): datanode loss and re-replication` |
| TV3 | `tests/chaos/kill_streaming_driver.sh` | `test(chaos): streaming driver kill and checkpoint recovery` |
| TV5 | `docs/chaos.md` | `docs(chaos): five scenarios with alerts and measurements` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV5 | `tests/chaos/cassandra_cap.sh` | `test(chaos): cassandra consistency under node loss` |

## Tuần 11 (7/12–13/12) — Chạy toàn bộ dữ liệu, viết báo cáo

| Người | File | Commit gợi ý |
|---|---|---|
| TV1 | `docs/lessons/01-data-ingestion.md` | `docs: runbook and lessons 01, 05, 10` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV1 | `docs/lessons/04-data-storage.md` | `docs: runbook and lessons 01, 05, 10` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV1 | `docs/lessons/05-system-integration.md` | `docs: runbook and lessons 01, 05, 10` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV1 | `docs/lessons/07-monitoring-debugging.md` | `docs: runbook and lessons 01, 05, 10` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV1 | `docs/lessons/11-fault-tolerance.md` | `docs: runbook and lessons 01, 05, 10` — **viết lại bằng số đo/trải nghiệm của nhóm** |
| TV1 | `docs/runbook.md` | `docs: runbook and lessons 01, 05, 10` |

## Tuần 12 (14/12–20/12) — Hoàn thiện & bảo vệ

| Người | File | Commit gợi ý |
|---|---|---|
| TV5 | `docs/defense-qa.md` | `docs: defense q&a and final report pdf` |
