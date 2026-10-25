# Tuần 02 (05/10 – 11/10/2026) — Hạ tầng lõi trên Kubernetes

**Tag release:** `v0.2-infra` · **Hạn nộp:** 23:59 Chủ nhật 11/10 · **Scrum master:** TV2 · **Review:** TV1→TV3, TV2→TV4, TV3→TV5, TV4→TV1, TV5→TV2 · **Liên hệ slide:** Ch.3 (HDFS), Ch.4 (NoSQL), Ch.5 (Kafka)

**Giờ ước tính:** TV1 7 · TV2 6 · TV3 7 · TV4 6 · TV5 7

> Code tham chiếu nằm trong repo tham chiếu (`ref`). Cách lấy file, chạy kiểm tra rồi tự commit: xem [docs/ke-hoach/porting-guide.md](../ke-hoach/porting-guide.md).

## Kế hoạch

### TV1 — Platform & Ingestion (~7 giờ)

- [ ] Namespace, ResourceQuota, Secret mẫu (1h)  
      Sản phẩm: `infra/k8s/00-namespaces.yaml`, `infra/k8s/security/secrets.example.yaml`  
      Commit gợi ý: `feat(infra): namespaces, quotas and secret template`
- [ ] Strimzi + Kafka 3 broker KRaft; topic `events` 6 partition, RF 3, min.insync 2; user SCRAM (5h)  
      Sản phẩm: `infra/k8s/kafka/`  
      Commit gợi ý: `feat(kafka): strimzi kraft cluster and events topic`
- [ ] Target `make k8s-core` dựng cả tầng dữ liệu (1h)  
      Sản phẩm: `Makefile`  
      Commit gợi ý: `chore: add k8s-core make target`

### TV2 — Batch Layer (~6 giờ)

- [ ] Job CSV → bronze Parquet (partition theo ngày, chạy lại không nhân đôi) (3h)  
      Sản phẩm: `ingestion/csv_to_parquet.py`  
      Commit gợi ý: `feat(ingestion): csv to date-partitioned parquet job`
- [ ] Chạy trên dữ liệu thật; so dung lượng/thời gian CSV vs Parquet (3h)  
      Sản phẩm: `benchmarks/results/csv_to_parquet.json`  
      Commit gợi ý: `perf(ingestion): measure csv vs parquet size and time`
- [ ] File đi kèm: `batch/bronze_layout_benchmark.py`

### TV3 — Speed Layer (~7 giờ)

- [ ] Image Spark kèm JAR Kafka, Avro, Cassandra, GraphFrames, PostgreSQL (3h)  
      Sản phẩm: `infra/docker/spark/Dockerfile`  
      Commit gợi ý: `feat(infra): spark 3.5 image with connectors`
- [ ] Spark Operator + RBAC; chạy SparkPi dạng SparkApplication (4h)  
      Sản phẩm: `infra/k8s/spark/spark-operator-values.yaml`, `infra/k8s/security/rbac.yaml`, `scripts/render_spark_app.py`  
      Commit gợi ý: `feat(infra): spark operator with sparkpi smoke test`
- [ ] File đi kèm: `docs/adr/0006-spark-version.md`

### TV4 — ML & Analytics (~6 giờ)

- [ ] HDFS 1 NameNode + 3 DataNode, job khởi tạo thư mục/quyền (4h)  
      Sản phẩm: `infra/k8s/hdfs/hdfs.yaml`  
      Commit gợi ý: `feat(hdfs): namenode and three datanodes`
- [ ] Avro schema cho sự kiện (2h)  
      Sản phẩm: `ingestion/avro/event.avsc`  
      Commit gợi ý: `feat(ingestion): avro event schema`
- [ ] File đi kèm: `infra/docker/hadoop/Dockerfile`

### TV5 — Serving, Giám sát & Kiểm thử (~7 giờ)

- [ ] Cassandra 3 node (16 vnode, GossipingPropertyFileSnitch) + Redis (4h)  
      Sản phẩm: `infra/k8s/cassandra/cassandra.yaml`, `infra/k8s/redis/redis.yaml`  
      Commit gợi ý: `feat(serving): three-node cassandra and redis`
- [ ] Bảng query-first cho 8 câu hỏi; job tạo schema + role (3h)  
      Sản phẩm: `serving/cassandra/schema.cql`, `infra/k8s/cassandra/init-schema-job.yaml`  
      Commit gợi ý: `feat(serving): query-first cassandra schema`

## Định nghĩa hoàn thành của tuần

- [ ] `make k8s-core` dựng được tầng dữ liệu; `kafka-topics --describe` ISR = 3; `hdfs dfsadmin -report` 3 live; `nodetool status` 3 UN; SparkPi Completed.

## Báo cáo tuần (điền trước khi gắn tag)

### Kết quả chính

- 

### Từng thành viên

| Thành viên | Đã làm (link PR) | Giờ thực tế | Tuần sau |
|---|---|---|---|
| TV1 |  |  |  |
| TV2 |  |  |  |
| TV3 |  |  |  |
| TV4 |  |  |  |
| TV5 |  |  |  |

### Khó khăn & cách xử lý (nguyên liệu cho lessons learned)

- 

### Số đo / ảnh chụp

- 
