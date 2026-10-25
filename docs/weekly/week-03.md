# Tuần 03 (12/10 – 18/10/2026) — Luồng dữ liệu đầu–cuối đầu tiên

**Tag release:** `v0.3-pipeline` · **Hạn nộp:** 23:59 Chủ nhật 18/10 · **Scrum master:** TV3 · **Review:** TV1→TV4, TV2→TV5, TV3→TV1, TV4→TV2, TV5→TV3 · **Liên hệ slide:** Ch.5 (producer, acks), Ch.8 (file sink, checkpoint)

**Giờ ước tính:** TV1 8 · TV2 8 · TV3 8 · TV4 5 · TV5 6

> Code tham chiếu nằm trong repo tham chiếu (`ref`). Cách lấy file, chạy kiểm tra rồi tự commit: xem [docs/ke-hoach/porting-guide.md](../ke-hoach/porting-guide.md).

## Kế hoạch

### TV1 — Platform & Ingestion (~8 giờ)

- [ ] Replayer v1: đọc CSV theo thời gian, `--speed`, key = user_id, acks=all + idempotent (4h)  
      Sản phẩm: `ingestion/replayer.py`  
      Commit gợi ý: `feat(ingestion): time-ordered kafka replayer`
- [ ] PostgreSQL + sinh lịch khuyến mãi + nạp JDBC song song (thay Sqoop) (4h)  
      Sản phẩm: `ingestion/promotions.py`, `infra/k8s/postgres/`  
      Commit gợi ý: `feat(ingestion): promotions in postgres with parallel jdbc load`
- [ ] File đi kèm: `ingestion/chaos.py`, `infra/docker/replayer/Dockerfile`, `infra/k8s/jobs/replayer.yaml`

### TV2 — Batch Layer (~8 giờ)

- [ ] Silver: luật DQ, cách ly dòng lỗi, khử trùng event_id, băm user_id, pandas UDF danh mục/giá (5h)  
      Sản phẩm: `batch/silver.py`, `shoppulse/quality.py`, `shoppulse/udfs.py`  
      Commit gợi ý: `feat(batch): silver layer with dq rules and pii hashing`
- [ ] dim_product (row_number), dim_user (RDD.zipWithIndex), broadcast join khuyến mãi; ghi kết quả DQ (3h)  
      Sản phẩm: `batch/silver.py`, `benchmarks/results/silver_dq.json`  
      Commit gợi ý: `feat(batch): product and user dimensions, promo broadcast join`
- [ ] File đi kèm: `batch/__init__.py`

### TV3 — Speed Layer (~8 giờ)

- [ ] Kafka → HDFS bronze (append, checkpoint, partition ngày) (4h)  
      Sản phẩm: `streaming/bronze_sink.py`, `streaming/common.py`  
      Commit gợi ý: `feat(streaming): kafka to hdfs bronze sink`
- [ ] Listener xuất metric Prometheus cho mọi job streaming (2h)  
      Sản phẩm: `streaming/metrics.py`  
      Commit gợi ý: `feat(streaming): prometheus query listener`
- [ ] Test số vào = số ra khi chạy lại (2h)  
      Sản phẩm: `tests/integration/test_streaming_exactly_once.py`  
      Commit gợi ý: `test(streaming): exactly-once count after restart`
- [ ] File đi kèm: `streaming/__init__.py`, `streaming/sinks.py`, `infra/k8s/spark/apps/stream-bronze-sink.yaml`

### TV4 — ML & Analytics (~5 giờ)

- [ ] Đẩy CSV lên HDFS, chạy csv_to_parquet trên cụm (3h)  
      Sản phẩm: `Makefile` (target `load-history`)  
      Commit gợi ý: `chore: load-history target runs ingestion on cluster`
- [ ] Phân tích độ thưa user × sản phẩm, độ dài phiên (chuẩn bị cho ML) (2h)  
      Sản phẩm: `docs/eda.md`  
      Commit gợi ý: `docs(eda): sparsity and session stats for modeling`

### TV5 — Serving, Giám sát & Kiểm thử (~6 giờ)

- [ ] Khung pytest + fixture Spark; test schema, UDF, cấu hình (4h)  
      Sản phẩm: `tests/conftest.py`, `tests/unit/test_schemas_udfs.py`, `tests/unit/test_config.py`  
      Commit gợi ý: `test: spark fixture and core unit tests`
- [ ] Đưa pytest vào CI (Java 17 + Spark local) (2h)  
      Sản phẩm: `.github/workflows/ci.yml`  
      Commit gợi ý: `ci: run pytest on java 17`
- [ ] File đi kèm: `tests/__init__.py`, `tests/unit/__init__.py`, `tests/integration/__init__.py`

## Định nghĩa hoàn thành của tuần

- [ ] video 2 phút replayer → Kafka → HDFS bronze; ≥ 15 test xanh trên CI; có `benchmarks/results/silver_dq.json`.

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
