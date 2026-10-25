# Tuần 04 (19/10 – 25/10/2026) — Batch layer v1 + Speed layer v1

**Tag release:** `v0.4-layers` · **Hạn nộp:** 23:59 Chủ nhật 25/10 · **Scrum master:** TV4 · **Review:** TV1→TV5, TV2→TV1, TV3→TV2, TV4→TV3, TV5→TV4 · **Liên hệ slide:** Ch.7 (DataFrame, window, pivot), Ch.8 (event-time window), Ch.4.6 (Presto)

**Giờ ước tính:** TV1 7 · TV2 8 · TV3 7 · TV4 7 · TV5 6

> Code tham chiếu nằm trong repo tham chiếu (`ref`). Cách lấy file, chạy kiểm tra rồi tự commit: xem [docs/ke-hoach/porting-guide.md](../ke-hoach/porting-guide.md).

## Kế hoạch

### TV1 — Platform & Ingestion (~7 giờ)

- [ ] Hive Metastore (image + PostgreSQL) và Trino 2 worker, connector Hive + Cassandra (5h)  
      Sản phẩm: `infra/docker/hive-metastore/`, `infra/k8s/hive-metastore/`, `infra/k8s/trino/`  
      Commit gợi ý: `feat(infra): hive metastore and trino with hive and cassandra catalogs`
- [ ] Triển khai Airflow (KubernetesExecutor, git-sync) (2h)  
      Sản phẩm: `infra/k8s/airflow/`  
      Commit gợi ý: `feat(infra): airflow with kubernetes executor`

### TV2 — Batch Layer (~8 giờ)

- [ ] gold_kpis: ROLLUP, PIVOT/UNPIVOT phễu, heatmap giờ × thứ, lũy kế/MA7 (3h)  
      Sản phẩm: `batch/gold_kpis.py`  
      Commit gợi ý: `feat(batch): gold kpis with rollup and funnel pivot`
- [ ] gold_customers: tái dựng phiên (lag + tổng lũy kế), RFM bằng ntile, user_features (3h)  
      Sản phẩm: `batch/gold_customers.py`  
      Commit gợi ý: `feat(batch): batch sessionization and rfm with window functions`
- [ ] gold_products: top-N bằng row_number, Gini bằng pandas UDAF, cặp xem cùng phiên (2h)  
      Sản phẩm: `batch/gold_products.py`  
      Commit gợi ý: `feat(batch): top-n products, gini udaf and co-view pairs`
- [ ] File đi kèm: `scripts/run_local_pipeline.sh`, `benchmarks/__init__.py`

### TV3 — Speed Layer (~7 giờ)

- [ ] KPI từng phút: watermark 10′, khử trùng trong watermark, stream-static join, update mode (4h)  
      Sản phẩm: `streaming/kpi_minute.py`  
      Commit gợi ý: `feat(streaming): per-minute kpis with 10-minute watermark`
- [ ] Sink idempotent: upsert Cassandra / Parquet theo batch_id (3h)  
      Sản phẩm: `streaming/sinks.py`  
      Commit gợi ý: `feat(streaming): idempotent foreachBatch sinks`
- [ ] File đi kèm: `infra/k8s/spark/apps/stream-kpi-minute.yaml`

### TV4 — ML & Analytics (~7 giờ)

- [ ] ALS implicit feedback (xem=1, giỏ=3, mua=5), chia theo thời gian, NDCG@10 so với baseline phổ biến (5h)  
      Sản phẩm: `ml/als_recommender.py`, `benchmarks/results/ml_als.json`  
      Commit gợi ý: `feat(ml): als recommender with time split and ndcg@10`
- [ ] Ghi chép chọn tham số (rank, alpha, regParam) (2h)  
      Sản phẩm: `docs/lessons/08-scaling.md` (nháp)  
      Commit gợi ý: `docs(ml): als tuning notes`

### TV5 — Serving, Giám sát & Kiểm thử (~6 giờ)

- [ ] Sinh DDL Trino từ schema Parquet thực tế; thử truy vấn liên nguồn (3h)  
      Sản phẩm: `serving/trino/generate_ddl.py`, `serving/trino/tables.sql`  
      Commit gợi ý: `feat(serving): generate trino ddl from parquet schema`
- [ ] Tài liệu mô hình dữ liệu Cassandra (partition key, N/R/W, consistency) (3h)  
      Sản phẩm: `docs/cassandra-data-model.md`  
      Commit gợi ý: `docs(serving): cassandra data model and consistency levels`
- [ ] File đi kèm: `serving/__init__.py`, `serving/trino/__init__.py`, `serving/trino/catalog/`

## Định nghĩa hoàn thành của tuần

- [ ] `SELECT count(*) FROM hive.shoppulse.events` chạy trên Trino; đủ 11 bảng gold; KPI từng phút ghi vào Cassandra; có `ml_als.json`.

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
