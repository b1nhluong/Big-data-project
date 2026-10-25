# Tuần 07 (09/11 – 15/11/2026) — Serving layer Lambda + phân tích nâng cao

**Tag release:** `v0.7-serving` · **Hạn nộp:** 23:59 Chủ nhật 15/11 · **Scrum master:** TV2 · **Review:** TV1→TV4, TV2→TV5, TV3→TV1, TV4→TV2, TV5→TV3 · **Liên hệ slide:** Ch.4.6 (Trino liên nguồn), Ch.9 (hợp nhất batch/speed), Ch.10

**Giờ ước tính:** TV1 6 · TV2 7 · TV3 6 · TV4 8 · TV5 7

> Code tham chiếu nằm trong repo tham chiếu (`ref`). Cách lấy file, chạy kiểm tra rồi tự commit: xem [docs/ke-hoach/porting-guide.md](../ke-hoach/porting-guide.md).

## Kế hoạch

### TV1 — Platform & Ingestion (~6 giờ)

- [ ] Triển khai Superset kết nối Trino (3h)  
      Sản phẩm: `infra/k8s/superset/values.yaml`  
      Commit gợi ý: `feat(infra): superset connected to trino`
- [ ] Bảo mật bước 1: Kafka SCRAM + ACL tối thiểu cho từng consumer group (3h)  
      Sản phẩm: `infra/k8s/kafka/users.yaml`  
      Commit gợi ý: `feat(kafka): scram users and least-privilege acls`

### TV2 — Batch Layer (~7 giờ)

- [ ] Job đối soát doanh thu speed vs batch theo (ngày, danh mục) (4h)  
      Sản phẩm: `batch/reconciliation.py`, `benchmarks/results/reconciliation.json`  
      Commit gợi ý: `feat(batch): speed vs batch reconciliation job`
- [ ] Gom file nhỏ an toàn (ghi tạm rồi đổi tên) (3h)  
      Sản phẩm: `batch/compaction.py`  
      Commit gợi ý: `feat(batch): safe small-file compaction`

### TV3 — Speed Layer (~6 giờ)

- [ ] Nạp PipelineModel vào streaming, chấm điểm phiên chưa mua → bảng `live_sessions` (4h)  
      Sản phẩm: `streaming/sessions.py`  
      Commit gợi ý: `feat(streaming): score open sessions with saved pipeline model`
- [ ] Ghi bảng `alerts`, `sessions_closed` vào Cassandra (2h)  
      Sản phẩm: `streaming/sessions.py`, `streaming/sinks.py`  
      Commit gợi ý: `feat(streaming): persist alerts and closed sessions`

### TV4 — ML & Analytics (~8 giờ)

- [ ] KMeans trên RFM, chọn k bằng silhouette (3h)  
      Sản phẩm: `ml/segmentation.py`  
      Commit gợi ý: `feat(ml): kmeans customer segmentation`
- [ ] Chuỗi thời gian: trung bình trượt, z-score bất thường, dự báo Holt từng danh mục bằng applyInPandas, WAPE (3h)  
      Sản phẩm: `ml/timeseries.py`  
      Commit gợi ý: `feat(ml): category revenue anomalies and forecast`
- [ ] Thống kê: tương quan, approxQuantile, chi-square tỉ lệ chuyển đổi theo khung giờ (2h)  
      Sản phẩm: `ml/stats.py`  
      Commit gợi ý: `feat(ml): correlation, quantiles and chi-square tests`

### TV5 — Serving, Giám sát & Kiểm thử (~7 giờ)

- [ ] View Lambda hợp nhất (batch đến hôm qua + realtime hôm nay) + view đối soát; truy vấn demo EXPLAIN ANALYZE (4h)  
      Sản phẩm: `serving/trino/views.sql`, `serving/trino/demo_queries.sql`  
      Commit gợi ý: `feat(serving): unified lambda views on trino`
- [ ] Đẩy gold sang Cassandra (gợi ý, also-viewed, profile); TTL, TWCS, tránh hot partition (3h)  
      Sản phẩm: `serving/export_serving.py`, `serving/cassandra/schema.cql`  
      Commit gợi ý: `feat(serving): export gold tables to cassandra with ttl`

## Định nghĩa hoàn thành của tuần

- [ ] `SELECT * FROM hive.shoppulse.kpi_daily_lambda` trả dữ liệu cả 2 lớp; có `reconciliation.json`; có 3 file kết quả ML (segmentation, timeseries, stats).

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
