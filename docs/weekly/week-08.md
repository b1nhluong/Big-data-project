# Tuần 08 (16/11 – 22/11/2026) — Bảo mật, API, kiểm thử

**Tag release:** `v0.8-secure` · **Hạn nộp:** 23:59 Chủ nhật 22/11 · **Scrum master:** TV3 · **Review:** TV1→TV5, TV2→TV1, TV3→TV2, TV4→TV3, TV5→TV4 · **Liên hệ slide:** Ch.1 (privacy & security), Ch.10 (PipelineModel)

**Giờ ước tính:** TV1 7 · TV2 8 · TV3 5 · TV4 6 · TV5 7

> Code tham chiếu nằm trong repo tham chiếu (`ref`). Cách lấy file, chạy kiểm tra rồi tự commit: xem [docs/ke-hoach/porting-guide.md](../ke-hoach/porting-guide.md).

## Kế hoạch

### TV1 — Platform & Ingestion (~7 giờ)

- [ ] Bảo mật bước 2: Cassandra role chỉ đọc, NetworkPolicy, RBAC, K8s Secrets, HDFS ACL (5h)  
      Sản phẩm: `infra/k8s/security/`, `infra/k8s/hdfs/hdfs.yaml`  
      Commit gợi ý: `feat(infra): network policies, rbac and hdfs acls`
- [ ] Kiểm chứng client không có quyền bị từ chối (ảnh log); nháp lessons 10 (2h)  
      Sản phẩm: `docs/lessons/10-security-governance.md`  
      Commit gợi ý: `docs(lessons): security verification evidence`

### TV2 — Batch Layer (~8 giờ)

- [ ] Unit test cho mọi transform gold; data dictionary (6h)  
      Sản phẩm: `tests/unit/test_batch.py`, `docs/data-dictionary.md`  
      Commit gợi ý: `test(batch): unit tests for gold transforms`
- [ ] Test tích hợp CSV → gold (2h)  
      Sản phẩm: `tests/integration/test_batch_pipeline.py`  
      Commit gợi ý: `test(batch): csv to gold integration test`

### TV3 — Speed Layer (~5 giờ)

- [ ] Dashboard Grafana realtime kinh doanh (KPI, bỏ giỏ, bot, điểm khả năng mua) (2h)  
      Sản phẩm: `dashboards/grafana/realtime-business.json`  
      Commit gợi ý: `feat(streaming): realtime business dashboard`
- [ ] Unit test cho KPI cửa sổ và trending; retry có backoff khi mất kết nối Kafka/HDFS (3h)  
      Sản phẩm: `tests/unit/`, `streaming/common.py`  
      Commit gợi ý: `test(streaming): window and trending unit tests`

### TV4 — ML & Analytics (~6 giờ)

- [ ] Dashboard Superset cho 8 câu hỏi kinh doanh (4h)  
      Sản phẩm: `dashboards/superset/queries.sql`  
      Commit gợi ý: `feat(analytics): superset dashboards for business questions`
- [ ] Lưu model (ALS, GBT) lên HDFS có phiên bản; bảng metric cuối (2h)  
      Sản phẩm: `ml/`, `docs/results.md`  
      Commit gợi ý: `feat(ml): versioned model storage on hdfs`

### TV5 — Serving, Giám sát & Kiểm thử (~7 giờ)

- [ ] API FastAPI `/v1/recommendations/{user_id}` + Dockerfile + HPA + metric Prometheus (6h)  
      Sản phẩm: `serving/api/`, `infra/k8s/api/api.yaml`, `tests/unit/test_api.py`  
      Commit gợi ý: `feat(api): recommendations api with hpa and metrics`
- [ ] Nháp lessons 09 từ số liệu DQ và bộ test (1h)  
      Sản phẩm: `docs/lessons/09-data-quality-testing.md`  
      Commit gợi ý: `docs(lessons): draft 09 data quality and testing`

## Định nghĩa hoàn thành của tuần

- [ ] có ảnh truy cập trái phép bị chặn; API gợi ý chạy trên cụm; ≥ 35 test xanh trên CI.

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
