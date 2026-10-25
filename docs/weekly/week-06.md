# Tuần 06 (02/11 – 08/11/2026) — Stateful streaming, đồ thị, dashboard — DEMO GIỮA KỲ

**Tag release:** `v0.6-midterm` · **Hạn nộp:** 23:59 Chủ nhật 08/11 · **Scrum master:** TV1 · **Review:** TV1→TV3, TV2→TV4, TV3→TV5, TV4→TV1, TV5→TV2 · **Liên hệ slide:** Ch.8 (state, timeout), Ch.9 (Lambda), Ch.10 (Spark ML)

**Giờ ước tính:** TV1 6 · TV2 7 · TV3 7 · TV4 7 · TV5 6

> Code tham chiếu nằm trong repo tham chiếu (`ref`). Cách lấy file, chạy kiểm tra rồi tự commit: xem [docs/ke-hoach/porting-guide.md](../ke-hoach/porting-guide.md).

## Kế hoạch

### TV1 — Platform & Ingestion (~6 giờ)

- [ ] `make up` dựng toàn cụm một lệnh + kiểm tra nhanh mọi dịch vụ (3h)  
      Sản phẩm: `Makefile`  
      Commit gợi ý: `chore: one-command cluster bring-up and smoke check`
- [ ] Kịch bản demo giữa kỳ 5 phút, chuẩn bị môi trường (2h)  
      Sản phẩm: `docs/demo-script.md`  
      Commit gợi ý: `docs: midterm demo script`
- [ ] Báo cáo giữa kỳ phần hạ tầng (1h)  
      Sản phẩm: `docs/report/bao-cao.md`  
      Commit gợi ý: `docs(report): midterm infrastructure section`

### TV2 — Batch Layer (~7 giờ)

- [ ] DAG Airflow hằng đêm + khuôn SparkApplication; chạy trọn 1 ngày (5h)  
      Sản phẩm: `airflow/dags/shoppulse_daily.py`, `airflow/dags/templates/spark_batch.yaml`  
      Commit gợi ý: `feat(batch): nightly airflow dag silver to gold`
- [ ] Báo cáo giữa kỳ phần batch (2h)  
      Sản phẩm: `docs/report/bao-cao.md`  
      Commit gợi ý: `docs(report): midterm batch section`

### TV3 — Speed Layer (~7 giờ)

- [ ] Sessionization có trạng thái: timeout 30′ → cảnh báo bỏ giỏ ra topic `alerts`; phát hiện bot; RocksDB state store (7h)  
      Sản phẩm: `streaming/sessions.py`, `tests/unit/test_sessions_state.py`  
      Commit gợi ý: `feat(streaming): stateful sessions with cart-abandon and bot alerts`
- [ ] File đi kèm: `infra/k8s/spark/apps/stream-sessions.yaml`

### TV4 — ML & Analytics (~7 giờ)

- [ ] Đồ thị sản phẩm: PageRank, label propagation, connected components; "khách cũng xem" (5h)  
      Sản phẩm: `ml/product_graph.py`  
      Commit gợi ý: `feat(ml): co-view product graph with graphframes`
- [ ] PageRank tự cài bằng DataFrame để so với GraphFrames/MapReduce (2h)  
      Sản phẩm: `ml/product_graph.py`  
      Commit gợi ý: `feat(ml): native dataframe pagerank for comparison`

### TV5 — Serving, Giám sát & Kiểm thử (~6 giờ)

- [ ] Dashboard Grafana sinh bằng mã (kỹ thuật + KPI realtime) (4h)  
      Sản phẩm: `dashboards/grafana/build.py`  
      Commit gợi ý: `feat(monitoring): grafana dashboards as code`
- [ ] Ghép báo cáo giữa kỳ (mục 1–2 + tiến độ) (2h)  
      Sản phẩm: `docs/report/bao-cao.md`  
      Commit gợi ý: `docs(report): assemble midterm report`
- [ ] File đi kèm: `dashboards/grafana/streaming-ops.json`

## Định nghĩa hoàn thành của tuần

- [ ] demo end-to-end 5 phút trước lớp; có báo cáo giữa kỳ; DAG Airflow xanh; cảnh báo bỏ giỏ xuất hiện trên topic `alerts`.

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
