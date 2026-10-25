# Tuần 09 (23/11 – 29/11/2026) — Thí nghiệm hiệu năng & mở rộng

**Tag release:** `v0.9-bench` · **Hạn nộp:** 23:59 Chủ nhật 29/11 · **Scrum master:** TV4 · **Review:** TV1→TV2, TV2→TV3, TV3→TV4, TV4→TV5, TV5→TV1 · **Liên hệ slide:** Ch.1 (I/O), Ch.3.3 (định dạng file), Ch.5 (acks), Ch.7

**Giờ ước tính:** TV1 7 · TV2 8 · TV3 6 · TV4 6 · TV5 5

> Code tham chiếu nằm trong repo tham chiếu (`ref`). Cách lấy file, chạy kiểm tra rồi tự commit: xem [docs/ke-hoach/porting-guide.md](../ke-hoach/porting-guide.md).

## Kế hoạch

### TV1 — Platform & Ingestion (~7 giờ)

- [ ] Kafka: acks 0/1/all × thông lượng/độ trễ (kafka-producer-perf-test); 3 vs 6 partition (4h)  
      Sản phẩm: `docs/benchmarks.md`  
      Commit gợi ý: `perf(kafka): acks and partition count benchmarks`
- [ ] Định dạng file trên HDFS thật: CSV/JSON/Avro/Parquet/ORC (dung lượng, tốc độ quét) (3h)  
      Sản phẩm: `batch/format_benchmark.py`  
      Commit gợi ý: `perf(hdfs): file format size and scan benchmark`

### TV2 — Batch Layer (~8 giờ)

- [ ] Strong scaling (1/2/4 executor) và weak scaling (1/2/4/7 tháng dữ liệu) (5h)  
      Sản phẩm: `batch/optimization.py`, `docs/benchmarks.md`  
      Commit gợi ý: `perf(batch): strong and weak scaling runs`
- [ ] Chạy lại bộ tối ưu trên dữ liệu thật (3h)  
      Sản phẩm: `benchmarks/results/optimization.json`  
      Commit gợi ý: `perf(batch): rerun optimization suite on full data`

### TV3 — Speed Layer (~6 giờ)

- [ ] Trigger interval vs độ trễ end-to-end; thông lượng tối đa trước khi lag tăng (tăng dần `--speed`) (6h)  
      Sản phẩm: `docs/benchmarks.md`  
      Commit gợi ý: `perf(streaming): trigger interval and max throughput`

### TV4 — ML & Analytics (~6 giờ)

- [ ] Thời gian train ALS/GBT theo số executor (3h)  
      Sản phẩm: `docs/benchmarks.md`, `docs/lessons/08-scaling.md`  
      Commit gợi ý: `perf(ml): training time vs executors`
- [ ] Tổng hợp mọi số liệu thành bảng/biểu đồ (3h)  
      Sản phẩm: `benchmarks/report.py`, `docs/results.md`  
      Commit gợi ý: `feat(benchmarks): report generator for all results`

### TV5 — Serving, Giám sát & Kiểm thử (~5 giờ)

- [ ] Load test API bằng Locust, quan sát HPA tăng pod (3h)  
      Sản phẩm: `benchmarks/locustfile.py`  
      Commit gợi ý: `perf(api): locust load test with hpa`
- [ ] Chọn ngưỡng cảnh báo Prometheus từ số đo (2h)  
      Sản phẩm: `infra/k8s/monitoring/`  
      Commit gợi ý: `feat(monitoring): alert thresholds from benchmark results`

## Định nghĩa hoàn thành của tuần

- [ ] `docs/benchmarks.md` có bảng + biểu đồ cho ≥ 6 thí nghiệm; ngưỡng cảnh báo được chọn từ số đo.

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
