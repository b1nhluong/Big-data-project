# Tuần 05 (26/10 – 01/11/2026) — Tối ưu batch, streaming nâng cao, bơm lỗi

**Tag release:** `v0.5-optimize` · **Hạn nộp:** 23:59 Chủ nhật 01/11 · **Scrum master:** TV5 · **Review:** TV1→TV2, TV2→TV3, TV3→TV4, TV4→TV5, TV5→TV1 · **Liên hệ slide:** Ch.6 (MapReduce, combiner), Ch.7 (cache, join), Ch.8 (sliding window)

**Giờ ước tính:** TV1 8 · TV2 8 · TV3 7 · TV4 8 · TV5 6

> Code tham chiếu nằm trong repo tham chiếu (`ref`). Cách lấy file, chạy kiểm tra rồi tự commit: xem [docs/ke-hoach/porting-guide.md](../ke-hoach/porting-guide.md).

## Kế hoạch

### TV1 — Platform & Ingestion (~8 giờ)

- [ ] Bơm lỗi: trùng 1%, trễ ≤ 15′ 2%, sai thứ tự, flash sale x10 (3h)  
      Sản phẩm: `ingestion/chaos.py`, `tests/unit/test_chaos.py`  
      Commit gợi ý: `feat(ingestion): fault injection modes for replayer`
- [ ] MapReduce 2 job (Hadoop Streaming, có combiner) đếm cặp sản phẩm; so với Spark (5h)  
      Sản phẩm: `mapreduce/`, `tests/unit/test_mapreduce.py`, `benchmarks/results/mapreduce_vs_spark.json`  
      Commit gợi ý: `feat(mapreduce): co-view pair count with combiner`
- [ ] File đi kèm: `infra/docker/hadoop-python/Dockerfile`, `infra/k8s/jobs/mapreduce-coview.yaml`

### TV2 — Batch Layer (~8 giờ)

- [ ] Bộ thí nghiệm tối ưu: sort-merge vs broadcast vs bucketed join, partition pruning, cache, Python UDF vs pandas UDF, salting, AQE; lưu plan (6h)  
      Sản phẩm: `batch/optimization.py`, `docs/plans/`  
      Commit gợi ý: `perf(batch): join, pruning, cache and skew experiments`
- [ ] Nháp lessons 02 và 06 từ kết quả (2h)  
      Sản phẩm: `docs/lessons/02-spark-processing.md`, `docs/lessons/06-performance.md`  
      Commit gợi ý: `docs(lessons): draft 02 spark and 06 performance`
- [ ] File đi kèm: `batch/rdd_vs_dataframe.py`

### TV3 — Speed Layer (~7 giờ)

- [ ] Trending cửa sổ trượt 10′/1′ → Redis ZSET (thay nguyên tử) (4h)  
      Sản phẩm: `streaming/trending.py`  
      Commit gợi ý: `feat(streaming): sliding-window trending to redis zset`
- [ ] Đo % sự kiện trễ bị watermark loại khi bơm lỗi (3h)  
      Sản phẩm: `streaming/metrics.py`, `docs/lessons/03-stream-processing.md`  
      Commit gợi ý: `feat(streaming): track late events dropped by watermark`
- [ ] File đi kèm: `infra/k8s/spark/apps/stream-trending.yaml`

### TV4 — ML & Analytics (~8 giờ)

- [ ] Đặc trưng phiên chống rò rỉ nhãn (dùng chung batch + stream) (3h)  
      Sản phẩm: `ml/features.py`  
      Commit gợi ý: `feat(ml): leakage-safe session features`
- [ ] Pipeline GBT vs Logistic Regression, trọng số lớp, chia theo thời gian, AUC (5h)  
      Sản phẩm: `ml/purchase_propensity.py`, `benchmarks/results/ml_propensity.json`  
      Commit gợi ý: `feat(ml): purchase propensity pipeline, gbt vs logistic regression`

### TV5 — Serving, Giám sát & Kiểm thử (~6 giờ)

- [ ] kube-prometheus-stack, PodMonitor (Spark, Kafka, API), Kafka exporter, luật cảnh báo (6h)  
      Sản phẩm: `infra/k8s/monitoring/`  
      Commit gợi ý: `feat(monitoring): prometheus stack, podmonitors and alert rules`

## Định nghĩa hoàn thành của tuần

- [ ] có `optimization.json` và `docs/plans/*.txt`; `mapreduce_vs_spark.json` 0 cặp lệch; `ZREVRANGE trending:latest 0 9` có dữ liệu; có `ml_propensity.json`; Prometheus thu được metric Spark và Kafka.

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
