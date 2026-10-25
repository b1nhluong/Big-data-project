# Tuần 01 (28/09 – 04/10/2026) — Khởi động, đề xuất, khung repo

**Tag release:** `v0.1-proposal` · **Hạn nộp:** 23:59 Chủ nhật 04/10 · **Scrum master:** TV1 · **Review:** TV1→TV2, TV2→TV3, TV3→TV4, TV4→TV5, TV5→TV1 · **Liên hệ slide:** Ch.1 (5V), Ch.2 (hệ sinh thái Hadoop)

**Giờ ước tính:** TV1 7 · TV2 6 · TV3 6 · TV4 5 · TV5 6

> Code tham chiếu nằm trong repo tham chiếu (`ref`). Cách lấy file, chạy kiểm tra rồi tự commit: xem [docs/ke-hoach/porting-guide.md](../ke-hoach/porting-guide.md).

## Kế hoạch

### TV1 — Platform & Ingestion (~7 giờ)

- [ ] Tạo repo, cấu trúc thư mục, bảo vệ nhánh main (PR + 1 review + CI xanh), CODEOWNERS, template PR/Issue (2h)  
      Sản phẩm: `.github/`, `README.md` (khung)  
      Commit gợi ý: `chore: init repo structure and branch protection`
- [ ] CI GitHub Actions: ruff + kubeconform (1h)  
      Sản phẩm: `.github/workflows/ci.yml`  
      Commit gợi ý: `ci: add ruff and kubeconform checks`
- [ ] Cài Tailscale + k3s thử trên 2 laptop, ghi lại lỗi gặp phải (3h)  
      Sản phẩm: `infra/k3s/install-server.sh`, `infra/k3s/join-agent.sh`  
      Commit gợi ý: `feat(infra): k3s server and agent install scripts`
- [ ] ADR-0002: Kubernetes thay YARN/Docker Compose (1h)  
      Sản phẩm: `docs/adr/0002-kubernetes.md`  
      Commit gợi ý: `docs(adr): 0002 kubernetes over yarn and compose`
- [ ] File đi kèm: `.gitignore`, `infra/k3s/kind-config.yaml`

### TV2 — Batch Layer (~6 giờ)

- [ ] Script tải REES46 + bộ sinh dữ liệu mẫu cùng định dạng (3h)  
      Sản phẩm: `scripts/download_rees46.sh`, `ingestion/generate_sample.py`  
      Commit gợi ý: `feat(ingestion): rees46 download script and sample generator`
- [ ] Khung package dùng chung: cấu hình theo profile, SparkSession, schema, event_id (3h)  
      Sản phẩm: `shoppulse/config.py`, `shoppulse/spark.py`, `shoppulse/schemas.py`, `conf/shoppulse.yaml`  
      Commit gợi ý: `feat(core): shared config, spark session and event schema`
- [ ] File đi kèm: `requirements.txt`, `requirements-dev.txt`, `pytest.ini`, `shoppulse/__init__.py`, `shoppulse/io.py`, `ingestion/__init__.py`

### TV3 — Speed Layer (~6 giờ)

- [ ] Đề xuất: bài toán, 8 câu hỏi, 5V có số liệu, phạm vi & giới hạn (3h)  
      Sản phẩm: `docs/report/bao-cao.md` (mục 1)  
      Commit gợi ý: `docs(report): project proposal`
- [ ] Sơ đồ kiến trúc Lambda v1 + ADR-0001 Lambda hay Kappa (3h)  
      Sản phẩm: `README.md`, `docs/adr/0001-lambda-vs-kappa.md`  
      Commit gợi ý: `docs(adr): 0001 lambda vs kappa`

### TV4 — ML & Analytics (~5 giờ)

- [ ] EDA trên mẫu 1% bằng PySpark: null, trùng, phân phối giá, phễu (4h)  
      Sản phẩm: `docs/eda.md`  
      Commit gợi ý: `docs(eda): null, duplicate and funnel stats on 1% sample`
- [ ] ADR-0005: CSV vs Avro vs Parquet vs ORC (1h)  
      Sản phẩm: `docs/adr/0005-file-formats.md`  
      Commit gợi ý: `docs(adr): 0005 file formats`
- [ ] File đi kèm: `batch/eda.py`

### TV5 — Serving, Giám sát & Kiểm thử (~6 giờ)

- [ ] Board GitHub Projects: 12 milestone, issue tuần 1–2 có người nhận (2h)  
      Sản phẩm: GitHub Projects  
      Commit gợi ý: (không có commit)
- [ ] ADR-0003 Cassandra vs MongoDB vs HBase, ADR-0004 HDFS vs MinIO (2h)  
      Sản phẩm: `docs/adr/0003-*.md`, `docs/adr/0004-*.md`  
      Commit gợi ý: `docs(adr): 0003 cassandra and 0004 hdfs`
- [ ] Bảng đối chiếu chương slide ↔ thành phần dự án (2h)  
      Sản phẩm: `README.md`  
      Commit gợi ý: `docs: map course chapters to components`
- [ ] File đi kèm: `docs/plan.md`, `docs/weekly/`, `docs/ke-hoach/`

## Định nghĩa hoàn thành của tuần

- [ ] `v0.1-proposal` có proposal, sơ đồ, 5 ADR, CI xanh; ảnh `kubectl get nodes` ≥ 2 node Ready.

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
