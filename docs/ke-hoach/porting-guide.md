# Hướng dẫn: từ repo tham chiếu sang repo chính

Nhóm có hai repo:

| Repo | Vai trò | Ai ghi |
|---|---|---|
| `big_data_claude` (private) | **Repo tham chiếu.** Toàn bộ hệ thống đã viết sẵn, chạy được trên một máy. Chỉ để đọc, chạy thử, so sánh. | Không ai commit vào đây trong học kỳ |
| repo chính của nhóm | **Repo nộp cho thầy.** Mỗi tuần, mỗi người đưa phần việc của mình vào, chạy được trên cụm, rồi commit bằng tài khoản của chính mình. | Mỗi người, phần của mình |

Việc của từng người từng tuần: [`docs/weekly/week-NN.md`](../weekly/). File nào thuộc tuần nào, ai phụ trách: [`file-index.md`](file-index.md).

---

## 1. Chuẩn bị máy (mỗi người, một lần)

| Công cụ | Phiên bản | Ghi chú |
|---|---|---|
| Java | **17** | **Không dùng Java 21 trở lên**: Spark 3.5 + Arrow lỗi `DirectByteBuffer ... not available` ở mọi pandas UDF. macOS: `brew install openjdk@17`, rồi `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` |
| Python | **3.11** | PySpark 3.5 chưa hỗ trợ 3.13. macOS: `brew install python@3.11` |
| Git | bất kỳ | `git config --global user.name "Tên"` và `user.email` **trùng email GitHub**, nếu không commit sẽ không được tính vào biểu đồ Contributors |
| Docker / OrbStack, kubectl, helm ≥ 3.14 | | Cần từ tuần 2 |

Kiểm tra nhanh:

```bash
java -version        # phải là 17.x
python3.11 --version
```

## 2. Lấy repo tham chiếu để đọc và chạy thử (một lần)

```bash
git clone https://github.com/b1nhluong/big_data_claude.git shoppulse-ref
cd shoppulse-ref
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
make test              # 39 test phải xanh (~1 phút)
make local-pipeline    # chạy toàn bộ pipeline trên dữ liệu mẫu, không cần Kubernetes
```

Đọc phần của mình ở đây trước, chạy được ở đây trước, rồi mới đưa sang repo chính.

## 3. Nối repo tham chiếu vào repo chính (mỗi người, một lần)

```bash
git clone <URL repo chính> shoppulse
cd shoppulse
git remote add ref https://github.com/b1nhluong/big_data_claude.git
git fetch ref
```

## 4. Quy trình cho mỗi việc trong tuần

Ví dụ: TV3, tuần 4, việc "KPI từng phút".

```bash
git switch main && git pull
git switch -c feat/tv3-kpi-minute

# 1. Lấy đúng file của việc này từ repo tham chiếu (xem cột "Sản phẩm" trong docs/weekly/week-04.md)
git checkout ref/main -- streaming/kpi_minute.py

# 2. Đọc hiểu từng dòng. Sửa cho hợp cụm của nhóm (tên service, tài nguyên, tham số).
#    Phần nào chưa hiểu: hỏi trong nhóm hoặc hỏi Claude trước khi commit.

# 3. Kiểm tra (bảng mục 5)
make lint
python -m pytest tests/unit -q        # khi test tương ứng đã có trong repo chính

# 4. Commit bằng tài khoản của mình, message theo cột "Commit gợi ý"
git add streaming/kpi_minute.py
git commit -m "feat(streaming): per-minute kpis with 10-minute watermark"
git push -u origin feat/tv3-kpi-minute
# 5. Mở PR theo mẫu, người review theo lịch trong docs/plan.md, CI xanh mới merge
```

Nên tách việc lớn thành vài commit trong tuần (ví dụ: commit khung, rồi watermark, rồi sink) thay vì dồn một commit vào tối Chủ nhật.

**Ba quy tắc:**

1. **Chỉ commit phần của mình**, bằng tài khoản của mình, sau khi đã chạy được. Không commit hộ, không lùi ngày commit.
2. **Không chép file kết quả** (`benchmarks/results/*.json`) và **không chép số liệu** trong báo cáo, lessons learned, `docs/benchmarks.md`, `docs/chaos.md`. Số trong repo tham chiếu đo trên dữ liệu mẫu. Nhóm chạy lại trên dữ liệu thật, trên cụm của mình, rồi ghi số thật.
3. **Ghi rõ nguồn** theo quy định của môn: repo tham chiếu được dựng với sự hỗ trợ của công cụ AI. Nếu môn yêu cầu khai báo, ghi trong README và báo cáo.

## 5. Kiểm tra theo loại file

| Loại file | Lệnh kiểm tra trước khi commit |
|---|---|
| Python (`shoppulse/`, `batch/`, `streaming/`, `ml/`, `ingestion/`, `serving/`) | `make lint`, rồi test tương ứng (bảng mục 6) |
| Manifest K8s (`infra/k8s/`) | `kubectl apply --dry-run=server -f <file>`; sau khi apply: `kubectl get pods -n <ns>` Running |
| Dockerfile (`infra/docker/`, `serving/api/`) | `docker build -f <Dockerfile> .` thành công trên máy của mình |
| Helm values | `helm upgrade --install ... --dry-run` |
| SQL Trino / CQL | Chạy thật trong `trino` / `cqlsh` trên cụm, chụp kết quả cho báo cáo tuần |
| Tài liệu | Mở bản xem trước trên GitHub, kiểm tra link |

## 6. Code và test tương ứng

| Code | Test |
|---|---|
| `shoppulse/config.py`, `shoppulse/schemas.py`, `shoppulse/udfs.py` | `tests/unit/test_config.py`, `tests/unit/test_schemas_udfs.py` |
| `batch/silver.py`, `batch/gold_*.py` | `tests/unit/test_batch.py`, `tests/integration/test_batch_pipeline.py` |
| `ml/features.py`, `batch/reconciliation.py` | `tests/unit/test_features_reconcile.py` |
| `ingestion/chaos.py` | `tests/unit/test_chaos.py` |
| `mapreduce/` | `tests/unit/test_mapreduce.py` |
| `streaming/sessions.py` | `tests/unit/test_sessions_state.py` |
| `streaming/bronze_sink.py`, `streaming/sinks.py` | `tests/integration/test_streaming_exactly_once.py` |
| `serving/api/` | `tests/unit/test_api.py` |

Test của một module có thể vào repo chính muộn hơn chính module đó (theo kế hoạch tuần). Trong lúc chờ, chạy test đó trong repo tham chiếu (`shoppulse-ref`) sau khi đã chép bản sửa của mình sang.

## 7. Thứ tự phụ thuộc cần nhớ

| Tuần | Ai | Lưu ý |
|---|---|---|
| 1 | TV2 | `shoppulse/` (config, spark, schemas, io) và `requirements*.txt` phải vào trước. Mọi module khác import từ đây |
| 1 | TV1 | CI tuần 1 chỉ chạy `ruff` + `kubeconform`. Job `pytest` trong `.github/workflows/ci.yml` của repo tham chiếu để TV5 thêm vào ở tuần 3, khi đã có `tests/conftest.py` |
| 2 | TV4 | Build image `shoppulse/hadoop:3.4.1` (`infra/docker/hadoop/Dockerfile`) trước khi apply `infra/k8s/hdfs/hdfs.yaml` |
| 3 | TV1 | `ingestion/replayer.py` import `ingestion/chaos.py`, nên chép cả hai ở tuần 3. Tuần 5 TV1 bật các chế độ bơm lỗi trên cụm, đo và viết `tests/unit/test_chaos.py` |
| 3 | TV3 | `tests/integration/test_streaming_exactly_once.py` dùng `streaming/sinks.py`, nên chép `sinks.py` ở tuần 3. Tuần 4 TV3 hoàn thiện phần upsert Cassandra trong file đó |
| 3 | TV5 | `tests/conftest.py` là fixture chung. Từ tuần 3 ai thêm test cũng dựa vào nó |

## 8. Chạy trên laptop chip Apple (arm64)

- k3s không chạy trực tiếp trên macOS. Mỗi máy Mac tạo một máy ảo Ubuntu 22.04 (OrbStack hoặc Multipass), cấp 10–12 GB RAM, rồi cài k3s trong máy ảo theo `docs/runbook.md`.
- Đã kiểm tra trên Docker Hub (05/10/2026): Spark 3.5.7, Hive 4.0.1, Trino 476, Cassandra 4.1, Redis, PostgreSQL, Python 3.11 đều có bản arm64. Riêng image chính thức `apache/hadoop` **chỉ có amd64**, nên repo dùng `shoppulse/hadoop:3.4.1` tự build từ `infra/docker/hadoop/Dockerfile`.
- Các chart Helm còn lại (Strimzi, Spark Operator, Airflow, Prometheus, Superset) chưa kiểm tra từng image. Nếu pod nào báo `exec format error`, image đó thiếu bản arm64: ghi lại, đổi tag hoặc cho pod đó chạy trên node amd64 (`nodeSelector: kubernetes.io/arch: amd64`).
- `make images-k3s` phải chạy **trên từng node**: mỗi node tự build image đúng kiến trúc của mình rồi nạp vào containerd. Cụm trộn máy Mac (arm64) và máy Windows/Linux (amd64) vẫn chạy được theo cách này.

## 9. Tuần 1 đã qua

Tuần 1 (28/9–4/10) chưa có commit nào trong repo chính. Làm bù trong 2 ngày đầu tuần 2:

1. TV1 tạo repo chính, bảo vệ nhánh `main`, thêm `.github/` (không có job pytest), `.gitignore`, script k3s.
2. TV2 đưa `shoppulse/`, `conf/`, `requirements*.txt`, `pytest.ini`.
3. TV3, TV4, TV5 đưa ADR, proposal, EDA, kế hoạch (`docs/plan.md`, `docs/weekly/`).

Ghi rõ trong `docs/weekly/week-01.md` là việc tuần 1 được làm bù vào ngày nào.
