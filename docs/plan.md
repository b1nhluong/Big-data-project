# Kế hoạch 12 tuần (28/09 – 20/12/2026) — nhóm 5 người

Mỗi tuần kết thúc bằng một tag release trên GitHub và một báo cáo tuần trong `docs/weekly/week-NN.md`.
File báo cáo tuần đã có sẵn danh sách việc, file và commit gợi ý của từng người; nhóm điền phần kết quả thực tế trước khi gắn tag.
Bản đầy đủ có sơ đồ: [docs/ke-hoach/](ke-hoach/) (PDF) và tài liệu online của nhóm.

## Vai trò

| Vai | Sở hữu | Lessons learned |
|---|---|---|
| TV1 — Platform & Ingestion (nhóm trưởng kỹ thuật) | Cụm k3s, Kafka, PostgreSQL, Hive Metastore, Trino (triển khai), Airflow (triển khai), Superset (triển khai), replayer + bơm lỗi, MapReduce, bảo mật, CI | 01, 05, 10 |
| TV2 — Batch Layer | `shoppulse/` (thư viện chung), CSV → bronze, silver/gold, UDF/UDAF, join, tối ưu, DAG Airflow, đối soát, gom file nhỏ, scaling | 02, 04, 06 |
| TV3 — Speed Layer | Image Spark + Spark Operator, Structured Streaming, sink idempotent, sessionization, chấm điểm realtime | 03, 11 |
| TV4 — ML & Analytics | HDFS (triển khai), EDA, MLlib, GraphFrames, chuỗi thời gian, thống kê, dashboard Superset, tổng hợp số liệu | 08 |
| TV5 — Serving, Giám sát & Kiểm thử | Cassandra, Redis, DDL + view Lambda trên Trino, export gold → Cassandra, API FastAPI, Prometheus/Grafana, khung pytest, `docs/chaos.md`, ghép báo cáo | 07, 09 |

## Lịch tổng quan

| Tuần | Ngày | Mục tiêu | Tag | Scrum master |
|---|---|---|---|---|
| 1 | 28/9–4/10 | Khởi động, đề xuất, khung repo | `v0.1-proposal` | TV1 |
| 2 | 5/10–11/10 | Hạ tầng lõi trên Kubernetes | `v0.2-infra` | TV2 |
| 3 | 12/10–18/10 | Luồng dữ liệu đầu–cuối đầu tiên | `v0.3-pipeline` | TV3 |
| 4 | 19/10–25/10 | Batch layer v1 + Speed layer v1 | `v0.4-layers` | TV4 |
| 5 | 26/10–1/11 | Tối ưu batch, streaming nâng cao, bơm lỗi | `v0.5-optimize` | TV5 |
| 6 | 2/11–8/11 | Stateful streaming, đồ thị, dashboard — DEMO GIỮA KỲ | `v0.6-midterm` | TV1 |
| 7 | 9/11–15/11 | Serving layer Lambda + phân tích nâng cao | `v0.7-serving` | TV2 |
| 8 | 16/11–22/11 | Bảo mật, API, kiểm thử | `v0.8-secure` | TV3 |
| 9 | 23/11–29/11 | Thí nghiệm hiệu năng & mở rộng | `v0.9-bench` | TV4 |
| 10 | 30/11–6/12 | Chaos test, chứng minh chịu lỗi | `v0.10-chaos` | TV5 |
| 11 | 7/12–13/12 | Chạy toàn bộ dữ liệu, viết báo cáo | `v0.11-rc` | TV1 |
| 12 | 14/12–20/12 | Hoàn thiện & bảo vệ | `v1.0` | TV2 |

## Cặp review xoay vòng (lặp lại mỗi 4 tuần)

| Tuần | TV1 review | TV2 review | TV3 review | TV4 review | TV5 review |
|---|---|---|---|---|---|
| 1, 5, 9 | TV2 | TV3 | TV4 | TV5 | TV1 |
| 2, 6, 10 | TV3 | TV4 | TV5 | TV1 | TV2 |
| 3, 7, 11 | TV4 | TV5 | TV1 | TV2 | TV3 |
| 4, 8, 12 | TV5 | TV1 | TV2 | TV3 | TV4 |

## Khối lượng ước tính (giờ)

| Tuần | TV1 | TV2 | TV3 | TV4 | TV5 |
|---|---|---|---|---|---|
| 1 | 7 | 6 | 6 | 5 | 6 |
| 2 | 7 | 6 | 7 | 6 | 7 |
| 3 | 8 | 8 | 8 | 5 | 6 |
| 4 | 7 | 8 | 7 | 7 | 6 |
| 5 | 8 | 8 | 7 | 8 | 6 |
| 6 | 6 | 7 | 7 | 7 | 6 |
| 7 | 6 | 7 | 6 | 8 | 7 |
| 8 | 7 | 8 | 5 | 6 | 7 |
| 9 | 7 | 8 | 6 | 6 | 5 |
| 10 | 5 | 5 | 6 | 5 | 6 |
| 11 | 8 | 7 | 7 | 7 | 7 |
| 12 | 4 | 4 | 4 | 4 | 4 |
| **Tổng** | **80** | **82** | **76** | **74** | **73** |

Ngoài khối lượng trên, mỗi người dành khoảng 1 giờ/tuần để review PR, họp và viết báo cáo tuần.

## Quy trình mỗi tuần

1. **Thứ Hai**: họp 30 phút, tạo Milestone `Week NN`; mỗi dòng việc trong `docs/weekly/week-NN.md` thành một Issue có người nhận.
2. **Trong tuần**: mỗi Issue làm trên một nhánh `feat/<tv>-<việc>`. Commit theo Conventional Commits, commit nhỏ và nhiều lần, mở PR theo mẫu, cần một người review theo bảng trên và CI phải xanh.
3. **Thứ Năm**: kiểm tra nhanh tiến độ. Việc nào trễ thì ghi lý do vào báo cáo tuần và chuyển sang tuần sau.
4. **Chủ nhật**: scrum master của tuần điền báo cáo tuần, merge vào `main`, gắn tag và tạo GitHub Release kèm ảnh chụp hoặc video.

## Mốc kiểm tra

| Mốc | Tuần | Tiêu chí |
|---|---|---|
| Luồng đầu–cuối | 3 | Replayer → Kafka → HDFS bronze chạy trên K8s, test exactly-once xanh |
| Demo giữa kỳ | 6 | Demo 5 phút gồm KPI realtime, cảnh báo bỏ giỏ, bảng gold, DAG Airflow |
| Đủ checklist | 9 | Mọi mục trong bảng yêu cầu của giảng viên có code + số đo |
| Bảo vệ | 12 | Báo cáo PDF, slide, video demo; mỗi người trả lời được câu hỏi về phần người khác |
