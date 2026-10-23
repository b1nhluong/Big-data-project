# ADR-0002: Kubernetes (k3s nhiều node) thay cho YARN hoặc Docker Compose

**Trạng thái:** Chấp nhận · **Người viết:** TV1 · **Tuần:** 1

## Bối cảnh
Giảng viên yêu cầu triển khai trên Kubernetes hoặc cloud; Docker Compose không được khuyến khích. Slide chương 2 dùng YARN làm bộ quản lý tài nguyên. Nhóm có 4 laptop, mỗi máy 8–16 GB RAM.

## Lựa chọn
| Phương án | Nhận xét |
|---|---|
| YARN (Hadoop thuần) | Đúng slide, nhưng chỉ quản lý job Hadoop/Spark; Kafka, Cassandra, Trino, API vẫn cần công cụ khác |
| Docker Compose | Đơn giản, nhưng một máy, không có tự phục hồi, bị giảng viên trừ điểm |
| kind (K8s trong Docker, 1 máy) | Dễ dựng, nhưng mọi "node" chung RAM của một máy, chaos test không thật |
| **k3s trên 2–3 laptop qua Tailscale** | Cụm thật, rút mạng một máy là sự cố thật; k3s nhẹ (khoảng 500 MB) |

## Quyết định
Chọn **k3s nhiều node**, dự phòng bằng kind (`infra/k3s/kind-config.yaml`). Spark chạy trên K8s qua Spark Operator: K8s đóng vai trò của YARN (cấp phát CPU/RAM cho executor) và đồng thời quản lý mọi dịch vụ khác.

## Hệ quả
- MapReduce không có YARN nên chạy bằng LocalJobRunner trong một pod, đọc/ghi HDFS thật (`infra/k8s/jobs/mapreduce-coview.yaml`).
- Cần Tailscale ổn định khi demo → có video dự phòng.
