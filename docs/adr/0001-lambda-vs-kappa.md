# ADR-0001: Kiến trúc Lambda thay vì Kappa

**Trạng thái:** Chấp nhận · **Người viết:** TV3 · **Tuần:** 1

## Bối cảnh
Hệ thống cần cả số liệu tức thời (KPI từng phút, cảnh báo bỏ giỏ) lẫn số liệu chính xác trên toàn bộ lịch sử (RFM, mô hình gợi ý, báo cáo doanh thu). Giảng viên cho phép chọn Lambda hoặc Kappa.

## Lựa chọn
| | Lambda | Kappa |
|---|---|---|
| Ý tưởng | Batch layer tính lại từ dữ liệu gốc; speed layer bù phần mới | Chỉ có stream; muốn tính lại thì phát lại log Kafka |
| Ưu | Sai sót của speed layer được batch sửa; ML và truy vấn lịch sử tự nhiên | Một mã nguồn, một hệ thống |
| Nhược | Hai đường xử lý, phải giữ logic nhất quán | Phát lại 285 triệu sự kiện rất tốn; retention Kafka phải dài; ML batch vẫn cần |

## Quyết định
Chọn **Lambda**, vì: (1) ALS, KMeans, đồ thị và time series bản chất là batch; (2) slide chương 9 trình bày Lambda; (3) có thể **đo** được giá trị của batch layer qua job đối soát.

## Hệ quả
- Bắt buộc dùng chung logic giữa hai lớp (`shoppulse/quality.py`, `schemas.py`, `ml/features.py`) — xem lesson 09.
- Kafka giữ 7 ngày nên vẫn đủ để phát lại kiểu Kappa khi speed layer cần khôi phục. Báo cáo có phần thảo luận so sánh.
