# Lesson 08 — Scaling

**Người viết:** TV4

> **Mẫu của giảng viên:** Vấn đề (bối cảnh, thách thức, tác động) → Các giải pháp đã thử và đánh đổi →
> Giải pháp cuối (cách hiện thực, số đo) → Bài học và khuyến nghị. Ô "(điền)" là số đo trên cụm ở tuần 9–10.

## 1. Vấn đề
Hai câu hỏi cần trả lời: (1) thêm executor có làm job nhanh tương ứng không; (2) mô hình có còn tốt khi dữ liệu lớn và thưa hơn không. Ở lần chạy đầu, ALS **thua** baseline "phổ biến nhất" (NDCG@10 0,064 so với 0,142).

## 2. Phân tích
- Dữ liệu mẫu ban đầu sinh ngẫu nhiên danh mục cho mỗi phiên, nên **không có tín hiệu cá nhân hoá**. Trong trường hợp đó, "phổ biến nhất" chính là mô hình sinh dữ liệu và luôn thắng. Sau khi bổ sung sở thích danh mục cho từng user vào bộ sinh dữ liệu, ALS chỉ gợi ý sản phẩm mới đạt NDCG@10 **0,162**, cao hơn baseline **0,137**.
- Baseline phải được đánh giá trên **cùng tập user** với ALS (ALS dùng `coldStartStrategy=drop`).

## 3. Thí nghiệm mở rộng (cụm — điền ở tuần 9)
| Strong scaling (7 tháng dữ liệu) | 1 executor | 2 | 4 |
|---|---|---|---|
| silver (s) | | | |
| gold_customers (s) | | | |
| ALS train (s) | | | |

| Weak scaling (executor tỉ lệ với dữ liệu) | 1 tháng / 1 exec | 2 / 2 | 4 / 4 |
|---|---|---|---|
| Thời gian (s) | | | |

| API (Locust) | 50 user | 200 user | 500 user |
|---|---|---|---|
| p95 (ms) / số pod HPA | | | |

## 4. Bài học
Luôn có baseline. Một mô hình phức tạp mà thua baseline đơn giản cho thấy có vấn đề ở dữ liệu hoặc ở cách đánh giá, không nên bỏ qua. Mở rộng ngang chỉ hiệu quả tới khi chi phí shuffle và mạng giữa các laptop lớn hơn phần tính toán được chia nhỏ.
