# EDA dữ liệu hành vi (mẫu 100%, 300,900 dòng)

> Sinh bởi `python -m batch.eda`. Chạy lại trên dữ liệu thật để cập nhật.

## Phân bố loại sự kiện

| event_type | số dòng | tỉ lệ |
|---|---|---|
| view | 266,294 | 88.50% |
| cart | 22,391 | 7.44% |
| purchase | 10,436 | 3.47% |
| remove_from_cart | 1,779 | 0.59% |

## Tỉ lệ thiếu (%)

| category_code | brand | price | user_session |
|---|---|---|---|
| 31.94 | 17.36 | 0.0 | 0.0 |

**Dòng trùng hoàn toàn (cùng event_id):** 1,262 (0.419%)

## Phân vị giá đơn mua

| p1 | p25 | p50 | p75 | p99 |
|---|---|---|---|---|
| 0.0 | 36.25 | 72.94 | 166.13 | 3906.54 |

Đơn mua giá 0: 18

## Sự kiện mỗi ngày

| min | avg | max |
|---|---|---|
| 49 | 10,376 | 27,354 |

## Số sự kiện mỗi phiên

| p50 | p90 | p99 | p99.9 |
|---|---|---|---|
| 5.0 | 16.0 | 31.0 | 597.0 |

Phiên dài nhất: 597 sự kiện (nghi vấn bot).

## Top danh mục cấp 1

| cat_l1 | events | purchases |
|---|---|---|
| unknown | 96112 | 3230 |
| electronics | 46199 | 1444 |
| appliances | 24002 | 930 |
| computers | 20537 | 751 |
| kids | 19921 | 589 |
| apparel | 18641 | 749 |
| sport | 14868 | 472 |
| furniture | 12431 | 502 |
| accessories | 9848 | 305 |
| construction | 9803 | 356 |

**Skew:** 5 sản phẩm phổ biến nhất chiếm 12.28% tổng sự kiện — cần lưu ý khi join theo product_id.
