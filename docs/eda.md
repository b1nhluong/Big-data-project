# EDA dữ liệu hành vi (mẫu 1%, 1,100,979 dòng)

> Sinh bởi `python -m batch.eda --fraction 0.01` trên mẫu 1% của dữ liệu thật REES46 tháng 10–11/2019 (109.950.743 sự kiện).

## Phân bố loại sự kiện

| event_type | số dòng | tỉ lệ |
|---|---|---|
| view | 1,044,774 | 94.89% |
| cart | 39,559 | 3.59% |
| purchase | 16,646 | 1.51% |

## Tỉ lệ thiếu (%)

| category_code | brand | price | user_session |
|---|---|---|---|
| 32.23 | 13.92 | 0.0 | 0.0 |

**Dòng trùng hoàn toàn (cùng event_id):** 28 (0.003%)

## Phân vị giá đơn mua

| p1 | p25 | p50 | p75 | p99 |
|---|---|---|---|---|
| 1.06 | 84.61 | 172.19 | 358.57 | 2574.07 |

Đơn mua giá 0: 0

## Sự kiện mỗi ngày

| min | avg | max |
|---|---|---|
| 11,357 | 18,049 | 65,102 |

## Số sự kiện mỗi phiên

| p50 | p90 | p99 | p99.9 |
|---|---|---|---|
| 1.0 | 1.0 | 2.0 | 43.0 |

Phiên dài nhất: 43 sự kiện (nghi vấn bot).

## Top danh mục cấp 1

| cat_l1 | events | purchases |
|---|---|---|
| electronics | 401965 | 9138 |
| unknown | 354847 | 4125 |
| appliances | 134619 | 1762 |
| computers | 64915 | 630 |
| apparel | 45778 | 229 |
| furniture | 33718 | 200 |
| auto | 22233 | 195 |
| construction | 18077 | 166 |
| kids | 13278 | 120 |
| accessories | 6343 | 37 |

**Skew:** 5 sản phẩm phổ biến nhất chiếm 3.90% tổng sự kiện — cần lưu ý khi join theo product_id.
