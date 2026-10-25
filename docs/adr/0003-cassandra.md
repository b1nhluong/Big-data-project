# ADR-0003: Cassandra (+ Redis) làm serving layer

**Trạng thái:** Chấp nhận · **Người viết:** TV4 · **Tuần:** 1

## Bối cảnh
Serving layer nhận ghi liên tục từ streaming (hàng nghìn upsert/giây) và phục vụ truy vấn theo khoá với độ trễ thấp. Hệ thống cần chịu được mất một node.

## Lựa chọn
| | Cassandra | MongoDB | HBase | Redis |
|---|---|---|---|---|
| Mô hình (slide 4.1) | Column family | Document | Column family trên HDFS | Key-value trong RAM |
| Kiến trúc | Ngang hàng kiểu Dynamo, không có master | Replica set + sharding (có primary) | Master + RegionServer + ZooKeeper | Đơn node / cluster |
| Ghi | Rất nhanh (LSM, ghi ở mọi node) | Tốt | Tốt | Rất nhanh |
| Chọn C hay A (CAP) | Tuỳ chỉnh theo từng truy vấn | Thiên CP | CP | — |
| Chi phí vận hành trên laptop | 3 pod | 3 pod | Cần ZooKeeper + HMaster + RS | 1 pod |

## Quyết định
**Cassandra** cho bảng serving, vì ghi nhanh, không có điểm chết đơn lẻ, và minh hoạ trực tiếp slide 4.2–4.3 (consistent hashing, N/R/W, hinted handoff, Merkle repair). **Redis** cho top-K trending nhờ ZSET O(log n). Không dùng MongoDB vì dữ liệu serving là bảng phẳng truy vấn theo khoá, không cần document lồng nhau.
