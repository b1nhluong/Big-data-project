# Mô hình dữ liệu Cassandra (serving layer)

Nguyên tắc **query-first**: xác định truy vấn trước, rồi mới thiết kế bảng sao cho mỗi truy vấn chỉ chạm **một partition** và không partition nào lớn vô hạn. DDL: [`serving/cassandra/schema.cql`](../serving/cassandra/schema.cql).

| Bảng | Truy vấn phục vụ | Partition key | Clustering | Kích thước partition tối đa | TTL |
|---|---|---|---|---|---|
| `kpi_minute` | KPI/phút của một danh mục trong ngày (Q1) | `(day, cat_l1)` | `window_start DESC` | 1.440 dòng/ngày | 30 ngày |
| `trending_by_window` | Lịch sử top-K (Q2) | `day` | `window_end DESC, rank` | 1.440 × 50 dòng | 7 ngày |
| `alerts_by_day` | Cảnh báo mới nhất theo loại (Q3, Q4) | `(day, alert_type)` | `event_ts DESC, session_id` | Vài chục nghìn | 7 ngày |
| `live_sessions` | Xác suất mua của một phiên (Q5) | `session_id` | — | 1 dòng | 1 ngày |
| `sessions_closed` | Phiên đã đóng trong ngày | `day` | `session_id` | ~100 nghìn* | 30 ngày |
| `recs_by_user` | Gợi ý cho một user (Q6) | `user_key` | `rank` | 10 dòng | — |
| `also_viewed_by_product` | "Khách cũng xem" (Q6) | `product_id` | `rank` | 10 dòng | — |
| `customer_profile` | Hồ sơ RFM + cụm (Q7) | `user_key` | — | 1 dòng | — |

\* Với dữ liệu thật (khoảng 2 triệu phiên/ngày), `sessions_closed` nên đổi partition key thành `(day, bucket)` với `bucket = hash(session_id) % 16` để tránh partition quá lớn (hot partition). Đây là việc của tuần 7.

## Tại sao không dùng khoá tự tăng hay truy vấn tuỳ ý?
Cassandra phân tán dữ liệu bằng **consistent hashing** trên partition key (slide 4.3): mỗi node giữ 16 vnode trên vòng token. Truy vấn không chỉ rõ partition key buộc phải quét mọi node (`ALLOW FILTERING`). Vì vậy mỗi truy vấn cần có một bảng riêng, và dữ liệu được nhân bản theo truy vấn (denormalize).

## Consistency (N = 3)
| Thao tác | Mức | R + W > N? | Lý do |
|---|---|---|---|
| Streaming ghi (W) | `LOCAL_QUORUM` = 2 | — | Chịu mất 1 node mà vẫn ghi được |
| Dashboard Grafana/Trino đọc | `ONE` | Không | Ưu tiên độ trễ; KPI chậm vài trăm ms vẫn chấp nhận được |
| Đọc cần chính xác (đối soát) | `LOCAL_QUORUM` = 2 | 2 + 2 > 3 ✔ | Luôn thấy bản ghi mới nhất |
| API gợi ý | `LOCAL_ONE` | Không | Gợi ý đổi theo ngày, độ trễ quan trọng hơn |

## Cơ chế Dynamo trong Cassandra (dùng khi demo)
| Slide 4.3 | Cassandra | Cách quan sát |
|---|---|---|
| Consistent hashing + virtual node | `num_tokens: 16` | `nodetool ring` |
| Preference list (N) | Replication factor 3 | `nodetool getendpoints shoppulse recs_by_user <key>` |
| Sloppy quorum + hinted handoff | Hint lưu ở coordinator khi replica chết | `nodetool listpendinghints` trong lúc chạy `tests/chaos/cassandra_cap.sh` |
| Anti-entropy bằng Merkle tree | `nodetool repair` | Log repair |
| Gossip | Gossip giữa các node | `nodetool gossipinfo` |
| Vector clock | Cassandra dùng **last-write-wins theo timestamp** (khác Dynamo) | Nêu trong báo cáo |
