-- [TV3] Mô hình dữ liệu Cassandra theo nguyên tắc QUERY-FIRST: mỗi bảng phục vụ đúng
-- một truy vấn của dashboard/API; khoá phân vùng chọn để mỗi truy vấn chỉ chạm 1 partition
-- và không partition nào phình vô hạn (xem docs/cassandra-data-model.md).
--
-- Replication: NetworkTopologyStrategy, RF = 3 (N = 3 trong slide Dynamo).
-- Ghi LOCAL_QUORUM (W = 2) + đọc LOCAL_QUORUM (R = 2) -> R + W > N -> đọc luôn thấy bản mới nhất.
-- Dashboard realtime chấp nhận ONE để ưu tiên độ trễ/khả dụng (AP trong CAP).

CREATE KEYSPACE IF NOT EXISTS shoppulse
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}
  AND durable_writes = true;

USE shoppulse;

-- Q1: KPI từng phút của một danh mục trong một ngày (speed layer)
--     SELECT * FROM kpi_minute WHERE day = ? AND cat_l1 = ? AND window_start >= ?
CREATE TABLE IF NOT EXISTS kpi_minute (
  day date, cat_l1 text, window_start timestamp, window_end timestamp,
  views bigint, carts bigint, removes bigint, purchases bigint, revenue double,
  updated_at timestamp,
  PRIMARY KEY ((day, cat_l1), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 2592000      -- 30 ngày: dữ liệu cũ đã có ở batch layer
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};

-- Q2: top sản phẩm hot theo cửa sổ (lịch sử; bản mới nhất nằm ở Redis)
CREATE TABLE IF NOT EXISTS trending_by_window (
  day date, window_end timestamp, rank int, product_id bigint, score double, views bigint,
  window_start timestamp,
  PRIMARY KEY ((day), window_end, rank)
) WITH CLUSTERING ORDER BY (window_end DESC, rank ASC)
  AND default_time_to_live = 604800;

-- Q3: cảnh báo trong ngày theo loại (bỏ giỏ, bot) — mới nhất trước
CREATE TABLE IF NOT EXISTS alerts_by_day (
  day date, alert_type text, event_ts timestamp, session_id text, alert_id text,
  user_key text, detail text, n_views int, n_carts int,
  PRIMARY KEY ((day, alert_type), event_ts, session_id)
) WITH CLUSTERING ORDER BY (event_ts DESC, session_id ASC)
  AND default_time_to_live = 604800;

-- Q4: xác suất mua của một phiên đang mở (CSKH tra cứu theo session)
CREATE TABLE IF NOT EXISTS live_sessions (
  session_id text PRIMARY KEY, user_key text, purchase_prob double, n_views int, n_carts int,
  n_products int, duration_s double, entry_cat text, last_event_ts timestamp, updated_at timestamp
) WITH default_time_to_live = 86400;

CREATE TABLE IF NOT EXISTS sessions_closed (
  day date, session_id text, user_key text, n_events int, n_views int, n_carts int,
  n_purchases int, revenue double, duration_s double, entry_cat text, ended_at timestamp,
  PRIMARY KEY ((day), session_id)
) WITH default_time_to_live = 2592000;

-- Q5: gợi ý cho một user (batch layer: ALS) — 1 partition / user, 10 dòng
CREATE TABLE IF NOT EXISTS recs_by_user (
  user_key text, rank int, product_id bigint, score double,
  PRIMARY KEY ((user_key), rank)
);

-- Q6: "khách xem sản phẩm này cũng xem" (đồ thị sản phẩm)
CREATE TABLE IF NOT EXISTS also_viewed_by_product (
  product_id bigint, rank int, other_product_id bigint, weight bigint,
  PRIMARY KEY ((product_id), rank)
);

-- Q7: hồ sơ khách hàng (RFM + cụm KMeans)
CREATE TABLE IF NOT EXISTS customer_profile (
  user_key text PRIMARY KEY, segment text, cluster int, recency_days int, frequency bigint,
  monetary double
);

-- Phân quyền (bật PasswordAuthenticator + CassandraAuthorizer trong cassandra.yaml)
-- Mật khẩu thật lấy từ Secret, script infra/k8s/cassandra/init-schema-job.yaml thay vào.
CREATE ROLE IF NOT EXISTS shoppulse_writer WITH PASSWORD = '__WRITER_PASSWORD__' AND LOGIN = true;
CREATE ROLE IF NOT EXISTS dashboard_reader WITH PASSWORD = '__READER_PASSWORD__' AND LOGIN = true;
GRANT MODIFY ON KEYSPACE shoppulse TO shoppulse_writer;
GRANT SELECT ON KEYSPACE shoppulse TO shoppulse_writer;
GRANT SELECT ON KEYSPACE shoppulse TO dashboard_reader;
