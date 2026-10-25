-- Sinh tự động bởi serving/trino/generate_ddl.py — KHÔNG sửa tay
CREATE SCHEMA IF NOT EXISTS hive.shoppulse WITH (location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake');

CREATE TABLE IF NOT EXISTS hive.shoppulse.events (
  event_id varchar,
  event_ts timestamp(3),
  event_hour integer,
  event_dow integer,
  event_type varchar,
  product_id bigint,
  category_id bigint,
  category_code varchar,
  cat_l1 varchar,
  cat_l2 varchar,
  cat_l3 varchar,
  brand varchar,
  price double,
  price_bucket varchar,
  user_key varchar,
  user_session varchar,
  promo_campaign_id integer,
  discount_pct integer,
  source varchar,
  event_date date
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/silver/events',
  format = 'PARQUET',
  partitioned_by = ARRAY['event_date']
);
CALL hive.system.sync_partition_metadata('shoppulse', 'events', 'FULL');

CREATE TABLE IF NOT EXISTS hive.shoppulse.dim_product (
  product_id bigint,
  category_id bigint,
  category_code varchar,
  cat_l1 varchar,
  cat_l2 varchar,
  cat_l3 varchar,
  brand varchar,
  last_price double,
  first_seen timestamp(3),
  last_seen timestamp(3),
  median_price double,
  n_events bigint
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/silver/dim_product',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.daily_kpis (
  cat_l1 varchar,
  views bigint,
  carts bigint,
  removes bigint,
  purchases bigint,
  revenue double,
  buyers bigint,
  orders bigint,
  visitors bigint,
  cart_rate double,
  conversion_rate double,
  aov double,
  event_date date
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/daily_kpis',
  format = 'PARQUET',
  partitioned_by = ARRAY['event_date']
);
CALL hive.system.sync_partition_metadata('shoppulse', 'daily_kpis', 'FULL');

CREATE TABLE IF NOT EXISTS hive.shoppulse.funnel_long (
  cat_l1 varchar,
  stage varchar,
  events bigint
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/funnel_long',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.hourly_heatmap (
  event_dow integer,
  0 bigint,
  1 bigint,
  2 bigint,
  3 bigint,
  4 bigint,
  5 bigint,
  6 bigint,
  7 bigint,
  8 bigint,
  9 bigint,
  10 bigint,
  11 bigint,
  12 bigint,
  13 bigint,
  14 bigint,
  15 bigint,
  16 bigint,
  17 bigint,
  18 bigint,
  19 bigint,
  20 bigint,
  21 bigint,
  22 bigint,
  23 bigint
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/hourly_heatmap',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.promo_effect (
  cat_l1 varchar,
  on_promo boolean,
  views bigint,
  carts bigint,
  removes bigint,
  purchases bigint,
  revenue double,
  buyers bigint,
  orders bigint,
  visitors bigint,
  cart_rate double,
  conversion_rate double,
  aov double
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/promo_effect',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.revenue_trend (
  event_date date,
  revenue double,
  purchases bigint,
  revenue_cumulative double,
  revenue_ma7 double,
  revenue_prev_day double,
  dod_growth double
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/revenue_trend',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.top_products (
  product_id bigint,
  cat_l1 varchar,
  brand varchar,
  views bigint,
  carts bigint,
  purchases bigint,
  revenue double,
  event_date date,
  rank integer,
  revenue_share double
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/top_products',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.brand_concentration (
  brand varchar,
  n_products bigint,
  revenue double,
  gini double,
  median_product_revenue double
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/brand_concentration',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.customer_rfm (
  user_key varchar,
  last_purchase timestamp(3),
  frequency bigint,
  monetary double,
  recency_days integer,
  r_score integer,
  f_score integer,
  m_score integer,
  rfm_code varchar,
  segment varchar
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/customer_rfm',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.customer_clusters (
  user_key varchar,
  recency_days integer,
  frequency bigint,
  monetary double,
  segment varchar,
  cluster integer
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/customer_clusters',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.sessions (
  user_key varchar,
  session_seq bigint,
  session_start timestamp(3),
  session_end timestamp(3),
  n_events bigint,
  n_views bigint,
  n_carts bigint,
  n_purchases bigint,
  revenue double,
  n_products bigint,
  avg_gap_s double,
  entry_cat_l1 varchar,
  duration_s bigint,
  converted boolean,
  cart_abandoned boolean,
  session_key varchar,
  session_date date
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/sessions',
  format = 'PARQUET',
  partitioned_by = ARRAY['session_date']
);
CALL hive.system.sync_partition_metadata('shoppulse', 'sessions', 'FULL');

CREATE TABLE IF NOT EXISTS hive.shoppulse.product_pagerank (
  id bigint,
  pagerank double,
  cat_l1 varchar,
  brand varchar
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/product_pagerank',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.revenue_timeseries (
  cat_l1 varchar,
  ds date,
  revenue double,
  purchases bigint,
  revenue_ma7 double,
  mu14 double,
  sd14 double,
  zscore double,
  is_anomaly boolean
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/revenue_timeseries',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.revenue_forecast (
  cat_l1 varchar,
  ds date,
  yhat double,
  kind varchar
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/revenue_forecast',
  format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.shoppulse.reconciliation (
  day date,
  cat_l1 varchar,
  b_views bigint,
  b_purchases bigint,
  b_revenue double,
  s_views bigint,
  s_purchases bigint,
  s_revenue double,
  diff_views bigint,
  diff_purchases bigint,
  abs_diff_revenue double,
  rel_diff_revenue double,
  checked_at timestamp(3)
) WITH (
  external_location = 'hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake/gold/reconciliation',
  format = 'PARQUET'
);
