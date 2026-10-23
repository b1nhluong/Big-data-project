"""[TV2] Gold — khách hàng: tái dựng phiên, RFM, đặc trưng người dùng.

Bảng đầu ra (gold/*):
  sessions        phiên tự tái dựng theo khoảng lặng 30 phút (lag + tổng lũy kế)
  customer_rfm    Recency/Frequency/Monetary, điểm 1–5, nhãn phân khúc (Python UDF)
  user_features   đặc trưng hành vi mỗi user (dùng cho join lớn & ML)

Ghi chú mở rộng: ntile() trên cửa sổ KHÔNG partition sẽ dồn toàn bộ dữ liệu về
1 partition. Với hàng triệu user, mặc định dùng ngưỡng approxQuantile (song song);
bản ntile giữ lại để đối chiếu (--rfm-method ntile) — xem docs/lessons/02.
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from shoppulse.config import load_config
from shoppulse.io import save_report, timer, write_table
from shoppulse.spark import get_spark, setup_logging
from shoppulse.udfs import segment_label

log = logging.getLogger("shoppulse.gold_customers")


def rebuild_sessions(events: DataFrame, gap_minutes: int = 30) -> DataFrame:
    """Chuỗi biến đổi: lag -> cờ phiên mới -> sum lũy kế -> session_seq -> tổng hợp."""
    w = Window.partitionBy("user_key").orderBy("event_ts", "event_id")
    flagged = (events
               .withColumn("prev_ts", F.lag("event_ts").over(w))
               .withColumn("gap_s", F.col("event_ts").cast("long") - F.col("prev_ts").cast("long"))
               .withColumn("new_session",
                           F.when(F.col("prev_ts").isNull() | (F.col("gap_s") > gap_minutes * 60), 1)
                            .otherwise(0))
               .withColumn("session_seq", F.sum("new_session").over(
                   w.rowsBetween(Window.unboundedPreceding, 0))))
    return (flagged.groupBy("user_key", "session_seq").agg(
                F.min("event_ts").alias("session_start"),
                F.max("event_ts").alias("session_end"),
                F.count("*").alias("n_events"),
                F.count(F.when(F.col("event_type") == "view", 1)).alias("n_views"),
                F.count(F.when(F.col("event_type") == "cart", 1)).alias("n_carts"),
                F.count(F.when(F.col("event_type") == "purchase", 1)).alias("n_purchases"),
                F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)), 2).alias("revenue"),
                F.countDistinct("product_id").alias("n_products"),
                F.avg(F.when(F.col("new_session") == 0, F.col("gap_s"))).alias("avg_gap_s"),
                F.first("cat_l1", ignorenulls=True).alias("entry_cat_l1"))
            .withColumn("duration_s", F.col("session_end").cast("long") - F.col("session_start").cast("long"))
            .withColumn("converted", F.col("n_purchases") > 0)
            .withColumn("cart_abandoned", (F.col("n_carts") > 0) & (F.col("n_purchases") == 0))
            .withColumn("session_date", F.to_date("session_start"))
            .withColumn("session_key", F.concat_ws("#", "user_key", "session_seq")))


def rfm_base(events: DataFrame) -> DataFrame:
    ref = events.agg(F.max("event_ts")).first()[0]
    buys = events.filter(F.col("event_type") == "purchase")
    return (buys.groupBy("user_key").agg(
                F.max("event_ts").alias("last_purchase"),
                F.countDistinct("user_session").alias("frequency"),
                F.round(F.sum("price"), 2).alias("monetary"))
            .withColumn("recency_days",
                        F.datediff(F.lit(ref).cast("timestamp"), F.col("last_purchase"))))


def _score_by_quantiles(df: DataFrame, col: str, higher_is_better: bool) -> DataFrame:
    qs = df.approxQuantile(col, [0.2, 0.4, 0.6, 0.8], 0.01)
    expr = F.lit(5)
    for i, q in enumerate(reversed(qs)):          # 0.8, 0.6, 0.4, 0.2
        expr = F.when(F.col(col) <= q, 4 - i).otherwise(expr)
    score = expr if higher_is_better else (6 - expr)
    return df.withColumn(f"{col}_score", score.cast("int"))


def score_rfm(base: DataFrame, method: str = "quantile") -> DataFrame:
    if method == "ntile":
        # Cửa sổ toàn cục: đúng nhưng không mở rộng được (1 partition)
        scored = (base
                  .withColumn("r_score", F.ntile(5).over(Window.orderBy(F.desc("recency_days"))))
                  .withColumn("f_score", F.ntile(5).over(Window.orderBy("frequency")))
                  .withColumn("m_score", F.ntile(5).over(Window.orderBy("monetary"))))
    else:
        scored = _score_by_quantiles(base, "recency_days", higher_is_better=False)
        scored = _score_by_quantiles(scored, "frequency", higher_is_better=True)
        scored = _score_by_quantiles(scored, "monetary", higher_is_better=True)
        scored = (scored.withColumnRenamed("recency_days_score", "r_score")
                        .withColumnRenamed("frequency_score", "f_score")
                        .withColumnRenamed("monetary_score", "m_score"))
    return (scored.withColumn("rfm_code", F.concat_ws("", "r_score", "f_score", "m_score"))
                  .withColumn("segment", segment_label("r_score", "f_score", "m_score")))


def user_features(events: DataFrame, sessions: DataFrame) -> DataFrame:
    base = events.groupBy("user_key").agg(
        F.count("*").alias("u_events"),
        F.count(F.when(F.col("event_type") == "view", 1)).alias("u_views"),
        F.count(F.when(F.col("event_type") == "cart", 1)).alias("u_carts"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("u_purchases"),
        F.countDistinct("product_id").alias("u_products"),
        F.round(F.avg(F.when(F.col("event_type") == "view", F.col("price"))), 2).alias("u_avg_view_price"),
        F.countDistinct("event_date").alias("u_active_days"),
        F.min("event_ts").alias("u_first_seen"), F.max("event_ts").alias("u_last_seen"))
    # danh mục yêu thích = danh mục có nhiều sự kiện nhất (row_number trong mỗi user)
    w = Window.partitionBy("user_key").orderBy(F.desc("n"), "cat_l1")
    fav = (events.groupBy("user_key", "cat_l1").agg(F.count("*").alias("n"))
           .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
           .select("user_key", F.col("cat_l1").alias("u_fav_cat")))
    sess = sessions.groupBy("user_key").agg(
        F.count("*").alias("u_sessions"),
        F.round(F.avg("duration_s"), 1).alias("u_avg_session_s"),
        F.sum(F.col("cart_abandoned").cast("int")).alias("u_abandoned_carts"))
    return (base.join(fav, "user_key", "left").join(sess, "user_key", "left")
            .withColumn("u_conversion", F.round(F.col("u_purchases") / F.greatest(F.col("u_views"), F.lit(1)), 4)))


def run(rfm_method: str = "quantile") -> dict:
    cfg = load_config()
    spark = get_spark("gold-customers", cfg, hive=False)
    events = spark.read.parquet(cfg.lake("silver", "events")).cache()
    m: dict = {"rfm_method": rfm_method}
    with timer(m, "sessions_s"):
        write_table(rebuild_sessions(events, cfg.session_gap_minutes).repartition("session_date"),
                    cfg.lake("gold", "sessions"), partition_by=["session_date"])
    sessions = spark.read.parquet(cfg.lake("gold", "sessions"))
    m["sessions_rebuilt"] = sessions.count()
    m["sessions_source"] = events.select("user_session").distinct().count()
    m["abandoned_cart_rate"] = round(sessions.filter("n_carts > 0").agg(
        F.avg(F.col("cart_abandoned").cast("double"))).first()[0] or 0.0, 4)
    with timer(m, "rfm_s"):
        rfm = score_rfm(rfm_base(events), rfm_method)
        write_table(rfm, cfg.lake("gold", "customer_rfm"))
    m["segments"] = {r["segment"]: r["count"] for r in
                     spark.read.parquet(cfg.lake("gold", "customer_rfm")).groupBy("segment").count().collect()}
    with timer(m, "user_features_s"):
        write_table(user_features(events, sessions), cfg.lake("gold", "user_features"))
    events.unpersist()
    save_report(spark, cfg, "gold_customers", m)
    return m


def main() -> None:
    setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--rfm-method", choices=["quantile", "ntile"], default="quantile")
    run(ap.parse_args().rfm_method)


if __name__ == "__main__":
    main()
