"""[TV2] Silver -> Gold: bảng KPI phục vụ dashboard (batch view của Lambda).

Bảng đầu ra (gold/*):
  daily_kpis       KPI theo ngày × danh mục (ROLLUP -> có dòng 'all')
  funnel_matrix    ma trận phễu danh mục × loại sự kiện (PIVOT)
  funnel_long      cùng dữ liệu ở dạng dài (UNPIVOT) cho Superset
  hourly_heatmap   số đơn theo thứ × giờ (PIVOT 24 cột)
  promo_effect     tỉ lệ chuyển đổi có/không khuyến mãi
  revenue_trend    doanh thu ngày + lũy kế + trung bình trượt 7 ngày (window)
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from shoppulse.config import load_config
from shoppulse.io import save_report, timer, write_table
from shoppulse.schemas import EVENT_TYPES
from shoppulse.spark import get_spark, setup_logging

log = logging.getLogger("shoppulse.gold_kpis")


def _is(t: str):
    return F.col("event_type") == t


def kpi_aggs() -> list:
    return [
        F.count(F.when(_is("view"), 1)).alias("views"),
        F.count(F.when(_is("cart"), 1)).alias("carts"),
        F.count(F.when(_is("remove_from_cart"), 1)).alias("removes"),
        F.count(F.when(_is("purchase"), 1)).alias("purchases"),
        F.round(F.sum(F.when(_is("purchase"), F.col("price")).otherwise(0.0)), 2).alias("revenue"),
        F.approx_count_distinct(F.when(_is("purchase"), F.col("user_key"))).alias("buyers"),
        F.approx_count_distinct(F.when(_is("purchase"), F.col("user_session"))).alias("orders"),
        F.approx_count_distinct("user_key").alias("visitors"),
    ]


def with_rates(df: DataFrame) -> DataFrame:
    return (df.withColumn("cart_rate", F.round(F.col("carts") / F.col("views"), 4))
              .withColumn("conversion_rate", F.round(F.col("purchases") / F.col("views"), 4))
              .withColumn("aov", F.round(F.col("revenue") / F.col("orders"), 2)))


def daily_kpis(events: DataFrame) -> DataFrame:
    agg = (events.rollup("event_date", "cat_l1").agg(*kpi_aggs())
           .filter(F.col("event_date").isNotNull())
           .withColumn("cat_l1", F.coalesce("cat_l1", F.lit("all"))))
    return with_rates(agg)


def funnel(events: DataFrame) -> tuple[DataFrame, DataFrame]:
    matrix = (events.groupBy("cat_l1").pivot("event_type", list(EVENT_TYPES)).count().na.fill(0)
              .withColumn("view_to_cart", F.round(F.col("cart") / F.col("view"), 4))
              .withColumn("cart_to_purchase", F.round(F.col("purchase") / F.col("cart"), 4))
              .withColumn("view_to_purchase", F.round(F.col("purchase") / F.col("view"), 4)))
    long = matrix.unpivot("cat_l1", list(EVENT_TYPES), "stage", "events")
    return matrix, long


def hourly_heatmap(events: DataFrame) -> DataFrame:
    return (events.filter(_is("purchase")).groupBy("event_dow")
            .pivot("event_hour", list(range(24))).count().na.fill(0).orderBy("event_dow"))


def promo_effect(events: DataFrame) -> DataFrame:
    return with_rates(events.withColumn("on_promo", F.col("promo_campaign_id").isNotNull())
                      .groupBy("cat_l1", "on_promo").agg(*kpi_aggs()))


def revenue_trend(daily: DataFrame) -> DataFrame:
    total = daily.filter(F.col("cat_l1") == "all").select("event_date", "revenue", "purchases")
    w_cum = Window.orderBy("event_date").rowsBetween(Window.unboundedPreceding, 0)
    w_7 = Window.orderBy("event_date").rowsBetween(-6, 0)
    w_prev = Window.orderBy("event_date")
    # Bảng nhỏ (1 dòng/ngày) nên window không partition là chấp nhận được
    return (total.withColumn("revenue_cumulative", F.round(F.sum("revenue").over(w_cum), 2))
                 .withColumn("revenue_ma7", F.round(F.avg("revenue").over(w_7), 2))
                 .withColumn("revenue_prev_day", F.lag("revenue").over(w_prev))
                 .withColumn("dod_growth", F.round(F.col("revenue") / F.col("revenue_prev_day") - 1, 4)))


def run() -> dict:
    cfg = load_config()
    spark = get_spark("gold-kpis", cfg, hive=False)
    events = spark.read.parquet(cfg.lake("silver", "events")).cache()
    m: dict = {"rows": events.count()}
    with timer(m, "daily_kpis_s"):
        daily = daily_kpis(events)
        write_table(daily, cfg.lake("gold", "daily_kpis"), partition_by=["event_date"])
    with timer(m, "funnel_s"):
        matrix, long = funnel(events)
        write_table(matrix, cfg.lake("gold", "funnel_matrix"))
        write_table(long, cfg.lake("gold", "funnel_long"))
    with timer(m, "heatmap_s"):
        write_table(hourly_heatmap(events), cfg.lake("gold", "hourly_heatmap"))
    with timer(m, "promo_s"):
        write_table(promo_effect(events), cfg.lake("gold", "promo_effect"))
    daily = spark.read.parquet(cfg.lake("gold", "daily_kpis"))
    with timer(m, "trend_s"):
        write_table(revenue_trend(daily), cfg.lake("gold", "revenue_trend"))
    tot = daily.filter("cat_l1 = 'all'").agg(F.sum("revenue").alias("r"), F.sum("purchases").alias("p"),
                                             F.sum("views").alias("v")).first()
    m.update({"total_revenue": tot["r"], "total_purchases": tot["p"], "total_views": tot["v"]})
    events.unpersist()
    save_report(spark, cfg, "gold_kpis", m)
    return m


def main() -> None:
    setup_logging()
    argparse.ArgumentParser().parse_args()
    run()


if __name__ == "__main__":
    main()
