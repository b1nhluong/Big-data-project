"""[TV2] Gold — sản phẩm: top-N theo danh mục, mức độ tập trung thương hiệu, cặp xem cùng phiên.

Bảng đầu ra (gold/*):
  product_daily        chỉ số từng sản phẩm mỗi ngày
  top_products         top-10 doanh thu mỗi (ngày, danh mục) — row_number()
  brand_concentration  Gini doanh thu theo sản phẩm của mỗi brand — pandas UDAF
  coview_pairs         cặp sản phẩm cùng xuất hiện trong một phiên (đầu vào đồ thị
                       sản phẩm và bài so sánh MapReduce vs Spark)
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from shoppulse.config import load_config
from shoppulse.io import save_report, timer, write_table
from shoppulse.spark import get_spark, setup_logging
from shoppulse.udfs import gini_udaf, median_udaf

log = logging.getLogger("shoppulse.gold_products")


def product_daily(events: DataFrame) -> DataFrame:
    return events.groupBy("event_date", "product_id", "cat_l1", "brand").agg(
        F.count(F.when(F.col("event_type") == "view", 1)).alias("views"),
        F.count(F.when(F.col("event_type") == "cart", 1)).alias("carts"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases"),
        F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)), 2).alias("revenue"))


def top_products(daily: DataFrame, n: int = 10) -> DataFrame:
    w = Window.partitionBy("event_date", "cat_l1").orderBy(F.desc("revenue"), F.desc("views"), "product_id")
    return (daily.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= n)
                 .withColumn("revenue_share",
                             F.round(F.col("revenue") / F.sum("revenue").over(
                                 Window.partitionBy("event_date", "cat_l1")), 4)))


def brand_concentration(daily: DataFrame) -> DataFrame:
    per_product = (daily.filter(F.col("brand").isNotNull())
                   .groupBy("brand", "product_id").agg(F.sum("revenue").alias("revenue")))
    # Spark không cho trộn pandas UDAF với hàm tổng hợp thường trong cùng một agg()
    builtin = per_product.groupBy("brand").agg(
        F.count("*").alias("n_products"), F.round(F.sum("revenue"), 2).alias("revenue"))
    custom = per_product.groupBy("brand").agg(
        gini_udaf("revenue").alias("gini"), median_udaf("revenue").alias("median_product_revenue"))
    return (builtin.join(custom, "brand")
            .withColumn("gini", F.round("gini", 4))
            .withColumn("median_product_revenue", F.round("median_product_revenue", 2))
            .orderBy(F.desc("revenue")))


def coview_pairs(events: DataFrame, max_products: int = 50, min_support: int = 2) -> DataFrame:
    """Sinh cặp (a<b) bằng hàm bậc cao trên mảng — tránh self-join bùng nổ."""
    per_session = (events.filter(F.col("event_type").isin("view", "cart"))
                   .groupBy("user_session").agg(F.array_sort(F.collect_set("product_id")).alias("p"))
                   .filter((F.size("p") >= 2) & (F.size("p") <= max_products)))
    pairs = per_session.select(F.explode(F.expr(
        "flatten(transform(sequence(0, size(p) - 2), i -> "
        "transform(slice(p, i + 2, size(p) - i - 1), x -> named_struct('a', p[i], 'b', x))))"
    )).alias("pair"))
    return (pairs.groupBy(F.col("pair.a").alias("product_a"), F.col("pair.b").alias("product_b"))
                 .agg(F.count("*").alias("sessions"))
                 .filter(F.col("sessions") >= min_support))


def run(min_support: int = 2) -> dict:
    cfg = load_config()
    spark = get_spark("gold-products", cfg, hive=False)
    events = spark.read.parquet(cfg.lake("silver", "events"))
    m: dict = {}
    with timer(m, "product_daily_s"):
        write_table(product_daily(events), cfg.lake("gold", "product_daily"), partition_by=["event_date"])
    daily = spark.read.parquet(cfg.lake("gold", "product_daily")).cache()
    with timer(m, "top_products_s"):
        write_table(top_products(daily), cfg.lake("gold", "top_products"))
    with timer(m, "brand_concentration_s"):
        write_table(brand_concentration(daily), cfg.lake("gold", "brand_concentration"))
    with timer(m, "coview_pairs_s"):
        write_table(coview_pairs(events, min_support=min_support), cfg.lake("gold", "coview_pairs"))
    m["coview_pairs"] = spark.read.parquet(cfg.lake("gold", "coview_pairs")).count()
    top_brand = spark.read.parquet(cfg.lake("gold", "brand_concentration")).first()
    m["top_brand"] = top_brand.asDict() if top_brand else None
    daily.unpersist()
    save_report(spark, cfg, "gold_products", m)
    return m


def main() -> None:
    setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-support", type=int, default=2)
    run(ap.parse_args().min_support)


if __name__ == "__main__":
    main()
