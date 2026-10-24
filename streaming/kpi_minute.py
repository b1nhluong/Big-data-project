"""[TV3] KPI từng phút theo danh mục (speed layer của Lambda).

  * withWatermark 10 phút + dropDuplicatesWithinWatermark(event_id): khử trùng sự kiện
    gửi lại mà state không phình vô hạn
  * stream-static join với silver/dim_product (broadcast) để chuẩn hoá danh mục
  * cửa sổ tumbling 1 phút theo event-time, output mode UPDATE
  * foreachBatch -> sink idempotent (Cassandra upsert theo (day, cat_l1, window_start))

    spark-submit streaming/kpi_minute.py --source kafka --trigger "10 seconds"
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shoppulse.quality import is_valid
from shoppulse.config import Config, load_config
from shoppulse.io import path_exists
from shoppulse.spark import get_spark, setup_logging
from streaming.common import add_common_args, apply_trigger, read_events, run_query
from streaming.metrics import install
from streaming.sinks import make_sink

log = logging.getLogger("shoppulse.kpi_minute")


def category_lookup(spark, cfg: Config) -> DataFrame | None:
    path = cfg.lake("silver", "dim_product")
    if not path_exists(spark, path):
        return None
    return spark.read.parquet(path).select("product_id", F.col("cat_l1").alias("dim_cat_l1"))


def transform(events: DataFrame, cfg: Config, dim: DataFrame | None) -> DataFrame:
    deduped = (events.withWatermark("event_ts", cfg.watermark_delay)
                     .dropDuplicatesWithinWatermark(["event_id"])
            .filter(is_valid()))
    fallback = F.coalesce(F.split(F.lower(F.col("category_code")), r"\.").getItem(0), F.lit("unknown"))
    if dim is not None:
        enriched = deduped.join(F.broadcast(dim), "product_id", "left") \
                          .withColumn("cat_l1", F.coalesce("dim_cat_l1", fallback))
    else:
        enriched = deduped.withColumn("cat_l1", fallback)
    is_ = lambda t: F.col("event_type") == t  # noqa: E731
    return (enriched.groupBy(F.window("event_ts", cfg.kpi_window).alias("w"), "cat_l1").agg(
                F.count(F.when(is_("view"), 1)).alias("views"),
                F.count(F.when(is_("cart"), 1)).alias("carts"),
                F.count(F.when(is_("remove_from_cart"), 1)).alias("removes"),
                F.count(F.when(is_("purchase"), 1)).alias("purchases"),
                F.round(F.sum(F.when(is_("purchase"), F.col("price")).otherwise(0.0)), 2).alias("revenue"))
            .select(F.to_date(F.col("w.start")).alias("day"), "cat_l1",
                    F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                    "views", "carts", "removes", "purchases", "revenue",
                    F.current_timestamp().alias("updated_at")))


def build(spark, cfg, args):
    events = read_events(spark, cfg, args.source, args.format, args.max_per_trigger,
                         max_files_per_trigger=args.max_files_per_trigger,
                         group_id="shoppulse-kpi")
    result = transform(events, cfg, category_lookup(spark, cfg))
    sink = make_sink(cfg, "kpi_minute")
    writer = (result.writeStream.queryName("kpi_minute").outputMode("update")
              .option("checkpointLocation", cfg.checkpoint("kpi_minute"))
              .foreachBatch(sink.write))
    return apply_trigger(writer, args)


def main() -> None:
    setup_logging()
    args = add_common_args(argparse.ArgumentParser()).parse_args()
    cfg = load_config()
    spark = get_spark("kpi-minute", cfg, hive=False)
    install(spark, cfg.get("metrics_port"))
    run_query(spark, build(spark, cfg, args))


if __name__ == "__main__":
    main()
