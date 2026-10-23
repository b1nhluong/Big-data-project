"""[TV2] Bronze -> Silver: làm sạch, kiểm tra chất lượng, khử trùng, ẩn danh, làm giàu.

Đầu vào : bronze/events_batch (lịch sử) ∪ bronze/events_stream (từ speed layer)
Đầu ra  : silver/events            (partition event_date)
          silver/events_quarantine (dòng lỗi + lý do)
          silver/dim_product       (thuộc tính mới nhất của sản phẩm)
          silver/dim_user          (user_key -> user_idx số nguyên cho ALS)

Kỹ thuật Spark thể hiện: pandas UDF nghiệp vụ, broadcast join, window function
(row_number), RDD zipWithIndex, dynamic partition overwrite, partition pruning.

    spark-submit batch/silver.py                   # toàn bộ
    spark-submit batch/silver.py --dates 2019-10-05 # chỉ một ngày (Airflow)
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType, StringType

from shoppulse.config import Config, load_config
from shoppulse.io import path_exists, save_report, timer, write_table
from shoppulse.quality import dq_reason
from shoppulse.schemas import parse_event_time
from shoppulse.spark import get_spark, setup_logging
from shoppulse.udfs import price_bucket, split_category

log = logging.getLogger("shoppulse.silver")

SILVER_COLUMNS = [
    "event_id", "event_ts", "event_date", "event_hour", "event_dow", "event_type",
    "product_id", "category_id", "category_code", "cat_l1", "cat_l2", "cat_l3", "brand",
    "price", "price_bucket", "user_key", "user_session", "promo_campaign_id",
    "discount_pct", "source",
]


def read_bronze(spark: SparkSession, cfg: Config, dates: list[str] | None) -> DataFrame:
    frames = []
    for name in ("events_batch", "events_stream"):
        path = cfg.lake("bronze", name)
        if path_exists(spark, path):
            frames.append(spark.read.parquet(path))
    if not frames:
        raise FileNotFoundError("Chưa có dữ liệu bronze — chạy ingestion/csv_to_parquet.py trước")
    common = ["event_id", "event_time", "event_type", "product_id", "category_id",
              "category_code", "brand", "price", "user_id", "user_session", "event_ts",
              "event_date", "source"]
    df = frames[0].select(*common)
    for other in frames[1:]:
        df = df.unionByName(other.select(*common))
    if dates:
        # partition pruning: Spark chỉ đọc thư mục event_date=... cần thiết
        df = df.filter(F.col("event_date").isin(dates))
    return df


def validate(df: DataFrame) -> DataFrame:
    """Gắn cột dq_reason (null = hợp lệ) — luật dùng chung với streaming (shoppulse/quality.py)."""
    return df.withColumn("dq_reason", dq_reason())


def promotions_by_hour(spark: SparkSession, cfg: Config) -> DataFrame | None:
    """Trải lịch khuyến mãi thành (cat_l1, giờ) -> mức giảm lớn nhất.

    Biến điều kiện join theo khoảng (range join) thành equi-join nhỏ để broadcast.
    """
    path = cfg.lake("silver", "dim_promotion")
    if not path_exists(spark, path):
        log.warning("Không có dim_promotion — bỏ qua làm giàu khuyến mãi")
        return None
    promo = spark.read.parquet(path)
    cats = [r["cat_l1"] for r in promo.select("cat_l1").distinct().collect() if r["cat_l1"] != "all"]
    hours = promo.withColumn(
        "promo_hour",
        F.explode(F.sequence(F.date_trunc("hour", "start_ts"),
                             F.date_trunc("hour", F.col("end_ts") - F.expr("INTERVAL 1 SECOND")),
                             F.expr("INTERVAL 1 HOUR"))))
    # 'all' = áp dụng cho mọi danh mục
    hours = hours.withColumn(
        "cat_l1", F.when(F.col("cat_l1") == "all", F.array(*[F.lit(c) for c in cats + ["unknown"]]))
                   .otherwise(F.array("cat_l1"))).withColumn("cat_l1", F.explode("cat_l1"))
    w = Window.partitionBy("cat_l1", "promo_hour").orderBy(F.desc("discount_pct"), "campaign_id")
    return (hours.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
            .select("cat_l1", "promo_hour", F.col("campaign_id").alias("promo_campaign_id"),
                    "discount_pct"))


def enrich(df: DataFrame, salt: str, promo: DataFrame | None) -> DataFrame:
    df = (df
          .withColumn("event_type", F.lower(F.trim("event_type")))
          .withColumn("brand", F.lower(F.trim("brand")))
          .withColumn("cat", split_category(F.col("category_code")))
          .withColumn("cat_l1", F.col("cat.l1")).withColumn("cat_l2", F.col("cat.l2"))
          .withColumn("cat_l3", F.col("cat.l3")).drop("cat")
          .withColumn("price_bucket", price_bucket(F.col("price")))
          # PII: user_id gốc bị thay bằng SHA-256(salt || user_id) — không đảo ngược được
          .withColumn("user_key", F.sha2(F.concat(F.lit(salt), F.col("user_id").cast("string")), 256))
          .withColumn("event_hour", F.hour("event_ts"))
          .withColumn("event_dow", F.dayofweek("event_ts")))
    if promo is not None:
        df = (df.withColumn("promo_hour", F.date_trunc("hour", "event_ts"))
                .join(F.broadcast(promo), ["cat_l1", "promo_hour"], "left").drop("promo_hour"))
    else:
        df = (df.withColumn("promo_campaign_id", F.lit(None).cast("int"))
                .withColumn("discount_pct", F.lit(None).cast("int")))
    return df.select(*SILVER_COLUMNS)


def build_dim_product(events: DataFrame) -> DataFrame:
    w = Window.partitionBy("product_id").orderBy(F.desc("event_ts"))
    latest = (events.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
              .select("product_id", "category_id", "category_code", "cat_l1", "cat_l2",
                      "cat_l3", "brand", F.col("price").alias("last_price")))
    stats = events.groupBy("product_id").agg(
        F.min("event_ts").alias("first_seen"), F.max("event_ts").alias("last_seen"),
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.count("*").alias("n_events"))
    return latest.join(stats, "product_id")


def build_dim_user(spark: SparkSession, events: DataFrame) -> DataFrame:
    """ALS cần id số nguyên -> đánh số dày đặc bằng RDD.zipWithIndex (không cần 1 partition)."""
    users = events.groupBy("user_key").agg(F.min("event_ts").alias("first_seen"))
    indexed = users.rdd.zipWithIndex().map(lambda r: (r[0]["user_key"], r[0]["first_seen"], int(r[1])))
    schema = StructType([StructField("user_key", StringType()),
                         StructField("first_seen", users.schema["first_seen"].dataType),
                         StructField("user_idx", LongType())])
    return spark.createDataFrame(indexed, schema)


def run(dates: list[str] | None = None) -> dict:
    cfg = load_config()
    spark = get_spark("silver", cfg, hive=False)
    m: dict = {"dates": dates or "all"}
    bronze = read_bronze(spark, cfg, dates)
    bronze = bronze.withColumn("event_ts", F.coalesce("event_ts", parse_event_time()))
    checked = validate(bronze).cache()
    m["rows_in"] = checked.count()
    m["dq_counts"] = {r["dq_reason"] or "valid": r["count"]
                      for r in checked.groupBy("dq_reason").count().collect()}

    bad = checked.filter(F.col("dq_reason").isNotNull())
    write_table(bad.withColumn("event_date", F.coalesce("event_date", F.lit("1970-01-01").cast("date"))),
                cfg.lake("silver", "events_quarantine"), partition_by=["event_date"])

    good = checked.filter(F.col("dq_reason").isNull()).drop("dq_reason")
    deduped = good.dropDuplicates(["event_id"])
    promo = promotions_by_hour(spark, cfg)
    silver = enrich(deduped, cfg.pii_salt, promo)
    with timer(m, "write_seconds"):
        write_table(silver.repartition("event_date"), cfg.lake("silver", "events"),
                    partition_by=["event_date"])
    events = spark.read.parquet(cfg.lake("silver", "events"))
    m["rows_out"] = events.count()
    m["duplicates_removed"] = m["dq_counts"].get("valid", 0) - m["rows_out"] if not dates else None
    m["null_brand_pct"] = round(events.filter(F.col("brand").isNull()).count() / max(m["rows_out"], 1) * 100, 2)
    m["unknown_category_pct"] = round(events.filter(F.col("cat_l1") == "unknown").count() / max(m["rows_out"], 1) * 100, 2)
    m["promo_event_pct"] = round(events.filter(F.col("promo_campaign_id").isNotNull()).count() / max(m["rows_out"], 1) * 100, 2)

    with timer(m, "dims_seconds"):
        write_table(build_dim_product(events), cfg.lake("silver", "dim_product"))
        write_table(build_dim_user(spark, events), cfg.lake("silver", "dim_user"))
    checked.unpersist()
    save_report(spark, cfg, "silver_dq", m)
    return m


def main() -> None:
    setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default=None, help="danh sách ngày, cách nhau bởi dấu phẩy")
    args = ap.parse_args()
    run(args.dates.split(",") if args.dates else None)


if __name__ == "__main__":
    main()
