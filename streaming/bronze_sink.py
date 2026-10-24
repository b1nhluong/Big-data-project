"""[TV3] Kafka -> HDFS bronze/events_stream (Parquet), exactly-once.

File sink của Structured Streaming ghi log _spark_metadata: chỉ các file của batch
đã commit mới "tồn tại" với người đọc; kết hợp checkpoint offset Kafka -> kill
driver giữa chừng rồi chạy lại vẫn không mất, không trùng (tests/chaos/verify_exactly_once.py).

    spark-submit streaming/bronze_sink.py --source kafka --trigger "1 minute"
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import functions as F

from shoppulse.config import load_config
from shoppulse.spark import get_spark, setup_logging
from streaming.common import add_common_args, apply_trigger, read_events, run_query
from streaming.metrics import install

log = logging.getLogger("shoppulse.bronze_sink")

COLUMNS = ["event_id", "event_time", "event_type", "product_id", "category_id", "category_code",
           "brand", "price", "user_id", "user_session", "event_ts", "event_date", "source",
           "kafka_partition", "kafka_offset", "ingest_ts"]


def build(spark, cfg, args):
    events = read_events(spark, cfg, args.source, args.format, args.max_per_trigger,
                         max_files_per_trigger=args.max_files_per_trigger,
                         group_id="shoppulse-bronze")
    out = events.withColumn("source", F.lit("stream")).select(*COLUMNS)
    writer = (out.writeStream.queryName("bronze_sink").format("parquet")
              .option("path", cfg.lake("bronze", "events_stream"))
              .option("checkpointLocation", cfg.checkpoint("bronze_sink"))
              .partitionBy("event_date").outputMode("append"))
    return apply_trigger(writer, args)


def main() -> None:
    setup_logging()
    args = add_common_args(argparse.ArgumentParser()).parse_args()
    cfg = load_config()
    spark = get_spark("bronze-sink", cfg, hive=False)
    install(spark, cfg.get("metrics_port"))
    run_query(spark, build(spark, cfg, args))


if __name__ == "__main__":
    main()
