"""[TV3] Nguồn dữ liệu streaming dùng chung: Kafka (cụm) hoặc thư mục JSON (local/test)."""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shoppulse.config import REPO_ROOT, Config
from shoppulse.schemas import EVENT_SCHEMA, parse_event_time

AVSC_PATH = REPO_ROOT / "ingestion" / "avro" / "event.avsc"


def read_events(spark: SparkSession, cfg: Config, source: str | None = None, fmt: str = "json",
                max_per_trigger: int = 100_000, starting_offsets: str = "earliest",
                group_id: str | None = None, max_files_per_trigger: int = 4) -> DataFrame:
    source = source or cfg.get("source_mode", "kafka")
    if source == "kafka":
        reader = (spark.readStream.format("kafka").options(**cfg.kafka_options())
                  .option("subscribe", cfg.topic_events)
                  .option("startingOffsets", starting_offsets)
                  .option("maxOffsetsPerTrigger", max_per_trigger)
                  # Không im lặng bỏ qua khi offset bị xoá do retention: phải biết là mất dữ liệu
                  .option("failOnDataLoss", "true"))
        if group_id:
            reader = reader.option("kafka.group.id", group_id)   # để Kafka exporter đo consumer lag
        raw = reader.load()
        if fmt == "avro":
            from pyspark.sql.avro.functions import from_avro
            parsed = raw.select(from_avro("value", AVSC_PATH.read_text()).alias("e"),
                                "partition", "offset", "timestamp")
        else:
            parsed = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                                "partition", "offset", "timestamp")
        df = parsed.select("e.*", F.col("partition").alias("kafka_partition"),
                           F.col("offset").alias("kafka_offset"),
                           F.col("timestamp").alias("kafka_ts"))
    elif source == "files":
        df = (spark.readStream.schema(EVENT_SCHEMA).option("maxFilesPerTrigger", max_files_per_trigger)
              .json(cfg.stream_input())
              .withColumn("kafka_partition", F.lit(None).cast("int"))
              .withColumn("kafka_offset", F.lit(None).cast("long"))
              .withColumn("kafka_ts", F.current_timestamp()))
    else:
        raise ValueError(source)
    return (df.withColumn("event_ts", parse_event_time())
              .withColumn("event_date", F.to_date("event_ts")))


def add_common_args(ap: argparse.ArgumentParser) -> argparse.ArgumentParser:
    ap.add_argument("--source", choices=["kafka", "files"], default=None)
    ap.add_argument("--format", choices=["json", "avro"], default="json")
    ap.add_argument("--trigger", default="30 seconds", help="processingTime trigger")
    ap.add_argument("--available-now", action="store_true",
                    help="xử lý hết dữ liệu hiện có rồi dừng (test/backfill)")
    ap.add_argument("--max-per-trigger", type=int, default=100_000,
                    help="Kafka: maxOffsetsPerTrigger")
    ap.add_argument("--max-files-per-trigger", type=int, default=4, help="nguồn files")
    return ap


def run_query(spark: SparkSession, writer) -> None:
    """Chạy query tới khi dừng; tắt Spark gọn gàng (tránh lỗi callback listener khi thoát)."""
    query = writer.start()
    try:
        query.awaitTermination()
    finally:
        from streaming.metrics import uninstall
        uninstall(spark)
        spark.stop()


def apply_trigger(writer, args):
    if args.available_now:
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=args.trigger)
