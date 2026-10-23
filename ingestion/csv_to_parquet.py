"""[TV1] Nạp dữ liệu lịch sử REES46 (CSV/CSV.gz) vào tầng bronze trên HDFS dạng Parquet.

- Giữ nguyên dữ liệu gốc (bronze = bất biến, chỉ thêm cột kỹ thuật)
- Thêm event_id (khử trùng nhất quán với đường stream), event_ts, event_date
- Partition theo event_date -> partition pruning cho mọi job phía sau
- Ghi dynamic partition overwrite -> chạy lại (idempotent) không nhân đôi dữ liệu

    spark-submit ingestion/csv_to_parquet.py --input "data/raw/*.csv"
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import functions as F

from shoppulse.config import load_config
from shoppulse.io import save_report, timer, write_table
from shoppulse.schemas import RAW_SCHEMA, event_id_col, parse_event_time
from shoppulse.spark import get_spark, setup_logging

log = logging.getLogger("shoppulse.csv_to_parquet")


def run(input_glob: str | None = None, files_per_partition: int = 4) -> dict:
    cfg = load_config()
    spark = get_spark("csv-to-parquet", cfg, hive=False)
    input_glob = input_glob or cfg.raw("*.csv*")
    out = cfg.lake("bronze", "events_batch")
    metrics: dict = {"input": input_glob, "output": out}

    raw = (spark.read.option("header", True).option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord", "_corrupt")
           .schema(RAW_SCHEMA).csv(input_glob))
    bronze = (raw
              .withColumn("event_id", event_id_col())
              .withColumn("event_ts", parse_event_time())
              .withColumn("event_date", F.to_date("event_ts"))
              .withColumn("source", F.lit("history"))
              .withColumn("ingested_at", F.current_timestamp()))
    # Mỗi partition ngày gom về ít file (tránh small files trên HDFS)
    bronze = bronze.repartition(F.col("event_date"), (F.rand(7) * files_per_partition).cast("int"))
    with timer(metrics, "write_seconds"):
        write_table(bronze, out, partition_by=["event_date"])
    df = spark.read.parquet(out)
    metrics["rows"] = df.count()
    metrics["days"] = df.select("event_date").distinct().count()
    metrics["null_event_ts"] = df.filter(F.col("event_ts").isNull()).count()
    save_report(spark, cfg, "csv_to_parquet", metrics)
    log.info("bronze/events_batch: %s dòng, %s ngày", metrics["rows"], metrics["days"])
    return metrics


def main() -> None:
    setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=None, help="glob file CSV (mặc định raw_root/*.csv*)")
    ap.add_argument("--files-per-partition", type=int, default=4)
    args = ap.parse_args()
    run(args.input, args.files_per_partition)


if __name__ == "__main__":
    main()
