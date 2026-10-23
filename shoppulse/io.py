"""Tiện ích đọc/ghi bảng trong data lake và lưu báo cáo (metrics, benchmark)."""
from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql import DataFrame, SparkSession

from shoppulse.config import REPO_ROOT, Config

log = logging.getLogger("shoppulse.io")


def write_table(df: DataFrame, path: str, partition_by: list[str] | None = None,
                mode: str = "overwrite", max_records_per_file: int | None = None) -> None:
    writer = df.write.mode(mode).format("parquet")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if max_records_per_file:
        writer = writer.option("maxRecordsPerFile", max_records_per_file)
    writer.save(path)
    log.info("Đã ghi %s", path)


def read_table(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm  # type: ignore[attr-defined]
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())  # type: ignore[attr-defined]
    return bool(fs.exists(hpath))


def save_report(spark: SparkSession | None, cfg: Config, name: str, payload: dict[str, Any]) -> str:
    """Lưu báo cáo JSON: vào lake/reports (mọi profile) và benchmarks/results (local)."""
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "profile": cfg.profile, **payload}
    text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    local_dir = REPO_ROOT / "benchmarks" / "results"
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / f"{name}.json"
    local_path.write_text(text, encoding="utf-8")
    if spark is not None and not cfg.is_local:
        spark.sparkContext.parallelize([text], 1).saveAsTextFile(
            cfg.lake("reports", f"{name}/{int(time.time())}"))
    log.info("Báo cáo %s -> %s", name, local_path)
    return str(local_path)


@contextmanager
def timer(results: dict[str, float], key: str) -> Iterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        results[key] = round(time.perf_counter() - start, 3)
        log.info("%s: %.3fs", key, results[key])


def dir_size_bytes(path: str) -> int:
    """Kích thước thư mục local (dùng cho benchmark định dạng file)."""
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            if not f.startswith((".", "_")):
                total += os.path.getsize(os.path.join(root, f))
    return total


def hadoop_size_bytes(spark: SparkSession, path: str) -> int:
    jvm = spark._jvm  # type: ignore[attr-defined]
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())  # type: ignore[attr-defined]
    return int(fs.getContentSummary(hpath).getLength())


def repo_path(*parts: str) -> str:
    return str(REPO_ROOT.joinpath(*parts))
