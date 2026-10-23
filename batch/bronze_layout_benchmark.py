"""[TV2] Thí nghiệm bố cục lưu trữ cho tầng bronze (slide 3.3 — định dạng cột, nén, thống kê min/max).

Vì sao có file này: chạy csv_to_parquet trên 110 triệu dòng thật (10–11/2019), Parquet vẫn nặng
8,9 GB = 64% CSV. Đo từng cột cho thấy event_id (SHA-1 dạng hex) chiếm ~46% và user_session ~38%:
hai cột chuỗi gần như ngẫu nhiên, lại bị xếp lộn xộn do repartition theo rand().

Thí nghiệm lấy bronze của một vài ngày, ghi lại theo nhiều bố cục rồi đo:
  - dung lượng (tổng và từng cột), thời gian ghi
  - thời gian 3 truy vấn điển hình: đếm phiên, tra cứu một user (hưởng lợi từ thống kê min/max
    của row group khi dữ liệu được sắp xếp), đếm event_id khác nhau (khử trùng)

Biến thể:
  baseline_random   như csv_to_parquet hiện tại: chia file ngẫu nhiên, event_id hex, snappy
  sorted            chia theo khoảng user_id + sắp xếp (user_id, user_session, event_ts)
  sorted_binary_id  như sorted + event_id lưu nhị phân 20 byte (unhex) thay vì chuỗi 40 ký tự
  sorted_zstd       như sorted + nén zstd thay snappy

    python -m batch.bronze_layout_benchmark --dates 2019-11-17 --repeat 3
"""
from __future__ import annotations

import argparse
import glob
import logging
import os
import time
from collections import Counter

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shoppulse.config import load_config
from shoppulse.io import dir_size_bytes, hadoop_size_bytes, save_report
from shoppulse.spark import get_spark, setup_logging

log = logging.getLogger("shoppulse.bronze_layout_benchmark")

SORT_KEYS = ["user_id", "user_session", "event_ts"]


def build_variant(df: DataFrame, name: str, files: int) -> tuple[DataFrame, str]:
    """Trả về (DataFrame đã bố cục, codec nén)."""
    if name == "baseline_random":
        return df.repartition(files, (F.rand(7) * files).cast("int")), "snappy"
    ranged = df.repartitionByRange(files, "user_id").sortWithinPartitions(*SORT_KEYS)
    if name == "sorted":
        return ranged, "snappy"
    if name == "sorted_binary_id":
        return ranged.withColumn("event_id", F.unhex("event_id")), "snappy"
    if name == "sorted_zstd":
        return ranged, "zstd"
    raise ValueError(name)


def column_sizes(path: str) -> dict[str, int]:
    """Dung lượng nén của từng cột, đọc từ footer Parquet (chỉ với đường dẫn local)."""
    import pyarrow.parquet as pq
    sizes: Counter = Counter()
    for f in glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True):
        md = pq.ParquetFile(f).metadata
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            for c in range(rg.num_columns):
                col = rg.column(c)
                sizes[col.path_in_schema] += col.total_compressed_size
    return dict(sizes.most_common())


def best_of(repeat: int, fn) -> float:
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return round(min(times), 3)


def run(dates: list[str], files: int, repeat: int, variants: list[str]) -> dict:
    cfg = load_config()
    spark = get_spark("bronze-layout-bench", cfg, hive=False)
    src_path = cfg.lake("bronze", "events_batch")
    src = spark.read.parquet(src_path).filter(F.col("event_date").isin(dates)).drop("event_date")
    rows = src.count()
    hot_user = (src.groupBy("user_id").count().orderBy(F.desc("count")).first() or [None])[0]
    log.info("Nguồn: %s dòng (%s), user tra cứu: %s", rows, ", ".join(dates), hot_user)

    size_of = dir_size_bytes if cfg.is_local else (lambda p: hadoop_size_bytes(spark, p))
    results: dict = {"dates": dates, "rows": rows, "files_per_variant": files, "repeat": repeat,
                     "lookup_user_id": hot_user, "variants": {}}
    for name in variants:
        out = cfg.lake("bench", f"bronze_layout/{name}")
        df, codec = build_variant(src, name, files)
        t0 = time.perf_counter()
        df.write.mode("overwrite").option("compression", codec).parquet(out)
        write_s = round(time.perf_counter() - t0, 3)
        size = size_of(out)
        spark.catalog.clearCache()
        data = spark.read.parquet(out)
        r = {
            "codec": codec,
            "write_seconds": write_s,
            "size_mb": round(size / 1e6, 1),
            "bytes_per_row": round(size / max(rows, 1), 1),
            "q_sessions_seconds": best_of(repeat, lambda: data.select(F.countDistinct("user_session")).collect()),
            "q_user_lookup_seconds": best_of(repeat, lambda: data.filter(F.col("user_id") == hot_user).count()),
            "q_distinct_event_id_seconds": best_of(repeat, lambda: data.select(F.countDistinct("event_id")).collect()),
        }
        if cfg.is_local:
            r["column_mb"] = {k: round(v / 1e6, 1) for k, v in column_sizes(out).items()}
        results["variants"][name] = r
        log.info("%-17s %8.1f MB  ghi %6.1fs  phiên %5.2fs  tra user %5.2fs  khử trùng %5.2fs",
                 name, r["size_mb"], write_s, r["q_sessions_seconds"], r["q_user_lookup_seconds"],
                 r["q_distinct_event_id_seconds"])

    base = results["variants"].get("baseline_random")
    if base:
        for r in results["variants"].values():
            r["size_vs_baseline"] = round(r["size_mb"] / base["size_mb"], 3)
    save_report(spark, cfg, "bronze_layout", results)
    return results


def main() -> None:
    setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="2019-11-17", help="danh sách ngày, cách nhau bởi dấu phẩy")
    ap.add_argument("--files", type=int, default=4, help="số file mỗi biến thể")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--variants", default="baseline_random,sorted,sorted_binary_id,sorted_zstd")
    args = ap.parse_args()
    run([d.strip() for d in args.dates.split(",")], args.files, args.repeat,
        [v.strip() for v in args.variants.split(",")])


if __name__ == "__main__":
    main()
