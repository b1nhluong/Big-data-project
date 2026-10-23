"""[TV2] Khám phá dữ liệu (EDA) bằng PySpark trên bronze -> docs/eda.md.

Chạy trên mẫu (--fraction 0.01) khi mới tải dữ liệu, hoặc toàn bộ trên cụm.
    python -m batch.eda --fraction 0.01
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from shoppulse.config import REPO_ROOT, load_config
from shoppulse.spark import get_spark, setup_logging


def md_table(rows: list[dict]) -> str:
    if not rows:
        return "_(trống)_"
    cols = list(rows[0].keys())
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    out += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for r in rows]
    return "\n".join(out)


def run(fraction: float) -> str:
    cfg = load_config()
    spark = get_spark("eda", cfg, hive=False)
    df = spark.read.parquet(cfg.lake("bronze", "events_batch"))
    if fraction < 1:
        df = df.sample(fraction=fraction, seed=11)
    df = df.cache()
    n = df.count()
    parts = [f"# EDA dữ liệu hành vi (mẫu {fraction:.0%}, {n:,} dòng)", "",
             "> Sinh bởi `python -m batch.eda`. Chạy lại trên dữ liệu thật để cập nhật.", ""]
    parts += ["## Phân bố loại sự kiện", "", md_table([
        {"event_type": r["event_type"], "số dòng": f"{r['count']:,}", "tỉ lệ": f"{r['count'] / n:.2%}"}
        for r in df.groupBy("event_type").count().orderBy(F.desc("count")).collect()]), ""]
    nulls = df.select([F.round(F.avg(F.col(c).isNull().cast("double")) * 100, 2).alias(c)
                       for c in ["category_code", "brand", "price", "user_session"]]).first().asDict()
    parts += ["## Tỉ lệ thiếu (%)", "", md_table([nulls]), ""]
    dup = n - df.select("event_id").distinct().count()
    parts += [f"**Dòng trùng hoàn toàn (cùng event_id):** {dup:,} ({dup / max(n, 1):.3%})", ""]
    zero_price = df.filter((F.col("event_type") == "purchase") & (F.col("price") == 0)).count()
    q = df.filter("event_type = 'purchase'").approxQuantile("price", [0.01, 0.25, 0.5, 0.75, 0.99], 0.01)
    parts += ["## Phân vị giá đơn mua", "", md_table([dict(zip(["p1", "p25", "p50", "p75", "p99"],
                                                            [round(x, 2) for x in q]))]),
              "", f"Đơn mua giá 0: {zero_price:,}", ""]
    per_day = df.groupBy("event_date").count().agg(F.min("count").alias("min"), F.avg("count").alias("avg"),
                                                   F.max("count").alias("max")).first()
    parts += ["## Sự kiện mỗi ngày", "", md_table([{k: f"{v:,.0f}" for k, v in per_day.asDict().items()}]), ""]
    sess = df.groupBy("user_session").count()
    sq = sess.approxQuantile("count", [0.5, 0.9, 0.99, 0.999], 0.001)
    parts += ["## Số sự kiện mỗi phiên", "", md_table([dict(zip(["p50", "p90", "p99", "p99.9"], sq))]),
              "", f"Phiên dài nhất: {sess.agg(F.max('count')).first()[0]:,} sự kiện (nghi vấn bot).", ""]
    top_cat = (df.withColumn("cat_l1", F.coalesce(F.split("category_code", r"\.")[0], F.lit("unknown")))
               .groupBy("cat_l1").agg(F.count("*").alias("events"),
                                      F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"))
               .orderBy(F.desc("events")).limit(10).collect())
    parts += ["## Top danh mục cấp 1", "", md_table([r.asDict() for r in top_cat]), ""]
    top_p = df.groupBy("product_id").count().orderBy(F.desc("count")).limit(5).collect()
    share = sum(r["count"] for r in top_p) / n
    parts += [f"**Skew:** 5 sản phẩm phổ biến nhất chiếm {share:.2%} tổng sự kiện — cần lưu ý khi join theo product_id.", ""]
    text = "\n".join(parts)
    path = Path(REPO_ROOT / "docs" / "eda.md")
    path.write_text(text, encoding="utf-8")
    df.unpersist()
    print(f"Đã ghi {path}")
    return text


if __name__ == "__main__":
    setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--fraction", type=float, default=1.0)
    run(ap.parse_args().fraction)
