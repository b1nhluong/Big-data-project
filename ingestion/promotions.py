"""[TV1] Nguồn dữ liệu quan hệ: lịch khuyến mãi / flash sale trong PostgreSQL.

Hai lệnh con:
  generate  sinh lịch khuyến mãi -> file SQL (init PostgreSQL) + CSV (chạy local)
  load      Spark đọc PostgreSQL qua JDBC (song song theo campaign_id, giống Sqoop
            trong hệ sinh thái Hadoop — slide chương 2) -> silver/dim_promotion

    python -m ingestion.promotions generate --start 2019-10-01 --days 213
    spark-submit ingestion/promotions.py load            # profile k8s: JDBC
    python -m ingestion.promotions load --from-csv       # profile local
"""
from __future__ import annotations

import argparse
import csv
import os
import random
from datetime import datetime, timedelta

from shoppulse.config import REPO_ROOT

L1_CATEGORIES = ["electronics", "computers", "appliances", "apparel", "furniture", "kids",
                 "sport", "construction", "auto", "accessories", "all"]
SQL_PATH = REPO_ROOT / "infra" / "k8s" / "postgres" / "init" / "01_promotions.sql"


def generate(start: str, days: int, seed: int = 7) -> list[dict]:
    rnd = random.Random(seed)
    base = datetime.fromisoformat(start)
    rows, cid = [], 1
    for d in range(days):
        day = base + timedelta(days=d)
        # 1-3 chiến dịch mỗi ngày; ngày 10/10, 11/11... là mega sale toàn sàn
        n = rnd.randint(1, 3)
        for _ in range(n):
            cat = rnd.choice(L1_CATEGORIES[:-1])
            start_h = rnd.choice([0, 8, 12, 19, 20])
            dur = rnd.choice([2, 4, 6, 24])
            rows.append({"campaign_id": cid, "name": f"flash-{cat}-{day:%m%d}-{start_h:02d}h",
                         "cat_l1": cat, "start_ts": day + timedelta(hours=start_h),
                         "end_ts": day + timedelta(hours=start_h + dur),
                         "discount_pct": rnd.choice([10, 15, 20, 30, 50])})
            cid += 1
        if day.month == day.day:  # 10/10, 11/11, 12/12
            rows.append({"campaign_id": cid, "name": f"mega-sale-{day:%m%d}", "cat_l1": "all",
                         "start_ts": day, "end_ts": day + timedelta(hours=24), "discount_pct": 40})
            cid += 1
    return rows


def write_outputs(rows: list[dict], csv_path: str) -> None:
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow({**r, "start_ts": f"{r['start_ts']:%Y-%m-%d %H:%M:%S}",
                        "end_ts": f"{r['end_ts']:%Y-%m-%d %H:%M:%S}"})
    SQL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SQL_PATH, "w", encoding="utf-8") as fh:
        fh.write("-- Sinh tự động bởi ingestion/promotions.py generate\n")
        fh.write("CREATE TABLE IF NOT EXISTS promotions (\n  campaign_id INT PRIMARY KEY,\n"
                 "  name TEXT NOT NULL,\n  cat_l1 TEXT NOT NULL,\n  start_ts TIMESTAMP NOT NULL,\n"
                 "  end_ts TIMESTAMP NOT NULL,\n  discount_pct INT NOT NULL\n);\n")
        fh.write("TRUNCATE promotions;\nINSERT INTO promotions VALUES\n")
        values = [f"({r['campaign_id']}, '{r['name']}', '{r['cat_l1']}', "
                  f"'{r['start_ts']:%Y-%m-%d %H:%M:%S}', '{r['end_ts']:%Y-%m-%d %H:%M:%S}', "
                  f"{r['discount_pct']})" for r in rows]
        fh.write(",\n".join(values) + ";\n")


def load(from_csv: bool) -> int:
    from pyspark.sql import functions as F

    from shoppulse.config import load_config
    from shoppulse.io import write_table
    from shoppulse.spark import get_spark

    cfg = load_config()
    spark = get_spark("load-promotions", cfg, hive=False)
    if from_csv:
        df = (spark.read.option("header", True).option("inferSchema", True)
              .csv(cfg.raw("promotions/promotions.csv")))
    else:
        # Đọc song song: Spark chia campaign_id thành numPartitions khoảng
        bounds = (spark.read.format("jdbc").option("url", cfg.postgres_jdbc)
                  .option("query", "SELECT min(campaign_id) lo, max(campaign_id) hi FROM promotions")
                  .option("user", cfg.secret("POSTGRES_USER", "shoppulse"))
                  .option("password", cfg.secret("POSTGRES_PASSWORD", ""))
                  .option("driver", "org.postgresql.Driver").load().first())
        df = (spark.read.format("jdbc").option("url", cfg.postgres_jdbc)
              .option("dbtable", "promotions")
              .option("user", cfg.secret("POSTGRES_USER", "shoppulse"))
              .option("password", cfg.secret("POSTGRES_PASSWORD", ""))
              .option("driver", "org.postgresql.Driver")
              .option("partitionColumn", "campaign_id")
              .option("lowerBound", bounds["lo"]).option("upperBound", bounds["hi"])
              .option("numPartitions", 4).load())
    df = (df.withColumn("start_ts", F.to_timestamp("start_ts"))
            .withColumn("end_ts", F.to_timestamp("end_ts")))
    write_table(df, cfg.lake("silver", "dim_promotion"))
    return df.count()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--start", default="2019-10-01")
    g.add_argument("--days", type=int, default=213)       # 10/2019 -> 4/2020
    g.add_argument("--csv", default=str(REPO_ROOT / "data" / "raw" / "promotions" / "promotions.csv"))
    ld = sub.add_parser("load")
    ld.add_argument("--from-csv", action="store_true")
    args = ap.parse_args()
    if args.cmd == "generate":
        rows = generate(args.start, args.days)
        write_outputs(rows, args.csv)
        print(f"{len(rows)} chiến dịch -> {args.csv}, {SQL_PATH}")
    else:
        print(f"dim_promotion: {load(args.from_csv)} dòng")


if __name__ == "__main__":
    main()
