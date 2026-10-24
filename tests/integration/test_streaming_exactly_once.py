"""Kiểm thử exactly-once: chạy streaming 3 lần (thêm dữ liệu giữa chừng, rồi chạy lại khi
không có dữ liệu mới). Kết quả cuối phải khớp tuyệt đối với tính toán batch trên dữ liệu gốc
đã khử trùng — không mất, không đếm hai lần, dù có 5% sự kiện bị gửi trùng."""
import json
import os
from argparse import Namespace
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F

from ingestion.chaos import ChaosConfig, inject
from ingestion.generate_sample import generate
from ingestion.replayer import to_message
from streaming import bronze_sink, kpi_minute
from streaming.sinks import read_latest

ARGS = Namespace(source="files", format="json", max_per_trigger=1000, max_files_per_trigger=1,
                 available_now=True, trigger="1 second")


def write_files(directory, messages, start_idx, per_file=500):
    os.makedirs(directory, exist_ok=True)
    for i in range(0, len(messages), per_file):
        with open(os.path.join(directory, f"part-{start_idx + i:08d}.json"), "w") as fh:
            fh.write("\n".join(json.dumps(m) for m in messages[i:i + per_file]) + "\n")


def run_job(module, spark, cfg):
    q = module.build(spark, cfg, ARGS).start()
    q.awaitTermination()
    return q.lastProgress


def test_kpi_and_bronze_exactly_once(spark, cfg):
    df = generate(4000, 2, datetime(2019, 10, 1, tzinfo=timezone.utc), seed=3)
    df = df.assign(event_time=df.event_time.dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
                   _ts=df.event_time.astype("int64") / 1e9)
    recs = df.where(pd.notna(df), None).to_dict("records")
    msgs = [to_message(r, 0.0, 0.0, 0.0) for r in inject(recs, ChaosConfig(duplicate_rate=0.05, seed=2))]
    half = len(msgs) // 2
    stream_dir = cfg.stream_input()

    write_files(stream_dir, msgs[:half], 0)
    run_job(kpi_minute, spark, cfg)
    run_job(bronze_sink, spark, cfg)
    write_files(stream_dir, msgs[half:], half)
    run_job(kpi_minute, spark, cfg)
    run_job(bronze_sink, spark, cfg)
    progress = run_job(kpi_minute, spark, cfg)          # lần 3: không có dữ liệu mới
    assert progress is None or progress["numInputRows"] == 0

    # Bronze: giữ nguyên mọi thông điệp (kể cả trùng), không nhân đôi khi chạy lại
    bronze = spark.read.parquet(cfg.lake("bronze", "events_stream"))
    assert bronze.count() == len(msgs)

    # KPI: so với "sự thật" tính bằng pandas trên dữ liệu đã khử trùng + lọc DQ
    truth = pd.DataFrame(msgs).drop_duplicates("event_id")
    truth = truth[~((truth.event_type == "purchase") & (truth.price == 0))]
    speed = read_latest(spark, cfg.lake("serving", "kpi_minute"), ["day", "cat_l1", "window_start"])
    got = speed.agg(F.sum("views"), F.sum("purchases"), F.round(F.sum("revenue"), 2)).first()
    exp_rev = round(truth.loc[truth.event_type == "purchase", "price"].sum(), 2)
    assert got[0] == (truth.event_type == "view").sum()
    assert got[1] == (truth.event_type == "purchase").sum()
    assert abs(got[2] - exp_rev) < 0.05
