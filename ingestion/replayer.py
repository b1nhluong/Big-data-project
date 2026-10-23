"""[TV1] Phát lại sự kiện REES46 theo đúng nhịp thời gian vào Kafka (hoặc thư mục file).

Tính năng
  --speed N          tua nhanh N lần (0 = gửi nhanh nhất có thể)
  --rewrite-time     dời mốc thời gian về "bây giờ" để dashboard trông như trực tiếp
  --burst-*          mô phỏng flash sale: trong khoảng thời gian chỉ định tốc độ x factor
  --duplicate-rate / --late-rate / --max-late-s   bơm lỗi (ingestion/chaos.py)
  --sink kafka|files --format json|avro

Đảm bảo phía producer (slide chương 5): acks=all + enable.idempotence=true
-> retry không tạo bản ghi trùng trong một phiên producer; key = user_id -> mọi sự
kiện của một user vào cùng partition nên giữ thứ tự theo user.

    python -m ingestion.replayer --input "data/raw/*.csv" --speed 60 --sink kafka
    python -m ingestion.replayer --input "data/raw/*.csv" --speed 0 --sink files --max-events 50000
"""
from __future__ import annotations

import argparse
import glob
import io
import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Iterator

import pandas as pd

from ingestion.chaos import ChaosConfig, ChaosStats, inject
from shoppulse.config import REPO_ROOT, load_config
from shoppulse.schemas import event_id_py, normalize_time_str

log = logging.getLogger("shoppulse.replayer")
AVSC = REPO_ROOT / "ingestion" / "avro" / "event.avsc"
TIME_FMT = "%Y-%m-%d %H:%M:%S"


def _month_key(path: str) -> tuple:
    """REES46 đặt tên file theo tháng '2019-Oct.csv' -> sắp theo thời gian, không theo chữ cái."""
    ts = pd.to_datetime(os.path.basename(path).split(".")[0], format="%Y-%b", errors="coerce")
    return (0, ts.value, path) if not pd.isna(ts) else (1, 0, path)


def read_events(pattern: str, chunksize: int = 50_000, max_events: int | None = None) -> Iterator[dict]:
    files = sorted(glob.glob(pattern), key=_month_key)
    if not files:
        raise FileNotFoundError(pattern)
    sent = 0
    for path in files:
        log.info("Đọc %s", path)
        for chunk in pd.read_csv(path, chunksize=chunksize, dtype={"category_code": "string",
                                                                   "brand": "string",
                                                                   "user_session": "string"}):
            ts = pd.to_datetime(chunk["event_time"].str.replace(" UTC", "", regex=False),
                                format=TIME_FMT, utc=True)
            chunk = chunk.assign(_ts=ts.astype("int64") / 1e9)
            for rec in chunk.to_dict("records"):
                for k in ("category_code", "brand", "user_session"):
                    if pd.isna(rec.get(k)):
                        rec[k] = None
                if pd.isna(rec.get("price")):
                    rec["price"] = None
                if pd.isna(rec.get("category_id")):
                    rec["category_id"] = None
                yield rec
                sent += 1
                if max_events and sent >= max_events:
                    return


def to_message(rec: dict, time_offset_s: float, speed: float, t0: float) -> dict:
    orig = normalize_time_str(str(rec["event_time"]))
    event_id = event_id_py(orig, rec["event_type"], rec["product_id"], rec["user_id"], rec["user_session"])
    if time_offset_s or speed:
        # co giãn thời gian: t' = t0' + (t - t0) / speed  (giữ khoảng cách tương đối)
        scaled = rec["_ts"] if not speed else t0 + (rec["_ts"] - t0) / speed
        new_ts = datetime.fromtimestamp(scaled + time_offset_s, tz=timezone.utc)
        event_time = new_ts.strftime(TIME_FMT)
    else:
        event_time = orig
    return {
        "event_id": event_id, "event_time": event_time, "event_type": rec["event_type"],
        "product_id": int(rec["product_id"]),
        "category_id": int(rec["category_id"]) if rec["category_id"] is not None else None,
        "category_code": rec["category_code"], "brand": rec["brand"],
        "price": float(rec["price"]) if rec["price"] is not None else None,
        "user_id": int(rec["user_id"]), "user_session": rec["user_session"],
        "ingest_ts": int(time.time() * 1000),
    }


class KafkaSink:
    def __init__(self, cfg, fmt: str, topic: str):
        from confluent_kafka import Producer

        conf = {
            "bootstrap.servers": cfg.kafka_bootstrap,
            "acks": "all", "enable.idempotence": True,          # exactly-once phía producer
            "compression.type": "lz4", "linger.ms": 20, "batch.size": 256_000,
            "client.id": f"replayer-{uuid.uuid4().hex[:6]}",
        }
        if cfg.get("kafka_security"):
            conf.update({"security.protocol": cfg.kafka_security,
                         "sasl.mechanism": cfg.get("kafka_sasl_mechanism", "SCRAM-SHA-512"),
                         "sasl.username": cfg.secret("KAFKA_USERNAME", ""),
                         "sasl.password": cfg.secret("KAFKA_PASSWORD", "")})
        self.producer = Producer(conf)
        self.topic = topic
        self.fmt = fmt
        self.errors = 0
        if fmt == "avro":
            import fastavro
            self._fastavro = fastavro
            self.schema = fastavro.parse_schema(json.loads(AVSC.read_text()))

    def _encode(self, msg: dict) -> bytes:
        if self.fmt == "avro":
            buf = io.BytesIO()
            self._fastavro.schemaless_writer(buf, self.schema, msg)
            return buf.getvalue()
        return json.dumps(msg, ensure_ascii=False).encode("utf-8")

    def _cb(self, err, _msg) -> None:
        if err is not None:
            self.errors += 1
            log.error("Gửi lỗi: %s", err)

    def send(self, msg: dict) -> None:
        while True:
            try:
                self.producer.produce(self.topic, key=str(msg["user_id"]).encode(),
                                      value=self._encode(msg), on_delivery=self._cb)
                break
            except BufferError:                 # hàng đợi cục bộ đầy -> chờ broker
                self.producer.poll(0.5)
        self.producer.poll(0)

    def close(self) -> None:
        self.producer.flush(30)


class FileSink:
    """Ghi JSON lines, mỗi file N sự kiện; ghi file tạm rồi rename (atomic) để Spark
    file source không đọc phải file đang ghi dở."""

    def __init__(self, directory: str, roll_every: int = 5_000):
        os.makedirs(directory, exist_ok=True)
        self.dir, self.roll_every, self.buf, self.n_files = directory, roll_every, [], 0

    def send(self, msg: dict) -> None:
        self.buf.append(json.dumps(msg, ensure_ascii=False))
        if len(self.buf) >= self.roll_every:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        name = f"events-{int(time.time() * 1000)}-{self.n_files:06d}.json"
        tmp = os.path.join(self.dir, f".{name}.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write("\n".join(self.buf) + "\n")
        os.replace(tmp, os.path.join(self.dir, name))
        self.buf, self.n_files = [], self.n_files + 1

    def close(self) -> None:
        self.flush()


def replay(args) -> dict:
    cfg = load_config()
    sink = (KafkaSink(cfg, args.format, cfg.topic_events) if args.sink == "kafka"
            else FileSink(args.out_dir or cfg.stream_input(), args.roll_every))
    stats = ChaosStats()
    chaos = ChaosConfig(args.duplicate_rate, args.late_rate, args.max_late_s, args.seed)
    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    wall0, t0, offset, sent = time.time(), None, 0.0, 0
    burst_lo = args.burst_start_min * 60
    burst_hi = burst_lo + args.burst_duration_min * 60
    for rec in inject(read_events(args.input, max_events=args.max_events), chaos, stats):
        if stop["flag"]:
            break
        if t0 is None:
            t0 = rec["_ts"]
            if args.rewrite_time:
                offset = time.time() - t0
        if args.speed > 0:
            # thời gian thực cần chờ; trong khoảng burst tốc độ nhân factor
            elapsed_event = max(rec["_ts"] - t0, 0.0)
            target = elapsed_event / args.speed
            if args.burst_factor > 1 and burst_lo <= elapsed_event / args.speed:
                in_burst = min(elapsed_event / args.speed, burst_hi) - burst_lo
                target -= in_burst * (1 - 1 / args.burst_factor)
            delay = wall0 + target - time.time()
            if delay > 0:
                time.sleep(min(delay, 5))
        msg = to_message(rec, offset if args.rewrite_time else 0.0,
                         args.speed if args.rewrite_time else 0.0, t0)
        sink.send(msg)
        sent += 1
        if sent % 50_000 == 0:
            rate = sent / max(time.time() - wall0, 1e-6)
            log.info("Đã gửi %s sự kiện (%.0f sự kiện/giây)", f"{sent:,}", rate)
    sink.close()
    elapsed = time.time() - wall0
    result = {"sent": sent, "seconds": round(elapsed, 1),
              "events_per_second": round(sent / max(elapsed, 1e-6), 1), "chaos": stats.as_dict()}
    if args.sink == "kafka":
        result["delivery_errors"] = sink.errors  # type: ignore[attr-defined]
    log.info("Kết thúc: %s", result)
    report = REPO_ROOT / "benchmarks" / "results" / "replayer_last_run.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default=str(REPO_ROOT / "data" / "raw" / "*.csv"))
    ap.add_argument("--sink", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--format", choices=["json", "avro"], default="json")
    ap.add_argument("--out-dir", default=None, help="thư mục cho --sink files")
    ap.add_argument("--roll-every", type=int, default=5_000)
    ap.add_argument("--speed", type=float, default=60.0)
    ap.add_argument("--rewrite-time", action="store_true")
    ap.add_argument("--max-events", type=int, default=None)
    ap.add_argument("--burst-start-min", type=float, default=0.0)
    ap.add_argument("--burst-duration-min", type=float, default=0.0)
    ap.add_argument("--burst-factor", type=float, default=1.0)
    ap.add_argument("--duplicate-rate", type=float, default=0.0)
    ap.add_argument("--late-rate", type=float, default=0.0)
    ap.add_argument("--max-late-s", type=float, default=900.0)
    ap.add_argument("--seed", type=int, default=13)
    return ap


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    replay(build_parser().parse_args())


if __name__ == "__main__":
    main()
