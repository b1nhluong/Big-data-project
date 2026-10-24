"""[TV3] Sink cho foreachBatch — idempotent để đạt exactly-once end-to-end.

Nguyên tắc (slide chương 8): nguồn phát lại được (Kafka offset) + checkpoint/WAL
+ sink idempotent = exactly-once. Khi Spark chạy lại một micro-batch sau sự cố,
cùng batch_id sẽ ghi lại đúng dữ liệu đó:
  * Cassandra: INSERT là upsert theo khoá chính -> ghi lại không tạo bản sao
  * Parquet (local): mỗi batch ghi vào thư mục batch_id=N với mode overwrite
  * Redis: ZADD/SET ghi đè theo khoá
  * Kafka (alerts): at-least-once; alert_id tất định để bên tiêu thụ khử trùng
"""
from __future__ import annotations

import logging
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from shoppulse.config import Config

log = logging.getLogger("shoppulse.sinks")

# bảng đích trên Cassandra (xem serving/cassandra/schema.cql)
CASSANDRA_TABLES = {
    "kpi_minute": "kpi_minute",
    "trending": "trending_by_window",
    "alerts": "alerts_by_day",
    "live_sessions": "live_sessions",
    "sessions_closed": "sessions_closed",
}


class BatchSink(Protocol):
    def write(self, df: DataFrame, batch_id: int) -> None: ...


class ParquetBatchSink:
    """Local/test: mô phỏng upsert — mỗi batch một thư mục, ghi đè khi chạy lại."""

    def __init__(self, path: str):
        self.path = path

    def write(self, df: DataFrame, batch_id: int) -> None:
        # cột batch_id đến từ tên thư mục (partition discovery) khi đọc lại
        df.write.mode("overwrite").parquet(f"{self.path}/batch_id={batch_id}")


class CassandraSink:
    def __init__(self, keyspace: str, table: str):
        self.keyspace, self.table = keyspace, table

    def write(self, df: DataFrame, batch_id: int) -> None:
        (df.write.format("org.apache.spark.sql.cassandra")
           .options(keyspace=self.keyspace, table=self.table)
           .option("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
           .mode("append").save())


class KafkaBatchSink:
    def __init__(self, cfg: Config, topic: str, key_col: str):
        self.cfg, self.topic, self.key_col = cfg, topic, key_col

    def write(self, df: DataFrame, batch_id: int) -> None:
        (df.select(F.col(self.key_col).cast("string").alias("key"),
                   F.to_json(F.struct(*df.columns)).alias("value"))
           .write.format("kafka").options(**self.cfg.kafka_options())
           .option("topic", self.topic).save())


class MultiSink:
    def __init__(self, *sinks: BatchSink):
        self.sinks = sinks

    def write(self, df: DataFrame, batch_id: int) -> None:
        for s in self.sinks:
            s.write(df, batch_id)


def make_sink(cfg: Config, name: str) -> BatchSink:
    if cfg.get("sink_mode") == "k8s":
        sink: BatchSink = CassandraSink(cfg.cassandra_keyspace, CASSANDRA_TABLES[name])
        if name == "alerts":
            sink = MultiSink(sink, KafkaBatchSink(cfg, cfg.topic_alerts, "session_id"))
        return sink
    return ParquetBatchSink(cfg.lake("serving", name))


def read_latest(spark: SparkSession, path: str, keys: list[str]) -> DataFrame:
    """Đọc sink Parquet local: với mỗi khoá lấy bản ghi của batch mới nhất (= upsert)."""
    df = spark.read.parquet(path)
    w = Window.partitionBy(*keys).orderBy(F.desc("batch_id"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---------------------------------------------------------------- Redis top-K
def write_topk_redis(client, key: str, rows: list[tuple[str, float]], ttl_s: int = 3600) -> None:
    """Thay toàn bộ ZSET một cách nguyên tử: ghi vào khoá tạm rồi RENAME."""
    tmp = f"{key}:tmp"
    pipe = client.pipeline(transaction=True)
    pipe.delete(tmp)
    if rows:
        pipe.zadd(tmp, {member: score for member, score in rows})
        pipe.rename(tmp, key)
        pipe.expire(key, ttl_s)
    pipe.execute()


def redis_client(cfg: Config):
    import redis

    return redis.Redis(host=cfg.redis_host, port=int(cfg.get("redis_port", 6379)),
                       password=cfg.secret("REDIS_PASSWORD"), socket_timeout=5,
                       decode_responses=True)
