"""Tạo SparkSession dùng chung cho mọi job (batch, streaming, ML)."""
from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

from shoppulse.config import REPO_ROOT, Config, load_config

log = logging.getLogger("shoppulse")


def get_spark(app_name: str, cfg: Config | None = None, extra_conf: dict | None = None,
              hive: bool = True) -> SparkSession:
    cfg = cfg or load_config()
    builder = SparkSession.builder.appName(f"shoppulse-{app_name}")
    if cfg.get("spark_master"):
        builder = builder.master(cfg.spark_master)

    conf = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(cfg.get("shuffle_partitions", 64)),
        # Adaptive Query Execution: gộp partition nhỏ, xử lý skew join tự động
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        # Prometheus endpoint của Spark UI (/metrics/prometheus) — dùng cho giám sát
        "spark.ui.prometheus.enabled": "true",
        "spark.sql.streaming.metricsEnabled": "true",
    }
    if cfg.get("warehouse_dir"):
        wh = cfg.warehouse_dir if "://" in str(cfg.warehouse_dir) else str((REPO_ROOT / cfg.warehouse_dir).resolve())
        conf["spark.sql.warehouse.dir"] = wh
    if cfg.is_local:
        conf.update({
            "spark.driver.memory": os.environ.get("SPARK_DRIVER_MEMORY", "3g"),
            "spark.ui.showConsoleProgress": "false",
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
        })
        metastore = REPO_ROOT / "data" / "metastore_db"
        conf["javax.jdo.option.ConnectionURL"] = f"jdbc:derby:;databaseName={metastore};create=true"
        # Đặt đường dẫn trong ngoặc kép: Spark tách extraJavaOptions theo dấu cách, nên thư mục
        # có dấu cách (ví dụ ".../Big Data/...") sẽ làm JVM không khởi động được (JAVA_GATEWAY_EXITED).
        conf["spark.driver.extraJavaOptions"] = f'-Dderby.system.home="{REPO_ROOT / "data"}"'
    if cfg.get("hive_metastore_uri"):
        conf["spark.hadoop.hive.metastore.uris"] = cfg.hive_metastore_uri
    if cfg.get("cassandra_host"):
        conf["spark.cassandra.connection.host"] = cfg.cassandra_host
        user = cfg.secret("CASSANDRA_USERNAME")
        if user:
            conf["spark.cassandra.auth.username"] = user
            conf["spark.cassandra.auth.password"] = cfg.secret("CASSANDRA_PASSWORD", "")
    conf.update(extra_conf or {})
    for key, value in conf.items():
        builder = builder.config(key, value)
    if hive:
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def setup_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
