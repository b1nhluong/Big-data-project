"""Fixture dùng chung: SparkSession local nhỏ gọn + data lake tạm cho từng phiên test."""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def lake(tmp_path_factory):
    base = tmp_path_factory.mktemp("lake")
    os.environ.update({
        "SHOPPULSE_PROFILE": "local",
        "SHOPPULSE_LAKE_ROOT": str(base / "lake"),
        "SHOPPULSE_CHECKPOINT_ROOT": str(base / "checkpoints"),
        "SHOPPULSE_MODELS_ROOT": str(base / "models"),
        "SHOPPULSE_STREAM_INPUT_DIR": str(base / "stream_in"),
        "SHOPPULSE_RAW_ROOT": str(base / "raw"),
        "SHOPPULSE_WAREHOUSE_DIR": str(base / "warehouse"),
        "SHOPPULSE_SHUFFLE_PARTITIONS": "2",
    })
    return base


@pytest.fixture(scope="session")
def spark(lake):
    from shoppulse.config import load_config
    from shoppulse.spark import get_spark

    s = get_spark("tests", load_config(), extra_conf={"spark.master": "local[2]",
                                                      "spark.sql.shuffle.partitions": "2",
                                                      "spark.ui.enabled": "false"}, hive=False)
    yield s
    s.stop()


@pytest.fixture(scope="session")
def cfg(lake):
    from shoppulse.config import load_config
    return load_config()


def make_events(spark, rows):
    """rows: list of (ts, type, product_id, user_id, session, price[, category_code, brand])."""
    from pyspark.sql import functions as F
    from shoppulse.schemas import event_id_col, parse_event_time
    full = [(r + (None, None))[:8] if len(r) == 6 else r for r in rows]
    df = spark.createDataFrame(full, "event_time string, event_type string, product_id long, user_id long, "
                                     "user_session string, price double, category_code string, brand string")
    return (df.withColumn("category_id", F.lit(1).cast("long"))
              .withColumn("event_id", event_id_col())
              .withColumn("event_ts", parse_event_time())
              .withColumn("event_date", F.to_date("event_ts"))
              .withColumn("source", F.lit("test")))
