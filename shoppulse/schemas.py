"""Schema dữ liệu dùng chung giữa batch, streaming và replayer.

REES46 (https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
có 9 cột. Chúng tôi thêm `event_id` = SHA-1 của các trường định danh để khử trùng
nhất quán giữa đường batch (CSV lịch sử) và đường stream (Kafka).
"""
from __future__ import annotations

import hashlib

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

EVENT_TYPES = ("view", "cart", "remove_from_cart", "purchase")

RAW_SCHEMA = StructType([
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", LongType()),
    StructField("category_id", LongType()),
    StructField("category_code", StringType()),
    StructField("brand", StringType()),
    StructField("price", DoubleType()),
    StructField("user_id", LongType()),
    StructField("user_session", StringType()),
])

# Thông điệp trên Kafka topic shoppulse.events (JSON hoặc Avro — xem ingestion/avro/event.avsc)
EVENT_SCHEMA = StructType([StructField("event_id", StringType())] + RAW_SCHEMA.fields + [
    StructField("ingest_ts", LongType()),   # epoch millis lúc replayer gửi
])

ID_FIELDS = ("event_time", "event_type", "product_id", "user_id", "user_session")


def normalize_time_str(value: str) -> str:
    """'2019-10-01 00:00:04 UTC' -> '2019-10-01 00:00:04'."""
    return value.replace(" UTC", "").replace("T", " ").rstrip("Z")


def event_id_py(event_time: str, event_type: str, product_id, user_id, user_session) -> str:
    """Bản Python của event_id — phải khớp tuyệt đối với event_id_col()."""
    parts = [normalize_time_str(event_time), event_type, str(int(product_id)),
             str(int(user_id)), user_session or ""]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def normalized_time_col(col: str = "event_time") -> Column:
    return F.regexp_replace(F.regexp_replace(F.col(col), " UTC$", ""), "T|Z$", " ")


def event_id_col() -> Column:
    return F.sha1(F.concat_ws(
        "|",
        F.trim(normalized_time_col()),
        F.col("event_type"),
        F.col("product_id").cast("string"),
        F.col("user_id").cast("string"),
        F.coalesce(F.col("user_session"), F.lit("")),
    ))


def parse_event_time(col: str = "event_time") -> Column:
    return F.to_timestamp(F.trim(normalized_time_col(col)), "yyyy-MM-dd HH:mm:ss")
