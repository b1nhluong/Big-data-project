"""Luật chất lượng dữ liệu dùng CHUNG cho batch (silver) và streaming.

Bài học từ đối soát Lambda (docs/lessons/09): nếu speed layer không áp cùng luật với
batch layer (ví dụ đơn mua giá 0), hai lớp sẽ lệch nhau dù code tổng hợp đúng.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

from shoppulse.schemas import EVENT_TYPES


def dq_reason() -> Column:
    """null = hợp lệ; ngược lại là mã lỗi (thứ tự = mức nghiêm trọng)."""
    return (F.when(F.col("event_ts").isNull(), "bad_timestamp")
             .when(~F.col("event_type").isin(*EVENT_TYPES), "bad_event_type")
             .when(F.col("product_id").isNull() | F.col("user_id").isNull(), "missing_id")
             .when(F.col("price").isNull() | (F.col("price") < 0), "bad_price")
             .when((F.col("event_type") == "purchase") & (F.col("price") == 0), "zero_price_purchase"))


def is_valid() -> Column:
    return dq_reason().isNull()
