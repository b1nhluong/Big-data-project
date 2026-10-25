import numpy as np
import pytest

from shoppulse.schemas import event_id_py
from shoppulse.udfs import (gini_py, price_bucket, price_bucket_py, segment_label_py, split_category,
                            split_category_py)


def test_event_id_python_matches_spark(spark):
    from shoppulse.schemas import event_id_col
    rows = [("2019-10-01 00:00:04 UTC", "view", 44600062, 541312140, "72d76fde-8bb3"),
            ("2019-10-01T00:00:04Z", "view", 44600062, 541312140, "72d76fde-8bb3"),
            ("2019-11-30 23:59:59 UTC", "purchase", 1, 2, None)]
    df = spark.createDataFrame(rows, "event_time string, event_type string, product_id long, "
                                     "user_id long, user_session string")
    got = [r[0] for r in df.select(event_id_col()).collect()]
    assert got == [event_id_py(*r) for r in rows]
    assert got[0] == got[1], "hai định dạng thời gian của cùng sự kiện phải cho cùng id"


@pytest.mark.parametrize("code,expected", [
    ("electronics.smartphone", ("electronics", "smartphone", "unknown")),
    ("appliances.kitchen.washer", ("appliances", "kitchen", "washer")),
    ("a.b.c.d", ("a", "b", "c.d")),
    (None, ("unknown", "unknown", "unknown")),
    ("", ("unknown", "unknown", "unknown")),
])
def test_split_category_py(code, expected):
    assert split_category_py(code) == expected


@pytest.mark.parametrize("price,label", [(0, "invalid"), (-1, "invalid"), (None, "invalid"),
                                         (9.99, "<10"), (10, "10-50"), (299.9, "100-300"), (5000, ">=1000")])
def test_price_bucket_py(price, label):
    assert price_bucket_py(price) == label


def test_pandas_udfs_match_python(spark):
    prices = [0.0, 5.0, 10.0, 49.99, 50.0, 150.0, 999.0, 1000.0, None]
    codes = ["electronics.audio.headphone", None, "kids.toys"] * 3
    df = spark.createDataFrame(list(zip(prices, codes)), "price double, code string")
    out = df.select(price_bucket("price").alias("b"), split_category("code").alias("c")).collect()
    assert [r.b for r in out] == [price_bucket_py(p) for p in prices]
    assert [(r.c.l1, r.c.l2, r.c.l3) for r in out] == [split_category_py(c) for c in codes]


def test_gini():
    assert gini_py(np.array([5, 5, 5, 5])) == pytest.approx(0.0)
    assert gini_py(np.array([0, 0, 0, 100])) == pytest.approx(0.75)
    assert gini_py(np.array([])) == 0.0


def test_segment_labels():
    assert segment_label_py(5, 5, 5) == "champions"
    assert segment_label_py(1, 1, 1) == "hibernating"
    assert segment_label_py(5, 1, 2) == "new_customers"
    assert segment_label_py(1, 4, 4) == "at_risk"
