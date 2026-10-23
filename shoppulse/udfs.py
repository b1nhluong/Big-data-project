"""UDF nghiệp vụ (domain-specific) và UDAF tuỳ biến.

Có cả bản Python UDF (từng dòng) và pandas UDF (vector hoá qua Arrow) cho cùng
một logic để so sánh hiệu năng trong batch/optimization.py.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

CATEGORY_STRUCT = StructType([
    StructField("l1", StringType()),
    StructField("l2", StringType()),
    StructField("l3", StringType()),
])

PRICE_EDGES = [0.0, 10.0, 50.0, 100.0, 300.0, 1000.0]
PRICE_LABELS = ["<10", "10-50", "50-100", "100-300", "300-1000", ">=1000"]


# ----------------------------------------------------------------- pure python
def split_category_py(code: str | None) -> tuple[str, str, str]:
    """'electronics.audio.headphone' -> ('electronics','audio','headphone')."""
    if not code or not isinstance(code, str):
        return ("unknown", "unknown", "unknown")
    parts = [p.strip().lower() for p in code.split(".") if p.strip()]
    parts = (parts + ["unknown"] * 3)[:3]
    if len(code.split(".")) > 3:           # gộp phần dư vào cấp 3
        parts[2] = ".".join(code.lower().split(".")[2:])
    return tuple(parts)  # type: ignore[return-value]


def price_bucket_py(price: float | None) -> str:
    if price is None or (isinstance(price, float) and np.isnan(price)) or price <= 0:
        return "invalid"
    for edge, label in zip(PRICE_EDGES[1:], PRICE_LABELS[:-1]):
        if price < edge:
            return label
    return PRICE_LABELS[-1]


def segment_label_py(r: int, f: int, m: int) -> str:
    """Luật phân khúc RFM (điểm 1–5, 5 = tốt nhất)."""
    if r is None or f is None or m is None:
        return "unknown"
    if r >= 4 and f >= 4 and m >= 4:
        return "champions"
    if r >= 3 and f >= 3:
        return "loyal"
    if r >= 4 and f <= 2:
        return "new_customers"
    if r <= 2 and f >= 3:
        return "at_risk"
    if r <= 2 and f <= 2 and m >= 3:
        return "cant_lose"
    if r <= 2:
        return "hibernating"
    return "potential"


def gini_py(values: np.ndarray) -> float:
    """Hệ số Gini (0 = đều, 1 = tập trung) của phân phối doanh thu."""
    x = np.sort(np.asarray(values, dtype=float))
    x = x[~np.isnan(x)]
    if x.size == 0 or x.sum() == 0:
        return 0.0
    n = x.size
    cum = np.cumsum(x)
    return float((n + 1 - 2 * (cum / cum[-1]).sum()) / n)


# ----------------------------------------------------------------- spark UDFs
@F.pandas_udf(CATEGORY_STRUCT)
def split_category(codes: pd.Series) -> pd.DataFrame:
    rows = [split_category_py(c) for c in codes]
    return pd.DataFrame(rows, columns=["l1", "l2", "l3"])


@F.pandas_udf(StringType())
def price_bucket(prices: pd.Series) -> pd.Series:
    labels = pd.cut(prices, bins=PRICE_EDGES + [np.inf], labels=PRICE_LABELS, right=False)
    out = labels.astype(object).where(prices > 0, "invalid")
    return out.fillna("invalid").astype(str)


# Bản Python UDF thuần (chậm hơn) — chỉ dùng để benchmark
price_bucket_rowwise = F.udf(price_bucket_py, StringType())
segment_label = F.udf(segment_label_py, StringType())


@F.pandas_udf(DoubleType())
def gini_udaf(values: pd.Series) -> float:
    """UDAF: GROUP BY brand -> Gini của doanh thu theo sản phẩm."""
    return gini_py(values.to_numpy())


@F.pandas_udf(DoubleType())
def median_udaf(values: pd.Series) -> float:
    return float(values.median()) if len(values) else float("nan")
