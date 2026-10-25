"""[TV1] Sinh DDL Trino (serving/trino/tables.sql) từ schema Parquet thực tế của lake.

Tránh gõ tay kiểu dữ liệu sai lệch với file. Chạy sau khi pipeline đã tạo bảng:
    python -m serving.trino.generate_ddl
"""
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow as pa

from shoppulse.config import load_config

TABLES = {  # tên bảng -> (tầng, cột partition)
    "events": ("silver", "event_date"), "dim_product": ("silver", None),
    "daily_kpis": ("gold", "event_date"), "funnel_long": ("gold", None),
    "hourly_heatmap": ("gold", None), "promo_effect": ("gold", None),
    "revenue_trend": ("gold", None), "top_products": ("gold", None),
    "brand_concentration": ("gold", None), "customer_rfm": ("gold", None),
    "customer_clusters": ("gold", None), "sessions": ("gold", "session_date"),
    "product_pagerank": ("gold", None), "revenue_timeseries": ("gold", None),
    "revenue_forecast": ("gold", None), "reconciliation": ("gold", None),
}
HDFS_LAKE = "hdfs://hdfs-namenode.storage.svc.cluster.local:8020/shoppulse/lake"


def trino_type(t: pa.DataType) -> str:
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_dictionary(t):
        return "varchar"
    if pa.types.is_int64(t):
        return "bigint"
    if pa.types.is_int32(t):
        return "integer"
    if pa.types.is_floating(t):
        return "double"
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_timestamp(t):
        return "timestamp(3)"
    if pa.types.is_date(t):
        return "date"
    if pa.types.is_list(t):
        return f"array({trino_type(t.value_type)})"
    if pa.types.is_struct(t):
        return "row(" + ", ".join(f"{f.name} {trino_type(f.type)}" for f in t) + ")"
    raise TypeError(t)


def main() -> None:
    cfg = load_config("local")
    out = ["-- Sinh tự động bởi serving/trino/generate_ddl.py — KHÔNG sửa tay",
           f"CREATE SCHEMA IF NOT EXISTS hive.shoppulse WITH (location = '{HDFS_LAKE}');", ""]
    for name, (layer, part) in TABLES.items():
        path = cfg.lake(layer, name)
        if not Path(path).exists():
            continue
        schema = ds.dataset(path, format="parquet", partitioning="hive").schema
        cols = [f for f in schema if f.name != part] + ([schema.field(part)] if part else [])
        body = ",\n".join(f"  {f.name} {'date' if f.name == part else trino_type(f.type)}" for f in cols)
        props = [f"external_location = '{HDFS_LAKE}/{layer}/{name}'", "format = 'PARQUET'"]
        if part:
            props.append(f"partitioned_by = ARRAY['{part}']")
        out.append(f"CREATE TABLE IF NOT EXISTS hive.shoppulse.{name} (\n{body}\n) WITH (\n  "
                   + ",\n  ".join(props) + "\n);")
        if part:
            out.append(f"CALL hive.system.sync_partition_metadata('shoppulse', '{name}', 'FULL');")
        out.append("")
    target = Path(__file__).with_name("tables.sql")
    target.write_text("\n".join(out), encoding="utf-8")
    print(f"Đã ghi {target}")


if __name__ == "__main__":
    main()
