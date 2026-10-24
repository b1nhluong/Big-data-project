"""Render khuôn airflow/dags/templates/spark_batch.yaml để chạy tay 1 job batch (không qua Airflow).

    python scripts/render_spark_app.py --job silver --script batch/silver.py --args "--dates 2019-11-15" | kubectl apply -f -
"""
import argparse
import json
import shlex
import time
from pathlib import Path

TEMPLATE = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "templates" / "spark_batch.yaml"

ap = argparse.ArgumentParser()
ap.add_argument("--job", required=True)
ap.add_argument("--script", required=True)
ap.add_argument("--args", default="")
ap.add_argument("--executors", type=int, default=4)
a = ap.parse_args()
text = TEMPLATE.read_text(encoding="utf-8")
subs = {
    "{{ params.job }}": a.job, "{{ params.script }}": a.script,
    "{{ params.args | tojson }}": json.dumps(shlex.split(a.args)),
    "{{ params.executors | default(4) }}": str(a.executors),
    "{{ ds_nodash }}": time.strftime("%Y%m%d"), "{{ ti.try_number }}": str(int(time.time()) % 100000),
}
for k, v in subs.items():
    text = text.replace(k, v)
print(text)
