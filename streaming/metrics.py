"""[TV3] Xuất tiến độ Structured Streaming ra Prometheus (giám sát — lessons learned #7).

StreamingQueryListener (PySpark >= 3.4) nhận sự kiện onQueryProgress sau mỗi
micro-batch; ta cập nhật các gauge và mở endpoint HTTP :9108/metrics cho Prometheus
(PodMonitor trong infra/k8s/monitoring/podmonitors.yaml).
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

log = logging.getLogger("shoppulse.streaming.metrics")

try:
    from prometheus_client import Counter, Gauge, start_http_server
except ImportError:  # prometheus_client là tuỳ chọn khi chạy local
    Gauge = Counter = start_http_server = None  # type: ignore[assignment]

_G: dict = {}
_LISTENERS: list = []


def _gauges():
    if Gauge is None:
        return None
    if not _G:
        labels = ["query"]
        _G.update({
            "input_rps": Gauge("shoppulse_stream_input_rows_per_second", "Tốc độ vào", labels),
            "processed_rps": Gauge("shoppulse_stream_processed_rows_per_second", "Tốc độ xử lý", labels),
            "batch_ms": Gauge("shoppulse_stream_batch_duration_ms", "Thời gian micro-batch", labels),
            "input_rows": Gauge("shoppulse_stream_batch_input_rows", "Số dòng trong batch", labels),
            "state_rows": Gauge("shoppulse_stream_state_rows", "Số dòng state", labels),
            "state_bytes": Gauge("shoppulse_stream_state_memory_bytes", "Bộ nhớ state", labels),
            "dropped": Counter("shoppulse_stream_rows_dropped_by_watermark_total",
                               "Số sự kiện đến trễ quá watermark bị loại", labels),
            "watermark_lag": Gauge("shoppulse_stream_watermark_lag_seconds",
                                   "Độ trễ watermark so với thời gian thực", labels),
            "batch_id": Gauge("shoppulse_stream_batch_id", "batch id mới nhất", labels),
        })
    return _G


class PrometheusProgressListener(StreamingQueryListener):
    def onQueryStarted(self, event) -> None:
        log.info("Query bắt đầu: %s (%s)", event.name, event.id)

    def onQueryProgress(self, event) -> None:
        p = event.progress
        g = _gauges()
        name = p.name or str(p.id)
        state_rows = sum(s.numRowsTotal for s in p.stateOperators)
        dropped = sum(s.numRowsDroppedByWatermark for s in p.stateOperators)
        log.info("[%s] batch=%s rows=%s in/s=%.1f proc/s=%.1f state=%s dropped=%s",
                 name, p.batchId, p.numInputRows, p.inputRowsPerSecond or 0,
                 p.processedRowsPerSecond or 0, state_rows, dropped)
        if g is None:
            return
        g["input_rps"].labels(name).set(p.inputRowsPerSecond or 0)
        g["processed_rps"].labels(name).set(p.processedRowsPerSecond or 0)
        g["batch_ms"].labels(name).set(p.batchDuration)
        g["input_rows"].labels(name).set(p.numInputRows)
        g["state_rows"].labels(name).set(state_rows)
        g["state_bytes"].labels(name).set(sum(s.memoryUsedBytes for s in p.stateOperators))
        if dropped:
            g["dropped"].labels(name).inc(dropped)
        g["batch_id"].labels(name).set(p.batchId)
        wm = (json.loads(p.json).get("eventTime") or {}).get("watermark")
        if wm:
            wm_ts = datetime.fromisoformat(wm.replace("Z", "+00:00")).timestamp()
            g["watermark_lag"].labels(name).set(max(time.time() - wm_ts, 0))

    def onQueryIdle(self, event) -> None:  # Spark >= 3.5
        pass

    def onQueryTerminated(self, event) -> None:
        log.warning("Query dừng: %s exception=%s", event.id, event.exception)


def install(spark: SparkSession, port: int | None) -> None:
    if port and start_http_server is not None:
        try:
            start_http_server(port)
            log.info("Prometheus metrics tại :%s/metrics", port)
        except OSError as exc:
            log.warning("Không mở được cổng metrics %s: %s", port, exc)
    listener = PrometheusProgressListener()
    spark.streams.addListener(listener)
    _LISTENERS.append(listener)


def uninstall(spark: SparkSession) -> None:
    while _LISTENERS:
        spark.streams.removeListener(_LISTENERS.pop())
