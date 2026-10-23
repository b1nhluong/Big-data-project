"""[TV1] Bơm lỗi có kiểm soát vào luồng sự kiện để kiểm thử speed layer.

Vì biết trước dữ liệu gốc, ta đo được chính xác hệ thống xử lý lỗi thế nào:
  * duplicate  : gửi lại nguyên sự kiện (cùng event_id) — mô phỏng producer retry
  * late       : giữ sự kiện lại rồi gửi trễ 0..max_late_s giây (theo thời gian sự kiện)
                 -> vừa đến trễ vừa sai thứ tự; phần trễ > watermark sẽ bị Spark loại
Hàm thuần (không I/O) để kiểm thử đơn vị dễ dàng: tests/unit/test_chaos.py
"""
from __future__ import annotations

import heapq
import random
from dataclasses import dataclass, field
from typing import Iterable, Iterator


@dataclass
class ChaosConfig:
    duplicate_rate: float = 0.0
    late_rate: float = 0.0
    max_late_s: float = 900.0
    seed: int = 13


@dataclass
class ChaosStats:
    emitted: int = 0
    duplicates: int = 0
    delayed: int = 0
    max_delay_s: float = 0.0
    delays_s: list = field(default_factory=list)

    def as_dict(self) -> dict:
        beyond = lambda s: sum(1 for d in self.delays_s if d > s)  # noqa: E731
        return {"emitted": self.emitted, "duplicates": self.duplicates, "delayed": self.delayed,
                "max_delay_s": round(self.max_delay_s, 1),
                "delayed_beyond_600s": beyond(600), "delayed_beyond_300s": beyond(300)}


def inject(events: Iterable[dict], cfg: ChaosConfig, stats: ChaosStats | None = None,
           ts_key: str = "_ts") -> Iterator[dict]:
    """events: dict có khoá ts_key = epoch giây (float), đã sắp theo thời gian.

    Trả về cùng các dict (có thể lặp lại, có thể lệch thứ tự). Thêm khoá '_delay_s'
    cho sự kiện bị làm trễ để đo lường.
    """
    rnd = random.Random(cfg.seed)
    stats = stats if stats is not None else ChaosStats()
    held: list[tuple[float, int, dict]] = []
    counter = 0
    for ev in events:
        now = ev[ts_key]
        while held and held[0][0] <= now:        # thả các sự kiện đã "đến hạn"
            _, _, late_ev = heapq.heappop(held)
            stats.emitted += 1
            yield late_ev
        if cfg.late_rate and rnd.random() < cfg.late_rate:
            delay = rnd.uniform(1.0, cfg.max_late_s)
            late = dict(ev, _delay_s=round(delay, 1))
            counter += 1
            heapq.heappush(held, (now + delay, counter, late))
            stats.delayed += 1
            stats.delays_s.append(delay)
            stats.max_delay_s = max(stats.max_delay_s, delay)
            continue
        stats.emitted += 1
        yield ev
        if cfg.duplicate_rate and rnd.random() < cfg.duplicate_rate:
            stats.duplicates += 1
            stats.emitted += 1
            yield dict(ev)
    while held:
        _, _, late_ev = heapq.heappop(held)
        stats.emitted += 1
        yield late_ev
