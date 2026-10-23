"""Sinh dữ liệu mẫu cùng định dạng REES46 để phát triển/kiểm thử khi chưa tải bộ thật.

Dữ liệu giả lập có các đặc tính giống bộ thật (và các "lỗi" để kiểm thử):
  * phân phối độ phổ biến sản phẩm lệch (Zipf) -> skew khi join
  * ~30% category_code và ~15% brand bị thiếu
  * phễu view -> cart -> purchase, remove_from_cart
  * bot: phiên hàng trăm lượt xem cách nhau < 1 giây
  * giá = 0 ở một số dòng, dòng trùng lặp hoàn toàn
  * ngày flash sale có lưu lượng tăng vọt

Ví dụ:
    python -m ingestion.generate_sample --events 300000 --days 14 --out data/raw
"""
from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

CATEGORIES = [
    "electronics.smartphone", "electronics.audio.headphone", "electronics.video.tv",
    "electronics.clocks", "electronics.tablet", "computers.notebook", "computers.desktop",
    "computers.peripherals.printer", "appliances.kitchen.refrigerators",
    "appliances.kitchen.washer", "appliances.environment.vacuum", "appliances.iron",
    "apparel.shoes", "apparel.shoes.sneakers", "apparel.costume", "furniture.living_room.sofa",
    "furniture.bedroom.bed", "kids.toys", "kids.carriage", "sport.bicycle", "sport.tennis",
    "construction.tools.drill", "auto.accessories.player", "accessories.bag",
    "country_yard.lawn_mower", "medicine.tools.tonometer", "stationery.cartrige",
]
BRANDS = ["samsung", "apple", "xiaomi", "huawei", "lg", "sony", "lenovo", "hp", "acer",
          "asus", "bosch", "philips", "nike", "adidas", "puma", "lego", "indesit", "beko",
          "canon", "tefal"] + [f"brand{i:03d}" for i in range(180)]
HOUR_WEIGHTS = np.array([1, 0.6, 0.4, 0.3, 0.3, 0.5, 1, 2, 3, 4, 4.5, 5, 5, 5, 5, 5, 5.5, 6,
                         6.5, 7, 7, 6, 4, 2], dtype=float)


def build_catalog(rng: np.random.Generator, n_products: int) -> pd.DataFrame:
    cat_idx = rng.integers(0, len(CATEGORIES), n_products)
    brand_idx = np.minimum(rng.zipf(1.3, n_products) - 1, len(BRANDS) - 1)
    base_price = np.round(np.exp(rng.normal(4.3, 1.1, n_products)), 2)
    catalog = pd.DataFrame({
        "product_id": 1_000_000 + np.arange(n_products),
        "category_id": 2_053_013_550_000_000_000 + cat_idx * 1_000,
        "category_code": [CATEGORIES[i] for i in cat_idx],
        "brand": [BRANDS[i] for i in brand_idx],
        "price": base_price,
    })
    catalog.loc[rng.random(n_products) < 0.30, "category_code"] = None
    catalog.loc[rng.random(n_products) < 0.15, "brand"] = None
    return catalog


def generate(n_events: int, days: int, start: datetime, seed: int,
             n_users: int | None = None, n_products: int | None = None) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n_users = n_users or max(1_000, n_events // 25)
    n_products = n_products or max(500, n_events // 60)
    catalog = build_catalog(rng, n_products)
    popularity = 1.0 / np.arange(1, n_products + 1) ** 0.9       # Zipf
    popularity /= popularity.sum()
    by_cat = {c: np.flatnonzero(catalog["category_id"].to_numpy() == c)
              for c in catalog["category_id"].unique()}
    user_ids = 500_000_000 + rng.permutation(n_users * 3)[:n_users]
    # Mỗi user có 2 danh mục yêu thích -> dữ liệu có tín hiệu cá nhân hoá cho ALS
    cat_keys = list(by_cat.keys())
    user_pref = {int(u): (cat_keys[int(rng.integers(len(cat_keys)))], cat_keys[int(rng.integers(len(cat_keys)))])
                 for u in user_ids}
    cat_cdf = {c: np.cumsum(popularity[idx]) / popularity[idx].sum() for c, idx in by_cat.items()}

    def popular_in(cat) -> int:
        idx = by_cat[cat]
        return int(idx[min(np.searchsorted(cat_cdf[cat], rng.random()), len(idx) - 1)])
    user_activity = 1.0 / np.arange(1, n_users + 1) ** 0.6
    user_activity /= user_activity.sum()
    bot_users = set(rng.choice(user_ids, size=max(2, n_users // 200), replace=False).tolist())
    flash_days = set(rng.choice(days, size=max(1, days // 7), replace=False).tolist())
    day_weights = np.array([3.0 if d in flash_days else 1.0 for d in range(days)])
    day_weights /= day_weights.sum()
    hour_p = HOUR_WEIGHTS / HOUR_WEIGHTS.sum()

    rows: list[tuple] = []
    prod_ids = catalog["product_id"].to_numpy()
    cat_ids = catalog["category_id"].to_numpy()
    codes = catalog["category_code"].to_numpy()
    brands = catalog["brand"].to_numpy()
    prices = catalog["price"].to_numpy()

    # Rút mẫu hàng loạt một lần (nhanh hơn nhiều so với rng.choice(p=...) trong vòng lặp)
    n_sessions = n_events // 3 + 10
    s_users = rng.choice(user_ids, size=n_sessions, p=user_activity)
    s_days = rng.choice(days, size=n_sessions, p=day_weights)
    s_hours = rng.choice(24, size=n_sessions, p=hour_p)
    pop_stream = rng.choice(n_products, size=n_events * 2, p=popularity)
    pop_ptr = 0

    def popular(k: int) -> np.ndarray:
        nonlocal pop_ptr, pop_stream
        if pop_ptr + k > len(pop_stream):
            pop_stream = rng.choice(n_products, size=max(n_events, k), p=popularity)
            pop_ptr = 0
        out = pop_stream[pop_ptr:pop_ptr + k]
        pop_ptr += k
        return out

    s = 0
    while len(rows) < n_events:
        if s >= n_sessions:
            s = 0
        uid, day, hour = int(s_users[s]), int(s_days[s]), int(s_hours[s])
        s += 1
        t = start + timedelta(days=day, hours=hour, seconds=int(rng.integers(0, 3600)))
        session = str(uuid.UUID(int=(int(rng.integers(0, 2**63)) << 64) | int(rng.integers(0, 2**63))))
        is_bot = uid in bot_users and rng.random() < 0.7
        if is_bot:
            n = int(rng.integers(350, 600))
            for i in popular(n):
                t += timedelta(milliseconds=int(rng.integers(150, 700)))
                rows.append((t, "view", int(i), uid, session))
            continue
        if rng.random() < 0.7:
            first = popular_in(user_pref[uid][int(rng.integers(2))])
        else:
            first = int(popular(1)[0])
        n_views = int(rng.geometric(1 / 6))
        viewed = [first] + [popular_in(cat_ids[first]) if rng.random() < 0.75 else int(popular(1)[0])
                            for _ in range(n_views - 1)]
        for i in viewed:
            t += timedelta(seconds=float(rng.exponential(40)) + 1)
            rows.append((t, "view", i, uid, session))
        purchase_boost = 1.6 if day in flash_days else 1.0
        for i in dict.fromkeys(viewed):
            if rng.random() < 0.12:
                t += timedelta(seconds=float(rng.exponential(30)) + 1)
                rows.append((t, "cart", i, uid, session))
                if rng.random() < 0.08:
                    t += timedelta(seconds=float(rng.exponential(20)) + 1)
                    rows.append((t, "remove_from_cart", i, uid, session))
                elif rng.random() < 0.42 * purchase_boost:
                    t += timedelta(seconds=float(rng.exponential(90)) + 5)
                    rows.append((t, "purchase", i, uid, session))

    rows = rows[:n_events]
    ts, etype, pidx, uid, sess = zip(*rows)
    pidx = np.asarray(pidx)
    df = pd.DataFrame({
        "event_time": pd.to_datetime(list(ts), utc=True),
        "event_type": etype,
        "product_id": prod_ids[pidx],
        "category_id": cat_ids[pidx],
        "category_code": codes[pidx],
        "brand": brands[pidx],
        "price": np.round(prices[pidx] * rng.uniform(0.9, 1.1, len(pidx)), 2),
        "user_id": uid,
        "user_session": sess,
    })
    df.loc[rng.random(len(df)) < 0.002, "price"] = 0.0
    dups = df.sample(frac=0.003, random_state=seed)
    df = pd.concat([df, dups], ignore_index=True).sort_values("event_time", kind="stable")
    return df.reset_index(drop=True)


def write_monthly_csv(df: pd.DataFrame, out_dir: str) -> list[str]:
    os.makedirs(out_dir, exist_ok=True)
    df = df.copy()
    month_key = df["event_time"].dt.strftime("%Y-%b")
    df["event_time"] = df["event_time"].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    paths = []
    for key, part in df.groupby(month_key, sort=False):
        path = os.path.join(out_dir, f"{key}.csv")
        part.to_csv(path, index=False)
        paths.append(path)
    return paths


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--events", type=int, default=300_000)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--start", default="2019-10-01")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/raw")
    args = ap.parse_args()
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    df = generate(args.events, args.days, start, args.seed)
    paths = write_monthly_csv(df, args.out)
    print(f"Đã sinh {len(df):,} sự kiện -> {paths}")
    print(df["event_type"].value_counts().to_string())


if __name__ == "__main__":
    main()
