"""[TV4] Gợi ý sản phẩm bằng ALS (collaborative filtering, implicit feedback — slide chương 10).

Phản hồi ngầm: không có điểm đánh giá, chỉ có hành vi -> độ tin cậy
    confidence(u, i) = log1p( Σ trọng số sự kiện )  với view=1, cart=3, purchase=5
Đánh giá chuẩn: chia theo thời gian (tuần cuối làm test), đo Precision@10, NDCG@10,
MAP@10 bằng RankingEvaluator, so với baseline "phổ biến nhất" (không cá nhân hoá).
Kết quả gold/recommendations -> Cassandra recs_by_user (serving/export_serving.py).
"""
from __future__ import annotations

import argparse
import logging

from pyspark.ml.evaluation import RankingEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shoppulse.config import load_config
from shoppulse.io import save_report, timer, write_table
from shoppulse.spark import get_spark, setup_logging

log = logging.getLogger("shoppulse.als")
K = 10


def interactions(events: DataFrame, users: DataFrame, weights: dict) -> DataFrame:
    w = F.lit(0.0)
    for etype, val in weights.items():
        if val > 0:
            w = F.when(F.col("event_type") == etype, F.lit(float(val))).otherwise(w)
    return (events.withColumn("w", w).filter(F.col("w") > 0)
            .join(users.select("user_key", "user_idx"), "user_key")
            .groupBy(F.col("user_idx").cast("int").alias("user"), F.col("product_id").cast("int").alias("item"))
            .agg(F.log1p(F.sum("w")).alias("confidence"), F.max("event_ts").alias("last_ts")))


def ground_truth(test: DataFrame) -> DataFrame:
    return test.groupBy("user").agg(F.collect_set(F.col("item").cast("double")).alias("label"))


def ranking_metrics(pred_lists: DataFrame, truth: DataFrame) -> dict:
    joined = truth.join(pred_lists, "user")
    out = {}
    for metric in ("precisionAtK", "ndcgAtK", "meanAveragePrecisionAtK", "recallAtK"):
        out[metric] = round(RankingEvaluator(predictionCol="prediction", labelCol="label",
                                             metricName=metric, k=K).evaluate(joined), 4)
    out["users_evaluated"] = joined.count()
    return out


def run(rank: int, reg: float, alpha: float, max_iter: int) -> dict:
    cfg = load_config()
    spark = get_spark("als", cfg, hive=False)
    events = spark.read.parquet(cfg.lake("silver", "events"))
    users = spark.read.parquet(cfg.lake("silver", "dim_user"))
    data = interactions(events, users, cfg.event_weights).cache()
    cutoff = data.select(F.expr("percentile_approx(cast(last_ts as long), 0.85)")).first()[0]
    train = data.filter(F.col("last_ts").cast("long") <= cutoff)
    test = data.filter(F.col("last_ts").cast("long") > cutoff)
    m: dict = {"interactions": data.count(), "train": train.count(), "test": test.count(),
               "params": {"rank": rank, "regParam": reg, "alpha": alpha, "maxIter": max_iter}}

    als = ALS(userCol="user", itemCol="item", ratingCol="confidence", implicitPrefs=True,
              rank=rank, regParam=reg, alpha=alpha, maxIter=max_iter, coldStartStrategy="drop",
              nonnegative=True, seed=42, checkpointInterval=5)
    with timer(m, "train_s"):
        model = als.fit(train)
    truth = ground_truth(test)
    test_users = truth.select("user")
    # loại sản phẩm user đã tương tác trong train (gợi ý cái mới)
    recs = model.recommendForUserSubset(test_users, K * 3)
    seen = train.groupBy("user").agg(F.collect_set("item").alias("seen"))
    recs = (recs.join(seen, "user", "left")
            .withColumn("items", F.expr(
                "slice(filter(transform(recommendations, r -> r.item), x -> seen is null or !array_contains(seen, x)), 1, 10)"))
            .select("user", F.expr("transform(items, x -> cast(x as double))").alias("prediction")))
    m["als_new_items_only"] = ranking_metrics(recs, truth)
    recs_all = model.recommendForUserSubset(test_users, K).select(
        "user", F.expr("transform(recommendations, r -> cast(r.item as double))").alias("prediction"))
    m["als"] = ranking_metrics(recs_all, truth)

    popular = [float(r["item"]) for r in train.groupBy("item").agg(F.sum("confidence").alias("s"))
               .orderBy(F.desc("s")).limit(K).collect()]
    base = test_users.join(recs_all.select("user"), "user").withColumn("prediction", F.array(*[F.lit(x) for x in popular]))
    m["popularity_baseline"] = ranking_metrics(base, truth)
    m["ndcg_lift_vs_baseline"] = round(m["als"]["ndcgAtK"] / max(m["popularity_baseline"]["ndcgAtK"], 1e-9), 2)

    # Mô hình cuối: huấn luyện trên toàn bộ dữ liệu, gợi ý cho mọi user
    with timer(m, "final_train_s"):
        final = als.fit(data)
    all_recs = (final.recommendForAllUsers(K)
                .select("user", F.posexplode("recommendations").alias("pos", "rec"))
                .select(F.col("user").alias("user_idx"), (F.col("pos") + 1).alias("rank"),
                        F.col("rec.item").cast("long").alias("product_id"),
                        F.round(F.col("rec.rating"), 5).alias("score"))
                .join(users.select("user_key", "user_idx"), "user_idx"))
    write_table(all_recs, cfg.lake("gold", "recommendations"))
    # "Người mua cái này cũng thích": độ tương đồng cosine giữa item factors (top 10 hàng xóm)
    factors = final.itemFactors.select(F.col("id").alias("product_id"), "features")
    write_table(factors, cfg.lake("gold", "item_factors"))
    final.write().overwrite().save(cfg.model("als"))
    m["users_with_recs"] = spark.read.parquet(cfg.lake("gold", "recommendations")).select("user_key").distinct().count()
    data.unpersist()
    save_report(spark, cfg, "ml_als", m)
    return m


def main() -> None:
    setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank", type=int, default=32)
    ap.add_argument("--reg", type=float, default=0.05)
    ap.add_argument("--alpha", type=float, default=20.0)
    ap.add_argument("--max-iter", type=int, default=10)
    a = ap.parse_args()
    run(a.rank, a.reg, a.alpha, a.max_iter)


if __name__ == "__main__":
    main()
