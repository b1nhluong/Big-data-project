#!/usr/bin/env bash
# Chạy TOÀN BỘ pipeline ShopPulse trên 1 máy (profile local) với dữ liệu mẫu hoặc thật.
# Không cần Kafka/HDFS/Cassandra: streaming đọc thư mục JSON do replayer ghi, sink ghi Parquet.
#   bash scripts/run_local_pipeline.sh            # sinh 300k sự kiện mẫu, 28 ngày
#   EVENTS=0 bash scripts/run_local_pipeline.sh   # dùng file CSV có sẵn trong data/raw
set -euo pipefail
cd "$(dirname "$0")/.."
export SHOPPULSE_PROFILE=local PYTHONPATH=$PWD
EVENTS=${EVENTS:-300000}
DAYS=${DAYS:-28}
STREAM_EVENTS=${STREAM_EVENTS:-60000}
step() { echo; echo "=================== $* ==================="; }

if [ "$EVENTS" != "0" ]; then
  step "0. Sinh dữ liệu mẫu ($EVENTS sự kiện, $DAYS ngày)"
  # Bảo vệ dữ liệu thật: bước này xoá CSV trong data/raw để thay bằng dữ liệu mẫu.
  # Nếu thấy file > 200 MB (dữ liệu REES46 thật) thì dừng, không xoá.
  if find data/raw -maxdepth 1 -name '*.csv' -size +200M 2>/dev/null | grep -q .; then
    echo "data/raw đang chứa CSV thật (> 200 MB). Không xoá." >&2
    echo "Dùng EVENTS=0 để chạy trên dữ liệu thật, hoặc chuyển dữ liệu thật ra ngoài data/raw." >&2
    exit 1
  fi
  find data/raw -maxdepth 1 -name '*.csv' -delete 2>/dev/null || true
  python -m ingestion.generate_sample --events "$EVENTS" --days "$DAYS" --out data/raw
fi
python -m ingestion.promotions generate --days "$DAYS"

step "1. Ingestion: CSV -> bronze Parquet; PostgreSQL(CSV) -> dim_promotion"
python -m ingestion.csv_to_parquet
python -m ingestion.promotions load --from-csv

step "2. Speed layer: replayer (bơm lỗi) -> 4 job streaming"
rm -rf data/stream_in data/checkpoints data/lake/serving data/lake/bronze/events_stream
python -m ingestion.replayer --sink files --speed 0 --max-events "$STREAM_EVENTS" \
  --duplicate-rate 0.01 --late-rate 0.02 --max-late-s 900 --roll-every 2000
for job in bronze_sink kpi_minute trending; do
  python -m streaming.$job --source files --available-now --max-files-per-trigger 1
done

step "3. Batch layer: silver -> gold"
python -m batch.silver
python -m batch.gold_kpis
python -m batch.gold_customers
python -m batch.gold_products

step "4. Analytics & ML"
python -m ml.purchase_propensity
python -m ml.als_recommender
python -m ml.segmentation
python -m ml.product_graph --engine auto
python -m ml.timeseries
python -m ml.stats

step "5. Streaming có trạng thái + chấm điểm bằng mô hình vừa huấn luyện"
python -m streaming.sessions --source files --available-now --max-files-per-trigger 2

step "6. Lambda: đối soát speed vs batch"
python -m batch.reconciliation

step "7. Thí nghiệm hiệu năng + MapReduce vs Spark"
python -m batch.optimization --repeat 3
python -m batch.format_benchmark
python -m batch.rdd_vs_dataframe
python -m mapreduce.export_input
out=$(bash mapreduce/run_local.sh); echo "$out"
python -m mapreduce.compare --mr-output data/lake/mapreduce/output_local/pairs.tsv --mr-seconds "$(echo "$out" | awk '{print $4}')"

step "8. Tổng hợp báo cáo benchmark -> docs/results.md"
python -m benchmarks.report
echo "XONG. Kết quả: benchmarks/results/*.json, docs/results.md"
