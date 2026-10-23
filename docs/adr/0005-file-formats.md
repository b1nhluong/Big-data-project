# ADR-0005: Avro cho Kafka, Parquet cho data lake

**Trạng thái:** Chấp nhận · **Người viết:** TV4 · **Tuần:** 1 (xác nhận bằng số đo ở tuần 9)

## Bối cảnh
Slide 3.3 so sánh Text, SequenceFile, Avro, Parquet và ORC. Hai nơi cần chọn định dạng: thông điệp trên Kafka (từng bản ghi) và data lake (quét phân tích).

## Quyết định
- **Kafka: Avro** (`ingestion/avro/event.avsc`). Là định dạng dòng, nhị phân gọn, có schema và quy tắc tiến hoá schema (chỉ thêm trường có default). JSON được giữ làm tuỳ chọn để debug (`--format json`).
- **Lake: Parquet + Snappy**, partition theo ngày. Số đo local: dung lượng bằng 25% CSV, quét nhanh hơn 3,1×. ORC nén tốt hơn một chút (16%) nhưng Parquet được hệ sinh thái Python/Trino/Spark hỗ trợ đồng đều hơn.
- **Không dùng CSV/JSON trong lake**: không nén theo cột, không có schema, quét chậm.
