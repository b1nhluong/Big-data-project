# ADR-0006: Spark 3.5.7 + Java 17

**Trạng thái:** Chấp nhận · **Người viết:** TV1 · **Tuần:** 2

## Bối cảnh
Spark 4.x đã phát hành, nhưng các connector cần dùng (Cassandra, Kafka, GraphFrames) và image có sẵn quyết định phiên bản thực tế dùng được.

## Quyết định
- **Spark 3.5.7 / Scala 2.12**: có `dropDuplicatesWithinWatermark`, `applyInPandasWithState`, `DataFrame.unpivot` và StreamingQueryListener cho Python. Spark Cassandra Connector 3.5.1 và GraphFrames 0.12.2 (`graphframes-spark3_2.12`) đều hỗ trợ bản này.
- **Java 17**: Spark 3.5 với Java 21 làm pandas UDF lỗi (Arrow), xem lesson 05.
- Nâng lên Spark 4 được để lại làm hướng phát triển (có `transformWithStateInPandas`).
