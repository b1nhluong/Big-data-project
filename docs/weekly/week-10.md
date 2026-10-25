# Tuần 10 (30/11 – 06/12/2026) — Chaos test, chứng minh chịu lỗi

**Tag release:** `v0.10-chaos` · **Hạn nộp:** 23:59 Chủ nhật 06/12 · **Scrum master:** TV5 · **Review:** TV1→TV3, TV2→TV4, TV3→TV5, TV4→TV1, TV5→TV2 · **Liên hệ slide:** Ch.2 (fault tolerance), Ch.3.3 (re-replication), Ch.4.2–4.3 (CAP, quorum), Ch.5 (ISR), Ch.8 (exactly-once)

**Giờ ước tính:** TV1 5 · TV2 5 · TV3 6 · TV4 5 · TV5 6

> Code tham chiếu nằm trong repo tham chiếu (`ref`). Cách lấy file, chạy kiểm tra rồi tự commit: xem [docs/ke-hoach/porting-guide.md](../ke-hoach/porting-guide.md).

## Kế hoạch

### TV1 — Platform & Ingestion (~5 giờ)

- [ ] Tắt 1 broker Kafka khi đang ghi (ISR co lại, không mất tin); rút mạng 1 node k3s (5h)  
      Sản phẩm: `tests/chaos/kill_kafka_broker.sh`  
      Commit gợi ý: `test(chaos): kafka broker loss and node partition`

### TV2 — Batch Layer (~5 giờ)

- [ ] Tắt 1 DataNode: thời gian phát hiện, `hdfs fsck`, đọc vẫn chạy, tái nhân bản; job batch tự retry (5h)  
      Sản phẩm: `tests/chaos/kill_datanode.sh`  
      Commit gợi ý: `test(chaos): datanode loss and re-replication`

### TV3 — Speed Layer (~6 giờ)

- [ ] Kill driver/executor streaming → khôi phục từ checkpoint, đối soát khớp 100% (6h)  
      Sản phẩm: `tests/chaos/kill_streaming_driver.sh`  
      Commit gợi ý: `test(chaos): streaming driver kill and checkpoint recovery`

### TV4 — ML & Analytics (~5 giờ)

- [ ] Test tích hợp end-to-end sau sự cố (API, bảng gold, dashboard vẫn đúng) (3h)  
      Sản phẩm: `tests/integration/`  
      Commit gợi ý: `test(integration): end-to-end checks after failures`
- [ ] Quay và dựng video chaos 3 phút (2h)  
      Sản phẩm: Release `v0.10-chaos`  
      Commit gợi ý: (đính kèm Release)

### TV5 — Serving, Giám sát & Kiểm thử (~6 giờ)

- [ ] Cassandra: ONE vs QUORUM khi 2/3 node dừng; `nodetool repair` (3h)  
      Sản phẩm: `tests/chaos/cassandra_cap.sh`  
      Commit gợi ý: `test(chaos): cassandra consistency under node loss`
- [ ] Xác nhận cảnh báo Prometheus kích hoạt trong mỗi kịch bản; viết tổng hợp (3h)  
      Sản phẩm: `docs/chaos.md`  
      Commit gợi ý: `docs(chaos): five scenarios with alerts and measurements`

## Định nghĩa hoàn thành của tuần

- [ ] `docs/chaos.md` đủ 5 kịch bản (kỳ vọng, quan sát, số đo, ảnh); có video chaos 3 phút.

## Báo cáo tuần (điền trước khi gắn tag)

### Kết quả chính

- 

### Từng thành viên

| Thành viên | Đã làm (link PR) | Giờ thực tế | Tuần sau |
|---|---|---|---|
| TV1 |  |  |  |
| TV2 |  |  |  |
| TV3 |  |  |  |
| TV4 |  |  |  |
| TV5 |  |  |  |

### Khó khăn & cách xử lý (nguyên liệu cho lessons learned)

- 

### Số đo / ảnh chụp

- 
