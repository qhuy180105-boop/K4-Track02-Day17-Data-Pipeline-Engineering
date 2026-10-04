# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Quang Huy
**Repo:** https://github.com/qhuy180105-boop/K4-Track02-Day17-Data-Pipeline-Engineering
**Commit bài nộp:** 1fbac5a3d2074d7212ed14a30e0c179a66470653
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity AI — Hỗ trợ phân tích triệu chứng lỗi, kiểm thử pipeline, triển khai MERGE logic trong DuckDB, dbt microbatch và viết báo cáo.
**Nguồn tham khảo khác (nếu có):** Slide bài giảng Day 17 Data Pipeline Engineering, dbt Core & DuckDB documentation.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `test_silver_tickets_one_row_per_ticket` và `latest_state_wins` fail (silver_tickets có 24 hàng cho 12 tickets; T-91 chứa cả 3 bản ghi lịch sử). | `test_late_events_land_in_their_event_day` fail (sự kiện u05 ngày 08-12 đến muộn ngày 08-15 chỉ đếm (2, 1, 0) thay vì (5, 3, 1)); `LOOKBACK_DAYS=0 < 3`. | `test_cdc_delete_becomes_tombstone` fail (T-97 bị xoá trong CDC nhưng `is_deleted` vẫn là `False` và còn PII); T-97 còn trong training set mới nhất & RAG. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT INTO` làm mỗi batch chèn thêm hàng mới vào `silver_tickets`, làm lặp hàng và làm replay batch cũ ghi đè dữ liệu. | `LOOKBACK_DAYS` trong `config.py` đặt bằng 0 nên daily job chỉ tính toán duy nhất ngày ingest hiện tại, bỏ qua các event đến muộn của ngày trước đó. | `ticket_changes_sql` lấy `ticket_id` từ `after->>'ticket_id'`. Khi CDC delete (`op='d'`), `after` bị NULL khiến `ticket_id` NULL và bị lọc bởi `WHERE ticket_id IS NOT NULL`. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: Dùng `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`. | `pipeline/config.py`: Thay `LOOKBACK_DAYS = 0` bằng `LOOKBACK_DAYS = 3` (tương ứng `ceil(P99 lateness)` đo từ Bronze). | `pipeline/staging.py`: Sửa lấy khoá bằng `coalesce(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id', j->'key'->>'ticket_id')`. |
| **Khái niệm trên slide** | Primary Key Deduplication / Merge Upsert Strategy, Incremental CDC Processing. | Event Time vs Ingestion Time, Late-arriving Data, Overwrite Partition with Lookback Window. | CDC Deleting & Tombstone Records, Right to be Forgotten (GDPR) / Soft Delete propagation. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (P50: 0.00, P95: 2.90) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` quản lý trạng thái hiện tại của thực thể theo khoá, trong khi `gold_feature_daily` tính toán tổng hợp theo ngày (event_date) nên ghi đè partition theo cửa sổ lookback giúp tối ưu hiệu năng và cập nhật late-data.
- Tombstone thay vì xoá hẳn hàng trong Silver: Lưu tombstone (`is_deleted=true`, PII=null, giữ `_lsn`) đảm bảo tính idempotent, tránh việc replay batch cũ mang LSN nhỏ hơn làm "hồi sinh" dữ liệu đã xoá.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến và khả năng tái lập (reproducibility) của tập dữ liệu huấn luyện mô hình ML tại đúng mốc thời gian quá khứ.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu vừa/nhỏ xử lý dạng đơn nút (single-node tabular), DuckDB mang lại hiệu năng SQL OLAP cực cao ở local mà không tốn chi phí vận hành phức tạp của cụm Spark phân tán.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   *Trả lời:* Trong thực tế sản xuất khi tuân thủ pháp lý (GDPR / Right to be Forgotten), quy định xoá dữ liệu riêng tư luôn được ưu tiên cao hơn tính bất biến của snapshot. Cần thiết kế quy trình **Hard Purge / Anonymization pipeline** chuyên biệt để ẩn danh/xoá PII trong toàn bộ các snapshot lịch sử lưu trữ cũ (hoặc trigger rebuild lại snapshot mốc đó), chấp nhận thay đổi checksum của snapshot cũ để tuân thủ luật bảo vệ dữ liệu.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   *Trả lời:* Đặt chốt kiểm soát PII tại tầng Silver (Staging/Silver Ingestion). Thay vì chỉ dựa vào Regex (chỉ bắt được định dạng cố định), cần tích hợp mô hình Named Entity Recognition (NER) hoặc công cụ PII detection (như Microsoft Presidio / SpaCy NLP tiếng Việt) để nhận diện thực thể tên người (PERSON). Đo lường hiệu quả bằng Data Quality Audit Gate, tính chỉ số Precision/Recall trên tập mẫu kiểm định PII và quarantine bản ghi chứa nguy cơ rò rỉ.

## 5. Output (dán nguyên văn)

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 3.30s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ Push-Location dbt_project; ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17; Pop-Location
Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 7.45 seconds (7.45s).
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
