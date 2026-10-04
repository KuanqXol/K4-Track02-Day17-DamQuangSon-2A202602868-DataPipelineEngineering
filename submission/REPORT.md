# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Văn An / 20260001
**Repo:** K4-Track02-Day17-Data-Pipeline-Engineering
**Commit bài nộp:** fbf7454
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity AI Agent hỗ trợ phân tích triệu chứng bug, xây dựng logic MERGE LSN guard, xử lý CDC delete trong staging, cấu hình lookback P99 và cài đặt cache cho bước bonus LLM.
**Nguồn tham khảo khác (nếu có):** Slide bài giảng Track 02 Ngày 17: Data Pipeline Engineering (Debezium CDC, Silver idempotency, event time & lookback).

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `verify` báo `[XX] Silver silver_tickets has exactly one row per ticket_id (24 rows for 12 tickets)`, T-91 chứa cả 3 trạng thái cũ `[('low', 'open', None), ('high', 'open', None), ('high', 'closed', 'bug')]`; test `test_silver_tickets_one_row_per_ticket` và `test_silver_tickets_latest_state_wins` fail. | `verify` báo `[XX] Gold gold_feature_daily reconciles with a full recompute (c50b8851affe != 8630e04a61d1)` và `[XX] Gold u05's offline events of 08-12 (arrived 08-15) are counted on 08-12 (got (2, 0), expected (5, 1))`; 2 test contracts về feature_daily fail. | `verify` báo `[XX] Silver deleted ticket T-97 is a tombstone... (got [(False, 'u06', ...)])`, `[XX] Gold latest training snapshot excludes the deleted ticket T-97 (1 row(s))` và `[XX] Gold deletes propagate to the RAG index: no chunk of T-97 (2 chunk(s))`. |
| **Nguyên nhân gốc** | `pipeline/silver.py` dùng `INSERT INTO silver_tickets` thay vì MERGE theo khoá, khiến mỗi ngày chạy bị append trùng lặp; khi chạy lại batch cũ sẽ nhân bản dòng và làm sai lệch trạng thái mới nhất. | `pipeline/config.py` đặt `LOOKBACK_DAYS = 0`, daily run chỉ tính cho partition ngày hiện tại nên các sự kiện xảy ra ngày 08-12 nhưng đến trễ ở batch 08-15 không được tính bù vào ngày xảy ra sự kiện. | `pipeline/staging.py` chỉ trích xuất `ticket_id` từ `j->'value'->'after'->>'ticket_id'`. Bản ghi Debezium delete (`_op='d'`) có `after = null`, dẫn đến `ticket_id` bị null và bị loại bỏ ở clause `WHERE ticket_id IS NOT NULL`. |
| **Cách sửa** (file, vài dòng) | Sửa `pipeline/silver.py`: dùng `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn >= t._lsn THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT VALUES (...)`. | Sửa `pipeline/config.py`: đổi `LOOKBACK_DAYS = 3`, đúng bằng `ceil(p99)` đo đạc thực tế từ Bronze qua câu lệnh `python main.py --lateness`. | Sửa `pipeline/staging.py`: dùng `coalesce(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id') AS ticket_id`, giúp thao tác xoá mang `is_deleted=True` lan thành tombstone ở Silver và lọc bỏ khỏi Gold. |
| **Khái niệm trên slide** | Silver — Có khoá; Bốn cách viết idempotent; MERGE theo khoá thực thể có guard LSN chống batch cũ ghi đè batch mới. | Data về muộn; Event time vs Ingest time; Lookback = ceil(P99) "đo từ Bronze, đừng đoán"; Idempotent overwrite-partition. | CDC log-based (giải mã phong bì Debezium before/after/op/lsn); Xoá phải lan (tombstone tại Silver, lan truyền xuống training set & RAG index). |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` là bảng thực thể cần cập nhật từng dòng theo entity key (`ticket_id`), trong khi `gold_feature_daily` là bảng tổng hợp theo phân vùng ngày (`event_date`) nên ghi đè nguyên partition theo cửa sổ lookback đơn giản, nhanh và hoàn toàn idempotent.
- Tombstone thay vì xoá hẳn hàng trong Silver: Tombstone giữ lại bản ghi cùng LSN để ngăn các batch cũ chạy lại vô tình hồi sinh (resurrect) ticket đã xoá, đồng thời xoá sạch dữ liệu cá nhân (`user_id`, `subject`, `body` = NULL) để tuân thủ quyền riêng tư.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến và tái lập tuyệt đối (reproducibility) cho ML: model đã huấn luyện trong quá khứ cần đối soát đúng dữ liệu tại thời điểm đó mà không bị ảnh hưởng bởi thay đổi sau này.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: DuckDB chạy in-process cực nhanh không tốn tài nguyên quản lý cluster, dbt cung cấp chuẩn hoá data modeling, testing và data contracts toàn diện mà không cần overhead phức tạp của Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   - Đây là mâu thuẫn giữa Reproducibility và GDPR (Right to be Forgotten). Trong thực tế có 2 cách tiếp cận:
     (a) **Crypto-shredding (Khuyên dùng)**: Dữ liệu cá nhân trong Bronze/Silver/Gold được mã hoá bằng khoá riêng cho từng user (Key-per-User). Khi có yêu cầu xoá, hệ thống chỉ cần huỷ khoá mã hoá của user đó; văn bản trong mọi snapshot lịch sử lập tức trở thành dữ liệu rác không thể giải mã, bảo toàn trọn vẹn cấu trúc file bất biến mà không phải rewrite file Parquet.
     (b) **Quy trình Re-snapshot có kiểm toán**: Nếu luật định yêu cầu xoá vật lý dữ liệu (hard-delete), kích hoạt job audit re-partition đặc biệt: ghi đè snapshot cũ thành phiên bản vá (vd: `v2026-08-12-purged`) loại bỏ T-97, ghi nhật ký kiểm toán (audit log) có chữ ký số giải trình lý do vi phạm bất biến và đánh dấu các model ML cũ cần retrain nếu bị tác động.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   - **Vị trí và tầng đặt chốt PII**: Thiết lập cơ chế kiểm soát 2 lớp:
     - *Lớp 1 (Bronze → Silver)*: Tích hợp mô hình Named Entity Recognition (NER) chuyên biệt cho tiếng Việt (như PhoBERT-NER hoặc Presidio) để nhận diện thực thể Tên người (PER), Địa chỉ (LOC) theo ngữ cảnh văn bản tự do ngay khi landing vào Silver.
     - *Lớp 2 (Silver → Gold)*: Dùng chốt chặn đối soát chéo (Lookup-based gate) so khớp các token văn bản với bảng thông tin khách hàng nguồn (`users.full_name`, `customer_profiles`) để phát hiện các tên riêng bị lọt qua NER trước khi vào RAG index hoặc training set.
   - **Đo lường**: Tạo bộ dữ liệu kiểm thử vàng (gold standard benchmark) gồm các mẫu ticket thật được gán nhãn PII thủ công. Chạy pipeline qua bộ dữ liệu này và đo lường định kỳ các chỉ số Precision, Recall và F1-score; đặt ngưỡng contract cảnh báo nếu Recall nhận diện tên người < 99.9%.

## 5. Output (dán nguyên văn)

```text
$ python -X utf8 -m scripts.verify
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
```

```text
$ python -X utf8 -m pytest
..................................                                       [100%]
34 passed in 2.25s
```

```text
$ python -X utf8 -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

```text
$ python -X utf8 main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

```text
$ dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
05:48:02  Running with dbt=1.12.5
05:48:02  Registered adapter: duckdb=1.11.0
05:48:03  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:48:03  
05:48:03  Concurrency: 1 threads (target='dev')
05:48:03  
05:48:03  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:48:03  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.08s]
05:48:03  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:48:03  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
05:48:03  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:48:03  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.13s]
05:48:03  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:48:04  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.12s]
05:48:04  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:48:04  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.16s]
05:48:04  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:48:04  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
05:48:04  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:48:04  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
05:48:04  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:48:04  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
05:48:04  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:48:04  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
05:48:04  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:48:04  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
05:48:04  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:48:04  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
05:48:04  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:48:04  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
05:48:04  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:48:04  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
05:48:04  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:48:04  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
05:48:04  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:48:04  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
05:48:04  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:48:04  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:48:04  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.05s]
05:48:04  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:48:04  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
05:48:04  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:48:04  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
05:48:04  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:48:04  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.03s]
05:48:04  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:48:04  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
05:48:04  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:48:04  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.03s]
05:48:04  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:48:04  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.03s]
05:48:04  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.27s]
05:48:04  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:48:04  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
05:48:04  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:48:04  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
05:48:04  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:48:04  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
05:48:04  
05:48:04  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.33 seconds (1.33s).
05:48:04  
05:48:04  Completed successfully
05:48:04  
05:48:04  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```text
$ python -X utf8 -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Bonus B1: LLM Labelling với Cache + Quarantine

```text
$ python -X utf8 -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
