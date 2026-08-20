# Ma trận đáp ứng Data Engineering Roadmap

Tài liệu này đối chiếu repository với roadmap gốc tại
[Data Engineering Roadmap](https://jc-kendo.notion.site/Data-Engineering-Roadmap-1a52d3f816a5807db4ebc29f141dcd00).
Trạng thái “Đạt” dưới đây có nghĩa là đã có implementation và kiểm thử trong
môi trường Docker local; không đồng nghĩa hệ thống đã sẵn sàng triển khai
production nhiều node.

## Kết luận

Sáu tuần đã đáp ứng mục tiêu học tập và bài thực hành của roadmap theo nhánh
Python. Tuần 6 đã có pipeline xử lý service log riêng, không còn dùng pipeline
Promotion để thay thế yêu cầu log. Bộ kiểm thử tổng hiện là nguồn kiểm chứng
tái lập; số liệu benchmark Tuần 3 có artifact được lưu trong Git.

| Tuần | Yêu cầu roadmap | Implementation và bằng chứng | Trạng thái |
| --- | --- | --- | --- |
| 1 | Python/SQL, Linux CLI, MySQL/PostgreSQL, CSV → database, query nâng cao, index, execution plan, procedure, trigger | Import/clean Olist trong `exercises/week1/script/import_olist_to_postgres.py`; lab PostgreSQL và MySQL trong hai file `sql_practice_*`; test hợp đồng tại `exercises/week1/tests`; shell Linux nằm trong workspace Debian | Đạt |
| 2 | OLTP so với OLAP, ETL so với ELT, CAP, ACID so với BASE; chuyển 3NF sang star schema | Loader `load_olist_oltp.py`, `load_olist_olap.py`; schema có constraint/index/SCD2; báo cáo `exercises/week2/report/bao_cao_tuan_2.md` và test khái niệm | Đạt |
| 3 | Spark RDD/DataFrame/SQL, kiến trúc cluster, HDFS, Parquet/ORC, dataset từ 1 GB và benchmark | Spark Standalone + HDFS trong Compose; benchmark lưu tại `exercises/week3/benchmark_result.json`: 6.646.350 dòng, 1,1073 GiB; report đồng bộ số liệu thật | Đạt |
| 4 | Kafka producer/consumer/topic/partition/replication; Structured Streaming; Lambda so với Kappa; Kafka → Spark → file/database | Producer, Structured Streaming event-time/watermark/checkpoint, quarantine và metric trong `exercises/week4`; topic 3 partition được tạo idempotent; runbook và test tại `exercises/week4/tests` | Đạt |
| 5 | Airflow DAG/operator/schedule/monitor; NiFi processor/connection/controller service; REST/SOAP, API key/OAuth; ETL đa nguồn qua Spark | Hai DAG chủ động có cron, một DAG downstream do NiFi trigger; canonical NiFi blueprint v2 phân trang; adapter/test REST API key, OAuth2, SOAP; pipeline CSV + PostgreSQL + REST → Spark → Parquet | Đạt ở mức local; native export v2 đã xác thực trên NiFi 1.27.0 |
| 6 | Service log → HDFS rotate 5 phút, delay tối đa 1 phút; Spark report requests/phút và status distribution; lưu database/file; Airflow, Prometheus, Grafana và alert | Kafka → Spark Structured Streaming → HDFS partition 5 phút; micro-batch ≤60 giây; đo worst-case event-to-HDFS; report HDFS/PostgreSQL; DAG mỗi 5 phút; DQ trước atomic publish; dashboard và alert lag/failure | Đạt ở mức production-like local |

## Hợp đồng nghiệm thu chính

- Chạy `scripts/verify-roadmap.ps1` phải đạt Compose validation, Python compile,
  Prometheus validation và toàn bộ pytest.
- Tuần 3 chỉ được nghiệm thu khi input benchmark ít nhất 1 GiB; không dùng cờ
  bỏ qua dữ liệu nhỏ.
- Tuần 4 giữ `failOnDataLoss=true`; record lỗi đi quarantine thay vì biến mất.
- Tuần 5 đối soát source/inserted/duplicate/raw/accepted/rejected và rerun cùng
  batch không nhân đôi dữ liệu.
- Tuần 6 raw HDFS dùng hierarchy
  `ingest_date/ingest_hour/rotation_5m/stream_batch_id`; micro-batch không quá
  60 giây; DQ phải đạt trước khi publish report hoặc snapshot Promotion.

## Giới hạn ngoài phạm vi lựa chọn Docker local

- Kafka, HDFS, PostgreSQL và Airflow chưa có HA, backup/restore hay disaster
  recovery.
- Credential local chưa dùng secret manager, TLS nội bộ hoặc RBAC production.
- Chưa có schema registry/CDC, data catalog, lineage và CI/CD cloud.
- `exercises/week5/nifi/flow_definition_native.json` là runtime export v2 đã xác
  thực trên NiFi 1.27.0. Artifact cố ý không lưu sensitive parameter;
  blueprint v2 cùng configurator vẫn là nguồn chỉnh sửa chuẩn.
- Dữ liệu Olist và output runtime không nằm trong Git; clone mới phải tải Olist
  theo `data/olist/README.md`. Artifact benchmark Tuần 3 là ngoại lệ được lưu để
  chứng minh mốc 1 GiB.

Các giới hạn này không làm thiếu mục tiêu học tập của roadmap, nhưng phải được
giải quyết trước khi gọi hệ thống là production thật.
