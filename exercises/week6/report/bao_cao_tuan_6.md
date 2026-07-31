# BÁO CÁO THỰC HÀNH TUẦN 6

## Production pipeline, monitoring và cảnh báo

**Dự án:** DE Genesis - Lộ trình thực hành Data Engineering

**Phạm vi:** Tuần 6

**Ngày báo cáo:** 31/07/2026
**Trạng thái:** Hoàn thành triển khai, kiểm thử tự động và nghiệm thu tích hợp Docker.

# 1. Tóm tắt điều hành

Tuần 6 nâng pipeline Promotion API của tuần 5 thành một luồng production-like có
lịch chạy hằng ngày, cửa sổ incremental, backfill thủ công, audit đầy đủ, retry
có phân loại, data-quality gate trước và sau Spark, cùng lớp quan sát tập trung
bằng Prometheus và Grafana.

Giải pháp được triển khai theo lựa chọn đã duyệt `A2 B2 C1 D1`. Một metrics
exporter riêng tổng hợp số liệu pipeline từ PostgreSQL và kiểm tra trạng thái
Promotion API, Airflow, Spark master và PostgreSQL. Prometheus scrape exporter,
Spark master, Spark worker và chính Prometheus; Grafana được provision tự động
data source cùng dashboard. Sáu alert rule được nạp vào Prometheus nhưng không
gửi ra kênh ngoài, đúng phạm vi C1.

Nghiệm thu batch có dữ liệu đạt 250 raw record, 250 accepted record, 0 rejected
record và 112.650 curated record. Mười quality check đều đạt. Batch lỗi có kiểm
soát chứa một record thiếu `product_id` bị chặn đúng tại raw quality gate với tỷ
lệ lỗi 0,004 lớn hơn ngưỡng 0. Batch no-op kế tiếp thành công, không ghi raw mới
và giữ nguyên snapshot 112.650 dòng.

# 2. Phạm vi và quyết định đã duyệt

| Mã | Quyết định | Cách hiện thực |
| --- | --- | --- |
| A2 | Giám sát toàn pipeline | Exporter, Prometheus, Spark metrics và Grafana dashboard |
| B2 | DAG production riêng | Incremental window, audit, retry, backfill và DQ gate |
| C1 | Alert trong Prometheus | Sáu rule, không dùng Alertmanager hoặc webhook |
| D1 | Báo cáo DOCX riêng | Tuần 5 và tuần 6 có file độc lập |

Ngoài phạm vi gồm Alertmanager, email, Slack, secret manager, Kubernetes,
distributed Airflow executor và hạ tầng HA.

# 3. Kiến trúc tổng thể

[[ARCHITECTURE]]

Promotion API cung cấp dữ liệu thay đổi trong cửa sổ nửa mở
`[window_start, window_end)`. Airflow điều phối toàn bộ lifecycle. Dữ liệu được
lưu nguyên bản trước khi raw quality gate quyết định có cho phép Spark chạy hay
không. Spark làm mới snapshot curated trên dữ liệu promotion hợp lệ mới nhất.
Curated quality gate bảo vệ grain và các quy tắc tài chính trước khi audit run
được chốt thành công và watermark dịch chuyển.

Metrics exporter không nằm trong đường dữ liệu chính. Exporter đọc bảng audit,
quality result và kiểm tra health endpoint của dependency, nhờ đó lỗi
monitoring không làm pipeline ingestion thất bại.

# 4. Production DAG

DAG `de_genesis_week6_production_pipeline` có lịch `0 2 * * *`, tương ứng
02:00 UTC mỗi ngày. `catchup=False` ngăn Airflow tự sinh một lượng lớn run lịch
sử; backfill được kích hoạt có chủ đích qua `dag_run.conf`. `max_active_runs=1`
tránh hai snapshot refresh chạy đồng thời.

Chuỗi task:

1. `resolve_configuration`
2. `initialize_audit`
3. `check_dependencies`
4. `ingest_incremental`
5. `quality_gate_raw`
6. `run_spark_snapshot`
7. `quality_gate_curated`
8. `finalize_success`

Lỗi hạ tầng dùng hai lần retry, exponential backoff và retry delay 15 giây.
Execution timeout là 45 phút và SLA là một giờ. Hai quality gate đặt
`retries=0` vì lỗi dữ liệu có tính xác định; retry không làm record sai trở
thành hợp lệ.

# 5. Incremental window và backfill

API được mở rộng tham số `updated_before` để kết hợp với `updated_since`. Điều
kiện lọc là:

`updated_since <= updated_at < updated_before`

Cửa sổ nửa mở tránh trùng record ở biên giữa hai batch liên tiếp. Một backfill
window tối đa 31 ngày. Thời gian phải theo ISO-8601 và có múi giờ.

`batch_id` mặc định được suy ra xác định từ hai đầu cửa sổ. Giá trị tùy chỉnh
chỉ nhận chữ, số, dấu chấm, gạch dưới và gạch ngang, tối đa 120 ký tự.

Nếu cửa sổ không có thay đổi, Spark task thực hiện no-op: không ghi lại
snapshot, đọc số dòng curated hiện có vào audit và vẫn chạy curated quality
gate. Cách này giảm chi phí xử lý nhưng giữ được bằng chứng vận hành.

# 6. Mô hình dữ liệu

## 6.1 Raw

`week6_raw.promotions` lưu payload JSON gốc, trường truy vấn chính,
`source_updated_at`, SHA-256 payload, cờ hợp lệ và lỗi validation. Grain raw là
`batch_id + record_index`, vì vậy có thể giữ lại cả duplicate trong response và
vẫn chạy lại cùng batch theo cách idempotent.

## 6.2 Control

`week6_control.pipeline_runs` lưu window, scenario, attempt, trạng thái, thời
gian và số dòng của từng tầng. `quality_results` lưu actual, expected, trạng
thái và mô tả cho từng rule. `ingestion_watermarks` chỉ cập nhật sau khi mọi
quality gate đạt.

## 6.3 Curated

`week6_curated.sales_promotion` là snapshot doanh thu sau promotion. Grain:

`order_id + item_number + promotion_id + promotion_version`

Spark dùng transformation core của tuần 5, chọn version promotion mới nhất,
join với Olist theo `product_id` và khoảng hiệu lực, sau đó tính gross,
discount và net amount. Snapshot được thay thế trong một transaction
PostgreSQL để tránh trạng thái nửa cũ nửa mới.

Trong Airflow, Spark driver kết nối cụm standalone qua
`spark://spark-master:7077`. Chế độ `local[2]` chỉ là fallback khi chạy ngoài
Compose và không có `SPARK_MASTER_URL`. Custom Spark image chuẩn hóa Python
3.11 cho cả driver và worker; đây là điều kiện bắt buộc của PySpark khi chạy
phân tán.

# 7. Data-quality gate

## 7.1 Raw gate

| Rule | Điều kiện blocking |
| --- | --- |
| DQ01 | `accepted + rejected = raw` |
| DQ02 | Số dòng raw đã lưu bằng số dòng API |
| DQ03 | Cờ hợp lệ trong raw khớp audit |
| DQ04 | Tỷ lệ record lỗi không vượt ngưỡng |
| DQ05 | `source_updated_at` thuộc đúng incremental window |

Ngưỡng lỗi mặc định là 0 và có thể cấu hình trong `[0, 1]`.

## 7.2 Curated gate

| Rule | Điều kiện blocking |
| --- | --- |
| DQ06 | Số dòng curated khớp audit |
| DQ07 | Grain curated không trùng |
| DQ08 | Discount không âm |
| DQ09 | Net amount không thấp hơn freight |
| DQ10 | Snapshot có cùng số grain với `fact_sales` |

Nếu một rule thất bại, watermark không dịch chuyển và run được đánh dấu
`failed` sau failure callback.

# 8. Monitoring exporter

Exporter chạy tại cổng nội bộ 9108, thu dữ liệu từ bảng control của cả tuần 5
và tuần 6. Các metric chính:

| Metric | Ý nghĩa |
| --- | --- |
| `de_genesis_dependency_up` | Health của API, Airflow, Spark master và PostgreSQL |
| `de_genesis_pipeline_runs_total` | Tổng run theo pipeline và trạng thái |
| `de_genesis_pipeline_last_success_timestamp_seconds` | Lần thành công gần nhất |
| `de_genesis_pipeline_last_run_success` | Run gần nhất thành công hay không |
| `de_genesis_pipeline_last_duration_seconds` | Thời gian run gần nhất |
| `de_genesis_pipeline_last_rows` | Raw, accepted, rejected và curated |
| `de_genesis_pipeline_last_quality_failures` | Số DQ rule lỗi ở run gần nhất |

Exporter chịu được trường hợp schema tuần 5 hoặc tuần 6 chưa được tạo. Nếu
PostgreSQL lỗi, metric dependency chuyển về 0 thay vì làm HTTP server dừng.

# 9. Prometheus và alert rule

Spark master và worker được gắn `PrometheusServlet` qua
`config/spark/metrics.properties`. Sau khi cấu hình, Prometheus xác nhận bốn
target đều `up`: Prometheus, pipeline exporter, Spark master và Spark worker.

Sáu alert rule:

| Alert | Điều kiện |
| --- | --- |
| `PipelineMetricsExporterDown` | Exporter down ít nhất 1 phút |
| `PipelineDependencyDown` | Dependency down ít nhất 2 phút |
| `PipelineLastRunFailed` | Run gần nhất không thành công |
| `PipelineDataQualityFailed` | Run gần nhất có DQ failure |
| `PipelineStale` | Không thành công mới trong hơn 26 giờ |
| `SparkWorkerUnavailable` | Spark worker down ít nhất 2 phút |

`promtool` xác nhận file Prometheus hợp lệ và nạp đủ sáu rule. Theo lựa chọn C1,
alert chỉ hiển thị ở Prometheus; không có receiver bên ngoài.

# 10. Grafana provisioning

Grafana tự động nhận data source `Prometheus` với UID
`de-genesis-prometheus` và dashboard `DE Genesis - Pipeline Production`.
Dashboard có các panel:

- Trạng thái run gần nhất.
- Số quality check thất bại.
- Health của dependency.
- Thời gian xử lý.
- Số dòng raw, accepted, rejected và curated.

Trong phiên nghiệm thu, cổng 3000 của máy Windows đang được một tiến trình Java
ngoài stack sử dụng nên Grafana được chạy tạm ở cổng 3001. Cấu hình repo vẫn
giữ mặc định 3000 và cho phép đổi bằng `GRAFANA_PORT`.

# 11. Kết quả kiểm thử tự động

| Nhóm | Kết quả |
| --- | ---: |
| Week 5 + Week 6 unit/contract tests | 30/30 đạt |
| Mock Promotion API tests | 4/4 đạt |
| Tổng test | 34/34 đạt |
| Airflow DAG import error | 0 |
| Prometheus config | Hợp lệ |
| Prometheus alert rule | 6/6 được nạp |
| Prometheus scrape target | 4/4 `up` |
| Grafana data source | Được provision |
| Grafana dashboard | Được provision |

Test bao phủ validation cấu hình, giới hạn backfill, batch ID allow-list,
incremental pagination, cấu trúc DAG, retry guard, monitoring asset, Promotion
API contract và toàn bộ test tuần 5.

# 12. Nghiệm thu tích hợp

## 12.1 Batch có dữ liệu

Run `manual__week6_cluster_20260720_v3` thực thi trên Spark standalone:

| Chỉ số | Kết quả |
| --- | ---: |
| Raw | 250 |
| Accepted | 250 |
| Rejected | 0 |
| Curated | 112.650 |
| Quality check | 10/10 đạt |
| Thời gian audit | 42,796 giây |
| Watermark | 21/07/2026 00:00 UTC |

Spark master ghi nhận application
`week6-production-week6-cluster-20260720-v3`, cấp executor trên worker và nhận
unregister khi job hoàn tất.

## 12.2 Batch lỗi có kiểm soát

Run `manual__week6_invalid_20260720_v1` nhận 250 record, trong đó 249 accepted
và 1 rejected. `DQ04_invalid_rate` ghi actual `0,004`, expected `<= 0,0` và
chặn pipeline trước Spark. Thời gian 79,945 giây bao gồm retry theo cấu hình
ban đầu; sau quan sát này, hai DQ task đã được harden thành `retries=0`.

## 12.3 Batch no-op và phục hồi

Run `manual__week6_noop_20260721_v1` nhận 0 record mới, giữ 112.650 curated
record, hoàn tất trong 9,764 giây. Sau run này, alert về last-run failure trở
lại inactive.

# 13. Vận hành

Khởi động:

`.\scripts\start.ps1 -Target week6 -Build`

Các điểm truy cập mặc định:

| Dịch vụ | URL |
| --- | --- |
| Airflow | `http://localhost:8088` |
| Promotion API | `http://localhost:8000/docs` |
| Prometheus | `http://localhost:9090` |
| Prometheus alerts | `http://localhost:9090/alerts` |
| Grafana | `http://localhost:3000` |
| Pipeline metrics | `http://localhost:9108/metrics` |

Truy vấn audit và mẫu cấu hình trigger/backfill nằm trong
`exercises/week6/README.md`.

# 14. Bảo mật và giới hạn

Môi trường hiện tại dùng credential local, bind port trực tiếp và
`LocalExecutor`. Trước khi đưa lên production thật cần:

- Secret manager hoặc Airflow secrets backend.
- TLS tin cậy và service account quyền tối thiểu.
- Remote executor hoặc Kubernetes executor.
- Object storage cho artifact và Spark event log.
- HA cho PostgreSQL, Prometheus và Grafana.
- Retention policy và backup cho audit.
- Alertmanager có receiver xác thực nếu mở rộng ngoài C1.
- Network policy, rate limit và centralized logging.

# 15. Cấu trúc bàn giao

| Thành phần | Vị trí |
| --- | --- |
| Production DAG | `dags/de_genesis_week6_production_pipeline.py` |
| Airflow tasks | `dags/week6/` |
| Pipeline core | `exercises/week6/` |
| DDL | `exercises/week6/sql/` |
| Spark snapshot | `exercises/week6/spark/` |
| Metrics exporter | `exercises/week6/monitoring/` |
| Prometheus | `config/prometheus/` |
| Grafana | `config/grafana/` |
| Spark metrics | `config/spark/metrics.properties` |
| Test | `exercises/week6/tests/` |
| Hướng dẫn | `exercises/week6/README.md` |
| Báo cáo | `exercises/week6/report/` |

# 16. Kết luận

Tuần 6 đã bổ sung các năng lực còn thiếu để biến pipeline học tập tuần 5 thành
một hệ thống production-like có kiểm soát: incremental contract rõ ràng,
backfill giới hạn, audit theo run, quality gate blocking, snapshot idempotent,
watermark an toàn và observability đầu cuối.

Kết quả nghiệm thu chứng minh cả ba nhánh quan trọng: batch có dữ liệu thành
công, dữ liệu lỗi bị chặn trước Spark và batch không có thay đổi hoàn tất theo
chế độ no-op. Monitoring cung cấp được số liệu của cả tuần 5 và tuần 6, Spark
metrics hoạt động, dashboard tự provision và alert rule được Prometheus đánh
giá mà không gửi dữ liệu ra hệ thống ngoài.
