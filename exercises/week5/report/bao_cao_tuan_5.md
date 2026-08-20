# BÁO CÁO THỰC HÀNH TUẦN 5

## Airflow, NiFi và tích hợp Promotion API

**Dự án:** DE Genesis - Lộ trình thực hành Data Engineering

**Phạm vi:** Tuần 5

**Ngày báo cáo:** 14/08/2026
**Trạng thái:** Đã hoàn thiện mã nguồn và kiểm thử tự động; nghiệm thu tích hợp được ghi theo kết quả chạy thực tế.

# 1. Tóm tắt điều hành

Tuần 5 triển khai hai pipeline có cùng đầu vào, cùng data contract và cùng Spark
transformation core để so sánh hai cách tổ chức luồng dữ liệu. Pipeline thứ nhất
đặt Airflow ở trung tâm: Airflow gọi API, ghi raw và điều phối bước biến đổi.
Pipeline thứ hai đặt NiFi ở lớp ingestion: NiFi gọi API, ghi raw rồi kích hoạt
DAG downstream qua Airflow REST API.

Một pipeline thứ ba đáp ứng bài tập tích hợp đa nguồn của roadmap: Airflow chụp
CSV, PostgreSQL và REST API thành snapshot có thể replay; Spark đọc cả ba nguồn,
tạo hai data mart và lưu Parquet kèm báo cáo JSON.

Nguồn dữ liệu là Promotion API mô phỏng chạy local bằng FastAPI. API tạo 250
chương trình khuyến mại từ các `product_id` thật trong bộ dữ liệu Olist. Thiết kế
này tránh phụ thuộc Internet, rate limit bên ngoài hoặc thay đổi contract của
bên thứ ba, đồng thời vẫn cho phép tái tạo các lỗi HTTP 429, HTTP 500, timeout,
JSON sai cấu trúc, bản ghi lỗi và bản ghi trùng.

Kết quả triển khai bao gồm mock API, ba schema PostgreSQL, ba DAG Airflow, hai
Spark job, blueprint NiFi v2 có thể tự cấu hình, adapter REST API key/OAuth2 và
SOAP, các script đối chiếu, 37 test tự động và tài liệu vận hành tiếng Việt.
Watermark chỉ được cập nhật sau khi Spark và quality checks thành công.

# 2. Mục tiêu và phạm vi

## 2.1 Mục tiêu

- Hiểu ranh giới giữa ingestion, orchestration và transformation.
- Xử lý API phân trang, timeout, retry và response không hợp lệ.
- Thiết kế DAG có dependency, retry, timeout và idempotency.
- Kích hoạt DAG qua Airflow REST API.
- Dùng một Spark core cho nhiều nguồn ingestion.
- Quản lý dữ liệu theo các tầng raw, control và curated.
- Đối soát hai pipeline trên cùng input.
- Tích hợp thật ở mức lab ba nguồn CSV, PostgreSQL và REST API qua Spark.
- Mô hình hóa contract API key, OAuth2 Client Credentials và SOAP bằng fixture.

## 2.2 Ngoài phạm vi

- Không triển khai Kubernetes hoặc distributed executor.
- Không dùng credential và secret backend production.
- Không xây monitoring/alerting hoàn chỉnh; nội dung này thuộc tuần 6.
- Không coi Basic Auth và tài khoản mặc định là cấu hình production.

# 3. Các quyết định thiết kế đã phê duyệt

| Mã | Quyết định | Cách hiện thực |
| --- | --- | --- |
| A3 | Hai pipeline để so sánh | Airflow-centric và NiFi-centric |
| B2 | API mô phỏng local | FastAPI có seed xác định |
| C2 | NiFi trigger Airflow REST | POST DAG run sau khi raw hoàn tất |
| D2 | End-to-end chuẩn | Raw, curated, audit, test và tài liệu |
| E2 | Lưu flow trong repo | `nifi/flow_definition.json` |
| F1 | API chiến dịch sản phẩm | Promotion theo `product_id` Olist |
| G1 | FastAPI service riêng | Service `mock-api` trong Docker Compose |
| H1 | Hai DAG, chung Spark core | DAG ingestion và DAG downstream |
| I1 | Pipeline đa nguồn có lịch | CSV + PostgreSQL + REST, Spark, Parquet |
| J1 | Auth contract không thêm dịch vụ | API key/OAuth2/SOAP adapter + fixture |

# 4. Kiến trúc tổng thể

[[ARCHITECTURE]]

Promotion API là nguồn chung của hai pipeline so sánh. Việc dùng chung seed và
Spark core giúp benchmark phản ánh khác biệt ở lớp ingestion và orchestration
thay vì khác biệt logic nghiệp vụ. Pipeline đa nguồn là nhánh độc lập để kiểm
chứng yêu cầu CSV + database + API của roadmap.

## 4.1 Pipeline Airflow-centric

1. DAG kiểm tra PostgreSQL và Mock API.
2. Airflow gọi tuần tự tất cả các trang API.
3. Payload được kiểm tra và ghi vào `promotions_airflow`.
4. Spark đọc raw batch và dữ liệu bán hàng Olist.
5. Spark tính doanh thu sau khuyến mại.
6. Quality checks quyết định trạng thái run và watermark.

## 4.2 Pipeline NiFi-centric

1. NiFi sinh `batch_id`.
2. `InvokeHTTP` gọi Promotion API.
3. Flow phân loại response thành success, retryable và terminal.
4. Record hợp lệ được ghi vào `promotions_nifi`.
5. Khi batch hoàn tất, NiFi gọi Airflow REST API.
6. DAG downstream kiểm tra allow-list và raw batch trước khi chạy Spark.

## 4.3 Pipeline CSV + PostgreSQL + REST API

1. DAG chạy hằng ngày và kiểm tra PostgreSQL cùng Mock API.
2. CSV `data/sample/sales.csv` được chụp theo `batch_id`.
3. PostgreSQL dùng `COPY TO STDOUT`, không `fetchall()`, để stream tổng hợp Olist.
4. REST client gọi đến khi `pagination.has_next=false`.
5. Spark tạo mart `product_promotions` và `regional_sales`.
6. Kết quả được lưu Parquet cùng `summary.json` để đối soát số dòng nguồn/đích.

# 5. Thiết kế Promotion API

## 5.1 Contract

Endpoint chính:

`GET /api/v1/promotions?page=<n>&page_size=<n>&scenario=<name>`

Response gồm `data` và `pagination`. `pagination.has_next` là điều kiện dừng
vòng phân trang, không suy luận bằng kích thước trang cuối.

## 5.2 Dữ liệu seed

Service đọc tối đa 250 `product_id` đầu tiên từ
`olist_products_dataset.csv`. Nếu file không tồn tại trong môi trường test, API
dùng danh sách fallback xác định. Các chiến dịch có khoảng hiệu lực bao phủ dữ
liệu Olist 2016-2018 để tạo được kết quả join thực tế.

## 5.3 Failure scenarios

| Scenario | Mục đích kiểm thử |
| --- | --- |
| `rate_limit` | HTTP 429 và retry policy |
| `server_error` | HTTP 500 liên tục |
| `transient_500` | Thành công sau lỗi tạm thời |
| `timeout` | Read timeout |
| `malformed_json` | Vi phạm response contract |
| `invalid_record` | Data quality tại record |
| `duplicate` | Deduplication |
| `empty` | Batch không có dữ liệu |

# 6. Mô hình dữ liệu

## 6.1 Tầng raw

Hai bảng raw có cùng cấu trúc nhưng tách theo nguồn để benchmark độc lập. Mỗi
dòng giữ cả trường truy vấn thường dùng và payload JSON gốc. `payload_hash`
dùng SHA-256 trên JSON canonical để chống nạp lại cùng nội dung.

Khóa chống trùng:

`batch_id + source_system + promotion_id + payload_hash`

Khóa có `batch_id` để cùng payload ở snapshot ngày mới vẫn được giữ. Khi retry
chính batch, `ON CONFLICT DO NOTHING` ngăn nhân đôi. Audit tách
`source_count`, `inserted_count`, `duplicate_count`; các số đếm raw được truy vấn
lại từ PostgreSQL nên không ghi nhận “accepted” nếu record thực tế không tồn tại.
Vì record invalid có thể thiếu `promotion_id`, unique index dùng PostgreSQL 16
`NULLS NOT DISTINCT`; retry cùng batch không thể chèn thêm một bản sao có khóa
NULL. Migration giữ dòng có `ingestion_id` nhỏ nhất và dọn các bản sao chính xác
do index cũ để lại trước khi rebuild index.

## 6.2 Tầng control

`pipeline_runs` giữ trạng thái và số liệu đối soát của mỗi run.
`ingestion_watermarks` giữ mốc incremental gần nhất đã hoàn tất.
`quality_results` lưu từng rule, actual value và expected value.

## 6.3 Tầng curated

Grain của bảng curated là một sản phẩm trong một đơn hàng kết hợp với phiên bản
promotion có hiệu lực tại ngày mua.

Các công thức:

`gross_amount = item_price + freight_value`

`net_amount_after_discount = gross_amount - discount_amount`

Giảm giá chỉ áp dụng lên `item_price`, không áp dụng phí vận chuyển.

# 7. Thiết kế Airflow

## 7.1 DAG Airflow-centric

DAG `de_genesis_week5_airflow_ingestion` chạy lúc 06:00 hằng ngày,
`catchup=False` và `max_active_runs=1`. Các task lần lượt kiểm tra dependency,
nạp raw và chạy Spark core.

## 7.2 DAG NiFi downstream

DAG `de_genesis_week5_nifi_downstream` không tự gọi API. DAG chỉ nhận:

- `source_mode = nifi`
- `batch_id`
- `raw_table = week5_raw.promotions_nifi`

Tên bảng được kiểm tra bằng allow-list để tránh sử dụng giá trị tùy ý từ request
trong câu SQL.

## 7.3 DAG đa nguồn

DAG `de_genesis_week5_multisource` chạy lúc 06:30 hằng ngày. DAG NiFi downstream
chủ ý để `schedule=None` vì chỉ nhận sự kiện REST sau khi NiFi hoàn tất raw batch.

## 7.4 Retry và failure callback

Lỗi tạm thời được retry có giới hạn. Failure callback ghi trạng thái thất bại
nhưng không che khuất exception gốc nếu bản thân callback gặp lỗi.

# 8. Thiết kế NiFi

Blueprint v2 gồm 21 processor logic, hai Controller Service và contract liệt kê
toàn bộ relationship. Unit test buộc mỗi relationship phải được nối hoặc
auto-terminate. Flow đọc metadata pagination, tăng `page`, có chốt
`api.max.pages`, gom đủ parameter `api.expected.records=250` trước khi trigger
Airflow. Với page size 100, flow đi qua ba trang thay vì dừng ở trang đầu.

Credential Airflow và PostgreSQL có giá trị `null` trong flow definition. Script
cấu hình tạo/gán Parameter Context, tạo/bật Controller Service và lấy hai secret
từ biến môi trường lúc chạy; secret không được ghi vào blueprint.

Record không hợp lệ vẫn được ghi raw với `is_valid=false` để phép đối soát
accepted/rejected không mất dữ liệu. `dag_run_id = nifi__<batch_id>` tạo khóa idempotency ở Airflow. HTTP 409 biểu
thị batch đã được trigger trước đó và không tạo run mới.

## 8.1 Contract API key, OAuth2 và SOAP

Module `api_contracts.py` chuẩn hóa header API key, OAuth2 Client Credentials và
SOAP 1.1. Secret chỉ tồn tại trong tham số runtime. SOAP response được parser về
cùng cấu trúc dict với REST để downstream không phụ thuộc giao thức nguồn.

# 9. Spark transformation core

Spark job nhận `run_id`, `batch_id` và `source_mode`. Tên bảng không được nhận
tự do từ request mà được suy ra từ `source_mode` đã giới hạn.

Các bước xử lý:

1. Đọc promotion hợp lệ trong raw batch.
2. Đọc `fact_sales`, `dim_product` và `dim_date`.
3. Chọn version cao nhất cho mỗi promotion.
4. Join theo `product_id` và khoảng hiệu lực.
5. Tính gross, discount và net amount.
6. Upsert curated theo grain.
7. Chạy quality checks.
8. Chỉ cập nhật watermark nếu toàn bộ rule blocking đạt.

# 10. Idempotency và khả năng chạy lại

Idempotency được áp dụng ở bốn lớp:

- Payload: SHA-256 của JSON canonical.
- Raw: unique constraint theo batch, nguồn, promotion và hash.
- Airflow trigger: `dag_run_id` xác định theo batch.
- Curated: upsert theo source, order item, promotion và version.

Nếu Spark thất bại, raw batch vẫn tồn tại để chạy lại. Watermark không dịch
chuyển nên lần retry không bỏ sót dữ liệu.

# 11. Data quality

Các rule blocking kiểm tra khóa bắt buộc, khoảng hiệu lực, grain trùng,
discount âm và net amount thấp hơn freight. Báo cáo thiết kế ban đầu định nghĩa
14 rule; phiên bản hiện tại tự động hóa các rule quan trọng nhất trong Spark
job, các rule còn lại được kiểm tra tại ingestion hoặc script đối chiếu.

Đối soát cốt lõi:

`accepted_count + rejected_count = raw_count`

Hai bảng curated được so sánh hai chiều bằng `EXCEPT`. `difference_count = 0`
và số dòng bằng nhau là điều kiện tương đương.

# 12. Kiểm thử và kết quả xác minh

## 12.1 Unit test

Ngày 14/08/2026, test suite tuần 5 đạt 33/33 test. Test suite Mock API đạt 4/4
test. Tổng cộng 37/37 test đạt trong image workspace.

Phạm vi test:

- Validation promotion và công thức giảm giá.
- Hash không phụ thuộc thứ tự key JSON.
- Phân trang và retry HTTP 500.
- Phát hiện response sai contract.
- Cấu trúc DAG.
- Contract và sensitive parameter của NiFi flow.
- Phân trang NiFi qua đủ 250 record và contract relationship đầy đủ.
- Khóa raw theo batch cùng các biến đối soát source/inserted/duplicate.
- SQL regression cho retry record `promotion_id=NULL`, batch mới và rollback sạch.
- Snapshot đa nguồn có thể replay và lựa chọn Spark master từ Docker Compose.
- Header API key, OAuth2 token exchange và SOAP fixture/parser.
- Health, pagination và error scenario của Mock API.

## 12.2 Smoke test Mock API

API được khởi động local và trả:

| Chỉ số | Kết quả |
| --- | --- |
| Health | `ok` |
| Tổng seed | 250 |
| Page size thử nghiệm | 40 |
| Số trang | 7 |
| Scenario rate limit | HTTP 429 |

## 12.3 Kiểm tra cấu hình

`docker compose --profile workflow config --quiet` hoàn tất không lỗi.
`compileall` hoàn tất cho `mock_api`, `exercises/week5` và `dags`.
`git diff --check` không phát hiện whitespace error.

## 12.4 Nghiệm thu tích hợp Docker

Profile workflow đã khởi động thành công trên Docker Desktop 29.6.1. Mock API,
PostgreSQL, Airflow webserver, Airflow scheduler và NiFi đều ở trạng thái
running; Mock API và PostgreSQL đạt health check.

Airflow không có DAG import error và nhận đủ hai DAG tuần 5. Pipeline
Airflow-centric hoàn tất với 250 raw record, 250 accepted record, 0 rejected
record và 112.650 curated record. Năm quality check blocking đều đạt.

Kết quả runtime bên dưới là bằng chứng của flow v1 ngày 24/07/2026. Blueprint v2
đã thay logic pagination, idempotency và tự cấu hình Controller Service; trong
lần cập nhật 14/08/2026 mới chạy unit/static test, chưa ghi đè bằng chứng runtime
cũ hoặc tuyên bố benchmark mới khi chưa chạy lại toàn stack.

Contract NiFi - Airflow được nghiệm thu bằng một raw batch tương đương và
request REST thật. Airflow tạo DAG run `nifi__nifi-runtime-001`; lần gửi lại
cùng batch trả kết quả duplicate. DAG downstream hoàn tất với 112.650 curated
record.

| Pipeline | Raw | Accepted | Rejected | Curated | Thời gian |
| --- | ---: | ---: | ---: | ---: | ---: |
| Airflow-centric | 250 | 250 | 0 | 112.650 | 8,212 giây |
| NiFi downstream | 250 | 250 | 0 | 112.650 | 6,485 giây |

So sánh hai chiều cho kết quả `difference_count = 0`; hai bảng curated tương
đương trên cùng input. Thời gian trên chỉ là một lần chạy local, không đủ để
kết luận hiệu năng tổng quát.

# 13. Bảo mật và production hardening

Phiên bản local dùng Basic Auth để làm rõ cơ chế NiFi gọi Airflow REST. Khi đưa
lên production cần:

- Secret backend hoặc secrets manager.
- TLS có chứng chỉ tin cậy.
- Tài khoản dịch vụ quyền tối thiểu.
- Network policy và giới hạn egress.
- Remote executor hoặc Kubernetes executor.
- Object storage cho artifact lớn.
- Alerting, SLA và centralized logging.
- NiFi Registry cho versioned flow.

# 14. Cấu trúc bàn giao

| Thành phần | Vị trí |
| --- | --- |
| Mock API | `mock_api/` |
| DAG Airflow | `dags/` |
| DDL | `exercises/week5/sql/` |
| Spark core | `exercises/week5/spark/` |
| NiFi flow | `exercises/week5/nifi/` |
| Test | `exercises/week5/tests/`, `mock_api/tests/` |
| Script đối chiếu | `exercises/week5/scripts/` |
| Hướng dẫn | `exercises/week5/README.md` |
| Báo cáo | `exercises/week5/report/` |

# 15. Kết luận

Tuần 5 chuyển các thành phần rời rạc của tuần trước thành một pipeline có lớp
ingestion, orchestration, transformation, audit và quality rõ ràng. Hai
pipeline không nhân đôi business logic; điểm khác biệt được giữ tại lớp tiếp
nhận và kích hoạt. Thiết kế này vừa phục vụ mục tiêu học Airflow/NiFi, vừa tạo
nền tảng để tuần 6 bổ sung monitoring, alerting và hardening.

Phiên bản cập nhật đóng các khoảng trống roadmap quan trọng: DAG chủ động có
lịch, NiFi phân trang và route đầy đủ, raw idempotent theo batch, pipeline
CSV/PostgreSQL/REST đi qua Spark, cùng contract API key/OAuth2/SOAP có test.
