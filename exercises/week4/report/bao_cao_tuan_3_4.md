# MỤC LỤC

| Phần | Nội dung |
| --- | --- |
| Tóm tắt điều hành | Kết quả nổi bật và phạm vi hoàn thành |
| 1 | Bối cảnh, yêu cầu và mục tiêu |
| 2 | Kiến trúc giải pháp và môi trường thực thi |
| 3 | Tuần 3 - Thiết kế Spark batch và HDFS |
| 4 | Tuần 3 - Kết quả chạy và đối soát |
| 5 | Tuần 4 - Thiết kế Kafka và Structured Streaming |
| 6 | Tuần 4 - Kết quả chạy end-to-end |
| 7 | Kiểm thử, chất lượng và khả năng tái lập |
| 8 | Đánh giá, hạn chế và hướng phát triển |
| 9 | Kết luận |
| Phụ lục A | Runbook chạy lại toàn bộ bài thực hành |
| Phụ lục B | Từ điển dữ liệu và cấu trúc đầu ra |
| Phụ lục C | Ma trận bằng chứng và cấu trúc mã nguồn |
| Tài liệu tham khảo | Tệp dự án và tài liệu chính thức |

# TÓM TẮT ĐIỀU HÀNH

Báo cáo trình bày kết quả hoàn thiện hai chặng tiếp theo của dự án **DE Genesis**. Tuần 3 xây dựng một batch pipeline bằng Apache Spark, sử dụng HDFS làm hệ thống tệp phân tán và lưu dữ liệu cột ở hai định dạng Parquet/ORC. Tuần 4 xây dựng luồng sự kiện từ Kafka producer tới Spark Structured Streaming, xử lý theo event time, watermark và cửa sổ thời gian, sau đó ghi báo cáo Parquet cùng checkpoint.

<!-- FIGURE:overview -->

Các hạng mục không dừng ở mã nguồn. Toàn bộ luồng đã được chạy trên Docker Compose thực tế, dùng Spark master/worker, NameNode/DataNode và Kafka broker của repo. Kết quả cốt lõi:

- Job tuần 3 chạy thành công với cả đường dẫn local và URI HDFS.
- Fixture chức năng 10 dòng được kiểm tra, chuẩn hóa và đọc lại đủ từ cả Parquet lẫn ORC.
- Benchmark quy mô lớn xử lý **6.646.350 dòng**, input **1.188.929.308 byte (1,1073 GiB)** trên HDFS; toàn bộ join dimension không có khóa mồ côi.
- Đối soát xác nhận DataFrame, Spark SQL và RDD cho kết quả tương đương; checksum CSV, Parquet và ORC bằng nhau ở cả hai workload đo.
- Producer tuần 4 gửi 20 sự kiện kiểm thử có seed cố định; Kafka xác nhận tuần tự ở partition 0, offset 0 đến 19.
- Structured Streaming xử lý đủ 20 sự kiện sau khi event time tiến tới cửa sổ kế tiếp, tạo 18 nhóm tổng hợp; phát hiện 4 request có status code từ 500 trở lên.
- Chạy lại với cùng checkpoint không tạo dữ liệu trùng.
- Bộ kiểm thử tự động bao phủ batch 1 GiB, producer, phân loại JSON, quarantine, metric và aggregate; lệnh chạy nằm ở phụ lục A.

**Kết luận chính:** công việc tuần 3 và tuần 4 đã hình thành hai pipeline có thể chạy, kiểm tra và tái lập; đồng thời README đã được sửa bằng các lệnh đúng với image Docker thực tế.

# 1. BỐI CẢNH, YÊU CẦU VÀ MỤC TIÊU

## 1.1. Bối cảnh dự án

DE Genesis là môi trường thực hành Data Engineering sáu tuần. Tuần 1 và tuần 2 đã tạo nền tảng Python/SQL/PostgreSQL, mô hình OLTP 3NF và star schema OLAP theo Kimball. Tuần 3 chuyển trọng tâm sang xử lý phân tán theo batch; tuần 4 bổ sung dữ liệu sự kiện gần thời gian thực.

Hai tuần này kết nối hai kiểu workload quan trọng:

- **Batch:** dữ liệu hữu hạn được đọc, kiểm tra, biến đổi, tổng hợp và ghi thành bộ dữ liệu phân tích.
- **Streaming:** sự kiện đến liên tục qua message broker, được xử lý theo micro-batch và trạng thái được duy trì bằng checkpoint.

## 1.2. Yêu cầu tuần 3

Mục tiêu cần đạt:

1. Chạy Spark trên mô hình master/worker của Docker Compose.
2. Đọc dữ liệu CSV bán hàng bằng schema rõ ràng.
3. Kiểm tra chất lượng, tách dữ liệu hợp lệ và dữ liệu bị từ chối.
4. Tạo measure doanh thu và các bảng tổng hợp phục vụ phân tích.
5. Ghi dữ liệu cột ở Parquet và ORC, có partition hợp lý.
6. Đọc lại đầu ra để chứng minh không mất dòng.
7. Chạy được cùng một job với local filesystem và HDFS.
8. Xử lý dataset tối thiểu 1 GiB, có join, aggregate và benchmark CSV/Parquet/ORC.
9. Đối chiếu cùng logic bằng DataFrame, Spark SQL và RDD.

## 1.3. Yêu cầu tuần 4

Mục tiêu cần đạt:

1. Tạo Kafka topic và producer có thể chạy liên tục hoặc hữu hạn.
2. Yêu cầu Kafka xác nhận việc ghi sự kiện để tránh báo thành công giả.
3. Đọc topic bằng Spark Structured Streaming.
4. Parse JSON theo schema, phân loại và lưu bản ghi không hợp lệ để điều tra.
5. Xử lý theo event time, watermark và time window.
6. Ghi aggregate dạng Parquet và lưu checkpoint.
7. Có chế độ `available now` để kiểm thử hữu hạn.
8. Chứng minh checkpoint ngăn việc xử lý lại offset cũ.
9. Lưu bản ghi lỗi vào vùng quarantine kèm lý do và Kafka offset.
10. Ghi metric accepted/rejected theo micro-batch để vận hành và cảnh báo.
11. Giải thích lựa chọn Kappa so với Lambda cho luồng log hiện tại.

## 1.4. Tiêu chí hoàn thành

| Nhóm | Tiêu chí chấp nhận |
| --- | --- |
| Mã nguồn | Script có CLI, cấu hình bằng tham số/biến môi trường, thông báo lỗi rõ ràng |
| Dữ liệu | Có kiểm tra schema, kiểu dữ liệu, giá trị bắt buộc và số dòng |
| Lưu trữ | Parquet/ORC đọc lại được; HDFS hiển thị đúng cấu trúc đầu ra |
| Streaming | Producer được broker xác nhận; aggregate/quarantine/metric được ghi; checkpoint chạy lại không trùng |
| Kiểm thử | Có test tự động cho logic trọng yếu và có lần chạy end-to-end |
| Tài liệu | README tiếng Việt có dấu, lệnh chạy khớp container thực tế, có báo cáo DOCX |

# 2. KIẾN TRÚC GIẢI PHÁP VÀ MÔI TRƯỜNG THỰC THI

## 2.1. Thành phần kiến trúc

| Thành phần | Vai trò trong bài thực hành |
| --- | --- |
| Docker Compose | Dựng và kết nối các service trong cùng network `de-genesis` |
| Spark master/worker | Thực thi DataFrame batch và Structured Streaming |
| HDFS NameNode/DataNode | Quản lý namespace và lưu tệp phân tán của tuần 3 |
| Kafka broker | Lưu topic sự kiện và cấp offset cho tuần 4 |
| Workspace container | Chạy producer, pytest và công cụ đọc kiểm tra Parquet |
| Bind mount `/workspace` | Chia sẻ code, dữ liệu mẫu và output với máy Windows |

## 2.2. Phiên bản đã xác minh

| Công nghệ | Phiên bản thực tế |
| --- | --- |
| Docker Engine | 29.5.3 |
| Python trong workspace | 3.11.15 |
| Apache Spark | 3.5.1 |
| Scala của Spark | 2.12.18 |
| Java của Spark image | OpenJDK 11.0.22 |
| Hadoop | 3.2.1 |
| Confluent Platform/Kafka image | 7.6.1 |

## 2.3. Luồng dữ liệu tổng thể

Tuần 3 và tuần 4 dùng chung cụm Spark nhưng khác nguồn và trạng thái xử lý. Batch job đọc một snapshot xác định và luôn đối soát toàn bộ đầu ra. Streaming job đọc offset từ Kafka, giữ state cho aggregate và tiếp tục từ checkpoint trong lần chạy sau.

| Thuộc tính | Tuần 3 - Batch | Tuần 4 - Streaming |
| --- | --- | --- |
| Nguồn | Fixture CSV và fact Olist 1 GiB+ trên HDFS | Kafka topic |
| Đơn vị xử lý | Toàn bộ dataset của một lần chạy | Micro-batch theo offset |
| Thời gian | `order_date` | `event_time` parse từ `ts` |
| Trạng thái | Không cần state giữa các lần chạy | Checkpoint và state store |
| Đầu ra | Curated + summary + quality/benchmark report | Aggregate, quarantine, quality metric Parquet |
| Cơ chế an toàn | Đối soát dòng/checksum/API, HDFS fsck | Offset, checkpoint riêng từng sink, không cho phép mất dữ liệu mặc định |

# 3. TUẦN 3 - THIẾT KẾ SPARK BATCH VÀ HDFS

## 3.1. Dữ liệu đầu vào

Fixture mặc định là `data/sample/sales.csv`, gồm 10 dòng giao dịch và 10 cột
nghiệp vụ. Fixture này kiểm chứng logic nhanh; bằng chứng quy mô roadmap dùng fact
Olist mở rộng có kiểm soát trên HDFS và được trình bày riêng ở mục 3.6/4.7.

| Nhóm cột | Cột |
| --- | --- |
| Định danh | `order_id`, `customer_id`, `product_id` |
| Thời gian | `order_date` |
| Mô tả | `customer_name`, `product_name`, `category`, `region` |
| Measure nguồn | `quantity`, `unit_price` |

Script không dựa hoàn toàn vào cơ chế infer schema. Dữ liệu đầu vào được đọc tạm dưới dạng chuỗi để có thể kiểm soát lỗi chuyển đổi, sau đó mới ép kiểu ngày, số nguyên và decimal. Cách này giúp một giá trị sai không làm mất toàn bộ batch mà được chuyển sang tập `rejected_rows` với lý do cụ thể.

## 3.2. Pipeline xử lý

<!-- FIGURE:week3_flow -->

Các bước trong `spark_batch.py`:

1. Đọc tham số `--input`, `--output`, `--mode` và số shuffle partition.
2. Tạo SparkSession với timezone UTC.
3. Đọc riêng header để kiểm tra đủ 10 cột bắt buộc và không trùng tên cột.
4. Đọc CSV theo raw schema, bật chế độ permissive và giữ `_corrupt_record`.
5. Loại dòng trùng hoàn toàn theo các cột nguồn.
6. Trim chuỗi, parse ngày `yyyy-MM-dd`, cast `quantity` và `unit_price`.
7. Tạo `rejection_reason` cho từng quy tắc không đạt.
8. Tách `valid` và `rejected`, tính `line_amount = quantity × unit_price`.
9. Tạo ba aggregate: theo category, region và ngày.
10. Ghi curated sales ở Parquet/ORC với Snappy, partition theo năm/tháng.
11. Ghi rejected rows, aggregate và quality report.
12. Đọc lại Parquet/ORC và so số dòng với tập hợp lệ.

## 3.3. Quy tắc chất lượng dữ liệu

| Quy tắc | Hành động khi vi phạm |
| --- | --- |
| CSV sai cấu trúc | Ghi lý do `CSV sai cấu trúc` |
| `order_id`, `customer_id`, `product_id` rỗng | Đưa vào `rejected_rows` |
| `order_date` không parse được | Ghi sai định dạng `yyyy-MM-dd` |
| `category` hoặc `region` rỗng | Từ chối dòng |
| `quantity` rỗng hoặc nhỏ hơn hoặc bằng 0 | Từ chối dòng |
| `unit_price` rỗng hoặc âm | Từ chối dòng |
| Dòng nguồn trùng hoàn toàn | Giữ một dòng và tăng `duplicates_removed` |

Phép đối soát trung tâm là:

```text
source_rows = duplicates_removed + rejected_rows + valid_rows
```

Nếu công thức không đúng, job phát sinh lỗi và không công bố kết quả đạt.

## 3.4. Mô hình đầu ra

Dữ liệu hợp lệ giữ các cột nghiệp vụ, bổ sung `line_amount`, `order_year`, `order_month` và `_source_file`. Hai cột năm/tháng được dùng làm partition vật lý, nhờ đó truy vấn theo thời gian có thể bỏ qua partition không liên quan.

| Dataset | Grain | Định dạng |
| --- | --- | --- |
| `curated_sales_parquet` | Một dòng sản phẩm trong giao dịch nguồn | Parquet/Snappy |
| `curated_sales_orc` | Cùng grain với curated Parquet | ORC/Snappy |
| `rejected_rows` | Một dòng nguồn không đạt chất lượng | Parquet |
| `category_summary` | Một category | Parquet |
| `region_summary` | Một region | Parquet |
| `daily_summary` | Một ngày đặt hàng | Parquet |
| `quality_report` | Một lần chạy | JSON |

## 3.5. Local filesystem và HDFS

Hàm ghép đường dẫn giữ nguyên scheme của URI nên cùng một script dùng được cho:

- Local/bind mount: `/workspace/output/week3`.
- HDFS: `hdfs://namenode:9000/data/processed/week3`.

Tệp nguồn được đưa lên `/data/raw/sales.csv` bằng `hdfs dfs -put -f`. Job Spark chạy từ container master nhưng executor trên worker vẫn truy cập được HDFS qua hostname nội bộ `namenode:9000`.

## 3.6. Pipeline benchmark Olist 1 GiB+

Hai script chuyên biệt bổ sung cho tiêu chí quy mô:

1. `generate_olist_1gb.py` join khóa của `order_items` với order, customer,
   product và seller; sau đó nhân bản có kiểm soát bằng `replica_id` tới khi fact
   CSV trên HDFS vượt 1 GiB.
2. `olist_format_benchmark.py` đọc fact, kiểm tra orphan key, chạy cùng workload
   bằng DataFrame, Spark SQL và RDD, rồi ghi/đọc lại CSV, Parquet và ORC.
3. Mỗi format có warm-up trước khi đo; checksum logic được so sánh thay vì chỉ
   so số dòng. `hdfs fsck` được dùng để kiểm tra block thiếu/hỏng.

Dataset benchmark là dữ liệu Olist thật được mở rộng để kiểm tra kích thước và
throughput; `replica_id` cho biết rõ đây không phải 6,6 triệu giao dịch tự nhiên.

# 4. TUẦN 3 - KẾT QUẢ CHẠY VÀ ĐỐI SOÁT

## 4.1. Kết quả chất lượng tổng thể

| Chỉ tiêu | Giá trị |
| --- | ---: |
| Dòng nguồn | 10 |
| Dòng hợp lệ | 10 |
| Dòng trùng bị loại | 0 |
| Dòng bị từ chối | 0 |
| Đơn hàng phân biệt | 10 |
| Khách hàng phân biệt | 8 |
| Tổng số lượng | 45 |
| Tổng doanh thu | 3.684,00 |
| Dòng đọc lại từ Parquet | 10 |
| Dòng đọc lại từ ORC | 10 |

Quality report được ghi thành JSON và có `run_id`, thời điểm UTC, đường dẫn nguồn/đích cùng toàn bộ chỉ tiêu trên. Dữ liệu không chứa mật khẩu hoặc chuỗi kết nối.

## 4.2. Doanh thu theo danh mục

<!-- FIGURE:week3_revenue -->

| Danh mục | Đơn | Khách | Số lượng | Doanh thu | Trung bình/dòng |
| --- | ---: | ---: | ---: | ---: | ---: |
| Electronics | 5 | 4 | 9 | 2.980,00 | 596,00 |
| Furniture | 3 | 3 | 6 | 655,00 | 218,33 |
| Office | 2 | 2 | 30 | 49,00 | 24,50 |

Electronics chiếm khoảng 80,89% tổng doanh thu dù chỉ chiếm 9/45 đơn vị bán. Điều này phản ánh đơn giá cao của Laptop, Monitor và phụ kiện điện tử; ngược lại Office có số lượng lớn nhưng giá trị thấp.

## 4.3. Doanh thu theo khu vực

| Khu vực | Đơn | Khách | Số lượng | Doanh thu | Trung bình/dòng |
| --- | ---: | ---: | ---: | ---: | ---: |
| Hà Nội | 3 | 2 | 25 | 1.524,00 | 508,00 |
| Đà Nẵng | 3 | 2 | 6 | 1.340,00 | 446,67 |
| Hồ Chí Minh | 2 | 2 | 3 | 620,00 | 310,00 |
| Cần Thơ | 2 | 2 | 11 | 200,00 | 100,00 |

## 4.4. Kết quả theo ngày

| Ngày | Đơn | Số lượng | Doanh thu |
| --- | ---: | ---: | ---: |
| 01/06/2026 | 2 | 3 | 1.291,00 |
| 02/06/2026 | 1 | 1 | 180,00 |
| 03/06/2026 | 1 | 4 | 300,00 |
| 04/06/2026 | 2 | 11 | 1.220,00 |
| 05/06/2026 | 2 | 5 | 494,00 |
| 06/06/2026 | 1 | 20 | 24,00 |
| 07/06/2026 | 1 | 1 | 175,00 |

## 4.5. Bằng chứng HDFS

Lần chạy thứ hai dùng đầu vào `hdfs://namenode:9000/data/raw/sales.csv` và đầu ra `hdfs://namenode:9000/data/processed/week3`. Job trả về cùng các chỉ tiêu 10 dòng, 45 sản phẩm, doanh thu 3.684,00, Parquet 10 dòng và ORC 10 dòng.

Lệnh `hdfs dfs -du -h /data/processed/week3` xác nhận đầy đủ bảy dataset: category summary, curated ORC, curated Parquet, daily summary, quality report, region summary và rejected rows.

## 4.6. Nhận xét tuần 3

- Pipeline tách rõ raw, curated, rejected và aggregate.
- Decimal được dùng cho tiền thay vì float, tránh sai số biểu diễn không cần thiết.
- Parquet và ORC đều được đọc lại, nên kết quả không chỉ dựa vào việc thư mục đã được tạo.
- Cùng một code path hoạt động với local filesystem và HDFS.
- Số partition và chiến lược partition vật lý phải được điều chỉnh theo kích thước và mẫu truy vấn thực tế; benchmark dùng 16 shuffle/output partition.

## 4.7. Benchmark 1,1073 GiB đã kiểm chứng

Snapshot `exercises/week3/benchmark_result.json` ghi lại lần chạy ngày
14/07/2026 trên Spark Standalone 3.5.1. Input có 6.646.350 dòng và
1.188.929.308 byte (`1,1073 GiB`). Job chạy bằng một executor 4 core/3 GiB,
16 shuffle partition và HDFS replication 1.

| Format | Codec | Kích thước data | Tỷ lệ so với CSV | Full scan median | Filter/group median |
| --- | --- | ---: | ---: | ---: | ---: |
| CSV | Không nén | 2.362.228.970 byte | 100,00% | 14,144 giây | 8,314 giây |
| Parquet | Snappy | 203.140.258 byte | 8,60% | 1,170 giây | 1,246 giây |
| ORC | Snappy | 167.822.712 byte | 7,10% | 1,733 giây | 1,278 giây |

Đối soát cuối có `unmatched_orders/customers/products/sellers = 0`, kết quả
DataFrame = Spark SQL = RDD và checksum của ba format bằng nhau cho hai workload.
Phép đo gồm 6 warm-up và 18 trial; HDFS có 0 block thiếu, hỏng hoặc
under-replicated. Số liệu phản ánh máy lab Docker/Windows và có thể chịu tác động
của filesystem cache, không phải cold-disk benchmark hay SLA production.

# 5. TUẦN 4 - THIẾT KẾ KAFKA VÀ STRUCTURED STREAMING

## 5.1. Kiến trúc luồng sự kiện

<!-- FIGURE:week4_flow -->

Luồng gồm các tầng sau:

1. `kafka_producer.py` sinh sự kiện log dịch vụ.
2. Kafka lưu sự kiện trong topic và gán partition/offset.
3. `spark_streaming_kafka.py` đọc offset, parse JSON và gắn một lý do lỗi ưu tiên.
4. Nhánh hợp lệ được tổng hợp có trạng thái và ghi báo cáo cửa sổ Parquet.
5. Nhánh không hợp lệ ghi quarantine Parquet kèm topic/partition/offset.
6. `foreachBatch` ghi metric accepted/rejected idempotent theo `batch_id`.

## 5.2. Schema sự kiện

| Trường | Kiểu logic | Ý nghĩa |
| --- | --- | --- |
| `ts` | Timestamp ISO 8601 | Event time ở UTC |
| `service` | String | `catalog`, `checkout` hoặc `payment` |
| `method` | String | HTTP method |
| `path` | String | Endpoint được gọi |
| `status_code` | Integer | Mã phản hồi 100-599 |
| `latency_ms` | Integer | Độ trễ không âm, đơn vị mili giây |

## 5.3. Producer có xác nhận giao nhận

Producer hỗ trợ hai chế độ:

- `--count 0`: chạy liên tục tới khi nhấn Ctrl+C.
- `--count N`: gửi đúng N sự kiện rồi kết thúc.

Tham số `--seed` tạo chuỗi dữ liệu ngẫu nhiên tái lập; `--interval` điều chỉnh
tốc độ. `--invalid-every N` cố ý tạo latency âm ở mỗi sự kiện thứ N để kiểm thử
quarantine và mặc định bằng 0. Mỗi `send()` chờ future trả metadata, sau đó mới
tăng bộ đếm và in partition/offset. Cấu hình `acks="all"`, `retries=5` và timeout
giúp lần chạy không báo thành công trước khi broker xác nhận.

## 5.4. Parse, phân loại và quarantine

Spark đọc cột `value` của Kafka, cast sang string rồi dùng `from_json` với schema.
Bản ghi chỉ đi vào aggregate khi:

- Parse được `event_time` từ `ts`.
- Có `service`, `path` và method thuộc tập cho phép.
- `status_code` nằm trong 100-599.
- `latency_ms` lớn hơn hoặc bằng 0.

Các điều kiện này ngăn malformed JSON, timestamp sai hoặc dữ liệu đo lường âm
đi vào stateful aggregation. Dòng lỗi không bị loại im lặng: sink quarantine giữ
`raw_json`, `rejection_reason`, topic, partition, offset, Kafka timestamp và thời
điểm ghi. Một dòng chỉ có một lý do ưu tiên, giúp metric không đếm trùng.

## 5.5. Event time, watermark và cửa sổ

Aggregate có grain:

```text
một window × một service × một status_code
```

Measure được tính:

- `requests`: số sự kiện trong nhóm.
- `avg_latency_ms`: độ trễ trung bình.
- `max_latency_ms`: độ trễ lớn nhất.
- `is_error`: đúng khi status code lớn hơn hoặc bằng 500.

Watermark được gắn trước `groupBy(window(...))`. Ở chế độ append, Spark chỉ ghi cửa sổ đã đủ điều kiện đóng. Vì vậy cửa sổ mới nhất có thể chưa xuất hiện ngay nếu chưa có event time mới đẩy watermark đi tiếp; đây là hành vi thiết kế, không phải mất dữ liệu.

## 5.6. Checkpoint và khả năng phục hồi

Checkpoint chứa:

| Thành phần | Vai trò |
| --- | --- |
| `offsets` | Offset Kafka mà từng micro-batch dự kiến xử lý |
| `commits` | Micro-batch đã commit thành công |
| `sources` | Metadata nguồn Kafka |
| `state` | State store cho window aggregation |
| `metadata` | Định danh và cấu hình query |

Ba query dùng ba checkpoint riêng cho status report, quarantine và quality
metrics. Khi checkpoint đã tồn tại, `--starting-offsets` không ghi đè tiến độ cũ.
Do đó không được dùng chung checkpoint cho topic, query hoặc cấu hình window
khác. Validator CLI từ chối mọi đường dẫn sink/checkpoint bị trùng.

Metric được ghi vào thư mục xác định bởi `batch_id` với chế độ overwrite đúng
micro-batch đó. Khi `foreachBatch` retry, cùng batch không tạo thêm dòng metric.
Kafka `failOnDataLoss` mặc định là `true`; cờ `--allow-data-loss` chỉ dành cho
tình huống phục hồi lab đã được người vận hành chấp thuận.

## 5.7. Dependency Kafka của Spark

Connector Kafka không có sẵn trong core Spark image, nên lệnh chạy bổ sung package:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
```

Image hiện tại không có thư mục Ivy cache ghi được dưới `/home/spark`. Lệnh đã được sửa bằng `--conf spark.jars.ivy=/tmp/.ivy2`; lần đầu tải 11 artifact, các lần sau dùng cache và không tải lại.

## 5.8. Lambda và Kappa

Lambda duy trì batch layer và speed layer, thường phải đồng bộ hai implementation
cùng tạo một serving view. Kappa giữ một event log và một streaming path; backfill
được thực hiện bằng replay. Tuần 4 chọn Kappa vì Kafka là nguồn chuẩn và cùng logic
parse/validate/window được dùng cho cả chạy liên tục lẫn `availableNow`.

Parquet output không tự biến hệ thống thành Lambda vì repo không có batch path thứ
hai tính lại cùng báo cáo. Lambda chỉ nên được bổ sung nếu retention Kafka không
đủ cho lịch sử, hoặc workload tính lại lịch sử khác căn bản với realtime. Quyết
định này giảm logic trùng, đổi lại phải quản lý retention, checkpoint và replay
chặt chẽ.

# 6. TUẦN 4 - KẾT QUẢ CHẠY END-TO-END

## 6.1. Kịch bản kiểm chứng

Để không ảnh hưởng topic mặc định, lần chạy tạo topic riêng `service-logs-week4-verify-20260712`, một partition, replication factor 1. Producer chạy với `--count 20 --interval 0.2 --seed 42`.

Kafka xác nhận 20 sự kiện ở partition 0, offset 0-19. Structured Streaming dùng:

- `startingOffsets=earliest` trên checkpoint mới.
- Cửa sổ 1 giây và watermark 0 giây để kiểm chứng nhanh.
- Trigger `availableNow` để xử lý hết backlog rồi tự kết thúc.
- Output và checkpoint riêng trong `output/week4/verification_20260712`.

Sau lần xử lý đầu, 19 request đã thuộc các cửa sổ đóng. Một sự kiện offset 20 được gửi sau đó để đẩy event time; lần chạy tiếp theo ghi cửa sổ còn lại của tập 20 sự kiện kiểm thử. Sự kiện offset 20 đóng vai trò tiến watermark và nằm ở cửa sổ mới nhất chưa đóng.

## 6.2. Kết quả tổng hợp

<!-- FIGURE:week4_results -->

| Service | Nhóm cửa sổ | Request | Độ trễ TB có trọng số | Max latency | Server error |
| --- | ---: | ---: | ---: | ---: | ---: |
| catalog | 10 | 11 | 646,82 ms | 1.169 ms | 2 |
| checkout | 5 | 5 | 300,20 ms | 562 ms | 2 |
| payment | 3 | 4 | 580,25 ms | 885 ms | 0 |
| **Tổng** | **18** | **20** | **546,85 ms** | **1.169 ms** | **4** |

Tỷ lệ server error của tập 20 sự kiện là 20%. `catalog` có lưu lượng lớn nhất và max latency cao nhất. `checkout` có hai lỗi server trên năm request trong tập kiểm thử; đây là tín hiệu cần ưu tiên khi xây dashboard cảnh báo, dù kích thước mẫu còn nhỏ.

## 6.3. Kiểm chứng checkpoint

Job được chạy lại với cùng topic, output và checkpoint khi không có offset mới. Số nhóm vẫn là 17, tổng request vẫn là 19 ở thời điểm đó; không có bản ghi trùng. Sau khi offset 20 đến và job chạy tiếp, output tăng đúng phần cửa sổ vừa đóng thành 18 nhóm/20 request.

Điều này chứng minh query tiếp tục từ checkpoint thay vì đọc lại offset 0.

## 6.5. Phạm vi bằng chứng sau khi bổ sung quality path

Lần chạy 20 sự kiện ở trên xác minh Kafka → aggregate → Parquet và checkpoint.
Quarantine/metric được bổ sung sau lần đo này và được kiểm tra bằng unit test với
payload hợp lệ, latency âm, method thiếu và malformed JSON. Script
`exercises/week4/run_local.ps1` tạo một kịch bản Docker hữu hạn có chèn lỗi định
kỳ để chạy lại đầy đủ ba sink; báo cáo không dùng kết quả unit test để giả nhận
đã có một lần end-to-end mới.

## 6.4. Ý nghĩa của kết quả

- Producer và broker liên lạc thành công qua hostname nội bộ `kafka:29092`.
- Connector Spark-Kafka tương thích với Spark 3.5.1/Scala 2.12.
- JSON schema, event time và aggregate stateful chạy trên cụm master/worker.
- Parquet sink và checkpoint cùng tồn tại trên bind mount Windows.
- Watermark hoạt động đúng: chỉ cửa sổ đủ điều kiện mới được append.

# 7. KIỂM THỬ, CHẤT LƯỢNG VÀ KHẢ NĂNG TÁI LẬP

## 7.1. Bộ kiểm thử tự động

| Test | Phạm vi | Kết quả |
| --- | --- | --- |
| Week 3 quality split | Loại trùng, tách valid/rejected, lý do lỗi | Đạt |
| Week 3 aggregate | `line_amount`, số đơn, số lượng, doanh thu | Đạt |
| Week 3 URI helper | Ghép URI HDFS không làm mất scheme/authority | Đạt |
| Week 3 scale/format | Replica estimate, join, ba Spark API và checksum ba format | Đạt |
| Week 4 producer | Số event, metadata xác nhận, dữ liệu hợp lệ | Đạt |
| Week 4 parse/quarantine | JSON hợp lệ, JSON lỗi, latency âm, method thiếu và raw payload | Đạt |
| Week 4 metric | Accepted/rejected, tỷ lệ lỗi và retry cùng batch không nhân đôi | Đạt |
| Week 4 aggregate | Window, request count, avg/max latency, cờ lỗi | Đạt |

Hai thư mục test hiện chứa 12 test case. Kết quả chạy cụ thể phải được cập nhật từ
lệnh pytest ở phụ lục, không tái sử dụng số liệu bốn test của phiên bản cũ.

## 7.2. Kiểm tra tĩnh và CLI

Ba script chính cùng hai file test đã vượt qua `python -m py_compile`. Mỗi script có `--help` và kiểm tra tham số không hợp lệ:

- `--shuffle-partitions` phải lớn hơn hoặc bằng 1.
- `--count`, `--interval` không được âm.
- `--invalid-every` không được âm.
- `--delivery-timeout`, `--trigger-seconds`, `--stop-after-seconds` phải dương.
- Không dùng đồng thời `--available-now` và `--stop-after-seconds`.
- Sáu đường dẫn output/checkpoint phải khác nhau.

## 7.3. Ma trận kiểm chứng thực tế

| Hạng mục | Cách kiểm chứng | Kết quả |
| --- | --- | --- |
| Spark cluster | Driver kết nối `spark://spark-master:7077`, executor RUNNING | Đạt |
| Batch local | Ghi và đọc lại output trong `/workspace/output/week3` | Đạt |
| Batch HDFS | Đọc `/data/raw`, ghi `/data/processed/week3` | Đạt |
| Fixture Parquet/ORC | So row count với valid rows | 10 = 10 = 10 |
| Benchmark 1 GiB+ | 6.646.350 dòng, checksum ba format | Đạt |
| Kafka producer | Broker trả partition và offset | 20/20 xác nhận |
| Kafka connector | Ivy resolve 11 artifact và query khởi động | Đạt |
| Streaming sink | Đọc lại Parquet bằng pandas/pyarrow | 18 nhóm, 20 request |
| Checkpoint | Rerun không offset mới | Không trùng |

## 7.4. Khả năng tái lập

Các yếu tố giúp tái lập:

- Docker image và phiên bản dependency được khóa trong repo.
- Producer có `--seed` và `--count`.
- Batch có `--input`, `--output` và `--mode`.
- Streaming có topic, offset, window, watermark, trigger, output và checkpoint cấu hình qua CLI.
- Script `run_local.ps1` tạo topic, phát fixture lỗi và chạy `availableNow` không xóa output cũ.
- Mỗi bài có test chạy trong workspace container.
- README cung cấp lệnh đã được chạy thật trên Windows PowerShell.

# 8. ĐÁNH GIÁ, HẠN CHẾ VÀ HƯỚNG PHÁT TRIỂN

## 8.1. Điểm đã hoàn thiện

- Mã nguồn tách hàm rõ ràng, có thể unit test mà không cần khởi động toàn bộ pipeline trong từng test.
- Batch có data quality report và đọc lại hai định dạng đích.
- HDFS được sử dụng thật, không chỉ mô tả lý thuyết.
- Producer chờ Kafka xác nhận thay vì chỉ flush mù.
- Streaming phân biệt event time với processing time và có watermark.
- Checkpoint được giải thích và kiểm chứng bằng rerun.
- Dòng lỗi có quarantine truy vết được và micro-batch có metric accepted/rejected.
- `failOnDataLoss=true` là mặc định; đường dẫn checkpoint trùng bị từ chối sớm.
- README đã sửa hai lỗi vận hành thực tế: đường dẫn `spark-submit` và Ivy cache.

## 8.2. Hạn chế của môi trường học tập

| Hạn chế | Ảnh hưởng |
| --- | --- |
| Một Kafka broker, topic một partition | Không kiểm chứng scale-out hoặc failover |
| Dataset 1 GiB được mở rộng bằng `replica_id` | Đo scale/format nhưng không đại diện độ đa dạng của 6,6 triệu giao dịch tự nhiên |
| Schema JSON khai báo trong code | Chưa có Schema Registry/versioning |
| Quarantine nằm ở Parquet local | Truy vết được nhưng chưa có retention/replay tự động như dead-letter topic production |
| Output/checkpoint trên bind mount | Phù hợp local, chưa phải storage production |
| Metric mới ở mức file theo batch | Chưa scrape Kafka lag, state size hoặc phát alert tự động |
| Không bật TLS/SASL | Chỉ phù hợp môi trường phát triển |

## 8.3. Hướng nâng cấp production

1. Tăng số broker/partition và cấu hình replication phù hợp SLA.
2. Dùng Avro/Protobuf cùng Schema Registry để quản lý tương thích schema.
3. Đưa quarantine lên dead-letter topic có retention/replay và quyền truy cập rõ ràng.
4. Đưa output/checkpoint lên object storage hoặc HDFS HA, tránh bind mount đơn lẻ.
5. Export metric hiện có cùng input rate, Kafka lag, state size và batch duration sang Prometheus.
6. Đặt alert cho tỷ lệ 5xx, p95/p99 latency và số bản ghi bị loại.
7. Viết integration test có Testcontainers hoặc môi trường CI tách biệt.
8. Tối ưu partition, file size và compaction khi dữ liệu tăng.
9. Điều phối job bằng Airflow ở tuần sau, có retry và SLA rõ ràng.

# 9. KẾT LUẬN

Tuần 3 đã chuyển dữ liệu bán hàng từ CSV sang một batch pipeline có kiểm soát
chất lượng, aggregate, Parquet, ORC và HDFS. Ngoài fixture chức năng 10 dòng,
benchmark đã xử lý 6.646.350 dòng/1,1073 GiB và đối soát ba Spark API, checksum
ba format cùng sức khỏe HDFS.

Tuần 4 đã hình thành một streaming pipeline hoàn chỉnh ở mức thực hành: producer
phát sự kiện có xác nhận, Kafka cấp offset, Spark phân loại theo schema, aggregate
theo event time/watermark, lưu quarantine và metric, còn checkpoint bảo đảm tiếp
tục đúng tiến độ. Kịch bản cũ 20 sự kiện đã kiểm chứng aggregate/checkpoint; quality
path mới có unit test và runbook end-to-end riêng.

Hai tuần đã đạt yêu cầu về code, dữ liệu, vận hành, kiểm thử và tài liệu. Repo sẵn sàng làm nền cho các tuần Airflow, orchestration, monitoring và hardening tiếp theo.

## 9.1. Kết quả bàn giao

| Thành phần | Trạng thái | Ghi chú |
| --- | --- | --- |
| Spark batch tuần 3 | Hoàn thành | Chạy local và HDFS, có quality report |
| Parquet/ORC | Hoàn thành | Fixture 10/10; benchmark 6.646.350 dòng có checksum bằng nhau |
| Kafka producer | Hoàn thành | Hỗ trợ continuous/finite, seed và broker acknowledgement |
| Structured Streaming | Hoàn thành | Event time, watermark, aggregate, quarantine, metric và checkpoint |
| Kiểm thử | Hoàn thành | 12 test case cho tuần 3-4 và kiểm tra cú pháp |
| README | Hoàn thành | Lệnh chạy đúng image, tiếng Việt có dấu |
| Báo cáo | Hoàn thành | Markdown đã đồng bộ benchmark và quality path; DOCX sinh từ nguồn này |

Các artifact bàn giao đều nằm trong repo hiện tại, không tạo branch mới và không thực hiện merge vào nhánh khác.

# PHỤ LỤC A - RUNBOOK CHẠY LẠI TOÀN BỘ BÀI THỰC HÀNH

## A.1. Khởi động môi trường tuần 4

Profile tuần 4 đã bao gồm các service của tuần 3:

```powershell
.\scripts\start.ps1 -Target week4 -Build
docker compose ps
```

Các service cần ở trạng thái Up: workspace, Spark master/worker, NameNode/DataNode, Kafka, PostgreSQL và MySQL.

## A.2. Chạy tuần 3 với output local

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/exercises/week3/spark_batch.py
```

Đọc quality report:

```powershell
Get-ChildItem output/week3/quality_report -Filter *.json | Get-Content
```

## A.3. Chạy tuần 3 với HDFS

```powershell
docker compose exec namenode hdfs dfs -mkdir -p /data/raw
docker compose exec namenode hdfs dfs -put -f /workspace/data/sample/sales.csv /data/raw/sales.csv
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/exercises/week3/spark_batch.py --input hdfs://namenode:9000/data/raw/sales.csv --output hdfs://namenode:9000/data/processed/week3
docker compose exec namenode hdfs dfs -du -h /data/processed/week3
```

## A.4. Chạy benchmark Olist 1 GiB+

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 2g --executor-memory 3g --executor-cores 4 `
  --conf spark.cores.max=4 `
  /workspace/exercises/week3/generate_olist_1gb.py `
  --target-gib 1.0 --partitions 16

docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 2g --executor-memory 3g --executor-cores 4 `
  --conf spark.cores.max=4 `
  /workspace/exercises/week3/olist_format_benchmark.py `
  --shuffle-partitions 16 --output-partitions 16 `
  --warmups 1 --trials 3
```

Không dùng `--allow-small-input` hoặc `--allow-unmatched` trong lần nghiệm thu.
Xem đầy đủ điều kiện RAM, output và cách đọc report tại
`exercises/week3/README.md`.

## A.5. Tạo topic và chạy producer tuần 4

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic service-logs --partitions 1 --replication-factor 1
docker compose exec workspace python exercises/week4/kafka_producer.py --count 20 --interval 0.2 --invalid-every 5 --seed 42
```

Nếu muốn dữ liệu liên tục, bỏ `--count`, `--interval` và `--seed`.

## A.6. Chạy streaming liên tục

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /workspace/exercises/week4/spark_streaming_kafka.py
```

Nhấn Ctrl+C để dừng có kiểm soát.

## A.7. Chạy streaming hữu hạn để kiểm thử

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /workspace/exercises/week4/spark_streaming_kafka.py --starting-offsets earliest --available-now
```

Lệnh gọn để tạo topic, phát cả dữ liệu hợp lệ/lỗi và chạy đủ ba sink:

```powershell
.\exercises\week4\run_local.ps1 -Build
```

## A.8. Chạy kiểm thử tự động

```powershell
docker compose exec workspace pytest -q exercises/week3/tests exercises/week4/tests
docker compose exec workspace python -m py_compile exercises/week3/spark_batch.py exercises/week4/kafka_producer.py exercises/week4/spark_streaming_kafka.py
```

## A.9. Dừng môi trường

```powershell
.\scripts\stop.ps1
```

# PHỤ LỤC B - TỪ ĐIỂN DỮ LIỆU VÀ CẤU TRÚC ĐẦU RA

## B.1. Curated sales tuần 3

| Cột | Kiểu | Quy tắc |
| --- | --- | --- |
| `order_id` | String | Bắt buộc, đã trim |
| `order_date` | Date | Parse `yyyy-MM-dd` |
| `customer_id` | String | Bắt buộc |
| `customer_name` | String | Đã trim |
| `product_id` | String | Bắt buộc |
| `product_name` | String | Đã trim |
| `category` | String | Bắt buộc |
| `quantity` | Integer | Lớn hơn 0 |
| `unit_price` | Decimal(18,2) | Lớn hơn hoặc bằng 0 |
| `line_amount` | Decimal(20,2) | `quantity × unit_price` |
| `region` | String | Bắt buộc |
| `order_year` | Integer | Dùng partition |
| `order_month` | Integer | Dùng partition |
| `_source_file` | String | Truy vết file nguồn |

## B.2. Quality report tuần 3

| Cột | Ý nghĩa |
| --- | --- |
| `run_id` | UUID của lần chạy |
| `generated_at_utc` | Thời điểm tạo báo cáo UTC |
| `source_rows` | Tổng dòng đọc từ nguồn |
| `duplicates_removed` | Dòng trùng bị loại |
| `rejected_rows` | Dòng không đạt chất lượng |
| `valid_rows` | Dòng hợp lệ |
| `distinct_orders` | Số order khác nhau |
| `distinct_customers` | Số customer khác nhau |
| `total_quantity` | Tổng quantity |
| `total_revenue` | Tổng line amount |
| `parquet_rows`, `orc_rows` | Số dòng đọc lại từ hai định dạng |

## B.3. Streaming status report tuần 4

| Cột | Ý nghĩa |
| --- | --- |
| `window.start`, `window.end` | Biên cửa sổ event time |
| `service` | Dịch vụ được tổng hợp |
| `status_code` | Mã HTTP trong nhóm |
| `requests` | Số sự kiện |
| `avg_latency_ms` | Độ trễ trung bình |
| `max_latency_ms` | Độ trễ lớn nhất |
| `is_error` | Đúng nếu status code >= 500 |

## B.4. Streaming quarantine tuần 4

| Cột | Ý nghĩa |
| --- | --- |
| `raw_json` | Payload gốc để điều tra hoặc replay |
| `rejection_reason` | Lý do lỗi ưu tiên, không đếm trùng một payload |
| `source_topic` | Kafka topic nguồn |
| `source_partition`, `source_offset` | Vị trí chính xác trong event log |
| `kafka_timestamp` | Timestamp do Kafka gắn |
| `quarantined_at` | Thời điểm Spark ghi quarantine |

## B.5. Quality metric tuần 4

| Cột | Ý nghĩa |
| --- | --- |
| `batch_id` | ID ổn định của micro-batch trong checkpoint metric |
| `recorded_at` | Thời điểm ghi metric |
| `processed_records` | Tổng bản ghi batch |
| `accepted_records` | Bản ghi đi vào aggregate |
| `rejected_records` | Bản ghi đi vào quarantine |
| `rejection_ratio` | `rejected_records / processed_records` |

# PHỤ LỤC C - MA TRẬN BẰNG CHỨNG VÀ CẤU TRÚC MÃ NGUỒN

## C.1. Tệp triển khai

```text
exercises/
  week3/
    spark_batch.py
    generate_olist_1gb.py
    olist_format_benchmark.py
    benchmark_result.json
    tests/
      test_spark_batch.py
  week4/
    README.md
    run_local.ps1
    kafka_producer.py
    spark_streaming_kafka.py
    tests/
      test_week4_logic.py
    report/
      bao_cao_tuan_3_4.md
      bao_cao_tuan_3_4.docx
      build_report_docx.py
```

## C.2. Ánh xạ yêu cầu sang bằng chứng

| Yêu cầu | Mã nguồn | Bằng chứng chạy |
| --- | --- | --- |
| Spark batch | `spark_batch.py` | Fixture local và HDFS cùng đạt 10 dòng |
| Quy mô 1 GiB+ | `generate_olist_1gb.py` | 6.646.350 dòng, 1,1073 GiB trên HDFS |
| Join/ba Spark API | `olist_format_benchmark.py` | 0 orphan; DataFrame = SQL = RDD |
| Parquet/ORC | `write_datasets()` và benchmark | Fixture 10/10; checksum ba format bằng nhau |
| Data quality | `normalize_and_validate()` | Quality JSON và pytest |
| Kafka producer | `send_events()` | Offset 0-19 được xác nhận |
| JSON validation | `classify_logs()` | Pytest gồm JSON lỗi/latency âm/method thiếu |
| Quarantine | `rejected_logs()` | Giữ payload, lý do và Kafka offset |
| Metric chất lượng | `write_quality_metrics()` | Accepted/rejected và retry cùng batch idempotent |
| Window aggregate | `aggregate_logs()` | 18 nhóm, 20 request |
| Watermark | `build_streaming_report()` | Cửa sổ cuối chỉ ghi sau event mới |
| Checkpoint | DataStreamWriter | Rerun không tạo trùng |

## C.3. Lỗi vận hành đã phát hiện và sửa

| Hiện tượng | Nguyên nhân | Cách sửa |
| --- | --- | --- |
| `spark-submit` không tìm thấy | Binary không nằm trên PATH của image | Dùng `/opt/spark/bin/spark-submit` |
| Ivy `FileNotFoundException` | `/home/spark/.ivy2` không ghi/tạo được | Đặt `spark.jars.ivy=/tmp/.ivy2` |
| Cửa sổ cuối chưa xuất hiện | Append + watermark chưa đóng cửa sổ | Gửi event time mới hoặc chạy liên tục |
| Lệnh `startingOffsets` không đổi tiến độ | Checkpoint đã có offset | Dùng checkpoint mới khi đổi nguồn/cấu hình |
| Bản ghi lỗi biến mất khỏi phân tích | Chỉ filter nhánh hợp lệ | Ghi quarantine với payload/lý do/offset |
| Kafka thiếu offset nhưng job vẫn chạy | `failOnDataLoss=false` che giấu mất dữ liệu | Mặc định `true`, chỉ mở cờ khi phục hồi có chủ đích |
| Hai query dùng chung checkpoint | State/commit log có thể hỏng | CLI bắt buộc sáu đường dẫn khác nhau |

# TÀI LIỆU THAM KHẢO

1. `README.md` của dự án DE Genesis - hướng dẫn môi trường và lệnh chạy.
2. `docker-compose.yml` - cấu hình service, network, port, volume và profile.
3. `data/sample/sales.csv` và `data/sample/service_logs.jsonl` - dữ liệu mẫu.
4. Apache Spark Release 3.5.1: https://spark.apache.org/releases/spark-release-3-5-1.html
5. Apache Spark Structured Streaming Programming Guide, nhánh 3.5: https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html
6. Apache Spark ORC Files: https://spark.apache.org/docs/3.5.6/sql-data-sources-orc.html
7. Apache Spark Parquet Files: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
8. Apache Hadoop 3.2.1 FileSystem Shell: https://hadoop.apache.org/docs/r3.2.1/hadoop-project-dist/hadoop-common/FileSystemShell.html
9. Apache Kafka Documentation: https://kafka.apache.org/documentation/
