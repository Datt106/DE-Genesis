# BỘ 20 CÂU HỎI PHẢN BIỆN & CÂU TRẢ LỜI CHUYÊN SÂU (DÙNG CHO BÁO CÁO MENTOR)
## Bộ Câu Hỏi Từ Nhận Biết Đến Tầng Kiến Trúc Hệ Thống (Data Engineering DE Genesis)

---

## NÓM 1: MỨC ĐỘ NHẬN BIẾT & CƠ BẢN (BASIC & CORE CONCEPTS)

### Câu 1: Tổng thể dự án DE Genesis giải quyết bài toán gì và kiến trúc end-to-end gồm những thành phần chính nào?
* **Trả lời:**
  Dự án **DE Genesis** giải quyết bài toán xây dựng một hệ thống Data Engineering toàn diện thu nhỏ (production-like local) theo 6 tuần lộ trình:
  - **Bài toán:** Thu nhận dữ liệu từ nhiều nguồn (CSV lịch sử Olist, REST/SOAP API, Service Event Logs thời gian thực), làm sạch, mô hình hóa OLTP/OLAP, xử lý Big Data Batch & Event Streaming, điều phối có Data Quality Gate và giám sát cảnh báo tự động.
  - **6 Thành phần chính:**
    1. **Ingestion Layer:** Python Scripts, Apache NiFi, Kafka Producers.
    2. **Streaming Layer:** Apache Kafka (Event Log Storage).
    3. **Processing Layer:** Apache Spark (Spark SQL/DataFrame Batch & Spark Structured Streaming).
    4. **Storage Layer:** HDFS (Raw & Columnar Storage Parquet/ORC), PostgreSQL & MySQL (OLTP 3NF & OLAP Star Schema).
    5. **Orchestration Layer:** Apache Airflow (Scheduled Jobs, 8 Data Quality Gates, Atomic Publish).
    6. **Monitoring Layer:** Telemetry Exporter, Prometheus (14 Alert rules), Grafana Dashboards.

---

### Câu 2: Tại sao lại chuyển dữ liệu Olist từ 3NF sang Star Schema (Kimball)? Khác biệt giữa OLTP và OLAP là gì?
* **Trả lời:**
  - **OLTP (Online Transaction Processing):** Thiết kế theo chuẩn **3NF (Third Normal Form)** chia thành 12 bảng nhằm **triệt tiêu dư thừa dữ liệu (Data Redundancy)**, tối ưu cho các thao tác Ghi/Cập nhật (Insert/Update) của giao dịch hàng ngày với thuộc tính ACID.
  - **OLAP (Online Analytical Processing):** Thiết kế theo mô hình **Star Schema (Kimball)** gồm 4 Bảng Fact (Đo lường chỉ số kinh doanh) và 7 Bảng Dimension (Bối cảnh như Customer, Product, Time).
  - **Lý do chuyển đổi:** 3NF có quá nhiều phép JOIN phức tạp gây chậm truy vấn báo cáo. Star Schema giảm số lượng JOIN, tối ưu cho các truy vấn Đọc/Phân tích (Read-heavy aggregations), đồng thời hỗ trợ theo dõi lịch sử biến động dữ liệu qua **SCD Type 2**.

---

### Câu 3: HDFS là gì? Kiến trúc gồm những component nào và tại sao lại dùng HDFS thay vì đĩa Local hay RDBMS?
* **Trả lời:**
  - **HDFS (Hadoop Distributed File System)** là hệ thống tập tin phân tán lưu trữ các tập dữ liệu Big Data trên cụm máy tính.
  - **Kiến trúc gồm 2 phần chính:**
    1. **NameNode (Master):** Quản lý Namespace cây thư mục và vị trí các Block (Metadata).
    2. **DataNode (Worker):** Lưu trữ các khối dữ liệu thực sự (Data Blocks, mặc định 128MB).
  - **Lý do dùng HDFS thay vì Local FS / RDBMS:**
    - *Scale-out:* Mở rộng đĩa ngang không giới hạn bằng cách thêm DataNode.
    - *Fault Tolerance:* Tự động sao chép Block (Replication Factor = 3) phòng khi có máy hỏng.
    - *Mô hình WORM (Write Once Read Many):* Đọc luồng dữ liệu lớn song song cực nhanh cho Analytics Engine như Spark.
    - *Data Locality:* Cho phép Spark tính toán ngay tại node chứa dữ liệu, tránh nghẽn băng thông mạng.

---

### Câu 4: Định dạng Parquet và ORC khác gì so với CSV? Tại sao trong benchmark của dự án Parquet lại nhanh hơn CSV tới 12 lần?
* **Trả lời:**
  - **CSV:** Định dạng dạng Dòng (Row-based), văn bản thuần (Text), không nén, tốn đĩa và bắt buộc phải đọc toàn bộ file (Full Scan) ngay cả khi chỉ cần lấy 1 cột.
  - **Parquet / ORC:** Định dạng dạng Cột (Columnar), nhị phân (Binary), lưu theo từng khối dòng (Row Groups / Stripes).
  - **Nguyên nhân Parquet nhanh hơn CSV 12.09 lần (1.16s vs 14.14s trên 1.1GB / 6.64M dòng):**
    1. *Nén dữ liệu (Compression):* Nén Snappy giảm dung lượng đĩa từ 2.3GB xuống 203MB (giảm 90% I/O đĩa).
    2. *Projection Pushdown:* Chỉ đọc đúng các cột có trong câu lệnh `SELECT`, bỏ qua toàn bộ các cột khác.
    3. *Predicate Pushdown:* Dựa vào chỉ số Min/Max Metadata của từng Row Group để bỏ qua (skip) các block dữ liệu không thỏa điều kiện `WHERE`.

---

### Câu 5: Kiến trúc Lambda và Kappa khác nhau ở điểm nào? Dự án này sử dụng kiến trúc nào cho luồng Service Log và tại sao?
* **Trả lời:**
  - **Lambda Architecture:** Gồm 2 nhánh song song: *Batch Layer* (xử lý dữ liệu thô định kỳ tạo Batch View) + *Speed Layer* (xử lý realtime tạo Realtime View) -> hợp nhất tại *Serving Layer*.
    - *Nhược điểm:* Phải viết và duy trì 2 bộ codebase độc lập cho cùng 1 logic, dễ gây sai lệch dữ liệu giữa 2 nhánh.
  - **Kappa Architecture:** Bỏ hẳn Batch Layer. Tất cả dữ liệu đi qua một **Immutable Event Log** (Kafka). Dùng **duy nhất 1 Engine Streaming** (Spark Structured Streaming) cho cả realtime và backfill.
  - **Lý do dự án chọn Kappa:** Kafka giữ vai trò log bất biến hỗ trợ replay dữ liệu từ offset cũ khi cần backfill. Dùng chung 1 codebase PySpark Streaming xử lý cả realtime và re-processing giúp triệt tiêu 50% technical debt và đảm bảo tính nhất quán dữ liệu 100%.

---

## NHÓM 2: MỨC ĐỘ THÔNG HIỂU & VẬN HÀNH (UNDERSTANDING & OPERATION)

### Câu 6: Trong Kafka Producer, `acks=all` và `idempotent producer` có ý nghĩa gì?
* **Trả lời:**
  - `acks=all` (hoặc `acks=-1`): Yêu cầu Kafka Broker chỉ phản hồi ACK thành công khi tin nhắn đã được ghi nhận và đồng bộ thành công sang **toàn bộ các in-sync replicas (ISR)**. Giúp đảm bảo dữ liệu không bị mất ngay cả khi Leader Broker bị đứt điện.
  - `enable.idempotence=true`: Giúp Kafka Producer gán một ID duy nhất (`ProducerId` + `SequenceNumber`) cho mỗi tin nhắn. Nếu do sự cố mạng làm Producer gửi lại (retry), Kafka Broker sẽ tự động nhận biết và **loại bỏ tin nhắn trùng lặp**, đảm bảo cơ chế **Exactly-Once Delivery** tại tầng Ingestion.

---

### Câu 7: Spark Structured Streaming xử lý dữ liệu theo cơ chế nào? Khái niệm `Watermark` và `Event Time` giải quyết bài toán gì?
* **Trả lời:**
  - **Cơ chế:** Mặc định chạy theo cơ chế **Micro-batch Processing** (mỗi chu kỳ N giây đọc một đợt các offset mới từ Kafka thành 1 DataFrame nhỏ để xử lý).
  - **Event Time:** Thời gian sự kiện thực sự xảy ra ở phía client/service (được ghi trong payload `event_timestamp`), không phải thời gian Spark nhận được dữ liệu (Processing Time).
  - **Watermark:** Là ngưỡng thời gian chấp nhận dữ liệu muộn (Late Data Margin). Ví dụ `withWatermark("event_time", "10 minutes")` báo cho Spark biết: *"Hãy giữ lại state để chờ dữ liệu đến muộn tối đa 10 phút. Sau 10 phút, các event có timestamp cũ hơn Watermark sẽ bị loại bỏ (drop) để giải phóng tài nguyên RAM tĩnh."*

---

### Câu 8: Tại sao không dùng Airflow để chạy tiến trình Spark Structured Streaming liên tục 24/7 mà lại tách riêng hai thành phần này?
* **Trả lời:**
  - Airflow được thiết kế làm **Job Orchestrator cho các tác vụ hữu hạn (Batch Jobs)** có điểm đầu và điểm kết thúc rõ ràng.
  - Nếu dùng Airflow Task để kích hoạt và giữ tiến trình Streaming chạy 24/7, nó sẽ **chiếm dụng vĩnh viễn 1 Slot Worker/Executor của Airflow**, dẫn đến nghẽn tài nguyên và nghẽn lịch chạy của các DAG khác.
  - **Giải pháp trong dự án:** Spark Structured Streaming chạy dưới dạng một **Daemon Service độc lập** ngoài hệ thống (có Checkpointing tự phục hồi). Airflow chỉ đóng vai trò điều phối định kỳ các tác vụ hữu hạn như: chạy báo cáo closed-report, kiểm tra Data Quality Gate, backfill và dọn dẹp dữ liệu.

---

### Câu 9: SCD Type 2 là gì và được triển khai như thế nào trong bài lab OLAP của dự án?
* **Trả lời:**
  - **SCD Type 2 (Slowly Changing Dimension Type 2):** Là kỹ thuật lưu trữ toàn bộ lịch sử thay đổi thông tin thuộc tính theo thời gian bằng cách chèn một dòng ghi chép mới thay vì đè lên dòng cũ.
  - **Cách triển khai trong `load_olist_olap.py`:**
    - Mỗi dòng trong Bảng Dimension (như `dim_customer`, `dim_product`) được tạo một khóa thay thế **Surrogate Key** (khóa tự tăng độc lập với Natural Key từ nguồn).
    - Thêm các cột quản lý lịch sử: `effective_date` (ngày bắt đầu hiệu lực), `expiration_date` (ngày hết hiệu lực, mặc định `9999-12-31`), và `is_current` (boolean: `TRUE` nếu là thông tin hiện tại, `FALSE` nếu là lịch sử cũ).
    - Khi thông tin khách hàng đổi địa chỉ: Dòng cũ chuyển `is_current = FALSE` và đóng `expiration_date`. Dòng mới được INSERT với Surrogate Key mới và `is_current = TRUE`.

---

### Câu 10: Hệ thống giám sát (Prometheus & Grafana) theo dõi những metric quan trọng nào trong pipeline? Khi nào một Alert Rule sẽ bị chuyển sang `FIRING`?
* **Trả lời:**
  - **Metric chính:**
    1. *Streaming Consumer Lag:* Khoảng cách giữa offset lớn nhất của Kafka và offset Spark đã xử lý.
    2. *Micro-batch Duration:* Thời gian xử lý micro-batch của Spark.
    3. *Report Heartbeat & Status:* Trạng thái thành công/thất bại và timestamp thực thi gần nhất của Airflow DAG.
  - **Trạng thái `FIRING`:** Khi một biểu thức điều kiện trong `de_genesis_alerts.yml` thỏa mãn liên tục trong một khoảng thời gian chờ (thông số `for: 1m` hoặc `for: 5m`). Ví dụ: Nếu `spark_streaming_lag_ms > 60000` liên tục quá 1 phút, Prometheus sẽ chuyển alert từ `PENDING` sang `FIRING` và đẩy notification.

---

## NHÓM 3: KỸ THUẬT CHUYÊN SÂU (DEEP-DIVE TECHNICAL)

### Câu 11: Khi Spark đọc dữ liệu từ HDFS, cơ chế Data Locality và Partitioning diễn ra như thế nào? Block size 128MB ảnh hưởng gì đến Spark?
* **Trả lời:**
  - **Partitioning:** Khi Spark đọc một file từ HDFS, Spark Driver sẽ tự động chia file đó thành các **Input Partitions**. Mỗi Block HDFS (mặc định 128MB) tương ứng với 1 Partition của Spark DataFrame và sẽ được xử lý bởi 1 **Spark Task** riêng biệt.
  - **Data Locality (Vị trí tính toán):** Spark Scheduler cố gắng giao Task tính toán cho **Executor đang chạy trên chính DataNode lưu trữ HDFS Block đó** (`PROCESS_LOCAL` hoặc `NODE_LOCAL`). Nhờ vậy, Spark không cần phải truyền khối dữ liệu 128MB qua hạ tầng mạng card LAN, giúp tốc độ tính toán tăng lên gấp nhiều lần.

---

### Câu 12: Làm thế nào hệ thống đảm bảo tính `Idempotency` (không bị nhân đôi dữ liệu) khi Airflow phải retry một DAG run đã từng thành công?
* **Trả lời:**
  - **Idempotency:** Nghĩa là việc thực thi một thao tác nhiều lần luôn mang lại kết quả giống hệt như thực thi 1 lần duy nhất.
  - **Cơ chế trong Tuần 6:**
    1. *HDFS Staging:* Báo cáo ghi vào thư mục tạm được đặt tên chứa kèm `run_id` (uuid duy nhất của lần chạy).
    2. *PostgreSQL Canonical:* Khi publish dữ liệu từ Staging vào PostgreSQL, câu lệnh SQL sử dụng cơ chế **UPSERT** (`ON CONFLICT (service_name, minute_window) DO UPDATE...`) hoặc xóa dữ liệu cũ thuộc `run_id` đó trong một **Database Transaction** trước khi Re-insert.
    3. *Check thành công:* Nếu Airflow nhận thấy `run_id` này đã có marker xác nhận thành công trong bảng control (`pipeline_publish_log`), task sẽ lập tức trả về `NO-OP` (No Operation) và kết thúc an toàn mà không chèn trùng dữ liệu.

---

### Câu 13: Cơ chế Quarantine Strategy (Cách ly dữ liệu lỗi) trong Spark Streaming hoạt động ra sao?
* **Trả lời:**
  - **Đặt vấn đề:** Trong luồng streaming thời gian thực, nếu gặp phải tin nhắn bị lỗi định dạng (Corrupted JSON, sai kiểu dữ liệu, thiếu trường bắt buộc), nếu làm sập pipeline (Crash) thì toàn bộ hệ thống ngưng trệ, nếu bỏ qua (Drop) thì mất dữ liệu kiểm toán.
  - **Giải pháp Quarantine:**
    - Trong `spark_streaming_kafka.py`, Spark sử dụng `try_parse` hoặc Schema Validation.
    - Dữ liệu được rẽ làm 2 nhánh bằng câu lệnh điều kiện:
      - *Accepted Stream (Hợp lệ):* Đi qua transformation logic và ghi vào HDFS raw logs.
      - *Quarantine Stream (Lỗi):* Được giữ nguyên Payload thô gốc, bổ sung thêm các cột Metadata (`error_reason`, `kafka_topic`, `kafka_offset`, `received_timestamp`) và ghi riêng ra thư mục HDFS Quarantine / Bảng Postgres Quarantine để kỹ sư dữ liệu có thể soi và replay lại sau.

---

### Câu 14: Quy trình "Atomic Publish" (Xuất dữ liệu nguyên tử) ở Tuần 6 được thực hiện như thế nào giữa HDFS Staging và PostgreSQL Canonical Table?
* **Trả lời:**
  - **Mục đích:** Đảm bảo dữ liệu chỉ xuất hiện ở trạng thái hoàn chỉnh 100%, không người dùng nào đọc được dữ liệu dở dang (Partial Data).
  - **Quy trình 2 bước nguyên tử:**
    1. *HDFS Atomic Rename:* Spark ghi dữ liệu báo cáo vào thư mục Staging dạng tạm (`/staging/_tmp_run123/`). Sau khi 8 bài test Data Quality đạt, Airflow gọi lệnh `hdfs dfs -mv` để đổi tên thư mục sang đường dẫn chính thức `/canonical/year=.../` trong thời gian tính bằng miligiây (FileSystem Rename trên HDFS là thao tác Metadata atomic).
    2. *PostgreSQL Transactional Publish:* Việc chèn dữ liệu vào bảng PostgreSQL Canonical được bọc trọn gói trong câu lệnh `BEGIN TRANSACTION; ... COMMIT;`. Nếu có bất kỳ sự cố mạng nào xảy ra giữa chừng, toàn bộ transaction bị `ROLLBACK` về trạng thái ban đầu.

---

### Câu 15: Xử lý dữ liệu đến muộn (Late Event) được thiết kế thế nào? Tại sao `WEEK6_LOG_REPORT_SETTLEMENT_SECONDS` lại bằng 180s?
* **Trả lời:**
  - **Lateness Problem:** Do ngắt kết nối mạng ở phía client, một số log sinh ra lúc 20:00 nhưng đến 20:02 mới tới Kafka.
  - **Thiết kế Settlement 180 giây:**
    - Airflow không chốt báo cáo ngay khi vừa hết phút, mà lùi lại một khoảng chờ **Settlement Period = 180 giây**.
    - **Công thức:** `Settlement (180s) >= Max Event Lateness Delay (120s) + Micro-batch Execution Interval (30s) + Buffer An Toàn (30s)`.
    - Nhờ khoảng chờ 180s này, các event đến muộn trong vòng 2 phút vẫn kịp lọt vào micro-batch của Spark và được tính đúng vào cửa sổ thời gian (Window Bucket) của nó trước khi Airflow khóa báo cáo.

---

## NHÓM 4: TẦNG KIẾN TRÚC & QUYẾT ĐỊNH THIẾT KẾ (ARCHITECTURAL & TRADE-OFFS)

### Câu 16: Nếu hệ thống bị nghẽn (Backpressure / Lag tăng cao) ở luồng Kafka -> Spark Streaming, bạn xử lý và tuning thông số nào?
* **Trả lời:**
  - **Tài nguyên phần cứng:** Tăng số lượng Partition của Kafka Topic + Tăng số lượng Executing Cores của Spark để tăng số Task đọc song song.
  - **Tuning thông số Spark Structured Streaming:**
    1. `spark.streaming.kafka.maxRatePerPartition`: Giới hạn số lượng record tối đa được đọc từ mỗi Kafka Partition trong 1 micro-batch (ví dụ set = 1000) để ngăn Spark bị sập OOM khi dồn ứ hàng triệu tin nhắn.
    2. `spark.sql.shuffle.partitions`: Giảm thông số mặc định (200) xuống phù hợp với số lượng CPU cores của cluster (ví dụ set = 8 hoặc 16) để tránh tốn overhead tạo quá nhiều partition rỗng trong các phép `GROUP BY`.
    3. `spark.streaming.backpressure.enabled=true`: Bật cơ chế tự động hạ tốc độ nhận dữ liệu của Spark khi thấy thời gian xử lý micro-batch trước đó bắt đầu vượt quá ngưỡng chu kỳ micro-batch.

---

### Câu 17: Nếu một Spark Worker bị Out Of Memory (OOM - `java.lang.OutOfMemoryError: Java heap space`), nguyên nhân gốc rễ nằm ở đâu và khắc phục ra sao?
* **Trả lời:**
  - **Nguyên nhân gốc rễ (Root Causes):**
    1. *Data Skew (Lệch dữ liệu):* Một phím JOIN hoặc GROUP BY chứa quá nhiều dữ liệu làm cho 1 Partition phồng to bất thường, dồn toàn bộ dữ liệu đó về 1 Worker duy nhất.
    2. *Lưu State vô hạn trong Streaming:* Dùng `mapGroupsWithState` hoặc Watermark quá dài làm State Store trên RAM phồng to theo thời gian.
    3. *Dùng `.collect()` sai cách:* Kéo toàn bộ DataFrame khổng lồ từ cụm Worker về máy Driver.
  - **Cách khắc phục:**
    - Tuyệt đối không dùng `.collect()` trên dataset lớn.
    - Áp dụng kỹ thuật **Salting** (thêm tiền tố ngẫu nhiên vào khóa JOIN) để phân tán dữ liệu bị Skew.
    - Cấu hình Watermark ngắn hơn hoặc chuyển State Store sang lưu trên RocksDB đĩa thay vì Java Heap RAM (`spark.sql.streaming.stateStore.providerClass`).
    - Tăng `spark.executor.memoryOverhead` để dành không gian RAM off-heap cho PySpark/JVM memory.

---

### Câu 18: Làm thế nào để đảm bảo tính sẵn sàng cao (High Availability - HA) cho HDFS NameNode và Kafka Broker khi lên Production?
* **Trả lời:**
  - **Với HDFS:**
    - Đổi từ Single NameNode sang **HDFS NameNode HA**: Chạy 2 NameNode song song (1 **Active**, 1 **Standby**).
    - Sử dụng cụm **ZooKeeper** kết hợp **ZKFC (ZooKeeper Failover Controller)** để tự động phát hiện khi Active NameNode bị chết và lập tức bầu Standby NameNode lên làm Active.
    - Dùng cụm **Quorum Journal Manager (QJM)** để đồng bộ EditLogs giữa 2 NameNode.
  - **Với Kafka:**
    - Dựng cụm tối thiểu **3 Kafka Brokers** nằm trên 3 con máy vật lý/AZ khác nhau.
    - Thiết lập cho các Topic quan trọng: `replication.factor = 3` và `min.insync.replicas = 2`.
    - Bật thuộc tính **Rack Awareness** để Kafka tự động phân bổ các Partition Replicas sang các server thuộc các tủ rack/đường điện khác nhau.

---

### Câu 19: Tại sao dự án sử dụng Apache NiFi cho Tuần 5 mà không dùng hoàn toàn Airflow? Khi nào nên dùng NiFi, khi nào nên dùng Airflow?
* **Trả lời:**
  - **Điểm khác biệt cốt lõi:**
    - **Apache NiFi:** Là công cụ **Data Routing & Event-Driven Ingestion** (Xử lý luồng dữ liệu liên tục theo từng record/event với giao diện kéo thả No-Code/Low-Code, hỗ trợ backpressure ngay tầng UI).
    - **Apache Airflow:** Là công cụ **Workflow Orchestrator & Task Scheduler** (Điều phối cây phụ thuộc DAGs của các công việc định kỳ/batch job có code mã nguồn).
  - **Quy tắc lựa chọn:**
    - *Dùng NiFi khi:* Cần kết nối thu thập dữ liệu thô từ hàng trăm nguồn API/IoT/FTP hỗn hợp, cần routing dữ liệu linh hoạt, phân quyền người dùng điều khiển stream mà không muốn viết code phức tạp.
    - *Dùng Airflow khi:* Cần lập lịch chạy bài toán ETL/ELT phức tạp, cần kiểm soát Data Quality Gate chặt chẽ, chạy job Spark/DB có phụ thuộc lẫn nhau theo một sơ đồ DAG rõ ràng.
    - *Kết hợp (Tuần 5):* NiFi chịu trách nhiệm lắng nghe/gọi API thu nhận dữ liệu thô, sau đó NiFi gọi **Airflow REST API** để kích hoạt DAG downstream xử lý tính toán chuyên sâu.

---

### Câu 20: Nhược điểm lớn nhất của hệ thống hiện tại là gì và lộ trình nâng cấp lên Enterprise Production cần bổ sung những gì?
* **Trả lời:**
  - **Nhược điểm lớn nhất hiện tại:**
    1. *Single Point of Failure (SPOF):* Toàn bộ stack đang chạy trên 1 máy vật lý (Docker Compose), nếu máy sập thì toàn bộ hệ thống ngưng trệ.
    2. *Thiếu Data Governance:* Chưa có Schema Registry để quản lý sự thay đổi cấu trúc bảng (Schema Evolution), chưa có Data Catalog và Lineage tự động.
    3. *Bảo mật cơ bản:* Credential vẫn dùng file `.env` local, chưa bật mTLS mã hóa đường truyền giữa các dịch vụ.
  - **Lộ trình nâng cấp Enterprise (Production Backlog):**
    1. **Infrastructure:** Chuyển từ Docker Compose sang **Kubernetes (EKS/GKE)** hoặc dùng Managed Services (AWS EMR/MSK/MWAA, GCP Dataproc/PubSub/Composer).
    2. **Security:** Tích hợp **HashiCorp Vault** cho Secret Management, bật **OAuth2/mTLS** và phân quyền chi tiết **RBAC**.
    3. **Governance:** Triển khai **Confluent Schema Registry** cho Kafka, tích hợp **Apache Atlas / OpenMetadata** cho Data Catalog & Lineage tracking.
    4. **CI/CD & IaC:** Viết mã nguồn triển khai hạ tầng bằng **Terraform** và tự động hóa testing/deployment bằng **GitHub Actions / GitLab CI**.

---
*Tài liệu được biên soạn phục vụ cho phiên phản biện đồ án Data Engineering DE Genesis.*
