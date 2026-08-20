# Hướng dẫn chạy dự án DE Genesis

Tài liệu này hướng dẫn chạy toàn bộ roadmap Data Engineering 6 tuần trên Docker Desktop ở máy local. Các lệnh bên dưới dùng PowerShell và giả định đang đứng tại thư mục gốc của dự án.

Báo cáo kiến trúc, mức đáp ứng roadmap và giới hạn production nằm tại
[BAO_CAO_TONG_THE_DU_AN.md](BAO_CAO_TONG_THE_DU_AN.md).

> Đây là môi trường học tập production-like trên một máy, không phải cấu hình production thật. Mật khẩu mặc định, cụm một node và replication thấp chỉ phù hợp cho local lab.

## 1. Yêu cầu trước khi chạy

- Windows và PowerShell.
- Docker Desktop có Docker Compose V2, Docker Engine đang chạy.
- Kết nối Internet ở lần đầu để tải image, build image và tải package Spark/Kafka.
- Khuyến nghị cấp ít nhất 8 GB RAM cho Docker Desktop khi chạy Tuần 5 hoặc Tuần 6.
- Với benchmark Tuần 3, nên còn khoảng 6–8 GB RAM và đủ dung lượng đĩa cho input lớn hơn 1 GiB cùng nhiều bản output CSV/Parquet/ORC.

Mở PowerShell, chuyển tới thư mục dự án và kiểm tra môi trường:

```powershell
Set-Location C:\download\DE-Genesis

docker --version
docker compose version
docker info
.\scripts\check-env.ps1
```

Nếu `docker info` lỗi, cần mở Docker Desktop và chờ Engine khởi động xong trước khi tiếp tục.

## 2. Chuẩn bị cấu hình `.env`

Tạo `.env` từ file mẫu nếu file này chưa tồn tại:

```powershell
if (-not (Test-Path .env)) {
    Copy-Item .env.example .env
}
```

Không chép đè `.env` đang dùng nếu chưa rà lại mật khẩu, cổng và các đường dẫn checkpoint. Không commit credential thật vào Git.

Các lệnh trong tài liệu dùng tài khoản mặc định:

| Thành phần | Tài khoản mặc định | Biến cấu hình chính |
| --- | --- | --- |
| PostgreSQL | `de_user` / `de_password` | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT` |
| MySQL | `de_user` / `de_password` | `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`, `MYSQL_PORT` |
| Airflow | `admin` / `admin` | `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD`, `AIRFLOW_PORT` |
| NiFi | `admin` / `admin_password_123` | `NIFI_USER`, `NIFI_PASSWORD`, `NIFI_PORT` |
| Grafana | `admin` / `admin` | `GRAFANA_USER`, `GRAFANA_PASSWORD`, `GRAFANA_PORT` |

Nếu đã đổi các giá trị mặc định trong `.env`, hãy thay tài khoản hoặc database tương ứng trong các lệnh mẫu.

Các ràng buộc quan trọng của Tuần 6:

- `WEEK6_LOG_MICRO_BATCH_SECONDS` phải nằm trong khoảng 1–60 giây.
- `WEEK6_LOG_REPORT_SETTLEMENT_SECONDS` phải lớn hơn hoặc bằng tổng `WEEK6_LOG_MAX_EVENT_DELAY_SECONDS + WEEK6_LOG_MICRO_BATCH_SECONDS`. Cấu hình mẫu là `180 >= 120 + 30`.
- `HDFS_REPLICATION=1` phải khớp với cụm local chỉ có một DataNode.
- `WEEK6_HDFS_USER=root` là pseudo-user HDFS local để Airflow thực hiện atomic rename; không dùng cách xác thực này cho production.
- Khi chủ động tạo checkpoint log mới nhưng vẫn giữ dữ liệu PostgreSQL/HDFS cũ, phải đổi `WEEK6_LOG_GENERATION_ID` để tạo lineage mới.
- `WEEK6_LOG_STARTING_OFFSETS` chỉ có hiệu lực khi checkpoint chưa tồn tại.

## 3. Chuẩn bị dữ liệu Olist

Tải bộ **Brazilian E-Commerce Public Dataset by Olist** từ Kaggle, giải nén và đặt đúng chín file sau vào `data/olist`:

```text
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_orders_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

Kiểm tra tên file và bảo đảm file không rỗng:

```powershell
.\scripts\check-olist-data.ps1
```

Dữ liệu Olist là bắt buộc cho bài import Tuần 1, OLTP/OLAP Tuần 2, benchmark Olist Tuần 3 và pipeline đa nguồn thực tế Tuần 5. Demo Kafka Tuần 4 và pipeline log Tuần 6 có thể chạy khi chưa có Olist. Mock Promotion API sẽ dùng 250 sản phẩm xác định trước nếu thiếu file sản phẩm Olist.

## 4. Profile Docker Compose và cách khởi động

### 4.1. Cách được khuyến nghị: dùng script theo tuần

Lần đầu chạy một target hoặc sau khi thay Dockerfile/requirements, thêm `-Build`:

```powershell
.\scripts\start.ps1 -Target week1 -Build
```

Những lần sau có thể bỏ `-Build`:

```powershell
.\scripts\start.ps1 -Target week1
```

Ánh xạ target:

| Target | Profile được bật | Thành phần chính |
| --- | --- | --- |
| `week1`, `week2` | Core, không có tên profile | workspace, PostgreSQL, MySQL |
| `week3` | `bigdata` | Core + HDFS + Spark |
| `week4` | `bigdata`, `streaming` | Core + HDFS + Spark + Kafka |
| `week5` | `bigdata`, `streaming`, `workflow` | Core + Spark/HDFS/Kafka + Mock API + Airflow + NiFi |
| `week6` | `bigdata`, `streaming`, `workflow`, `monitoring` | Toàn bộ stack + producer/stream log + Prometheus + Grafana |
| `all` | Giống `week6` | Toàn bộ môi trường |

Ví dụ:

```powershell
.\scripts\start.ps1 -Target week3 -Build
.\scripts\start.ps1 -Target week4 -Build
.\scripts\start.ps1 -Target week5 -Build
.\scripts\start.ps1 -Target week6 -Build
```

### 4.2. Lệnh Docker Compose tương đương theo profile

`core` là cách gọi nhóm service không khai báo profile; dự án không có profile tên `core`.

```powershell
# Core: Tuần 1–2
docker compose up -d --build

# Core + bigdata: Tuần 3
docker compose --profile bigdata up -d --build

# Core + bigdata + streaming: Tuần 4
docker compose --profile bigdata --profile streaming up -d --build

# Core + bigdata + streaming + workflow: Tuần 5
docker compose --profile bigdata --profile streaming --profile workflow up -d --build

# Toàn bộ, gồm monitoring: Tuần 6
docker compose --profile bigdata --profile streaming --profile workflow `
  --profile monitoring up -d --build
```

Không nên chỉ bật `monitoring` khi chạy Tuần 6 vì exporter và alert còn phụ thuộc Airflow, Mock API, Kafka, Spark và HDFS. Script `start.ps1` đã bật đúng chuỗi profile phụ thuộc.

### 4.3. Kiểm tra trạng thái sau khi khởi động

```powershell
docker compose ps
docker compose ps -a
```

Chờ các service dài hạn chuyển sang `healthy` hoặc `running`. Hai service khởi tạo `kafka-init` và `airflow-init` kết thúc với exit code `0` là bình thường.

Khi cần xem nguyên nhân service chưa sẵn sàng:

```powershell
docker compose logs --tail 100 postgres
docker compose logs --tail 100 spark-master
docker compose logs --tail 100 kafka
docker compose logs --tail 100 airflow-scheduler
```

## 5. URL và cổng mặc định

| Thành phần | URL/cổng mặc định | Target tối thiểu | Ghi chú |
| --- | --- | --- | --- |
| PostgreSQL | `localhost:5432` | Tuần 1 | Database `de_roadmap` |
| MySQL | `localhost:3306` | Tuần 1 | Database `de_roadmap` |
| JupyterLab | <http://localhost:8888> | Tuần 1 | Không tự chạy; xem lệnh bên dưới |
| Spark Master UI | <http://localhost:8080> | Tuần 3 | Không cần đăng nhập |
| Spark Worker UI | <http://localhost:8081> | Tuần 3 | Không cần đăng nhập |
| Spark Master RPC | `localhost:7077` | Tuần 3 | Spark trong Compose dùng `spark://spark-master:7077` |
| HDFS NameNode UI | <http://localhost:9870> | Tuần 3 | Không cần đăng nhập |
| HDFS RPC | `localhost:9000` | Tuần 3 | Container dùng `hdfs://namenode:9000` |
| Kafka | `localhost:9092` | Tuần 4 | Container dùng `kafka:29092` |
| Mock API | <http://localhost:8000/docs> | Tuần 5 | Health tại <http://localhost:8000/health> |
| Airflow | <http://localhost:8088> | Tuần 5 | `admin` / `admin` |
| NiFi | <https://localhost:8443/nifi> | Tuần 5 | Chứng chỉ tự ký cho local |
| Prometheus | <http://localhost:9090> | Tuần 6 | Alert tại <http://localhost:9090/alerts> |
| Pipeline metrics | <http://localhost:9108/metrics> | Tuần 6 | Prometheus text format |
| Grafana | <http://localhost:3000> | Tuần 6 | Dashboard `DE Genesis - Pipeline Production` |

Để chạy JupyterLab thủ công, giữ cửa sổ PowerShell này mở và dùng token được in ra terminal:

```powershell
docker compose exec workspace jupyter lab `
  --ip=0.0.0.0 --port=8888 --no-browser --allow-root
```

## 6. Luồng chạy theo từng tuần

### 6.1. Tuần 1 — Python, Java, SQL, Linux, PostgreSQL và MySQL

Khởi động Core và kiểm tra Olist:

```powershell
.\scripts\start.ps1 -Target week1 -Build
.\scripts\check-olist-data.ps1
```

Import, làm sạch chín file Olist vào schema PostgreSQL `olist_practice`:

```powershell
docker compose exec workspace python `
  exercises/week1/script/import_olist_to_postgres.py
```

Chạy truy vấn PostgreSQL, index, execution plan, procedure và trigger:

```powershell
docker compose exec workspace psql -h postgres -U de_user -d de_roadmap `
  -f exercises/week1/script/sql_practice_postgres.sql
```

Chạy lab MySQL 8.0:

```powershell
Get-Content -Raw exercises/week1/script/sql_practice_mysql.sql |
  docker compose exec -T mysql mysql -ude_user -pde_password de_roadmap
```

Chạy bài Linux và Java:

```powershell
docker compose exec workspace bash exercises/week1/script/linux_basics.sh
docker compose exec workspace javac exercises/week1/script/OlistCsvJavaPractice.java
docker compose exec workspace java -cp exercises/week1/script OlistCsvJavaPractice
```

Kiểm thử Tuần 1:

```powershell
docker compose exec workspace pytest -q exercises/week1/tests
```

Tóm tắt import được ghi tại `output/week1/olist_import_summary.json`.

### 6.2. Tuần 2 — OLTP, OLAP và star schema

Tuần 2 dùng lại Core và yêu cầu schema nguồn `olist_practice` từ Tuần 1:

```powershell
.\scripts\start.ps1 -Target week2 -Build
docker compose exec workspace python exercises/week1/script/import_olist_to_postgres.py
```

Tạo lại OLTP 3NF và OLAP star schema theo cách tái lập:

```powershell
docker compose exec workspace python exercises/week2/load_olist_oltp.py `
  --if-exists replace
docker compose exec workspace python exercises/week2/load_olist_olap.py `
  --if-exists replace
```

Khi cần nạp tăng dần và giữ lịch sử SCD Type 2 đã có, chỉ dùng chế độ `merge` cho OLAP:

```powershell
docker compose exec workspace python exercises/week2/load_olist_olap.py `
  --if-exists merge
```

Kiểm thử và xem file đối soát:

```powershell
docker compose exec workspace pytest -q exercises/week2/tests
Get-Content output/week2/oltp_load_summary.json
Get-Content output/week2/olap_load_summary.json
```

### 6.3. Tuần 3 — Spark, HDFS, Parquet và ORC

#### Smoke test nhỏ

```powershell
.\scripts\start.ps1 -Target week3 -Build

docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /workspace/exercises/week3/spark_batch.py
```

Output smoke test nằm trong `output/week3`.

#### Benchmark chính với input tối thiểu 1 GiB

Cấp 4 core/4 GB cho Spark worker trong phiên PowerShell hiện tại rồi khởi động lại profile Tuần 3:

```powershell
$env:SPARK_WORKER_CORES = "4"
$env:SPARK_WORKER_MEMORY = "4G"
docker compose --profile bigdata up -d --build
docker compose --profile bigdata ps
```

Tạo fact CSV tối thiểu 1 GiB trên HDFS:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 2g `
  --executor-memory 3g `
  --executor-cores 4 `
  --conf spark.cores.max=4 `
  --conf spark.hadoop.dfs.replication=1 `
  /workspace/exercises/week3/generate_olist_1gb.py `
  --target-gib 1.0 `
  --partitions 16
```

Chạy join, aggregate, chuyển định dạng và benchmark:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 2g `
  --executor-memory 3g `
  --executor-cores 4 `
  --conf spark.cores.max=4 `
  --conf spark.hadoop.dfs.replication=1 `
  /workspace/exercises/week3/olist_format_benchmark.py `
  --shuffle-partitions 16 `
  --output-partitions 16 `
  --warmups 1 `
  --trials 3
```

Đọc kết quả và kiểm tra HDFS:

```powershell
docker compose exec namenode hdfs dfs -cat `
  /data/week3/benchmark/quality_report/part-*
docker compose exec namenode hdfs dfs -cat `
  /data/week3/benchmark/benchmark_summary/part-*
docker compose exec namenode hdfs fsck /data/week3 -files -blocks -locations
docker compose exec workspace pytest -q exercises/week3/tests
```

Không dùng `--allow-small-input` cho lần nghiệm thu benchmark 1 GiB.

### 6.4. Tuần 4 — Kafka và Spark Structured Streaming

Cách nhanh nhất để chạy end-to-end hữu hạn, có cả bản ghi lỗi để kiểm tra quarantine:

```powershell
.\exercises\week4\run_local.ps1 -Build
```

Script mặc định tạo topic `service-logs-week4-lab`, gửi 20 event với seed cố định, chạy Spark bằng `availableNow` và ghi output vào `output/week4/service-logs-week4-lab`.

Có thể tạo một lần chạy độc lập khác bằng topic mới:

```powershell
.\exercises\week4\run_local.ps1 `
  -Topic service-logs-week4-verify `
  -Count 100 `
  -IntervalSeconds 0.1 `
  -InvalidEvery 10
```

Để chạy liên tục, khởi động profile rồi mở hai cửa sổ PowerShell:

```powershell
.\scripts\start.ps1 -Target week4 -Build

docker compose exec kafka kafka-topics `
  --bootstrap-server kafka:29092 `
  --describe `
  --topic service-logs
```

Terminal 1 — producer:

```powershell
docker compose exec workspace python `
  exercises/week4/kafka_producer.py `
  --topic service-logs
```

Terminal 2 — streaming:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  /workspace/exercises/week4/spark_streaming_kafka.py `
  --topic service-logs
```

Nhấn `Ctrl+C` ở từng terminal để dừng có kiểm soát. Khi chạy lại, giữ đúng topic và checkpoint để Spark tiếp tục từ offset đã commit. Không dùng `--allow-data-loss` trừ khi chủ động chấp nhận mất offset trong lab.

Kiểm thử:

```powershell
docker compose exec workspace pytest -q `
  exercises/week3/tests exercises/week4/tests
```

### 6.5. Tuần 5 — Airflow, NiFi và tích hợp đa nguồn

Khởi động workflow và áp dụng DDL idempotent. Việc chạy DDL rõ ràng là cần thiết khi tái sử dụng volume PostgreSQL được tạo từ phiên bản dự án cũ:

```powershell
.\scripts\start.ps1 -Target week5 -Build

docker compose exec workspace psql -h postgres -U de_user -d de_roadmap `
  -v ON_ERROR_STOP=1 `
  -f exercises/week5/sql/create_week5_schemas.sql
```

Pipeline đa nguồn dùng schema `olist_olap`. Nếu chưa có, chạy theo thứ tự:

```powershell
.\scripts\check-olist-data.ps1
docker compose exec workspace python exercises/week1/script/import_olist_to_postgres.py
docker compose exec workspace python exercises/week2/load_olist_oltp.py
docker compose exec workspace python exercises/week2/load_olist_olap.py
```

Kiểm tra Mock API:

```powershell
Invoke-RestMethod http://localhost:8000/health
Start-Process http://localhost:8000/docs
```

Mở Airflow tại <http://localhost:8088>, đăng nhập `admin` / `admin`, rồi bật ba DAG:

- `de_genesis_week5_airflow_ingestion`
- `de_genesis_week5_nifi_downstream`
- `de_genesis_week5_multisource`

Có thể unpause bằng CLI:

```powershell
docker compose exec airflow-scheduler airflow dags unpause `
  de_genesis_week5_airflow_ingestion
docker compose exec airflow-scheduler airflow dags unpause `
  de_genesis_week5_nifi_downstream
docker compose exec airflow-scheduler airflow dags unpause `
  de_genesis_week5_multisource
```

Trigger DAG ingestion trên Airflow UI với cấu hình mẫu:

```json
{
  "batch_id": "airflow-demo-001",
  "scenario": "success"
}
```

Với NiFi, mở <https://localhost:8443/nifi> và làm đúng `exercises/week5/nifi/huong_dan_import_flow.md`. Trình duyệt có thể cảnh báo chứng chỉ tự ký; chỉ chấp nhận trong môi trường local. Sau khi import native flow v2, phải nạp lại hai sensitive parameter `airflow.password` và `postgres.password`; artifact không chứa secret.

Chạy pipeline CSV + PostgreSQL + REST độc lập ngoài Airflow:

```powershell
docker compose exec workspace python -m exercises.week5.multisource `
  --batch-id multisource-demo-001
docker compose exec workspace python -m exercises.week5.spark.multisource_report `
  --manifest output/week5/multisource/multisource-demo-001/manifest.json `
  --output-root output/week5/multisource
```

Kiểm tra audit và đối chiếu Airflow/NiFi:

```powershell
docker compose exec postgres psql -U de_user -d de_roadmap -c `
  "SELECT source_mode,batch_id,status,source_count,inserted_count,duplicate_count,raw_count,accepted_count,rejected_count,curated_count,started_at,finished_at FROM week5_control.pipeline_runs ORDER BY started_at DESC LIMIT 20;"

docker compose exec workspace python `
  exercises/week5/scripts/compare_pipelines.py
```

Kiểm thử Tuần 5:

```powershell
docker compose exec -w /workspace workspace python -m pytest -q `
  exercises/week5/tests
docker compose exec -w /workspace workspace python -m pytest -q `
  mock_api/tests
docker compose exec workspace psql -h postgres -U de_user -d de_roadmap `
  -v ON_ERROR_STOP=1 `
  -f exercises/week5/sql/test_null_idempotency.sql
```

### 6.6. Tuần 6 — Pipeline service log và monitoring

Khởi động toàn bộ stack:

```powershell
.\scripts\start.ps1 -Target week6 -Build
```

Nếu đang tái sử dụng volume PostgreSQL cũ, áp dụng lại DDL Tuần 5 và Tuần 6. Hai file được thiết kế idempotent:

```powershell
docker compose exec workspace psql -h postgres -U de_user -d de_roadmap `
  -v ON_ERROR_STOP=1 `
  -f exercises/week5/sql/create_week5_schemas.sql
docker compose exec workspace psql -h postgres -U de_user -d de_roadmap `
  -v ON_ERROR_STOP=1 `
  -f exercises/week6/sql/create_week6_schemas.sql
```

Producer log và Spark streaming được Compose chạy tự động. Lần đầu Spark có thể mất thêm thời gian để tải package Kafka/PostgreSQL. Kiểm tra:

```powershell
docker compose ps week6-log-producer week6-log-streaming
docker compose logs --tail 100 week6-log-producer
docker compose logs --tail 100 week6-log-streaming

docker compose exec kafka kafka-topics `
  --bootstrap-server kafka:29092 `
  --describe `
  --topic week6-service-logs

docker compose exec namenode hdfs dfs -ls -R `
  /data/week6/raw/service-logs
```

Mở Airflow và bật:

- `de_genesis_week6_log_report`
- `de_genesis_week6_production_pipeline`

Lệnh CLI tương đương:

```powershell
docker compose exec airflow-scheduler airflow dags unpause `
  de_genesis_week6_log_report
docker compose exec airflow-scheduler airflow dags unpause `
  de_genesis_week6_production_pipeline
```

Report log chạy mỗi 5 phút và chờ settlement mặc định 180 giây. Để backfill qua Airflow UI, trigger `de_genesis_week6_log_report` với một cửa sổ quá khứ tối đa 7 ngày, căn theo phút:

```json
{
  "window_start": "2026-08-13T00:00:00Z",
  "window_end": "2026-08-14T00:00:00Z"
}
```

Kiểm tra raw, report đóng và telemetry:

```powershell
docker compose exec namenode hdfs dfs -ls -R `
  /data/week6/raw/service-logs
docker compose exec namenode hdfs dfs -ls -R `
  /data/week6/reports/closed

docker compose exec postgres psql -U de_user -d de_roadmap -c `
  "SELECT stream_generation_id,stream_batch_id,status,raw_count,valid_count,invalid_count,ingestion_lag_seconds,finished_at FROM week6_control.log_stream_batches ORDER BY finished_at DESC NULLS LAST LIMIT 20;"

docker compose exec postgres psql -U de_user -d de_roadmap -c `
  "SELECT run_id,window_start,window_end,status,source_count,minute_report_count,status_report_count,finished_at FROM week6_control.log_report_runs ORDER BY started_at DESC LIMIT 20;"
```

Mở Prometheus, trang Alert và Grafana bằng các URL ở mục 5. Dashboard Grafana `DE Genesis - Pipeline Production` được provision tự động.

Kiểm thử Tuần 6:

```powershell
docker compose exec workspace python -m pytest -q exercises/week6/tests
```

## 7. Xác minh toàn bộ roadmap

Lệnh tổng hợp kiểm tra Docker Engine, cấu hình tất cả profile, biên dịch tĩnh mã Python, cấu hình/rule Prometheus và toàn bộ test:

```powershell
.\scripts\verify-roadmap.ps1
```

Kiểm tra thêm đủ chín file Olist:

```powershell
.\scripts\verify-roadmap.ps1 -CheckOlistData
```

Chỉ kiểm tra cấu hình và compile, bỏ qua pytest:

```powershell
.\scripts\verify-roadmap.ps1 -SkipTests
```

Lệnh verify là kiểm tra tĩnh/unit test; để xác nhận runtime, vẫn cần chạy smoke test hoặc flow end-to-end tương ứng và kiểm tra `docker compose ps`, log, HDFS, PostgreSQL và dashboard.

## 8. Quan sát log, dữ liệu và metric

### Trạng thái và tài nguyên container

```powershell
docker compose ps -a
docker stats
```

Theo dõi log realtime; nhấn `Ctrl+C` để thoát mà không dừng container:

```powershell
docker compose logs --tail 200 -f week6-log-streaming
docker compose logs --tail 200 -f airflow-scheduler
docker compose logs --tail 200 -f pipeline-metrics
```

### Airflow

```powershell
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```

### Kafka

```powershell
docker compose exec kafka kafka-topics `
  --bootstrap-server kafka:29092 --list
docker compose exec kafka kafka-topics `
  --bootstrap-server kafka:29092 `
  --describe --topic week6-service-logs
```

### HDFS

```powershell
docker compose exec namenode hdfs dfsadmin -report
docker compose exec namenode hdfs dfs -du -s -h /data/week6
docker compose exec namenode hdfs fsck /data/week6 -files -blocks -locations
```

Với cụm local một DataNode, replication phải là 1 và `fsck` không được có block missing/corrupt.

### Prometheus và Grafana

Kiểm tra endpoint metrics từ PowerShell:

```powershell
(Invoke-WebRequest http://localhost:9108/metrics).Content |
  Select-String "de_genesis_log_"
```

Các metric chính gồm:

- `de_genesis_log_ingestion_lag_seconds`
- `de_genesis_log_stream_last_batch_success`
- `de_genesis_log_stream_last_invalid_records`
- `de_genesis_log_report_last_run_success`
- `de_genesis_dependency_up`

Prometheus hiển thị target tại <http://localhost:9090/targets> và alert tại <http://localhost:9090/alerts>.

## 9. Dừng và reset dữ liệu

### Dừng nhưng giữ dữ liệu

```powershell
.\scripts\stop.ps1
```

Lệnh này dừng/xóa container và network Compose nhưng giữ named volume, vì vậy PostgreSQL, MySQL, Kafka, HDFS, NiFi, Prometheus và Grafana có thể tiếp tục từ dữ liệu cũ ở lần khởi động sau.

### Reset toàn bộ named volume

> **Cảnh báo phá hủy dữ liệu:** lệnh sau xóa toàn bộ named volume của project, gồm dữ liệu PostgreSQL/MySQL, Kafka topic và offset, HDFS, cấu hình/trạng thái NiFi, log Airflow, dữ liệu Prometheus và Grafana. Không thể khôi phục nếu không có backup.

```powershell
.\scripts\stop.ps1 -RemoveVolumes
```

Lệnh này không xóa các thư mục bind mount trên máy host như `data/`, `output/`, mã nguồn và `.env`.

Sau reset đầy đủ, khởi tạo lại target cần dùng:

```powershell
.\scripts\start.ps1 -Target week6 -Build
```

Không xóa riêng checkpoint streaming rồi chạy lại với cùng `WEEK6_LOG_GENERATION_ID`. Nếu cần bắt đầu checkpoint mới nhưng vẫn giữ database/raw cũ, hãy chọn một generation mới trong `.env`, ví dụ `WEEK6_LOG_GENERATION_ID=local-v2`, trước khi tạo lại service streaming.

## 10. Xử lý sự cố thường gặp trên Windows

### PowerShell chặn chạy script

Chỉ nới execution policy cho phiên PowerShell hiện tại:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

Sau đó chạy lại script từ thư mục gốc dự án.

### Không tìm thấy `docker.exe` hoặc Docker Engine chưa sẵn sàng

```powershell
Get-Command docker
docker info
```

Nếu Docker Desktop đã cài nhưng chưa mở đúng backend/frontend:

```powershell
.\scripts\start-docker-desktop.ps1
```

Chờ 30–90 giây rồi chạy lại `docker info`.

Nếu Docker Desktop báo lỗi:

```text
cannot find registry key "SOFTWARE\Docker Inc.\Docker Desktop"
```

Mở PowerShell bằng **Run as administrator** rồi chạy:

```powershell
.\scripts\fix-docker-desktop-registry.ps1
```

### Xung đột cổng

Tìm tiến trình đang giữ một cổng, ví dụ Grafana 3000:

```powershell
$grafanaListener = Get-NetTCPConnection -State Listen -LocalPort 3000 `
  -ErrorAction SilentlyContinue
$grafanaListener |
  Select-Object LocalAddress,LocalPort,OwningProcess
$grafanaListener |
  ForEach-Object { Get-Process -Id $_.OwningProcess }
```

Không cần dừng ứng dụng khác nếu có thể đổi cổng host trong `.env`. Ví dụ đổi:

```dotenv
GRAFANA_PORT=3001
```

Rồi chạy lại:

```powershell
.\scripts\start.ps1 -Target week6
```

Grafana khi đó ở `http://localhost:3001`. Có thể override tạm trong đúng phiên PowerShell mà không sửa `.env`:

```powershell
$env:GRAFANA_PORT = "3001"
.\scripts\start.ps1 -Target week6
```

Các cổng host có thể đổi bằng `.env` gồm `POSTGRES_PORT`, `MYSQL_PORT`, `JUPYTER_PORT`, `SPARK_MASTER_WEB_PORT`, `SPARK_MASTER_PORT`, `SPARK_WORKER_WEB_PORT`, `HDFS_NAMENODE_PORT`, `HDFS_RPC_PORT`, `KAFKA_PORT`, `MOCK_API_PORT`, `AIRFLOW_PORT`, `NIFI_PORT`, `PROMETHEUS_PORT`, `PIPELINE_METRICS_PORT` và `GRAFANA_PORT`.

### Container khởi động chậm hoặc unhealthy

```powershell
docker compose ps -a
docker compose logs --tail 200 week6-log-streaming
```

Có thể thay `week6-log-streaming` bằng service cần kiểm tra như `postgres`, `kafka`, `namenode`, `spark-worker`, `airflow-scheduler`, `nifi` hoặc `pipeline-metrics`.

Lần đầu NiFi, Airflow, Hadoop và Spark có thể khởi động lâu. Không trigger DAG trước khi PostgreSQL, Mock API, Airflow scheduler/webserver và Spark worker đã sẵn sàng.

### Airflow không thấy DAG

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs --tail 200 airflow-scheduler
```

Nếu không có import error nhưng DAG đang paused, unpause bằng UI hoặc lệnh ở mục Tuần 5/Tuần 6. Compose đặt DAG ở trạng thái paused khi được tạo lần đầu.

### NiFi không đăng nhập hoặc trình duyệt cảnh báo TLS

- Dùng đúng `NIFI_USER`/`NIFI_PASSWORD` trong `.env`.
- URL phải là `https://localhost:8443/nifi`, không phải HTTP.
- Chứng chỉ tự ký chỉ được chấp nhận trong local lab.
- Nếu import flow v2, nạp lại `airflow.password` và `postgres.password` trong Parameter Context.

### Spark không có tài nguyên để chạy report

Tuần 6 mặc định dùng hai core worker: một core tối đa cho streaming và một core tối đa cho report. Giữ `SPARK_WORKER_CORES` ít nhất là 2 khi chạy đầy đủ Tuần 6. Với benchmark Tuần 3, dùng cấu hình 4 core/4 GB ở mục 6.3.

### HDFS báo under-replicated block

Kiểm tra:

```powershell
docker compose exec namenode hdfs dfsadmin -report
docker compose exec namenode hdfs fsck /data -files -blocks -locations
```

Cụm local chỉ có một DataNode nên giữ `HDFS_REPLICATION=1`. Không đặt replication lớn hơn số DataNode đang sống.

### Thiếu dữ liệu Olist

Chạy lại:

```powershell
.\scripts\check-olist-data.ps1
```

Tên file phải khớp chính xác và nằm trực tiếp dưới `data/olist`, không nằm thêm một tầng thư mục sau khi giải nén.

## 11. Thứ tự chạy nghiệm thu đề xuất

Nếu muốn kiểm tra toàn bộ dự án từ trạng thái sạch:

1. Rà `.env` và chuẩn bị đủ chín file Olist.
2. Chỉ khi chấp nhận mất toàn bộ named volume cũ, chạy `.\scripts\stop.ps1 -RemoveVolumes`.
3. Chạy Tuần 1, import Olist và chạy SQL lab.
4. Chạy hai loader Tuần 2 theo thứ tự OLTP rồi OLAP.
5. Chạy smoke test Tuần 3; chạy benchmark 1 GiB khi đủ tài nguyên.
6. Chạy `run_local.ps1` Tuần 4 để xác nhận Kafka → Spark → file.
7. Khởi động Tuần 5, bật DAG, chạy Airflow/NiFi/multisource và đối chiếu audit.
8. Khởi động Tuần 6, bật DAG report, chờ report qua settlement, kiểm tra HDFS/PostgreSQL/Prometheus/Grafana.
9. Chạy `.\scripts\verify-roadmap.ps1 -CheckOlistData`.

Tài liệu chi tiết theo tuần nằm tại `exercises/week3/README.md`, `exercises/week4/README.md`, `exercises/week5/README.md` và `exercises/week6/README.md`.
