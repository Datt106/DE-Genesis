# DE Genesis - Môi trường thực hành Data Engineering 6 tuần

Repo này dựng một môi trường Docker để thực hành roadmap 6 tuần: Python/Java/SQL, PostgreSQL/MySQL, Spark, HDFS, Kafka, Airflow, NiFi, Prometheus và Grafana. Các service nặng được tách theo profile để bạn chỉ bật đúng phần đang học.

## Yêu cầu

- Docker Desktop đã bật.
- Khuyến nghị Docker Desktop có ít nhất 8 GB RAM nếu chạy tuần 5 hoặc tuần 6.
- PowerShell trên Windows.

Kiểm tra nhanh:

```powershell
.\scripts\check-env.ps1
```

## Khởi động

Tuần 1 và tuần 2 chỉ cần workspace, PostgreSQL và MySQL:

```powershell
.\scripts\start.ps1 -Target week1 -Build
```

Nếu Docker Desktop báo lỗi `cannot find registry key "SOFTWARE\Docker Inc.\Docker Desktop"` thì Docker đang bị mất registry cài đặt. Mở PowerShell bằng **Run as administrator** rồi chạy:

```powershell
.\scripts\fix-docker-desktop-registry.ps1
```

Sau đó chờ Docker Desktop chạy xong và thử lại lệnh khởi động tuần 1.

Nếu shortcut Docker Desktop tiếp tục lỗi do trỏ vào file root `C:\Program Files\Docker\Docker\Docker Desktop.exe`, dùng script sau để mở backend và frontend đúng đường dẫn:

```powershell
.\scripts\start-docker-desktop.ps1
```

Các mốc profile:

| Target | Service chính |
| --- | --- |
| `week1`, `week2` | workspace, PostgreSQL, MySQL |
| `week3` | thêm HDFS, Spark |
| `week4` | thêm Kafka |
| `week5` | thêm Airflow, NiFi |
| `week6` | thêm Prometheus, Grafana |
| `all` | bật toàn bộ môi trường |

Dừng môi trường:

```powershell
.\scripts\stop.ps1
```

Xóa cả volume dữ liệu Docker khi muốn làm lại từ đầu:

```powershell
.\scripts\stop.ps1 -RemoveVolumes
```

## Tài khoản và cổng mặc định

| Thành phần | URL hoặc cổng | Tài khoản |
| --- | --- | --- |
| PostgreSQL | `localhost:5432` | `de_user` / `de_password` |
| MySQL | `localhost:3306` | `de_user` / `de_password` |
| Jupyter trong workspace | `localhost:8888` | chạy thủ công nếu cần |
| Spark UI | <http://localhost:8080> | không cần đăng nhập |
| HDFS NameNode UI | <http://localhost:9870> | không cần đăng nhập |
| Kafka | `localhost:9092` | không cần đăng nhập |
| Airflow | <http://localhost:8088> | `admin` / `admin` |
| NiFi | <https://localhost:8443/nifi> | `admin` / `admin_password_123` |
| Prometheus | <http://localhost:9090> | không cần đăng nhập |
| Grafana | <http://localhost:3000> | `admin` / `admin` |

Nếu máy bạn đã dùng cổng nào đó, sửa trong file `.env`.

## Tuần 1: Python, SQL, PostgreSQL/MySQL

Nạp file CSV mẫu vào PostgreSQL:

```powershell
docker compose exec workspace python exercises/week1/script/import_olist_to_postgres.py
```

Chạy truy vấn phân tích và xem execution plan:

```powershell
docker compose exec workspace psql -h postgres -U de_user -d de_roadmap -f exercises/week1/script/sql_practice_postgres.sql
```

Mở shell trong container học Linux command line:

```powershell
docker compose exec workspace bash
```

## Tuần 2: OLTP và OLAP

Chạy trước script tuần 1 để có bảng `raw.sales`, sau đó tạo schema OLTP:

```powershell
docker compose exec workspace psql -h postgres -U de_user -d de_roadmap -f exercises/week2/schema_oltp.sql
```

Tạo star schema OLAP và report mẫu:

```powershell
docker compose exec workspace psql -h postgres -U de_user -d de_roadmap -f exercises/week2/schema_olap.sql
```

## Tuần 3: Spark, HDFS, Parquet/ORC

Khởi động profile big data:

```powershell
.\scripts\start.ps1 -Target week3 -Build
```

Chạy batch job bằng Spark:

```powershell
docker compose exec spark-master spark-submit --master spark://spark-master:7077 /workspace/exercises/week3/spark_batch.py
```

Kết quả nằm trong `output/week3`. Bạn có thể đưa dữ liệu lên HDFS để luyện lệnh:

```powershell
docker compose exec namenode hdfs dfs -mkdir -p /data/raw
docker compose exec namenode hdfs dfs -put -f /workspace/data/sample/sales.csv /data/raw/sales.csv
docker compose exec namenode hdfs dfs -ls /data/raw
```

## Tuần 4: Kafka và Spark Structured Streaming

Khởi động thêm Kafka:

```powershell
.\scripts\start.ps1 -Target week4 -Build
```

Tạo topic:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic service-logs --partitions 1 --replication-factor 1
```

Mở terminal thứ nhất để gửi log mẫu:

```powershell
docker compose exec workspace python exercises/week4/kafka_producer.py
```

Mở terminal thứ hai để chạy streaming job:

```powershell
docker compose exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /workspace/exercises/week4/spark_streaming_kafka.py
```

Kết quả streaming ghi vào `output/week4`.

## Tuần 5: Airflow, NiFi và tích hợp API

Khởi động workflow:

```powershell
.\scripts\start.ps1 -Target week5 -Build
```

Mở Airflow tại <http://localhost:8088>, đăng nhập `admin` / `admin`, rồi bật DAG `de_genesis_roadmap_etl`. DAG mẫu sẽ nạp CSV vào PostgreSQL và chạy Spark job tạo report log. Riêng output của DAG Airflow được ghi trong container tại `/tmp/de-genesis-airflow/week6` để tránh lỗi quyền khi Spark ghi trực tiếp lên bind mount Windows.

Mở NiFi tại <https://localhost:8443/nifi> để luyện dataflow kéo dữ liệu từ API hoặc file. Trình duyệt có thể cảnh báo chứng chỉ tự ký, bạn chọn tiếp tục cho môi trường local.

## Tuần 6: Production pipeline, monitoring và alerting

Khởi động đầy đủ:

```powershell
.\scripts\start.ps1 -Target week6 -Build
```

Chạy Spark job xử lý log mẫu:

```powershell
docker compose exec spark-master spark-submit --master spark://spark-master:7077 /workspace/exercises/week6/log_report.py
```

Report nằm trong `output/week6`. Prometheus ở <http://localhost:9090>, Grafana ở <http://localhost:3000>. Trong Grafana, thêm data source Prometheus với URL nội bộ:

```text
http://prometheus:9090
```

## Cấu trúc thư mục

```text
data/sample/             Dữ liệu mẫu CSV và log JSONL
exercises/week1/         Python + SQL cơ bản
exercises/week2/         OLTP và OLAP schema
exercises/week3/         Spark batch
exercises/week4/         Kafka producer và Spark streaming
exercises/week6/         Spark log report
dags/                    DAG mẫu cho Airflow
config/prometheus/       Cấu hình scrape cho Prometheus
scripts/                 Script PowerShell để bật/tắt môi trường
```

## Ghi chú

- Môi trường này phục vụ học tập local, chưa phải cấu hình bảo mật production.
- Lần đầu chạy `-Build` và pull image sẽ lâu vì Spark, Airflow, NiFi, Hadoop khá lớn.
- File `.env` được tạo từ `.env.example`; bạn có thể sửa cổng hoặc mật khẩu tại đó.
