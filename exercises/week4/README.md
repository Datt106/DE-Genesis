# Tuần 4 - Kafka và Spark Structured Streaming

Tuần 4 triển khai luồng log dịch vụ theo hướng Kappa ở mức Docker local:

```text
Producer → Kafka → Spark Structured Streaming
                    ├─ báo cáo cửa sổ Parquet
                    ├─ quarantine Parquet
                    └─ metric chất lượng theo micro-batch
```

Mỗi sink có checkpoint riêng. Job mặc định dừng khi Kafka không còn offset mà
checkpoint yêu cầu; chỉ dùng `--allow-data-loss` khi người vận hành chủ động chấp
nhận bỏ dữ liệu trong một lần phục hồi lab.

## 1. Thành phần và hợp đồng dữ liệu

| Tệp | Vai trò |
| --- | --- |
| `kafka_producer.py` | Sinh log hợp lệ có seed; có thể chèn lỗi định kỳ để kiểm thử quarantine |
| `spark_streaming_kafka.py` | Parse, phân loại, aggregate theo event time và ghi ba sink |
| `run_local.ps1` | Điều phối bài demo hữu hạn trên Docker Compose, không xóa output cũ |
| `tests/test_week4_logic.py` | Kiểm thử producer, phân loại, quarantine, metric và aggregate |

Schema sự kiện hợp lệ:

| Cột | Điều kiện |
| --- | --- |
| `ts` | Timestamp ISO 8601 parse được |
| `service` | Chuỗi không rỗng |
| `method` | `GET`, `POST`, `PUT`, `PATCH` hoặc `DELETE` |
| `path` | Chuỗi không rỗng |
| `status_code` | Từ 100 đến 599 |
| `latency_ms` | Số nguyên lớn hơn hoặc bằng 0 |

Bản ghi vi phạm không bị bỏ im lặng. Quarantine giữ nguyên `raw_json`, một
`rejection_reason`, topic, partition, offset, Kafka timestamp và thời điểm ghi.
Các lý do hiện có là `malformed_json`, `invalid_or_missing_ts`,
`missing_service`, `invalid_method`, `missing_path`, `invalid_status_code` và
`invalid_latency_ms`.

## 2. Chạy demo tái lập

Từ PowerShell ở thư mục gốc dự án:

```powershell
.\exercises\week4\run_local.ps1 -Build
```

Mặc định script tạo topic `service-logs-week4-lab`, gửi 20 sự kiện với seed 42
và cố ý tạo một sự kiện lỗi sau mỗi 5 sự kiện. Có thể đổi tham số:

```powershell
.\exercises\week4\run_local.ps1 `
  -Topic service-logs-week4-verify `
  -Count 100 `
  -IntervalSeconds 0.1 `
  -InvalidEvery 10
```

Script dùng `availableNow`, phù hợp cho kiểm thử hữu hạn. Checkpoint và output
được tách theo tên topic trong `output/week4/<topic>/`; chạy lại cùng topic chỉ
xử lý offset mới. Script không dừng các service khi hoàn tất để người học kiểm
tra output.

## 3. Chạy liên tục

Khởi động service. `kafka-init` tạo topic mới hoặc tăng topic hiện hữu lên ít
nhất số partition trong `SERVICE_LOG_PARTITIONS` (mặc định là 3), sau đó đọc
lại metadata để xác nhận cấu hình đã hội tụ:

```powershell
.\scripts\start.ps1 -Target week4 -Build
docker compose exec kafka kafka-topics `
  --bootstrap-server kafka:29092 `
  --describe `
  --topic service-logs
```

Kafka chỉ hỗ trợ tăng, không hỗ trợ giảm số partition. Vì vậy nếu topic đang có
nhiều hơn giá trị cấu hình, script giữ nguyên và vẫn xem là đạt yêu cầu tối
thiểu.

Mở PowerShell thứ nhất để chạy producer:

```powershell
docker compose exec workspace python `
  exercises/week4/kafka_producer.py `
  --topic service-logs
```

Mở PowerShell thứ hai để chạy streaming:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  /workspace/exercises/week4/spark_streaming_kafka.py `
  --topic service-logs
```

Nhấn Ctrl+C ở từng cửa sổ để dừng có kiểm soát. Nếu Docker Desktop hoặc máy host
khởi động lại, chạy lại lệnh với đúng topic và checkpoint; Spark tiếp tục từ
offset đã commit. Docker Compose hiện không tự khởi động lại job `spark-submit`,
vì vậy runbook này là orchestration local, chưa phải scheduler production.

## 4. Metric và kiểm tra vận hành

Mỗi micro-batch tạo đúng một thư mục
`quality_metrics/batch_<batch_id>` với các cột:

- `processed_records`;
- `accepted_records`;
- `rejected_records`;
- `rejection_ratio`;
- `recorded_at`.

Ghi lại cùng `batch_id` dùng chế độ overwrite đúng thư mục micro-batch, nên retry
không nhân đôi metric. Có thể đọc tất cả thư mục bằng Spark với tùy chọn
`recursiveFileLookup=true`. Các tín hiệu tối thiểu cần quan sát là query còn
active, Kafka lag, micro-batch duration, `rejected_records > 0` và tỷ lệ 5xx.
Prometheus/Grafana và cảnh báo tự động được triển khai ở tuần production sau,
không được giả định là đã có chỉ từ các file metric này.

## 5. Lambda và Kappa

| Tiêu chí | Lambda | Kappa |
| --- | --- | --- |
| Đường xử lý | Batch layer và speed layer song song | Một event log và một streaming path |
| Backfill | Chạy lại batch từ kho lịch sử | Replay Kafka hoặc nguồn lưu trữ bất biến |
| Logic nghiệp vụ | Dễ bị trùng giữa batch/stream | Một logic cần vận hành |
| Độ phức tạp | Cao hơn nhưng mạnh khi batch lịch sử khác realtime | Gọn hơn, phù hợp event-first và replay được |

Tuần 4 chọn **Kappa** vì nguồn chuẩn là Kafka, cùng một phép parse/validate/window
áp dụng cho cả chạy liên tục và `availableNow`. Lambda chỉ hợp lý khi dự án cần
một batch layer độc lập để tính lại lịch sử từ nguồn lâu dài mà Kafka không giữ
đủ retention, hoặc khi workload batch khác căn bản workload realtime. Parquet
output của bài lab chưa tự biến thiết kế thành Lambda vì không có batch path thứ
hai tạo cùng serving view.

## 6. Kiểm thử

```powershell
docker compose exec workspace pytest -q exercises/week3/tests exercises/week4/tests
docker compose exec workspace python -m py_compile `
  exercises/week4/kafka_producer.py `
  exercises/week4/spark_streaming_kafka.py
```

Unit test không cần Kafka broker; lần chạy `run_local.ps1` mới là kiểm thử
end-to-end Kafka → Spark → file.
