# Hướng dẫn dựng và import flow NiFi tuần 5

## Mục đích

File `flow_definition.json` là blueprint đã khóa processor, connection, parameter
và relationship. Script `scripts/configure_nifi_flow.py` dùng blueprint để tạo
Process Group qua NiFi REST API và tải bản export native về
`flow_definition_native.json`. Credential nhạy cảm không được ghi vào hai file.

## Chuẩn bị

1. Chạy `.\scripts\start.ps1 -Target week5 -Build`.
2. Chạy DDL:

   ```powershell
   docker compose exec workspace psql -h postgres -U de_user -d de_roadmap `
     -f exercises/week5/sql/create_week5_schemas.sql
   ```

3. Mở `https://localhost:8443/nifi`.
4. Đăng nhập bằng tài khoản trong `.env`.

## Parameter Context

Tạo context `week5-local`, nhập các parameter trong `flow_definition.json`.
Hai giá trị phải đánh dấu Sensitive:

- `airflow.password`
- `postgres.password`

Không ghi hai giá trị này trở lại file JSON trong repo.

## Tạo flow tự động

Chạy trên máy host:

```powershell
python exercises/week5/scripts/configure_nifi_flow.py `
  --password $env:NIFI_PASSWORD
```

Sau đó mở NiFi và kiểm tra Process Group vừa tạo. Nếu cần tạo thủ công hoặc sửa
processor, dùng phần mô tả bên dưới.

## Dựng flow thủ công

Tạo Process Group `DE Genesis Week 5 - Promotion Ingestion`, sau đó tạo processor
theo đúng thứ tự `01` đến `12` trong file định nghĩa. Mỗi processor dùng thuộc tính
và relationship tương ứng. PostgreSQL JDBC driver đã nằm tại:

```text
/opt/nifi/nifi-current/lib/postgresql.jar
```

Sau khi bật hai Controller Service, chạy từng processor từ trái sang phải để xác
nhận lỗi cấu hình trước khi bật toàn flow.

## Quy tắc vận hành

- Ghi hết raw batch trước khi gọi Airflow.
- Dùng `batch_id` làm correlation attribute.
- `409` từ Airflow là trigger trùng và không được tạo DAG run mới.
- `429` và `5xx` được retry có giới hạn; không nối relationship thành vòng lặp vô hạn
  khi chạy production.
- `401` đi vào queue riêng để sửa credential, không retry liên tục.
- Bản ghi thiếu `promotion_id` hoặc `product_id` đi vào invalid queue.

## Kiểm tra

Sau khi flow hoàn tất, kiểm tra:

```sql
SELECT batch_id, COUNT(*)
FROM week5_raw.promotions_nifi
GROUP BY batch_id
ORDER BY batch_id DESC;
```

Sau đó mở Airflow và xác nhận DAG
`de_genesis_week5_nifi_downstream` có run id dạng `nifi__<batch_id>`.

## Export sau khi hiệu chỉnh UI

Sau mỗi thay đổi trong NiFi UI, export lại Process Group và thay thế
`flow_definition.json`, nhưng phải xóa giá trị credential trước khi commit.
