# Hướng dẫn dựng và import flow NiFi tuần 5

## Mục đích

File `flow_definition.json` là blueprint v2 đã khóa processor, connection,
Parameter Context, Controller Service và relationship. Script
`scripts/configure_nifi_flow.py` dùng blueprint để cấu hình toàn bộ Process Group
qua NiFi REST API và tải snapshot native về `flow_definition_native.json`.
Credential nhạy cảm không được ghi vào hai file.

Runtime dùng Process Group `DE Genesis Week 5 - Promotion Ingestion v2`. Hậu tố
`v2` giúp configurator không trộn graph canonical với Process Group v1 cũ; flow
cũ được giữ lại ở trạng thái stopped để đối chiếu hoặc khôi phục.

`flow_definition_native.json` là runtime export v2 đã được xác thực trên
Apache NiFi 1.27.0: 21 processor đều `VALID`, 46 connection và hai
Controller Service đều `ENABLED/VALID`. Có thể import artifact này, nhưng
phải nạp lại hai sensitive parameter sau import vì snapshot cố ý không
lưu secret. Blueprint v2 và configurator vẫn là nguồn chỉnh sửa chuẩn.

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

Script tự tạo/gán context `week5-local` từ `flow_definition.json`. Hai giá trị
sau luôn được đánh dấu Sensitive và chỉ lấy từ môi trường lúc chạy:

- `airflow.password`
- `postgres.password`

Không ghi hai giá trị này trở lại file JSON trong repo.

## Tạo flow tự động

Khai báo secret runtime rồi chạy trên máy host:

```powershell
$env:AIRFLOW_ADMIN_PASSWORD = "<mật-khẩu-airflow>"
$env:POSTGRES_PASSWORD = "<mật-khẩu-postgres>"
python exercises/week5/scripts/configure_nifi_flow.py `
  --password $env:NIFI_PASSWORD
```

Script có tính chạy lại: tìm component theo tên, cập nhật property, nối connection
còn thiếu, bật Controller Service rồi export snapshot native. Script dừng
processor và tắt service trước khi cập nhật; processor được để ở trạng thái
stopped để người vận hành kiểm tra rồi mới bật flow. Nếu JDBC hoặc
property không hợp lệ, lỗi được ghi trong trường `warnings` thay vì bị bỏ qua.

Sau đó mở NiFi và kiểm tra Process Group vừa tạo. Nếu cần tạo thủ công hoặc sửa
processor, dùng phần mô tả bên dưới.

## Dựng flow thủ công

Tạo Process Group `DE Genesis Week 5 - Promotion Ingestion`, sau đó tạo processor
theo đúng thứ tự trong file định nghĩa. Mỗi processor dùng thuộc tính và
relationship tương ứng. PostgreSQL JDBC driver đã nằm tại:

```text
/opt/nifi/nifi-current/lib/postgresql.jar
```

Sau khi bật hai Controller Service, chạy từng processor từ trái sang phải để xác
nhận lỗi cấu hình trước khi bật toàn flow.

## Quy tắc vận hành

- Gọi trang 1, 2, 3... cho đến khi `pagination.has_next=false`.
- `api.max.pages` là chốt an toàn chống vòng lặp pagination vô hạn.
- Gom theo `batch_id` và chờ đủ `api.expected.records=250` trước khi gọi Airflow.
  `MergeContent` không hỗ trợ FlowFile Expression Language ở ngưỡng này, nên
  khi đổi dataset phải cập nhật parameter thay vì nhúng `${pagination.total}`.
- Ghi cả record hợp lệ và không hợp lệ để đối soát
  `raw_count = accepted_count + rejected_count`.
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

Không chỉnh tay snapshot native. Sau mỗi thay đổi blueprint, chạy lại script để
NiFi áp dụng rồi export `flow_definition_native.json`. Trước khi đưa snapshot vào
version control, kiểm tra không có giá trị credential nhạy cảm.
