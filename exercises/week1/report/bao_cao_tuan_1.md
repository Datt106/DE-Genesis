# Báo cáo tuần 1 - Python, Java, SQL, Linux, PostgreSQL và MySQL

## 1. Mục tiêu

Tuần 1 tập trung vào kỹ năng nền tảng của Data Engineering:

- Viết script Python đọc CSV, làm sạch dữ liệu và ghi vào PostgreSQL.
- Luyện Java cơ bản qua xử lý file CSV, class, object, collections và exception.
- Viết SQL phân tích dữ liệu với SELECT, JOIN, GROUP BY, HAVING, subquery, window function.
- Làm quen Linux command line trong container.
- Tạo bảng, constraint, foreign key, index, stored procedure, trigger và xem execution plan.
- Đối chiếu các thao tác cốt lõi giữa PostgreSQL 16 và MySQL 8.0 trong lab tách biệt.

Dataset dùng để luyện tập là **Brazilian E-Commerce Public Dataset by Olist** đặt tại `data/olist`.

## 2. Cấu trúc bài làm

```text
exercises/week1/script/
  import_olist_to_postgres.py     Script Python import toàn bộ Olist vào PostgreSQL
  sql_practice_postgres.sql       Truy vấn SQL phân tích và tối ưu
  sql_practice_mysql.sql          Lab MySQL về DDL, procedure, trigger và EXPLAIN
  linux_basics.sh                 Script luyện Linux command line
  OlistCsvJavaPractice.java       Code Java đọc CSV và thống kê customer

exercises/week1/report/
  bao_cao_tuan_1.md               Báo cáo tuần 1
```

## 3. Cách chạy

Khởi động môi trường tuần 1:

```powershell
.\scripts\start.ps1 -Target week1 -Build
```

Import Olist vào PostgreSQL:

```powershell
docker compose exec workspace python exercises/week1/script/import_olist_to_postgres.py
```

Chạy SQL luyện tập:

```powershell
docker compose exec workspace psql -h postgres -U de_user -d de_roadmap -f exercises/week1/script/sql_practice_postgres.sql
```

Chạy lab MySQL 8.0 từ PowerShell:

```powershell
Get-Content -Raw exercises/week1/script/sql_practice_mysql.sql |
  docker compose exec -T mysql mysql -ude_user -pde_password de_roadmap
```

Lab MySQL chỉ tạo các bảng có tiền tố `week1_`; pipeline Olist chính vẫn dùng
PostgreSQL để các tuần sau dùng chung một nguồn dữ liệu nhất quán.

Chạy script Linux:

```powershell
docker compose exec workspace bash exercises/week1/script/linux_basics.sh
```

Biên dịch và chạy Java:

```powershell
docker compose exec workspace javac exercises/week1/script/OlistCsvJavaPractice.java
docker compose exec workspace java -cp exercises/week1/script OlistCsvJavaPractice
```

Kết nối DBeaver:

```text
Host: localhost
Port: 5432
Database: de_roadmap
Username: de_user
Password: de_password
```

Sau khi kết nối, mở:

```text
de_roadmap -> Schemas -> olist_practice -> Tables
```

Kết nối MySQL cho lab đối chiếu:

```text
Host: localhost
Port: 3306
Database: de_roadmap
Username: de_user
Password: de_password
```

Trong DBeaver, chọn driver MySQL 8 và mở các bảng có tiền tố `week1_` trong
database `de_roadmap`.

## 4. Kết quả import dữ liệu

Script Python tạo schema `olist_practice` và nạp 9 bảng:

| Bảng | Số dòng |
| --- | ---: |
| customers | 99,441 |
| geolocation | 1,000,163 |
| order_items | 112,650 |
| order_payments | 103,886 |
| order_reviews | 99,224 |
| orders | 99,441 |
| product_category_name_translation | 71 |
| products | 32,951 |
| sellers | 3,095 |

Các bước xử lý chính trong Python:

- Đọc CSV bằng `pandas.read_csv`.
- Chuẩn hóa chuỗi rỗng thành `NULL`.
- Chuyển cột thời gian sang `datetime`.
- Chuyển cột số sang kiểu numeric hoặc integer.
- Sửa lỗi tên cột gốc của dataset: `lenght` thành `length`.
- Ghi DataFrame vào PostgreSQL bằng `DataFrame.to_sql`.
- Tạo primary key, foreign key và index sau khi import.

## 5. SQL đã thực hành

File `sql_practice_postgres.sql` bao gồm:

- SELECT cơ bản, ORDER BY, LIMIT.
- INSERT, UPDATE, DELETE trong transaction an toàn và `ROLLBACK`.
- INNER JOIN giữa orders, customers và order_items.
- LEFT JOIN để tìm sản phẩm chưa bán.
- RIGHT JOIN để giữ toàn bộ đơn hàng.
- FULL JOIN để đối chiếu seller và item đã bán.
- GROUP BY, HAVING để phân tích doanh thu category.
- Subquery và CTE để tìm bang có doanh thu cao hơn trung bình.
- Window function `RANK()` để tìm top category theo từng năm.
- `EXPLAIN ANALYZE` để xem execution plan.
- Stored procedure làm mới bảng metric theo trạng thái đơn hàng.
- Trigger ghi audit khi trạng thái trên bảng lab thay đổi.

Ví dụ truy vấn doanh thu theo bang:

```sql
SELECT
    c.customer_state,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS gross_revenue
FROM olist_practice.orders AS o
JOIN olist_practice.customers AS c
    ON c.customer_id = o.customer_id
JOIN olist_practice.order_items AS oi
    ON oi.order_id = o.order_id
GROUP BY c.customer_state
ORDER BY gross_revenue DESC;
```

## 6. Index và tối ưu truy vấn

Các index quan trọng đã tạo:

- `orders(customer_id)`
- `orders(order_purchase_timestamp)`
- `order_items(product_id)`
- `order_items(seller_id)`
- `order_payments(order_id)`
- `order_reviews(order_id)`
- `products(product_category_name)`
- `geolocation(geolocation_zip_code_prefix)`

Index giúp các truy vấn lọc theo thời gian, join theo khóa ngoại và phân tích theo product/seller chạy ổn định hơn. Với PostgreSQL, cần dùng `EXPLAIN ANALYZE` để kiểm tra thực tế query planner có dùng index hay không.

## 7. PostgreSQL và MySQL trong phạm vi tuần 1

Hai hệ quản trị được dùng với mục đích rõ ràng:

| Nội dung | PostgreSQL 16 | MySQL 8.0 |
| --- | --- | --- |
| Vai trò | Database chính chứa Olist và làm nguồn cho tuần 2 | Lab đối chiếu dialect, không nhân đôi toàn bộ Olist |
| Upsert | `ON CONFLICT ... DO UPDATE` | `ON DUPLICATE KEY UPDATE` |
| Stored procedure | `CREATE OR REPLACE PROCEDURE` và `CALL` | `CREATE PROCEDURE`, `DELIMITER` và `CALL` |
| Trigger | Hàm trigger PL/pgSQL + `EXECUTE FUNCTION` | Khối `BEGIN ... END` trực tiếp trong trigger |
| Execution plan | `EXPLAIN ANALYZE` | `EXPLAIN` |

Các bảng trigger/audit đều có tiền tố `week1_`. Phần cập nhật thử nằm trong
transaction có `ROLLBACK`, vì vậy không thay đổi bảng nghiệp vụ Olist. Stored
procedure PostgreSQL chỉ làm mới bảng tổng hợp riêng của lab.

## 8. Kết luận

Tuần 1 đã hoàn thành các phần nền tảng:

- Có script Python xử lý dữ liệu CSV lớn và đưa vào PostgreSQL.
- Có schema dữ liệu thực tế để luyện SQL trong DBeaver.
- Có truy vấn phân tích dữ liệu từ cơ bản đến nâng cao.
- Có lab procedure/trigger và execution plan chạy lại được trên cả PostgreSQL và MySQL.
- Có ví dụ Java để luyện OOP, collections, exception và file I/O.
- Có script Linux để luyện command line trong container.

Kết quả đạt được: có thể tự đọc dữ liệu từ file, làm sạch, ghi vào database, kết nối bằng DBeaver và viết truy vấn phân tích trên dataset thật.
