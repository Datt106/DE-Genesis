# Dữ liệu Olist

Thư mục này không lưu bộ dữ liệu Olist trong Git vì các file CSV khá lớn. Hãy tải bộ **Brazilian E-Commerce Public Dataset by Olist** từ nguồn chính thức của Kaggle, giải nén và đặt đúng chín file sau vào thư mục này:

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

Kiểm tra tên file trước khi chạy bài Tuần 1:

```powershell
.\scripts\check-olist-data.ps1
```

Các bài smoke test Tuần 4 và pipeline log Tuần 6 không phụ thuộc bộ dữ liệu này. Mock Promotion API cũng tự dùng 250 sản phẩm xác định trước nếu file sản phẩm Olist chưa có.
