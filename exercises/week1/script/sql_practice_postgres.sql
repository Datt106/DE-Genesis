-- Bai thuc hanh SQL tuan 1 voi Brazilian E-Commerce Public Dataset by Olist.
-- Chay sau khi da import bang script:
-- docker compose exec workspace python exercises/week1/script/import_olist_to_postgres.py

\set ON_ERROR_STOP on
SET search_path TO olist_practice, public;

-- 1. SELECT co ban: xem 10 don hang moi nhat.
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp
FROM orders
ORDER BY order_purchase_timestamp DESC
LIMIT 10;

-- 2. INSERT, UPDATE, DELETE trong transaction an toan.
-- ROLLBACK o cuoi de khong lam ban du lieu that.
BEGIN;

CREATE TEMP TABLE tmp_week1_dml_practice (
    id integer,
    note text
);

INSERT INTO tmp_week1_dml_practice (id, note)
VALUES
    (1, 'dong dau tien'),
    (2, 'dong thu hai');

UPDATE tmp_week1_dml_practice
SET note = 'da cap nhat'
WHERE id = 2;

DELETE FROM tmp_week1_dml_practice
WHERE id = 1;

SELECT * FROM tmp_week1_dml_practice;

ROLLBACK;

-- 3. INNER JOIN: doanh thu theo bang/thanh pho khach hang.
SELECT
    c.customer_state,
    c.customer_city,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS gross_revenue
FROM orders AS o
INNER JOIN customers AS c
    ON c.customer_id = o.customer_id
INNER JOIN order_items AS oi
    ON oi.order_id = o.order_id
GROUP BY c.customer_state, c.customer_city
ORDER BY gross_revenue DESC
LIMIT 20;

-- 4. LEFT JOIN: san pham chua tung ban duoc.
SELECT
    p.product_id,
    p.product_category_name,
    COUNT(oi.order_id) AS sold_items
FROM products AS p
LEFT JOIN order_items AS oi
    ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_category_name
HAVING COUNT(oi.order_id) = 0
ORDER BY p.product_id
LIMIT 20;

-- 5. RIGHT JOIN: don hang va so dong item, giu tat ca don hang.
SELECT
    o.order_status,
    COUNT(DISTINCT o.order_id) AS orders,
    COUNT(oi.order_item_id) AS item_rows
FROM order_items AS oi
RIGHT JOIN orders AS o
    ON o.order_id = oi.order_id
GROUP BY o.order_status
ORDER BY orders DESC;

-- 6. FULL JOIN: doi chieu seller voi item da ban.
SELECT
    COALESCE(s.seller_id, oi.seller_id) AS seller_id,
    s.seller_state,
    COUNT(oi.order_id) AS sold_items
FROM sellers AS s
FULL JOIN order_items AS oi
    ON oi.seller_id = s.seller_id
GROUP BY COALESCE(s.seller_id, oi.seller_id), s.seller_state
ORDER BY sold_items DESC
LIMIT 20;

-- 7. Aggregation + HAVING: category co doanh thu lon.
SELECT
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS category,
    COUNT(DISTINCT oi.order_id) AS orders,
    SUM(oi.price) AS product_revenue,
    AVG(oi.price) AS avg_item_price
FROM order_items AS oi
JOIN products AS p
    ON p.product_id = oi.product_id
LEFT JOIN product_category_name_translation AS t
    ON t.product_category_name = p.product_category_name
GROUP BY COALESCE(t.product_category_name_english, p.product_category_name, 'unknown')
HAVING SUM(oi.price) >= 100000
ORDER BY product_revenue DESC;

-- 8. Subquery: bang co doanh thu cao hon doanh thu trung binh theo bang.
WITH revenue_by_state AS (
    SELECT
        c.customer_state,
        SUM(oi.price + oi.freight_value) AS revenue
    FROM orders AS o
    JOIN customers AS c
        ON c.customer_id = o.customer_id
    JOIN order_items AS oi
        ON oi.order_id = o.order_id
    GROUP BY c.customer_state
)
SELECT
    customer_state,
    ROUND(revenue, 2) AS revenue
FROM revenue_by_state
WHERE revenue > (SELECT AVG(revenue) FROM revenue_by_state)
ORDER BY revenue DESC;

-- 9. Window function: top category theo tung nam.
WITH category_year_revenue AS (
    SELECT
        EXTRACT(YEAR FROM o.order_purchase_timestamp)::integer AS order_year,
        COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS category,
        SUM(oi.price) AS revenue
    FROM orders AS o
    JOIN order_items AS oi
        ON oi.order_id = o.order_id
    JOIN products AS p
        ON p.product_id = oi.product_id
    LEFT JOIN product_category_name_translation AS t
        ON t.product_category_name = p.product_category_name
    GROUP BY
        EXTRACT(YEAR FROM o.order_purchase_timestamp)::integer,
        COALESCE(t.product_category_name_english, p.product_category_name, 'unknown')
),
ranked AS (
    SELECT
        order_year,
        category,
        revenue,
        RANK() OVER (PARTITION BY order_year ORDER BY revenue DESC) AS revenue_rank
    FROM category_year_revenue
)
SELECT
    order_year,
    category,
    ROUND(revenue, 2) AS revenue,
    revenue_rank
FROM ranked
WHERE revenue_rank <= 5
ORDER BY order_year, revenue_rank;

-- 10. Execution plan: kiem tra index tren order_purchase_timestamp.
EXPLAIN ANALYZE
SELECT
    order_id,
    order_status,
    order_purchase_timestamp
FROM orders
WHERE order_purchase_timestamp >= TIMESTAMP '2018-01-01'
  AND order_purchase_timestamp < TIMESTAMP '2018-02-01'
ORDER BY order_purchase_timestamp;

-- 11. Stored procedure: làm mới bảng tổng hợp độc lập với bảng nghiệp vụ.
-- Bảng lab có thể tạo lại và không thay đổi dữ liệu Olist nguồn.
CREATE TABLE IF NOT EXISTS olist_practice.week1_order_status_metrics (
    order_status text PRIMARY KEY,
    total_orders bigint NOT NULL,
    refreshed_at timestamptz NOT NULL
);

CREATE OR REPLACE PROCEDURE olist_practice.refresh_week1_order_status_metrics()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE olist_practice.week1_order_status_metrics;

    INSERT INTO olist_practice.week1_order_status_metrics (
        order_status,
        total_orders,
        refreshed_at
    )
    SELECT
        order_status,
        COUNT(*) AS total_orders,
        CURRENT_TIMESTAMP
    FROM olist_practice.orders
    GROUP BY order_status;
END;
$$;

CALL olist_practice.refresh_week1_order_status_metrics();

SELECT
    order_status,
    total_orders,
    refreshed_at
FROM olist_practice.week1_order_status_metrics
ORDER BY total_orders DESC, order_status;

-- 12. Trigger: ghi vết thay đổi trạng thái trên bảng lab, không sửa bảng orders.
CREATE TABLE IF NOT EXISTS olist_practice.week1_order_status_lab (
    order_id text PRIMARY KEY,
    order_status text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS olist_practice.week1_order_status_audit (
    audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id text NOT NULL,
    old_status text NOT NULL,
    new_status text NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION olist_practice.audit_week1_order_status_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.order_status IS DISTINCT FROM OLD.order_status THEN
        NEW.updated_at := CURRENT_TIMESTAMP;
        INSERT INTO olist_practice.week1_order_status_audit (
            order_id,
            old_status,
            new_status
        )
        VALUES (
            OLD.order_id,
            OLD.order_status,
            NEW.order_status
        );
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_week1_order_status_audit
ON olist_practice.week1_order_status_lab;

CREATE TRIGGER trg_week1_order_status_audit
BEFORE UPDATE OF order_status ON olist_practice.week1_order_status_lab
FOR EACH ROW
EXECUTE FUNCTION olist_practice.audit_week1_order_status_change();

-- Dữ liệu thử được cô lập trong transaction và rollback sau khi kiểm chứng.
BEGIN;

INSERT INTO olist_practice.week1_order_status_lab (order_id, order_status)
VALUES ('week1-demo-order', 'created')
ON CONFLICT (order_id) DO UPDATE
SET order_status = EXCLUDED.order_status;

UPDATE olist_practice.week1_order_status_lab
SET order_status = 'approved'
WHERE order_id = 'week1-demo-order';

SELECT
    order_id,
    old_status,
    new_status,
    changed_at
FROM olist_practice.week1_order_status_audit
WHERE order_id = 'week1-demo-order'
ORDER BY audit_id DESC
LIMIT 1;

ROLLBACK;
