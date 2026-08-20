-- Lab MySQL 8.0 tuần 1, tách biệt với schema Olist chính trên PostgreSQL.
-- Chạy bằng lệnh được ghi trong báo cáo tuần 1.

USE de_roadmap;

CREATE TABLE IF NOT EXISTS week1_orders_lab (
    order_id VARCHAR(64) PRIMARY KEY,
    order_status VARCHAR(32) NOT NULL,
    gross_amount DECIMAL(12, 2) NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT chk_week1_gross_amount CHECK (gross_amount >= 0),
    INDEX idx_week1_orders_status (order_status)
);

CREATE TABLE IF NOT EXISTS week1_order_audit (
    audit_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    old_status VARCHAR(32) NOT NULL,
    new_status VARCHAR(32) NOT NULL,
    changed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

DROP TRIGGER IF EXISTS trg_week1_order_status_audit;

DELIMITER $$
CREATE TRIGGER trg_week1_order_status_audit
BEFORE UPDATE ON week1_orders_lab
FOR EACH ROW
BEGIN
    IF NOT (NEW.order_status <=> OLD.order_status) THEN
        SET NEW.updated_at = CURRENT_TIMESTAMP(6);
        INSERT INTO week1_order_audit (
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
END$$
DELIMITER ;

DROP PROCEDURE IF EXISTS sp_week1_revenue_by_status;

DELIMITER $$
CREATE PROCEDURE sp_week1_revenue_by_status(IN minimum_revenue DECIMAL(12, 2))
BEGIN
    SELECT
        order_status,
        COUNT(*) AS total_orders,
        ROUND(SUM(gross_amount), 2) AS gross_revenue
    FROM week1_orders_lab
    GROUP BY order_status
    HAVING SUM(gross_amount) >= minimum_revenue
    ORDER BY gross_revenue DESC, order_status;
END$$
DELIMITER ;

INSERT INTO week1_orders_lab (order_id, order_status, gross_amount)
VALUES
    ('mysql-demo-001', 'created', 125.50),
    ('mysql-demo-002', 'approved', 250.00),
    ('mysql-demo-003', 'approved', 90.00)
ON DUPLICATE KEY UPDATE
    order_status = VALUES(order_status),
    gross_amount = VALUES(gross_amount);

START TRANSACTION;

UPDATE week1_orders_lab
SET order_status = 'approved'
WHERE order_id = 'mysql-demo-001';

SELECT
    order_id,
    old_status,
    new_status,
    changed_at
FROM week1_order_audit
WHERE order_id = 'mysql-demo-001'
ORDER BY audit_id DESC
LIMIT 1;

ROLLBACK;

CALL sp_week1_revenue_by_status(100.00);

EXPLAIN
SELECT
    order_id,
    order_status,
    gross_amount
FROM week1_orders_lab
WHERE order_status = 'approved'
ORDER BY gross_amount DESC;
