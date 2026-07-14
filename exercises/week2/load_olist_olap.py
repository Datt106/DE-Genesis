from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine, URL
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REPORT_FILE = PROJECT_ROOT / "output" / "week2" / "olap_load_summary.json"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SOURCE_SYSTEM = "olist"

SOURCE_TABLES = {
    "customer_addresses",
    "customers",
    "order_items",
    "order_payments",
    "order_reviews",
    "order_statuses",
    "orders",
    "payment_methods",
    "postal_locations",
    "product_categories",
    "products",
    "sellers",
}

DIMENSION_TABLES = (
    "dim_date",
    "dim_customer",
    "dim_location",
    "dim_product",
    "dim_seller",
    "dim_order_status",
    "dim_payment_method",
)

FACT_TABLES = (
    "fact_sales",
    "fact_payments",
    "fact_order_lifecycle",
    "fact_reviews",
)

TARGET_TABLES = DIMENSION_TABLES + FACT_TABLES


def configure_console_encoding() -> None:
    """Tránh lỗi Unicode khi PowerShell đang dùng code page cũ."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8", errors="replace")


def build_default_db_url() -> str:
    if os.getenv("DATABASE_URL"):
        return os.environ["DATABASE_URL"]

    return URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER", "de_user"),
        password=os.getenv("POSTGRES_PASSWORD", "de_password"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "de_roadmap"),
    ).render_as_string(hide_password=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Chuyển mô hình Olist OLTP sang kho dữ liệu OLAP dạng star schema "
            "theo phương pháp Kimball."
        )
    )
    parser.add_argument(
        "--db-url",
        default=build_default_db_url(),
        help="SQLAlchemy PostgreSQL URL; mặc định đọc DATABASE_URL hoặc POSTGRES_*.",
    )
    parser.add_argument(
        "--source-schema",
        default="olist_oltp",
        help="Schema OLTP nguồn.",
    )
    parser.add_argument(
        "--target-schema",
        default="olist_olap",
        help="Schema OLAP đích.",
    )
    parser.add_argument(
        "--if-exists",
        choices=("replace", "merge", "fail"),
        default="replace",
        help=(
            "replace: tạo lại toàn bộ; merge: nạp tăng dần và giữ lịch sử SCD2; "
            "fail: dừng nếu schema đích đã tồn tại."
        ),
    )
    parser.add_argument(
        "--report-file",
        type=Path,
        default=DEFAULT_REPORT_FILE,
        help="File JSON lưu kết quả nạp và đối soát.",
    )
    return parser.parse_args()


def validate_identifier(value: str, label: str) -> None:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{label} không hợp lệ: {value!r}. "
            "Chỉ dùng chữ cái, chữ số, dấu gạch dưới và không bắt đầu bằng số."
        )


def qualified(schema: str, table_name: str) -> str:
    return f'"{schema}"."{table_name}"'


def execute_statements(connection: Connection, statements: list[str]) -> None:
    for statement in statements:
        connection.execute(text(statement))


def schema_exists(connection: Connection, schema: str) -> bool:
    return bool(
        connection.execute(
            text("SELECT to_regnamespace(:schema_name) IS NOT NULL"),
            {"schema_name": schema},
        ).scalar_one()
    )


def validate_source_schema(connection: Connection, source_schema: str) -> None:
    rows = connection.execute(
        text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema_name
              AND table_type = 'BASE TABLE'
            """
        ),
        {"schema_name": source_schema},
    )
    existing_tables = {row[0] for row in rows}
    missing_tables = sorted(SOURCE_TABLES - existing_tables)
    if missing_tables:
        raise ValueError(
            f"Schema nguồn {source_schema!r} thiếu bảng: {', '.join(missing_tables)}. "
            "Hãy chạy load_olist_oltp.py trước."
        )


def validate_target_schema(connection: Connection, target_schema: str) -> None:
    rows = connection.execute(
        text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema_name
              AND table_type = 'BASE TABLE'
            """
        ),
        {"schema_name": target_schema},
    )
    existing_tables = {row[0] for row in rows}
    missing_tables = sorted(set(TARGET_TABLES) - existing_tables)
    if missing_tables:
        raise ValueError(
            f"Schema đích {target_schema!r} không đúng phiên bản, thiếu bảng: "
            f"{', '.join(missing_tables)}. Hãy chạy lại với --if-exists replace."
        )


def prepare_target_schema(
    connection: Connection,
    target_schema: str,
    if_exists: str,
) -> bool:
    exists = schema_exists(connection, target_schema)
    if exists and if_exists == "fail":
        raise ValueError(
            f"Schema {target_schema!r} đã tồn tại. "
            "Dùng --if-exists replace hoặc --if-exists merge."
        )

    if exists and if_exists == "replace":
        connection.execute(text(f'DROP SCHEMA "{target_schema}" CASCADE'))
        exists = False

    if not exists:
        connection.execute(text(f'CREATE SCHEMA "{target_schema}"'))
        return True

    validate_target_schema(connection, target_schema)
    return False


def create_star_schema(connection: Connection, target_schema: str) -> None:
    s = target_schema
    statements = [
        f"""
        CREATE TABLE {qualified(s, "dim_date")} (
            date_key INTEGER PRIMARY KEY,
            full_date DATE UNIQUE,
            day_of_month SMALLINT,
            day_of_week SMALLINT,
            day_name TEXT,
            week_of_year SMALLINT,
            month_number SMALLINT,
            month_name TEXT,
            quarter_number SMALLINT,
            year_number SMALLINT,
            is_weekend BOOLEAN NOT NULL
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "dim_customer")} (
            customer_key BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_system TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            effective_from TIMESTAMP NOT NULL,
            effective_to TIMESTAMP NOT NULL,
            is_current BOOLEAN NOT NULL,
            version_number INTEGER NOT NULL CHECK (version_number > 0),
            CHECK (effective_from < effective_to)
        )
        """,
        f"""
        CREATE UNIQUE INDEX uq_dim_customer_current
        ON {qualified(s, "dim_customer")}(source_system, customer_id)
        WHERE is_current
        """,
        f"""
        CREATE INDEX idx_dim_customer_history
        ON {qualified(s, "dim_customer")}(
            source_system, customer_id, effective_from, effective_to
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "dim_location")} (
            location_key BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_system TEXT NOT NULL,
            postal_code_prefix TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            UNIQUE (source_system, postal_code_prefix)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "dim_product")} (
            product_key BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_system TEXT NOT NULL,
            product_id TEXT NOT NULL,
            category_name TEXT,
            category_name_english TEXT,
            product_name_length INTEGER,
            product_description_length INTEGER,
            product_photos_qty INTEGER,
            product_weight_g INTEGER,
            product_length_cm INTEGER,
            product_height_cm INTEGER,
            product_width_cm INTEGER,
            effective_from TIMESTAMP NOT NULL,
            effective_to TIMESTAMP NOT NULL,
            is_current BOOLEAN NOT NULL,
            version_number INTEGER NOT NULL CHECK (version_number > 0),
            CHECK (effective_from < effective_to)
        )
        """,
        f"""
        CREATE UNIQUE INDEX uq_dim_product_current
        ON {qualified(s, "dim_product")}(source_system, product_id)
        WHERE is_current
        """,
        f"""
        CREATE INDEX idx_dim_product_history
        ON {qualified(s, "dim_product")}(
            source_system, product_id, effective_from, effective_to
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "dim_seller")} (
            seller_key BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_system TEXT NOT NULL,
            seller_id TEXT NOT NULL,
            postal_code_prefix TEXT,
            city TEXT,
            state TEXT,
            effective_from TIMESTAMP NOT NULL,
            effective_to TIMESTAMP NOT NULL,
            is_current BOOLEAN NOT NULL,
            version_number INTEGER NOT NULL CHECK (version_number > 0),
            CHECK (effective_from < effective_to)
        )
        """,
        f"""
        CREATE UNIQUE INDEX uq_dim_seller_current
        ON {qualified(s, "dim_seller")}(source_system, seller_id)
        WHERE is_current
        """,
        f"""
        CREATE INDEX idx_dim_seller_history
        ON {qualified(s, "dim_seller")}(
            source_system, seller_id, effective_from, effective_to
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "dim_order_status")} (
            order_status_key SMALLINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_system TEXT NOT NULL,
            order_status TEXT NOT NULL,
            status_group TEXT NOT NULL,
            is_completed BOOLEAN NOT NULL,
            is_cancelled BOOLEAN NOT NULL,
            UNIQUE (source_system, order_status)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "dim_payment_method")} (
            payment_method_key SMALLINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_system TEXT NOT NULL,
            payment_type TEXT NOT NULL,
            payment_group TEXT NOT NULL,
            UNIQUE (source_system, payment_type)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "fact_sales")} (
            purchase_date_key INTEGER NOT NULL
                REFERENCES {qualified(s, "dim_date")}(date_key),
            shipping_limit_date_key INTEGER NOT NULL
                REFERENCES {qualified(s, "dim_date")}(date_key),
            customer_key BIGINT NOT NULL
                REFERENCES {qualified(s, "dim_customer")}(customer_key),
            shipping_location_key BIGINT NOT NULL
                REFERENCES {qualified(s, "dim_location")}(location_key),
            product_key BIGINT NOT NULL
                REFERENCES {qualified(s, "dim_product")}(product_key),
            seller_key BIGINT NOT NULL
                REFERENCES {qualified(s, "dim_seller")}(seller_key),
            order_status_key SMALLINT NOT NULL
                REFERENCES {qualified(s, "dim_order_status")}(order_status_key),
            order_id TEXT NOT NULL,
            item_number INTEGER NOT NULL,
            item_price NUMERIC(12, 2) NOT NULL CHECK (item_price >= 0),
            freight_value NUMERIC(12, 2) NOT NULL CHECK (freight_value >= 0),
            PRIMARY KEY (order_id, item_number)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "fact_payments")} (
            purchase_date_key INTEGER NOT NULL
                REFERENCES {qualified(s, "dim_date")}(date_key),
            customer_key BIGINT NOT NULL
                REFERENCES {qualified(s, "dim_customer")}(customer_key),
            shipping_location_key BIGINT NOT NULL
                REFERENCES {qualified(s, "dim_location")}(location_key),
            payment_method_key SMALLINT NOT NULL
                REFERENCES {qualified(s, "dim_payment_method")}(payment_method_key),
            order_status_key SMALLINT NOT NULL
                REFERENCES {qualified(s, "dim_order_status")}(order_status_key),
            order_id TEXT NOT NULL,
            payment_sequence INTEGER NOT NULL,
            installments INTEGER NOT NULL CHECK (installments >= 0),
            payment_value NUMERIC(12, 2) NOT NULL CHECK (payment_value >= 0),
            PRIMARY KEY (order_id, payment_sequence)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "fact_order_lifecycle")} (
            order_id TEXT PRIMARY KEY,
            purchase_date_key INTEGER NOT NULL
                REFERENCES {qualified(s, "dim_date")}(date_key),
            approved_date_key INTEGER NOT NULL
                REFERENCES {qualified(s, "dim_date")}(date_key),
            carrier_date_key INTEGER NOT NULL
                REFERENCES {qualified(s, "dim_date")}(date_key),
            delivered_date_key INTEGER NOT NULL
                REFERENCES {qualified(s, "dim_date")}(date_key),
            estimated_delivery_date_key INTEGER NOT NULL
                REFERENCES {qualified(s, "dim_date")}(date_key),
            customer_key BIGINT NOT NULL
                REFERENCES {qualified(s, "dim_customer")}(customer_key),
            shipping_location_key BIGINT NOT NULL
                REFERENCES {qualified(s, "dim_location")}(location_key),
            order_status_key SMALLINT NOT NULL
                REFERENCES {qualified(s, "dim_order_status")}(order_status_key),
            approval_hours NUMERIC(12, 2),
            delivery_days NUMERIC(12, 2),
            delivery_variance_days NUMERIC(12, 2)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "fact_reviews")} (
            review_created_date_key INTEGER NOT NULL
                REFERENCES {qualified(s, "dim_date")}(date_key),
            review_answered_date_key INTEGER NOT NULL
                REFERENCES {qualified(s, "dim_date")}(date_key),
            customer_key BIGINT NOT NULL
                REFERENCES {qualified(s, "dim_customer")}(customer_key),
            shipping_location_key BIGINT NOT NULL
                REFERENCES {qualified(s, "dim_location")}(location_key),
            order_status_key SMALLINT NOT NULL
                REFERENCES {qualified(s, "dim_order_status")}(order_status_key),
            review_id TEXT NOT NULL,
            order_id TEXT NOT NULL,
            review_score INTEGER NOT NULL CHECK (review_score BETWEEN 1 AND 5),
            response_hours NUMERIC(12, 2) NOT NULL,
            has_title BOOLEAN NOT NULL,
            has_message BOOLEAN NOT NULL,
            PRIMARY KEY (review_id, order_id)
        )
        """,
        f"""
        CREATE INDEX idx_fact_sales_dimensions
        ON {qualified(s, "fact_sales")}(
            purchase_date_key, product_key, customer_key, seller_key
        )
        """,
        f"""
        CREATE INDEX idx_fact_sales_location
        ON {qualified(s, "fact_sales")}(shipping_location_key)
        """,
        f"""
        CREATE INDEX idx_fact_sales_status
        ON {qualified(s, "fact_sales")}(order_status_key)
        """,
        f"""
        CREATE INDEX idx_fact_payments_dimensions
        ON {qualified(s, "fact_payments")}(
            purchase_date_key, payment_method_key, customer_key
        )
        """,
        f"""
        CREATE INDEX idx_fact_order_lifecycle_dimensions
        ON {qualified(s, "fact_order_lifecycle")}(
            purchase_date_key, order_status_key, customer_key
        )
        """,
        f"""
        CREATE INDEX idx_fact_reviews_dimensions
        ON {qualified(s, "fact_reviews")}(
            review_created_date_key, customer_key, order_status_key
        )
        """,
    ]
    execute_statements(connection, statements)


def insert_unknown_members(connection: Connection, target_schema: str) -> None:
    s = target_schema
    statements = [
        f"""
        INSERT INTO {qualified(s, "dim_date")} (
            date_key, full_date, day_of_month, day_of_week, day_name,
            week_of_year, month_number, month_name, quarter_number,
            year_number, is_weekend
        )
        VALUES (0, NULL, NULL, NULL, 'Không xác định',
                NULL, NULL, 'Không xác định', NULL, NULL, FALSE)
        ON CONFLICT (date_key) DO NOTHING
        """,
        f"""
        INSERT INTO {qualified(s, "dim_customer")} (
            customer_key, source_system, customer_id, effective_from,
            effective_to, is_current, version_number
        )
        VALUES (
            0, 'warehouse', 'UNKNOWN', TIMESTAMP '1900-01-01',
            TIMESTAMP '9999-12-31', TRUE, 1
        )
        ON CONFLICT (customer_key) DO NOTHING
        """,
        f"""
        INSERT INTO {qualified(s, "dim_location")} (
            location_key, source_system, postal_code_prefix, city, state,
            latitude, longitude
        )
        VALUES (0, 'warehouse', 'UNKNOWN', 'Không xác định', 'NA', NULL, NULL)
        ON CONFLICT (location_key) DO NOTHING
        """,
        f"""
        INSERT INTO {qualified(s, "dim_product")} (
            product_key, source_system, product_id, category_name,
            category_name_english, effective_from, effective_to,
            is_current, version_number
        )
        VALUES (
            0, 'warehouse', 'UNKNOWN', 'Không xác định', 'Unknown',
            TIMESTAMP '1900-01-01', TIMESTAMP '9999-12-31', TRUE, 1
        )
        ON CONFLICT (product_key) DO NOTHING
        """,
        f"""
        INSERT INTO {qualified(s, "dim_seller")} (
            seller_key, source_system, seller_id, postal_code_prefix,
            city, state, effective_from, effective_to, is_current,
            version_number
        )
        VALUES (
            0, 'warehouse', 'UNKNOWN', 'UNKNOWN', 'Không xác định', 'NA',
            TIMESTAMP '1900-01-01', TIMESTAMP '9999-12-31', TRUE, 1
        )
        ON CONFLICT (seller_key) DO NOTHING
        """,
        f"""
        INSERT INTO {qualified(s, "dim_order_status")} (
            order_status_key, source_system, order_status, status_group,
            is_completed, is_cancelled
        )
        VALUES (0, 'warehouse', 'UNKNOWN', 'Không xác định', FALSE, FALSE)
        ON CONFLICT (order_status_key) DO NOTHING
        """,
        f"""
        INSERT INTO {qualified(s, "dim_payment_method")} (
            payment_method_key, source_system, payment_type, payment_group
        )
        VALUES (0, 'warehouse', 'UNKNOWN', 'Không xác định')
        ON CONFLICT (payment_method_key) DO NOTHING
        """,
    ]
    execute_statements(connection, statements)


def load_dim_date(
    connection: Connection,
    source_schema: str,
    target_schema: str,
) -> None:
    src = source_schema
    dst = target_schema
    connection.execute(
        text(
            f"""
            WITH source_dates AS (
                SELECT purchased_at::date AS date_value
                FROM {qualified(src, "orders")}
                UNION ALL
                SELECT approved_at::date FROM {qualified(src, "orders")}
                UNION ALL
                SELECT delivered_to_carrier_at::date FROM {qualified(src, "orders")}
                UNION ALL
                SELECT delivered_to_customer_at::date FROM {qualified(src, "orders")}
                UNION ALL
                SELECT estimated_delivery_at::date FROM {qualified(src, "orders")}
                UNION ALL
                SELECT shipping_limit_at::date FROM {qualified(src, "order_items")}
                UNION ALL
                SELECT created_at::date FROM {qualified(src, "order_reviews")}
                UNION ALL
                SELECT answered_at::date FROM {qualified(src, "order_reviews")}
            ),
            bounds AS (
                SELECT MIN(date_value) AS min_date, MAX(date_value) AS max_date
                FROM source_dates
                WHERE date_value IS NOT NULL
            ),
            calendar AS (
                SELECT generated_date::date AS full_date
                FROM bounds
                CROSS JOIN LATERAL generate_series(
                    bounds.min_date, bounds.max_date, INTERVAL '1 day'
                ) AS generated_date
            )
            INSERT INTO {qualified(dst, "dim_date")} (
                date_key, full_date, day_of_month, day_of_week, day_name,
                week_of_year, month_number, month_name, quarter_number,
                year_number, is_weekend
            )
            SELECT
                TO_CHAR(full_date, 'YYYYMMDD')::integer,
                full_date,
                EXTRACT(DAY FROM full_date)::smallint,
                EXTRACT(ISODOW FROM full_date)::smallint,
                TRIM(TO_CHAR(full_date, 'Day')),
                EXTRACT(WEEK FROM full_date)::smallint,
                EXTRACT(MONTH FROM full_date)::smallint,
                TRIM(TO_CHAR(full_date, 'Month')),
                EXTRACT(QUARTER FROM full_date)::smallint,
                EXTRACT(YEAR FROM full_date)::smallint,
                EXTRACT(ISODOW FROM full_date) IN (6, 7)
            FROM calendar
            ON CONFLICT (date_key) DO NOTHING
            """
        )
    )


def load_type1_dimensions(
    connection: Connection,
    source_schema: str,
    target_schema: str,
) -> None:
    src = source_schema
    dst = target_schema
    statements = [
        f"""
        INSERT INTO {qualified(dst, "dim_location")} (
            source_system, postal_code_prefix, city, state, latitude, longitude
        )
        SELECT
            '{SOURCE_SYSTEM}', postal_code_prefix, city, state, latitude, longitude
        FROM {qualified(src, "postal_locations")}
        ON CONFLICT (source_system, postal_code_prefix) DO UPDATE
        SET city = EXCLUDED.city,
            state = EXCLUDED.state,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude
        """,
        f"""
        INSERT INTO {qualified(dst, "dim_order_status")} (
            source_system, order_status, status_group, is_completed, is_cancelled
        )
        SELECT
            '{SOURCE_SYSTEM}',
            order_status,
            CASE
                WHEN order_status = 'delivered' THEN 'Hoàn thành'
                WHEN order_status IN ('canceled', 'unavailable') THEN 'Không thành công'
                ELSE 'Đang xử lý'
            END,
            order_status = 'delivered',
            order_status = 'canceled'
        FROM {qualified(src, "order_statuses")}
        ON CONFLICT (source_system, order_status) DO UPDATE
        SET status_group = EXCLUDED.status_group,
            is_completed = EXCLUDED.is_completed,
            is_cancelled = EXCLUDED.is_cancelled
        """,
        f"""
        INSERT INTO {qualified(dst, "dim_payment_method")} (
            source_system, payment_type, payment_group
        )
        SELECT
            '{SOURCE_SYSTEM}',
            payment_type,
            CASE
                WHEN payment_type IN ('credit_card', 'debit_card') THEN 'Thẻ'
                WHEN payment_type = 'boleto' THEN 'Phiếu thanh toán'
                WHEN payment_type = 'voucher' THEN 'Voucher'
                ELSE 'Không xác định'
            END
        FROM {qualified(src, "payment_methods")}
        ON CONFLICT (source_system, payment_type) DO UPDATE
        SET payment_group = EXCLUDED.payment_group
        """,
    ]
    execute_statements(connection, statements)


def load_dim_customer(
    connection: Connection,
    source_schema: str,
    target_schema: str,
) -> None:
    connection.execute(
        text(
            f"""
            INSERT INTO {qualified(target_schema, "dim_customer")} (
                source_system, customer_id, effective_from, effective_to,
                is_current, version_number
            )
            SELECT
                '{SOURCE_SYSTEM}',
                source_customer.customer_id,
                TIMESTAMP '1900-01-01',
                TIMESTAMP '9999-12-31',
                TRUE,
                1
            FROM {qualified(source_schema, "customers")} AS source_customer
            WHERE NOT EXISTS (
                SELECT 1
                FROM {qualified(target_schema, "dim_customer")} AS target_customer
                WHERE target_customer.source_system = '{SOURCE_SYSTEM}'
                  AND target_customer.customer_id = source_customer.customer_id
            )
            """
        )
    )


def expire_changed_products(
    connection: Connection,
    source_schema: str,
    target_schema: str,
    load_time: datetime,
) -> None:
    connection.execute(
        text(
            f"""
            UPDATE {qualified(target_schema, "dim_product")} AS target
            SET effective_to = :load_time,
                is_current = FALSE
            FROM {qualified(source_schema, "products")} AS product
            LEFT JOIN {qualified(source_schema, "product_categories")} AS category
              ON category.category_name = product.category_name
            WHERE target.source_system = '{SOURCE_SYSTEM}'
              AND target.product_id = product.product_id
              AND target.is_current
              AND (
                    target.category_name IS DISTINCT FROM product.category_name
                 OR target.category_name_english
                    IS DISTINCT FROM category.category_name_english
                 OR target.product_name_length
                    IS DISTINCT FROM product.product_name_length
                 OR target.product_description_length
                    IS DISTINCT FROM product.product_description_length
                 OR target.product_photos_qty
                    IS DISTINCT FROM product.product_photos_qty
                 OR target.product_weight_g
                    IS DISTINCT FROM product.product_weight_g
                 OR target.product_length_cm
                    IS DISTINCT FROM product.product_length_cm
                 OR target.product_height_cm
                    IS DISTINCT FROM product.product_height_cm
                 OR target.product_width_cm
                    IS DISTINCT FROM product.product_width_cm
              )
            """
        ),
        {"load_time": load_time},
    )


def insert_current_products(
    connection: Connection,
    source_schema: str,
    target_schema: str,
    load_time: datetime,
) -> None:
    connection.execute(
        text(
            f"""
            INSERT INTO {qualified(target_schema, "dim_product")} (
                source_system,
                product_id,
                category_name,
                category_name_english,
                product_name_length,
                product_description_length,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm,
                effective_from,
                effective_to,
                is_current,
                version_number
            )
            SELECT
                '{SOURCE_SYSTEM}',
                product.product_id,
                product.category_name,
                category.category_name_english,
                product.product_name_length,
                product.product_description_length,
                product.product_photos_qty,
                product.product_weight_g,
                product.product_length_cm,
                product.product_height_cm,
                product.product_width_cm,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM {qualified(target_schema, "dim_product")} AS history
                        WHERE history.source_system = '{SOURCE_SYSTEM}'
                          AND history.product_id = product.product_id
                    )
                    THEN :load_time
                    ELSE TIMESTAMP '1900-01-01'
                END,
                TIMESTAMP '9999-12-31',
                TRUE,
                COALESCE((
                    SELECT MAX(history.version_number) + 1
                    FROM {qualified(target_schema, "dim_product")} AS history
                    WHERE history.source_system = '{SOURCE_SYSTEM}'
                      AND history.product_id = product.product_id
                ), 1)
            FROM {qualified(source_schema, "products")} AS product
            LEFT JOIN {qualified(source_schema, "product_categories")} AS category
              ON category.category_name = product.category_name
            WHERE NOT EXISTS (
                SELECT 1
                FROM {qualified(target_schema, "dim_product")} AS current_product
                WHERE current_product.source_system = '{SOURCE_SYSTEM}'
                  AND current_product.product_id = product.product_id
                  AND current_product.is_current
            )
            """
        ),
        {"load_time": load_time},
    )


def expire_changed_sellers(
    connection: Connection,
    source_schema: str,
    target_schema: str,
    load_time: datetime,
) -> None:
    connection.execute(
        text(
            f"""
            UPDATE {qualified(target_schema, "dim_seller")} AS target
            SET effective_to = :load_time,
                is_current = FALSE
            FROM {qualified(source_schema, "sellers")} AS seller
            JOIN {qualified(source_schema, "postal_locations")} AS location
              ON location.postal_code_prefix = seller.postal_code_prefix
            WHERE target.source_system = '{SOURCE_SYSTEM}'
              AND target.seller_id = seller.seller_id
              AND target.is_current
              AND (
                    target.postal_code_prefix
                        IS DISTINCT FROM seller.postal_code_prefix
                 OR target.city IS DISTINCT FROM location.city
                 OR target.state IS DISTINCT FROM location.state
              )
            """
        ),
        {"load_time": load_time},
    )


def insert_current_sellers(
    connection: Connection,
    source_schema: str,
    target_schema: str,
    load_time: datetime,
) -> None:
    connection.execute(
        text(
            f"""
            INSERT INTO {qualified(target_schema, "dim_seller")} (
                source_system,
                seller_id,
                postal_code_prefix,
                city,
                state,
                effective_from,
                effective_to,
                is_current,
                version_number
            )
            SELECT
                '{SOURCE_SYSTEM}',
                seller.seller_id,
                seller.postal_code_prefix,
                location.city,
                location.state,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM {qualified(target_schema, "dim_seller")} AS history
                        WHERE history.source_system = '{SOURCE_SYSTEM}'
                          AND history.seller_id = seller.seller_id
                    )
                    THEN :load_time
                    ELSE TIMESTAMP '1900-01-01'
                END,
                TIMESTAMP '9999-12-31',
                TRUE,
                COALESCE((
                    SELECT MAX(history.version_number) + 1
                    FROM {qualified(target_schema, "dim_seller")} AS history
                    WHERE history.source_system = '{SOURCE_SYSTEM}'
                      AND history.seller_id = seller.seller_id
                ), 1)
            FROM {qualified(source_schema, "sellers")} AS seller
            JOIN {qualified(source_schema, "postal_locations")} AS location
              ON location.postal_code_prefix = seller.postal_code_prefix
            WHERE NOT EXISTS (
                SELECT 1
                FROM {qualified(target_schema, "dim_seller")} AS current_seller
                WHERE current_seller.source_system = '{SOURCE_SYSTEM}'
                  AND current_seller.seller_id = seller.seller_id
                  AND current_seller.is_current
            )
            """
        ),
        {"load_time": load_time},
    )


def load_dimensions(
    connection: Connection,
    source_schema: str,
    target_schema: str,
    load_time: datetime,
) -> None:
    load_dim_date(connection, source_schema, target_schema)
    load_type1_dimensions(connection, source_schema, target_schema)
    load_dim_customer(connection, source_schema, target_schema)
    expire_changed_products(connection, source_schema, target_schema, load_time)
    insert_current_products(connection, source_schema, target_schema, load_time)
    expire_changed_sellers(connection, source_schema, target_schema, load_time)
    insert_current_sellers(connection, source_schema, target_schema, load_time)


def load_fact_sales(
    connection: Connection,
    source_schema: str,
    target_schema: str,
) -> None:
    src = source_schema
    dst = target_schema
    connection.execute(
        text(
            f"""
            INSERT INTO {qualified(dst, "fact_sales")} (
                purchase_date_key,
                shipping_limit_date_key,
                customer_key,
                shipping_location_key,
                product_key,
                seller_key,
                order_status_key,
                order_id,
                item_number,
                item_price,
                freight_value
            )
            SELECT
                COALESCE(purchase_date.date_key, 0),
                COALESCE(shipping_date.date_key, 0),
                COALESCE(customer.customer_key, 0),
                COALESCE(location.location_key, 0),
                COALESCE(product.product_key, 0),
                COALESCE(seller.seller_key, 0),
                COALESCE(status.order_status_key, 0),
                order_item.order_id,
                order_item.item_number,
                order_item.price,
                order_item.freight_value
            FROM {qualified(src, "order_items")} AS order_item
            JOIN {qualified(src, "orders")} AS source_order
              ON source_order.order_id = order_item.order_id
            JOIN {qualified(src, "customer_addresses")} AS address
              ON address.address_id = source_order.shipping_address_id
            LEFT JOIN {qualified(dst, "dim_date")} AS purchase_date
              ON purchase_date.full_date = source_order.purchased_at::date
            LEFT JOIN {qualified(dst, "dim_date")} AS shipping_date
              ON shipping_date.full_date = order_item.shipping_limit_at::date
            LEFT JOIN {qualified(dst, "dim_customer")} AS customer
              ON customer.source_system = '{SOURCE_SYSTEM}'
             AND customer.customer_id = address.customer_id
             AND source_order.purchased_at >= customer.effective_from
             AND source_order.purchased_at < customer.effective_to
            LEFT JOIN {qualified(dst, "dim_location")} AS location
              ON location.source_system = '{SOURCE_SYSTEM}'
             AND location.postal_code_prefix = address.postal_code_prefix
            LEFT JOIN {qualified(dst, "dim_product")} AS product
              ON product.source_system = '{SOURCE_SYSTEM}'
             AND product.product_id = order_item.product_id
             AND source_order.purchased_at >= product.effective_from
             AND source_order.purchased_at < product.effective_to
            LEFT JOIN {qualified(dst, "dim_seller")} AS seller
              ON seller.source_system = '{SOURCE_SYSTEM}'
             AND seller.seller_id = order_item.seller_id
             AND source_order.purchased_at >= seller.effective_from
             AND source_order.purchased_at < seller.effective_to
            LEFT JOIN {qualified(dst, "dim_order_status")} AS status
              ON status.source_system = '{SOURCE_SYSTEM}'
             AND status.order_status = source_order.order_status
            ON CONFLICT (order_id, item_number) DO UPDATE
            SET purchase_date_key = EXCLUDED.purchase_date_key,
                shipping_limit_date_key = EXCLUDED.shipping_limit_date_key,
                customer_key = EXCLUDED.customer_key,
                shipping_location_key = EXCLUDED.shipping_location_key,
                product_key = EXCLUDED.product_key,
                seller_key = EXCLUDED.seller_key,
                order_status_key = EXCLUDED.order_status_key,
                item_price = EXCLUDED.item_price,
                freight_value = EXCLUDED.freight_value
            """
        )
    )


def load_fact_payments(
    connection: Connection,
    source_schema: str,
    target_schema: str,
) -> None:
    src = source_schema
    dst = target_schema
    connection.execute(
        text(
            f"""
            INSERT INTO {qualified(dst, "fact_payments")} (
                purchase_date_key,
                customer_key,
                shipping_location_key,
                payment_method_key,
                order_status_key,
                order_id,
                payment_sequence,
                installments,
                payment_value
            )
            SELECT
                COALESCE(purchase_date.date_key, 0),
                COALESCE(customer.customer_key, 0),
                COALESCE(location.location_key, 0),
                COALESCE(method.payment_method_key, 0),
                COALESCE(status.order_status_key, 0),
                payment.order_id,
                payment.payment_sequence,
                payment.installments,
                payment.payment_value
            FROM {qualified(src, "order_payments")} AS payment
            JOIN {qualified(src, "orders")} AS source_order
              ON source_order.order_id = payment.order_id
            JOIN {qualified(src, "customer_addresses")} AS address
              ON address.address_id = source_order.shipping_address_id
            LEFT JOIN {qualified(dst, "dim_date")} AS purchase_date
              ON purchase_date.full_date = source_order.purchased_at::date
            LEFT JOIN {qualified(dst, "dim_customer")} AS customer
              ON customer.source_system = '{SOURCE_SYSTEM}'
             AND customer.customer_id = address.customer_id
             AND source_order.purchased_at >= customer.effective_from
             AND source_order.purchased_at < customer.effective_to
            LEFT JOIN {qualified(dst, "dim_location")} AS location
              ON location.source_system = '{SOURCE_SYSTEM}'
             AND location.postal_code_prefix = address.postal_code_prefix
            LEFT JOIN {qualified(dst, "dim_payment_method")} AS method
              ON method.source_system = '{SOURCE_SYSTEM}'
             AND method.payment_type = payment.payment_type
            LEFT JOIN {qualified(dst, "dim_order_status")} AS status
              ON status.source_system = '{SOURCE_SYSTEM}'
             AND status.order_status = source_order.order_status
            ON CONFLICT (order_id, payment_sequence) DO UPDATE
            SET purchase_date_key = EXCLUDED.purchase_date_key,
                customer_key = EXCLUDED.customer_key,
                shipping_location_key = EXCLUDED.shipping_location_key,
                payment_method_key = EXCLUDED.payment_method_key,
                order_status_key = EXCLUDED.order_status_key,
                installments = EXCLUDED.installments,
                payment_value = EXCLUDED.payment_value
            """
        )
    )


def load_fact_order_lifecycle(
    connection: Connection,
    source_schema: str,
    target_schema: str,
) -> None:
    src = source_schema
    dst = target_schema
    connection.execute(
        text(
            f"""
            INSERT INTO {qualified(dst, "fact_order_lifecycle")} (
                order_id,
                purchase_date_key,
                approved_date_key,
                carrier_date_key,
                delivered_date_key,
                estimated_delivery_date_key,
                customer_key,
                shipping_location_key,
                order_status_key,
                approval_hours,
                delivery_days,
                delivery_variance_days
            )
            SELECT
                source_order.order_id,
                COALESCE(purchase_date.date_key, 0),
                COALESCE(approved_date.date_key, 0),
                COALESCE(carrier_date.date_key, 0),
                COALESCE(delivered_date.date_key, 0),
                COALESCE(estimated_date.date_key, 0),
                COALESCE(customer.customer_key, 0),
                COALESCE(location.location_key, 0),
                COALESCE(status.order_status_key, 0),
                ROUND((
                    EXTRACT(EPOCH FROM (
                        source_order.approved_at - source_order.purchased_at
                    )) / 3600.0
                )::numeric, 2),
                ROUND((
                    EXTRACT(EPOCH FROM (
                        source_order.delivered_to_customer_at
                        - source_order.purchased_at
                    )) / 86400.0
                )::numeric, 2),
                ROUND((
                    EXTRACT(EPOCH FROM (
                        source_order.delivered_to_customer_at
                        - source_order.estimated_delivery_at
                    )) / 86400.0
                )::numeric, 2)
            FROM {qualified(src, "orders")} AS source_order
            JOIN {qualified(src, "customer_addresses")} AS address
              ON address.address_id = source_order.shipping_address_id
            LEFT JOIN {qualified(dst, "dim_date")} AS purchase_date
              ON purchase_date.full_date = source_order.purchased_at::date
            LEFT JOIN {qualified(dst, "dim_date")} AS approved_date
              ON approved_date.full_date = source_order.approved_at::date
            LEFT JOIN {qualified(dst, "dim_date")} AS carrier_date
              ON carrier_date.full_date = source_order.delivered_to_carrier_at::date
            LEFT JOIN {qualified(dst, "dim_date")} AS delivered_date
              ON delivered_date.full_date = source_order.delivered_to_customer_at::date
            LEFT JOIN {qualified(dst, "dim_date")} AS estimated_date
              ON estimated_date.full_date = source_order.estimated_delivery_at::date
            LEFT JOIN {qualified(dst, "dim_customer")} AS customer
              ON customer.source_system = '{SOURCE_SYSTEM}'
             AND customer.customer_id = address.customer_id
             AND source_order.purchased_at >= customer.effective_from
             AND source_order.purchased_at < customer.effective_to
            LEFT JOIN {qualified(dst, "dim_location")} AS location
              ON location.source_system = '{SOURCE_SYSTEM}'
             AND location.postal_code_prefix = address.postal_code_prefix
            LEFT JOIN {qualified(dst, "dim_order_status")} AS status
              ON status.source_system = '{SOURCE_SYSTEM}'
             AND status.order_status = source_order.order_status
            ON CONFLICT (order_id) DO UPDATE
            SET purchase_date_key = EXCLUDED.purchase_date_key,
                approved_date_key = EXCLUDED.approved_date_key,
                carrier_date_key = EXCLUDED.carrier_date_key,
                delivered_date_key = EXCLUDED.delivered_date_key,
                estimated_delivery_date_key = EXCLUDED.estimated_delivery_date_key,
                customer_key = EXCLUDED.customer_key,
                shipping_location_key = EXCLUDED.shipping_location_key,
                order_status_key = EXCLUDED.order_status_key,
                approval_hours = EXCLUDED.approval_hours,
                delivery_days = EXCLUDED.delivery_days,
                delivery_variance_days = EXCLUDED.delivery_variance_days
            """
        )
    )


def load_fact_reviews(
    connection: Connection,
    source_schema: str,
    target_schema: str,
) -> None:
    src = source_schema
    dst = target_schema
    connection.execute(
        text(
            f"""
            INSERT INTO {qualified(dst, "fact_reviews")} (
                review_created_date_key,
                review_answered_date_key,
                customer_key,
                shipping_location_key,
                order_status_key,
                review_id,
                order_id,
                review_score,
                response_hours,
                has_title,
                has_message
            )
            SELECT
                COALESCE(created_date.date_key, 0),
                COALESCE(answered_date.date_key, 0),
                COALESCE(customer.customer_key, 0),
                COALESCE(location.location_key, 0),
                COALESCE(status.order_status_key, 0),
                review.review_id,
                review.order_id,
                review.review_score,
                ROUND((
                    EXTRACT(EPOCH FROM (review.answered_at - review.created_at))
                    / 3600.0
                )::numeric, 2),
                review.review_title IS NOT NULL,
                review.review_message IS NOT NULL
            FROM {qualified(src, "order_reviews")} AS review
            JOIN {qualified(src, "orders")} AS source_order
              ON source_order.order_id = review.order_id
            JOIN {qualified(src, "customer_addresses")} AS address
              ON address.address_id = source_order.shipping_address_id
            LEFT JOIN {qualified(dst, "dim_date")} AS created_date
              ON created_date.full_date = review.created_at::date
            LEFT JOIN {qualified(dst, "dim_date")} AS answered_date
              ON answered_date.full_date = review.answered_at::date
            LEFT JOIN {qualified(dst, "dim_customer")} AS customer
              ON customer.source_system = '{SOURCE_SYSTEM}'
             AND customer.customer_id = address.customer_id
             AND source_order.purchased_at >= customer.effective_from
             AND source_order.purchased_at < customer.effective_to
            LEFT JOIN {qualified(dst, "dim_location")} AS location
              ON location.source_system = '{SOURCE_SYSTEM}'
             AND location.postal_code_prefix = address.postal_code_prefix
            LEFT JOIN {qualified(dst, "dim_order_status")} AS status
              ON status.source_system = '{SOURCE_SYSTEM}'
             AND status.order_status = source_order.order_status
            ON CONFLICT (review_id, order_id) DO UPDATE
            SET review_created_date_key = EXCLUDED.review_created_date_key,
                review_answered_date_key = EXCLUDED.review_answered_date_key,
                customer_key = EXCLUDED.customer_key,
                shipping_location_key = EXCLUDED.shipping_location_key,
                order_status_key = EXCLUDED.order_status_key,
                review_score = EXCLUDED.review_score,
                response_hours = EXCLUDED.response_hours,
                has_title = EXCLUDED.has_title,
                has_message = EXCLUDED.has_message
            """
        )
    )


def load_facts(
    connection: Connection,
    source_schema: str,
    target_schema: str,
) -> None:
    load_fact_sales(connection, source_schema, target_schema)
    load_fact_payments(connection, source_schema, target_schema)
    load_fact_order_lifecycle(connection, source_schema, target_schema)
    load_fact_reviews(connection, source_schema, target_schema)


def analyze_tables(connection: Connection, target_schema: str) -> None:
    for table_name in TARGET_TABLES:
        connection.execute(text(f"ANALYZE {qualified(target_schema, table_name)}"))


def fetch_row_counts(
    connection: Connection,
    target_schema: str,
) -> dict[str, int]:
    return {
        table_name: int(
            connection.execute(
                text(f"SELECT COUNT(*) FROM {qualified(target_schema, table_name)}")
            ).scalar_one()
        )
        for table_name in TARGET_TABLES
    }


def validate_fact_counts(
    connection: Connection,
    source_schema: str,
    target_counts: dict[str, int],
) -> None:
    expected_sources = {
        "fact_sales": "order_items",
        "fact_payments": "order_payments",
        "fact_order_lifecycle": "orders",
        "fact_reviews": "order_reviews",
    }
    for fact_table, source_table in expected_sources.items():
        source_count = int(
            connection.execute(
                text(f"SELECT COUNT(*) FROM {qualified(source_schema, source_table)}")
            ).scalar_one()
        )
        if source_count != target_counts[fact_table]:
            raise ValueError(
                f"Đối soát thất bại cho {fact_table}: "
                f"nguồn={source_count:,}, đích={target_counts[fact_table]:,}."
            )


def count_unexpected_unknown_keys(
    connection: Connection,
    target_schema: str,
) -> int:
    s = target_schema
    return int(
        connection.execute(
            text(
                f"""
                SELECT
                    (SELECT COUNT(*)
                     FROM {qualified(s, "fact_sales")}
                     WHERE purchase_date_key = 0
                        OR shipping_limit_date_key = 0
                        OR customer_key = 0
                        OR shipping_location_key = 0
                        OR product_key = 0
                        OR seller_key = 0
                        OR order_status_key = 0)
                  + (SELECT COUNT(*)
                     FROM {qualified(s, "fact_payments")}
                     WHERE purchase_date_key = 0
                        OR customer_key = 0
                        OR shipping_location_key = 0
                        OR payment_method_key = 0
                        OR order_status_key = 0)
                  + (SELECT COUNT(*)
                     FROM {qualified(s, "fact_order_lifecycle")}
                     WHERE purchase_date_key = 0
                        OR estimated_delivery_date_key = 0
                        OR customer_key = 0
                        OR shipping_location_key = 0
                        OR order_status_key = 0)
                  + (SELECT COUNT(*)
                     FROM {qualified(s, "fact_reviews")}
                     WHERE review_created_date_key = 0
                        OR review_answered_date_key = 0
                        OR customer_key = 0
                        OR shipping_location_key = 0
                        OR order_status_key = 0)
                """
            )
        ).scalar_one()
    )


def validate_current_scd_rows(
    connection: Connection,
    target_schema: str,
) -> None:
    for table_name, natural_key in (
        ("dim_customer", "customer_id"),
        ("dim_product", "product_id"),
        ("dim_seller", "seller_id"),
    ):
        duplicate_current_rows = int(
            connection.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT source_system, {natural_key}
                        FROM {qualified(target_schema, table_name)}
                        WHERE is_current
                        GROUP BY source_system, {natural_key}
                        HAVING COUNT(*) > 1
                    ) AS duplicated
                    """
                )
            ).scalar_one()
        )
        if duplicate_current_rows:
            raise ValueError(
                f"{table_name} có {duplicate_current_rows:,} natural key "
                "với nhiều phiên bản hiện hành."
            )


def write_report(
    report_file: Path,
    engine: Engine,
    source_schema: str,
    target_schema: str,
    load_mode: str,
    load_time: datetime,
    row_counts: dict[str, int],
    unexpected_unknown_keys: int,
) -> None:
    report: dict[str, Any] = {
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "load_effective_at_utc": load_time.replace(tzinfo=timezone.utc).isoformat(),
        "database": engine.url.render_as_string(hide_password=True),
        "source_schema": source_schema,
        "target_schema": target_schema,
        "load_mode": load_mode,
        "model": "Kimball star schema",
        "validation": "passed",
        "unexpected_unknown_keys": unexpected_unknown_keys,
        "dimensions": {
            table_name: row_counts[table_name]
            for table_name in DIMENSION_TABLES
        },
        "facts": {
            table_name: row_counts[table_name]
            for table_name in FACT_TABLES
        },
    }
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def run(args: argparse.Namespace) -> tuple[dict[str, int], int]:
    validate_identifier(args.source_schema, "Tên source schema")
    validate_identifier(args.target_schema, "Tên target schema")
    if args.source_schema == args.target_schema:
        raise ValueError("Source schema và target schema phải khác nhau.")

    load_time = datetime.now(timezone.utc).replace(tzinfo=None)
    engine = create_engine(args.db_url, pool_pre_ping=True)
    try:
        with engine.begin() as connection:
            validate_source_schema(connection, args.source_schema)
            is_new_schema = prepare_target_schema(
                connection,
                args.target_schema,
                args.if_exists,
            )
            if is_new_schema:
                create_star_schema(connection, args.target_schema)

            insert_unknown_members(connection, args.target_schema)
            load_dimensions(
                connection,
                args.source_schema,
                args.target_schema,
                load_time,
            )
            load_facts(connection, args.source_schema, args.target_schema)
            analyze_tables(connection, args.target_schema)

            row_counts = fetch_row_counts(connection, args.target_schema)
            validate_fact_counts(connection, args.source_schema, row_counts)
            validate_current_scd_rows(connection, args.target_schema)
            unexpected_unknown_keys = count_unexpected_unknown_keys(
                connection,
                args.target_schema,
            )
            if unexpected_unknown_keys:
                raise ValueError(
                    "Đối soát thất bại: có "
                    f"{unexpected_unknown_keys:,} khóa UNKNOWN ở thuộc tính bắt buộc."
                )

        write_report(
            args.report_file.expanduser().resolve(),
            engine,
            args.source_schema,
            args.target_schema,
            args.if_exists,
            load_time,
            row_counts,
            unexpected_unknown_keys,
        )
        return row_counts, unexpected_unknown_keys
    finally:
        engine.dispose()


def main() -> None:
    configure_console_encoding()
    args = parse_args()
    try:
        row_counts, unexpected_unknown_keys = run(args)
    except (ValueError, OSError, SQLAlchemyError) as error:
        print(f"\nNạp OLAP thất bại: {error}", file=sys.stderr)
        raise SystemExit(1) from error

    print(f"\nĐã nạp star schema Kimball {args.target_schema!r}:")
    print("\nDimensions:")
    for table_name in DIMENSION_TABLES:
        print(f"- {table_name}: {row_counts[table_name]:,} dòng")
    print("\nFacts:")
    for table_name in FACT_TABLES:
        print(f"- {table_name}: {row_counts[table_name]:,} dòng")
    print(f"\nKhóa UNKNOWN ngoài dự kiến: {unexpected_unknown_keys}")
    print("Đối soát: đạt")
    print(f"Báo cáo: {args.report_file.expanduser().resolve()}")


if __name__ == "__main__":
    main()
