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
DEFAULT_REPORT_FILE = PROJECT_ROOT / "output" / "week2" / "oltp_load_summary.json"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

SOURCE_TABLES = {
    "customers",
    "geolocation",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "product_category_name_translation",
}

TARGET_TABLES = (
    "postal_locations",
    "customers",
    "customer_addresses",
    "sellers",
    "product_categories",
    "products",
    "order_statuses",
    "orders",
    "order_items",
    "payment_methods",
    "order_payments",
    "order_reviews",
)


def configure_console_encoding() -> None:
    """Tránh lỗi Unicode khi chạy bằng PowerShell dùng code page cũ."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8", errors="replace")


def build_default_db_url() -> str:
    """Tạo URL kết nối từ biến môi trường của Docker Compose."""
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
            "Chuyển dữ liệu Olist đã làm sạch trong PostgreSQL sang mô hình OLTP 3NF."
        )
    )
    parser.add_argument(
        "--db-url",
        default=build_default_db_url(),
        help="SQLAlchemy PostgreSQL URL; mặc định đọc từ DATABASE_URL hoặc POSTGRES_*.",
    )
    parser.add_argument(
        "--source-schema",
        default="olist_practice",
        help="Schema chứa 9 bảng Olist đã làm sạch.",
    )
    parser.add_argument(
        "--target-schema",
        default="olist_oltp",
        help="Schema OLTP sẽ được tạo.",
    )
    parser.add_argument(
        "--if-exists",
        choices=("replace", "fail"),
        default="replace",
        help=(
            "replace: tạo lại schema đích trong một transaction; "
            "fail: dừng nếu schema đã tồn tại."
        ),
    )
    parser.add_argument(
        "--report-file",
        type=Path,
        default=DEFAULT_REPORT_FILE,
        help="File JSON lưu kết quả đối soát sau khi nạp.",
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
            f"Schema {source_schema!r} thiếu bảng: {', '.join(missing_tables)}. "
            "Hãy chạy script import tuần 1 trước."
        )


def prepare_target_schema(
    connection: Connection,
    target_schema: str,
    if_exists: str,
) -> None:
    schema_exists = connection.execute(
        text("SELECT to_regnamespace(:schema_name) IS NOT NULL"),
        {"schema_name": target_schema},
    ).scalar_one()

    if schema_exists and if_exists == "fail":
        raise ValueError(
            f"Schema {target_schema!r} đã tồn tại. "
            "Dùng --if-exists replace nếu muốn tạo lại."
        )

    if schema_exists:
        connection.execute(text(f'DROP SCHEMA "{target_schema}" CASCADE'))
    connection.execute(text(f'CREATE SCHEMA "{target_schema}"'))


def create_oltp_tables(connection: Connection, target_schema: str) -> None:
    s = target_schema
    statements = [
        f"""
        CREATE TABLE {qualified(s, "postal_locations")} (
            postal_code_prefix TEXT PRIMARY KEY,
            city TEXT NOT NULL,
            state CHAR(2) NOT NULL CHECK (state ~ '^[A-Z]{{2}}$'),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
            CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "customers")} (
            customer_id TEXT PRIMARY KEY
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "customer_addresses")} (
            address_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            customer_id TEXT NOT NULL
                REFERENCES {qualified(s, "customers")}(customer_id),
            postal_code_prefix TEXT NOT NULL
                REFERENCES {qualified(s, "postal_locations")}(postal_code_prefix),
            UNIQUE (customer_id, postal_code_prefix)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "sellers")} (
            seller_id TEXT PRIMARY KEY,
            postal_code_prefix TEXT NOT NULL
                REFERENCES {qualified(s, "postal_locations")}(postal_code_prefix)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "product_categories")} (
            category_name TEXT PRIMARY KEY,
            category_name_english TEXT
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "products")} (
            product_id TEXT PRIMARY KEY,
            category_name TEXT
                REFERENCES {qualified(s, "product_categories")}(category_name),
            product_name_length INTEGER CHECK (product_name_length >= 0),
            product_description_length INTEGER CHECK (product_description_length >= 0),
            product_photos_qty INTEGER CHECK (product_photos_qty >= 0),
            product_weight_g INTEGER CHECK (product_weight_g >= 0),
            product_length_cm INTEGER CHECK (product_length_cm >= 0),
            product_height_cm INTEGER CHECK (product_height_cm >= 0),
            product_width_cm INTEGER CHECK (product_width_cm >= 0)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "order_statuses")} (
            order_status TEXT PRIMARY KEY
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "orders")} (
            order_id TEXT PRIMARY KEY,
            shipping_address_id BIGINT NOT NULL
                REFERENCES {qualified(s, "customer_addresses")}(address_id),
            order_status TEXT NOT NULL
                REFERENCES {qualified(s, "order_statuses")}(order_status),
            purchased_at TIMESTAMP NOT NULL,
            approved_at TIMESTAMP,
            delivered_to_carrier_at TIMESTAMP,
            delivered_to_customer_at TIMESTAMP,
            estimated_delivery_at TIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "order_items")} (
            order_id TEXT NOT NULL
                REFERENCES {qualified(s, "orders")}(order_id) ON DELETE CASCADE,
            item_number INTEGER NOT NULL CHECK (item_number > 0),
            product_id TEXT NOT NULL
                REFERENCES {qualified(s, "products")}(product_id),
            seller_id TEXT NOT NULL
                REFERENCES {qualified(s, "sellers")}(seller_id),
            shipping_limit_at TIMESTAMP NOT NULL,
            price NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
            freight_value NUMERIC(12, 2) NOT NULL CHECK (freight_value >= 0),
            PRIMARY KEY (order_id, item_number)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "payment_methods")} (
            payment_type TEXT PRIMARY KEY
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "order_payments")} (
            order_id TEXT NOT NULL
                REFERENCES {qualified(s, "orders")}(order_id) ON DELETE CASCADE,
            payment_sequence INTEGER NOT NULL CHECK (payment_sequence > 0),
            payment_type TEXT NOT NULL
                REFERENCES {qualified(s, "payment_methods")}(payment_type),
            installments INTEGER NOT NULL CHECK (installments >= 0),
            payment_value NUMERIC(12, 2) NOT NULL CHECK (payment_value >= 0),
            PRIMARY KEY (order_id, payment_sequence)
        )
        """,
        f"""
        CREATE TABLE {qualified(s, "order_reviews")} (
            review_id TEXT NOT NULL,
            order_id TEXT NOT NULL
                REFERENCES {qualified(s, "orders")}(order_id) ON DELETE CASCADE,
            review_score INTEGER NOT NULL CHECK (review_score BETWEEN 1 AND 5),
            review_title TEXT,
            review_message TEXT,
            created_at TIMESTAMP NOT NULL,
            answered_at TIMESTAMP NOT NULL,
            PRIMARY KEY (review_id, order_id)
        )
        """,
    ]

    for statement in statements:
        connection.execute(text(statement))


def load_oltp_data(
    connection: Connection,
    source_schema: str,
    target_schema: str,
) -> None:
    src = source_schema
    dst = target_schema
    statements = [
        f"""
        WITH place_observations AS (
            SELECT
                geolocation_zip_code_prefix AS postal_code_prefix,
                geolocation_city AS city,
                geolocation_state AS state
            FROM {qualified(src, "geolocation")}
            UNION ALL
            SELECT customer_zip_code_prefix, customer_city, customer_state
            FROM {qualified(src, "customers")}
            UNION ALL
            SELECT seller_zip_code_prefix, seller_city, seller_state
            FROM {qualified(src, "sellers")}
        ),
        place_frequency AS (
            SELECT postal_code_prefix, city, state, COUNT(*) AS frequency
            FROM place_observations
            WHERE postal_code_prefix IS NOT NULL
              AND city IS NOT NULL
              AND state IS NOT NULL
            GROUP BY postal_code_prefix, city, state
        ),
        canonical_place AS (
            SELECT postal_code_prefix, city, state
            FROM (
                SELECT
                    postal_code_prefix,
                    city,
                    state,
                    ROW_NUMBER() OVER (
                        PARTITION BY postal_code_prefix
                        ORDER BY frequency DESC, state, city
                    ) AS position
                FROM place_frequency
            ) AS ranked_places
            WHERE position = 1
        ),
        coordinates AS (
            SELECT
                geolocation_zip_code_prefix AS postal_code_prefix,
                AVG(geolocation_lat) AS latitude,
                AVG(geolocation_lng) AS longitude
            FROM {qualified(src, "geolocation")}
            GROUP BY geolocation_zip_code_prefix
        )
        INSERT INTO {qualified(dst, "postal_locations")} (
            postal_code_prefix, city, state, latitude, longitude
        )
        SELECT
            place.postal_code_prefix,
            place.city,
            place.state,
            coordinates.latitude,
            coordinates.longitude
        FROM canonical_place AS place
        LEFT JOIN coordinates USING (postal_code_prefix)
        """,
        f"""
        INSERT INTO {qualified(dst, "customers")} (customer_id)
        SELECT DISTINCT customer_unique_id
        FROM {qualified(src, "customers")}
        """,
        f"""
        INSERT INTO {qualified(dst, "customer_addresses")} (
            customer_id, postal_code_prefix
        )
        SELECT DISTINCT customer_unique_id, customer_zip_code_prefix
        FROM {qualified(src, "customers")}
        ORDER BY customer_unique_id, customer_zip_code_prefix
        """,
        f"""
        INSERT INTO {qualified(dst, "sellers")} (seller_id, postal_code_prefix)
        SELECT seller_id, seller_zip_code_prefix
        FROM {qualified(src, "sellers")}
        """,
        f"""
        INSERT INTO {qualified(dst, "product_categories")} (
            category_name, category_name_english
        )
        SELECT
            categories.category_name,
            MAX(translations.product_category_name_english)
        FROM (
            SELECT product_category_name AS category_name
            FROM {qualified(src, "product_category_name_translation")}
            UNION
            SELECT product_category_name
            FROM {qualified(src, "products")}
            WHERE product_category_name IS NOT NULL
        ) AS categories
        LEFT JOIN {qualified(src, "product_category_name_translation")} AS translations
            ON translations.product_category_name = categories.category_name
        GROUP BY categories.category_name
        """,
        f"""
        INSERT INTO {qualified(dst, "products")} (
            product_id,
            category_name,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        )
        SELECT
            product_id,
            product_category_name,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        FROM {qualified(src, "products")}
        """,
        f"""
        INSERT INTO {qualified(dst, "order_statuses")} (order_status)
        SELECT DISTINCT order_status
        FROM {qualified(src, "orders")}
        """,
        f"""
        INSERT INTO {qualified(dst, "orders")} (
            order_id,
            shipping_address_id,
            order_status,
            purchased_at,
            approved_at,
            delivered_to_carrier_at,
            delivered_to_customer_at,
            estimated_delivery_at
        )
        SELECT
            source_order.order_id,
            address.address_id,
            source_order.order_status,
            source_order.order_purchase_timestamp,
            source_order.order_approved_at,
            source_order.order_delivered_carrier_date,
            source_order.order_delivered_customer_date,
            source_order.order_estimated_delivery_date
        FROM {qualified(src, "orders")} AS source_order
        JOIN {qualified(src, "customers")} AS source_customer
          ON source_customer.customer_id = source_order.customer_id
        JOIN {qualified(dst, "customer_addresses")} AS address
          ON address.customer_id = source_customer.customer_unique_id
         AND address.postal_code_prefix = source_customer.customer_zip_code_prefix
        """,
        f"""
        INSERT INTO {qualified(dst, "order_items")} (
            order_id,
            item_number,
            product_id,
            seller_id,
            shipping_limit_at,
            price,
            freight_value
        )
        SELECT
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value
        FROM {qualified(src, "order_items")}
        """,
        f"""
        INSERT INTO {qualified(dst, "payment_methods")} (payment_type)
        SELECT DISTINCT payment_type
        FROM {qualified(src, "order_payments")}
        """,
        f"""
        INSERT INTO {qualified(dst, "order_payments")} (
            order_id,
            payment_sequence,
            payment_type,
            installments,
            payment_value
        )
        SELECT
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        FROM {qualified(src, "order_payments")}
        """,
        f"""
        INSERT INTO {qualified(dst, "order_reviews")} (
            review_id,
            order_id,
            review_score,
            review_title,
            review_message,
            created_at,
            answered_at
        )
        SELECT
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp
        FROM {qualified(src, "order_reviews")}
        """,
    ]

    for statement in statements:
        connection.execute(text(statement))


def create_indexes(connection: Connection, target_schema: str) -> None:
    s = target_schema
    statements = [
        f"CREATE INDEX idx_customer_addresses_postal_code "
        f"ON {qualified(s, 'customer_addresses')}(postal_code_prefix)",
        f"CREATE INDEX idx_sellers_postal_code "
        f"ON {qualified(s, 'sellers')}(postal_code_prefix)",
        f"CREATE INDEX idx_products_category "
        f"ON {qualified(s, 'products')}(category_name)",
        f"CREATE INDEX idx_orders_shipping_address "
        f"ON {qualified(s, 'orders')}(shipping_address_id)",
        f"CREATE INDEX idx_orders_status_purchase "
        f"ON {qualified(s, 'orders')}(order_status, purchased_at)",
        f"CREATE INDEX idx_order_items_product "
        f"ON {qualified(s, 'order_items')}(product_id)",
        f"CREATE INDEX idx_order_items_seller "
        f"ON {qualified(s, 'order_items')}(seller_id)",
        f"CREATE INDEX idx_order_payments_type "
        f"ON {qualified(s, 'order_payments')}(payment_type)",
        f"CREATE INDEX idx_order_reviews_order "
        f"ON {qualified(s, 'order_reviews')}(order_id)",
    ]
    for statement in statements:
        connection.execute(text(statement))


def analyze_tables(connection: Connection, target_schema: str) -> None:
    """Cập nhật thống kê để PostgreSQL chọn index ngay sau lần nạp lớn."""
    for table_name in TARGET_TABLES:
        connection.execute(
            text(f"ANALYZE {qualified(target_schema, table_name)}")
        )


def fetch_target_counts(
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


def validate_loaded_data(
    connection: Connection,
    source_schema: str,
    target_schema: str,
    target_counts: dict[str, int],
) -> None:
    direct_mappings = {
        "sellers": "sellers",
        "products": "products",
        "orders": "orders",
        "order_items": "order_items",
        "order_payments": "order_payments",
        "order_reviews": "order_reviews",
    }

    for target_table, source_table in direct_mappings.items():
        source_count = int(
            connection.execute(
                text(f"SELECT COUNT(*) FROM {qualified(source_schema, source_table)}")
            ).scalar_one()
        )
        if source_count != target_counts[target_table]:
            raise ValueError(
                f"Đối soát thất bại cho {target_table}: "
                f"nguồn={source_count:,}, đích={target_counts[target_table]:,}."
            )

    expected_customer_count = int(
        connection.execute(
            text(
                f"""
                SELECT COUNT(DISTINCT customer_unique_id)
                FROM {qualified(source_schema, "customers")}
                """
            )
        ).scalar_one()
    )
    if expected_customer_count != target_counts["customers"]:
        raise ValueError(
            "Đối soát thất bại cho customers: "
            f"nguồn={expected_customer_count:,}, "
            f"đích={target_counts['customers']:,}."
        )

    expected_address_count = int(
        connection.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT customer_unique_id, customer_zip_code_prefix
                    FROM {qualified(source_schema, "customers")}
                ) AS unique_addresses
                """
            )
        ).scalar_one()
    )
    if expected_address_count != target_counts["customer_addresses"]:
        raise ValueError(
            "Đối soát thất bại cho customer_addresses: "
            f"nguồn={expected_address_count:,}, "
            f"đích={target_counts['customer_addresses']:,}."
        )

    orphan_count = int(
        connection.execute(
            text(
                f"""
                SELECT
                    (SELECT COUNT(*)
                     FROM {qualified(target_schema, "customer_addresses")} AS child
                     LEFT JOIN {qualified(target_schema, "customers")} AS parent
                       ON parent.customer_id = child.customer_id
                     WHERE parent.customer_id IS NULL)
                  + (SELECT COUNT(*)
                     FROM {qualified(target_schema, "orders")} AS child
                     LEFT JOIN {qualified(target_schema, "customer_addresses")} AS parent
                       ON parent.address_id = child.shipping_address_id
                     WHERE parent.address_id IS NULL)
                  + (SELECT COUNT(*)
                     FROM {qualified(target_schema, "order_items")} AS child
                     LEFT JOIN {qualified(target_schema, "products")} AS parent
                       ON parent.product_id = child.product_id
                     WHERE parent.product_id IS NULL)
                  + (SELECT COUNT(*)
                     FROM {qualified(target_schema, "order_items")} AS child
                     LEFT JOIN {qualified(target_schema, "sellers")} AS parent
                       ON parent.seller_id = child.seller_id
                     WHERE parent.seller_id IS NULL)
                  + (SELECT COUNT(*)
                     FROM {qualified(target_schema, "order_payments")} AS child
                     LEFT JOIN {qualified(target_schema, "orders")} AS parent
                       ON parent.order_id = child.order_id
                     WHERE parent.order_id IS NULL)
                  + (SELECT COUNT(*)
                     FROM {qualified(target_schema, "order_reviews")} AS child
                     LEFT JOIN {qualified(target_schema, "orders")} AS parent
                       ON parent.order_id = child.order_id
                     WHERE parent.order_id IS NULL)
                """
            )
        ).scalar_one()
    )
    if orphan_count:
        raise ValueError(f"Phát hiện {orphan_count:,} khóa ngoại không có bản ghi cha.")


def write_report(
    report_file: Path,
    engine: Engine,
    source_schema: str,
    target_schema: str,
    row_counts: dict[str, int],
) -> None:
    report: dict[str, Any] = {
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "database": engine.url.render_as_string(hide_password=True),
        "source_schema": source_schema,
        "target_schema": target_schema,
        "validation": "passed",
        "total_rows": sum(row_counts.values()),
        "tables": row_counts,
    }
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def run(args: argparse.Namespace) -> dict[str, int]:
    validate_identifier(args.source_schema, "Tên source schema")
    validate_identifier(args.target_schema, "Tên target schema")
    if args.source_schema == args.target_schema:
        raise ValueError("Source schema và target schema phải khác nhau.")

    engine = create_engine(args.db_url, pool_pre_ping=True)
    try:
        # PostgreSQL hỗ trợ transactional DDL: nếu một bước lỗi, cả schema đích rollback.
        with engine.begin() as connection:
            validate_source_schema(connection, args.source_schema)
            prepare_target_schema(connection, args.target_schema, args.if_exists)
            create_oltp_tables(connection, args.target_schema)
            load_oltp_data(connection, args.source_schema, args.target_schema)
            create_indexes(connection, args.target_schema)
            analyze_tables(connection, args.target_schema)
            row_counts = fetch_target_counts(connection, args.target_schema)
            validate_loaded_data(
                connection,
                args.source_schema,
                args.target_schema,
                row_counts,
            )

        write_report(
            args.report_file.expanduser().resolve(),
            engine,
            args.source_schema,
            args.target_schema,
            row_counts,
        )
        return row_counts
    finally:
        engine.dispose()


def main() -> None:
    configure_console_encoding()
    args = parse_args()
    try:
        row_counts = run(args)
    except (ValueError, OSError, SQLAlchemyError) as error:
        print(f"\nNạp OLTP thất bại: {error}", file=sys.stderr)
        raise SystemExit(1) from error

    print(f"\nĐã tạo schema OLTP {args.target_schema!r}:")
    for table_name, row_count in row_counts.items():
        print(f"- {table_name}: {row_count:,} dòng")
    print(f"\nĐối soát: đạt")
    print(f"Báo cáo: {args.report_file.expanduser().resolve()}")


if __name__ == "__main__":
    main()
