from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import DateTime, Float, Integer, Numeric, Text, create_engine, text
from sqlalchemy.engine import Connection, Engine, URL
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "olist"
DEFAULT_REPORT_FILE = PROJECT_ROOT / "output" / "week1" / "olist_import_summary.json"
DEFAULT_SCHEMA = "olist_practice"
SCHEMA_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


TABLES: dict[str, dict[str, Any]] = {
    "customers": {
        "file": "olist_customers_dataset.csv",
        "key": ["customer_id"],
        "text": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "text": [
            "geolocation_zip_code_prefix",
            "geolocation_city",
            "geolocation_state",
        ],
        "float": ["geolocation_lat", "geolocation_lng"],
    },
    "orders": {
        "file": "olist_orders_dataset.csv",
        "key": ["order_id"],
        "text": ["order_id", "customer_id", "order_status"],
        "datetime": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "key": ["order_id", "order_item_id"],
        "text": ["order_id", "product_id", "seller_id"],
        "int": ["order_item_id"],
        "decimal": ["price", "freight_value"],
        "datetime": ["shipping_limit_date"],
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "key": ["order_id", "payment_sequential"],
        "text": ["order_id", "payment_type"],
        "int": ["payment_sequential", "payment_installments"],
        "decimal": ["payment_value"],
    },
    "order_reviews": {
        "file": "olist_order_reviews_dataset.csv",
        "text": [
            "review_id",
            "order_id",
            "review_comment_title",
            "review_comment_message",
        ],
        "int": ["review_score"],
        "datetime": ["review_creation_date", "review_answer_timestamp"],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "key": ["product_id"],
        "rename": {
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
        },
        "text": ["product_id", "product_category_name"],
        "int": [
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "key": ["seller_id"],
        "text": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    },
    "product_category_name_translation": {
        "file": "product_category_name_translation.csv",
        "key": ["product_category_name"],
        "text": ["product_category_name", "product_category_name_english"],
    },
}


PRIMARY_KEY_STATEMENTS = [
    "ALTER TABLE {schema}.customers ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id)",
    "ALTER TABLE {schema}.orders ADD CONSTRAINT pk_orders PRIMARY KEY (order_id)",
    (
        "ALTER TABLE {schema}.order_items ADD CONSTRAINT pk_order_items "
        "PRIMARY KEY (order_id, order_item_id)"
    ),
    (
        "ALTER TABLE {schema}.order_payments ADD CONSTRAINT pk_order_payments "
        "PRIMARY KEY (order_id, payment_sequential)"
    ),
    "ALTER TABLE {schema}.products ADD CONSTRAINT pk_products PRIMARY KEY (product_id)",
    "ALTER TABLE {schema}.sellers ADD CONSTRAINT pk_sellers PRIMARY KEY (seller_id)",
    (
        "ALTER TABLE {schema}.product_category_name_translation "
        "ADD CONSTRAINT pk_category_translation PRIMARY KEY (product_category_name)"
    ),
]

FOREIGN_KEY_STATEMENTS = [
    (
        "ALTER TABLE {schema}.orders ADD CONSTRAINT fk_orders_customers "
        "FOREIGN KEY (customer_id) REFERENCES {schema}.customers(customer_id)"
    ),
    (
        "ALTER TABLE {schema}.order_items ADD CONSTRAINT fk_items_orders "
        "FOREIGN KEY (order_id) REFERENCES {schema}.orders(order_id)"
    ),
    (
        "ALTER TABLE {schema}.order_items ADD CONSTRAINT fk_items_products "
        "FOREIGN KEY (product_id) REFERENCES {schema}.products(product_id)"
    ),
    (
        "ALTER TABLE {schema}.order_items ADD CONSTRAINT fk_items_sellers "
        "FOREIGN KEY (seller_id) REFERENCES {schema}.sellers(seller_id)"
    ),
    (
        "ALTER TABLE {schema}.order_payments ADD CONSTRAINT fk_payments_orders "
        "FOREIGN KEY (order_id) REFERENCES {schema}.orders(order_id)"
    ),
    (
        "ALTER TABLE {schema}.order_reviews ADD CONSTRAINT fk_reviews_orders "
        "FOREIGN KEY (order_id) REFERENCES {schema}.orders(order_id)"
    ),
]

INDEX_STATEMENTS = [
    "CREATE INDEX idx_orders_customer_id ON {schema}.orders(customer_id)",
    (
        "CREATE INDEX idx_orders_purchase_timestamp "
        "ON {schema}.orders(order_purchase_timestamp)"
    ),
    "CREATE INDEX idx_items_product_id ON {schema}.order_items(product_id)",
    "CREATE INDEX idx_items_seller_id ON {schema}.order_items(seller_id)",
    "CREATE INDEX idx_payments_order_id ON {schema}.order_payments(order_id)",
    "CREATE INDEX idx_reviews_order_id ON {schema}.order_reviews(order_id)",
    "CREATE INDEX idx_products_category ON {schema}.products(product_category_name)",
    (
        "CREATE INDEX idx_geolocation_zip_code "
        "ON {schema}.geolocation(geolocation_zip_code_prefix)"
    ),
]


def build_default_db_url() -> str:
    """Tao URL ket noi tu bien moi truong cua Docker Compose."""
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


def non_negative_integer(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("Gia tri phai lon hon hoac bang 0.")
    return number


def positive_integer(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Gia tri phai lon hon 0.")
    return number


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Doc, lam sach va nap Brazilian E-Commerce Olist CSV vao PostgreSQL."
    )
    parser.add_argument(
        "--db-url",
        default=build_default_db_url(),
        help="SQLAlchemy PostgreSQL URL; mac dinh doc tu cac bien POSTGRES_*.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Thu muc chua cac file CSV Olist.",
    )
    parser.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help="Schema dich trong PostgreSQL.",
    )
    parser.add_argument(
        "--if-exists",
        choices=["replace", "append", "fail"],
        default="replace",
        help="Cach xu ly khi bang da ton tai.",
    )
    parser.add_argument(
        "--sample-rows",
        type=non_negative_integer,
        default=0,
        help="Chi nap N dong dau tien moi file; 0 nghia la nap toan bo.",
    )
    parser.add_argument(
        "--chunksize",
        type=positive_integer,
        default=5_000,
        help="So dong ghi vao PostgreSQL trong moi dot.",
    )
    parser.add_argument(
        "--report-file",
        type=Path,
        default=DEFAULT_REPORT_FILE,
        help="File JSON luu thong ke sau khi import.",
    )
    return parser.parse_args()


def validate_schema_name(schema: str) -> None:
    if not SCHEMA_NAME_PATTERN.fullmatch(schema):
        raise ValueError(
            "Ten schema khong hop le. Chi dung chu cai, chu so, dau gach duoi "
            "va khong bat dau bang chu so."
        )


def configured_columns(config: dict[str, Any]) -> set[str]:
    columns: set[str] = set()
    for column_type in ("text", "int", "float", "decimal", "datetime"):
        columns.update(config.get(column_type, []))
    return columns


def normalize_empty_text(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").str.strip()
    return cleaned.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "null": pd.NA})


def validate_csv_columns(
    csv_path: Path,
    source_columns: list[str],
    config: dict[str, Any],
) -> None:
    rename_mapping = config.get("rename", {})
    normalized_columns = {rename_mapping.get(column, column) for column in source_columns}
    missing_columns = sorted(configured_columns(config) - normalized_columns)

    if missing_columns:
        missing_text = ", ".join(missing_columns)
        raise ValueError(f"{csv_path.name} thieu cot bat buoc: {missing_text}")


def text_dtype_mapping(
    source_columns: list[str],
    config: dict[str, Any],
) -> dict[str, str]:
    """Gan StringDtype ngay khi doc de khong mat so 0 o dau ma buu chinh."""
    rename_mapping = config.get("rename", {})
    text_columns = set(config.get("text", []))
    return {
        source_column: "string"
        for source_column in source_columns
        if rename_mapping.get(source_column, source_column) in text_columns
    }


def validate_key_columns(
    dataframe: pd.DataFrame,
    table_name: str,
    key_columns: list[str],
) -> None:
    if not key_columns:
        return

    null_key_rows = int(dataframe[key_columns].isna().any(axis=1).sum())
    if null_key_rows:
        raise ValueError(
            f"Bang {table_name} co {null_key_rows:,} dong bi thieu gia tri khoa."
        )

    duplicated_key_rows = int(dataframe.duplicated(subset=key_columns, keep=False).sum())
    if duplicated_key_rows:
        key_text = ", ".join(key_columns)
        raise ValueError(
            f"Bang {table_name} co {duplicated_key_rows:,} dong trung khoa ({key_text})."
        )


def read_and_clean_csv(
    csv_path: Path,
    config: dict[str, Any],
    sample_rows: int,
    table_name: str = "",
) -> pd.DataFrame:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Khong tim thay file: {csv_path}")

    source_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
    validate_csv_columns(csv_path, source_columns, config)

    dataframe = pd.read_csv(
        csv_path,
        nrows=sample_rows or None,
        dtype=text_dtype_mapping(source_columns, config),
        low_memory=False,
    )
    dataframe = dataframe.rename(columns=config.get("rename", {}))

    for column in config.get("text", []):
        dataframe[column] = normalize_empty_text(dataframe[column])

    for column in config.get("int", []):
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").astype("Int64")

    for column in config.get("float", []):
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")

    for column in config.get("decimal", []):
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").round(2)

    for column in config.get("datetime", []):
        dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce")

    validate_key_columns(dataframe, table_name or csv_path.stem, config.get("key", []))
    return dataframe


def build_dtype_mapping(config: dict[str, Any]) -> dict[str, Any]:
    dtype_mapping: dict[str, Any] = {}

    for column in config.get("text", []):
        dtype_mapping[column] = Text()

    for column in config.get("int", []):
        dtype_mapping[column] = Integer()

    for column in config.get("float", []):
        dtype_mapping[column] = Float()

    for column in config.get("decimal", []):
        dtype_mapping[column] = Numeric(12, 2)

    for column in config.get("datetime", []):
        dtype_mapping[column] = DateTime()

    return dtype_mapping


def prepare_schema(connection: Connection, schema: str, if_exists: str) -> None:
    if if_exists == "replace":
        connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def dataframe_summary(
    table_name: str,
    csv_path: Path,
    dataframe: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "table": table_name,
        "source_file": csv_path.name,
        "rows": len(dataframe),
        "columns": len(dataframe.columns),
        "missing_values": int(dataframe.isna().sum().sum()),
    }


def import_tables(
    connection: Connection,
    data_dir: Path,
    schema: str,
    if_exists: str,
    sample_rows: int,
    chunksize: int,
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []

    for table_name, config in TABLES.items():
        csv_path = data_dir / config["file"]
        dataframe = read_and_clean_csv(csv_path, config, sample_rows, table_name)
        dataframe.to_sql(
            table_name,
            con=connection,
            schema=schema,
            if_exists=if_exists,
            index=False,
            dtype=build_dtype_mapping(config),
            chunksize=chunksize,
            method="multi",
        )

        summary = dataframe_summary(table_name, csv_path, dataframe)
        summaries.append(summary)
        print(
            f"Imported {schema}.{table_name}: {summary['rows']:,} rows, "
            f"{summary['missing_values']:,} missing values"
        )

    return summaries


def execute_statements(
    connection: Connection,
    statements: list[str],
    schema: str,
) -> None:
    for statement in statements:
        connection.execute(text(statement.format(schema=schema)))


def apply_constraints_and_indexes(
    connection: Connection,
    schema: str,
    if_exists: str,
    sample_rows: int,
) -> None:
    if if_exists == "append":
        print("Skip constraints/indexes because --if-exists append was used.")
        return

    execute_statements(connection, PRIMARY_KEY_STATEMENTS, schema)

    if sample_rows:
        print(
            "Skip foreign keys in sample mode because rows are sampled "
            "independently from each CSV."
        )
    else:
        execute_statements(connection, FOREIGN_KEY_STATEMENTS, schema)

    execute_statements(connection, INDEX_STATEMENTS, schema)


def fetch_row_counts(
    connection: Connection,
    schema: str,
) -> dict[str, int]:
    row_counts: dict[str, int] = {}
    for table_name in TABLES:
        result = connection.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
        ).scalar_one()
        row_counts[table_name] = int(result)
    return row_counts


def print_row_counts(row_counts: dict[str, int]) -> None:
    print("\nRow counts:")
    for table_name, row_count in row_counts.items():
        print(f"- {table_name}: {row_count:,}")


def write_import_report(
    report_file: Path,
    engine: Engine,
    schema: str,
    sample_rows: int,
    table_summaries: list[dict[str, Any]],
    row_counts: dict[str, int],
) -> None:
    report = {
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "database": engine.url.render_as_string(hide_password=True),
        "schema": schema,
        "sample_rows_per_file": sample_rows,
        "total_rows": sum(row_counts.values()),
        "tables": [
            {**summary, "database_rows": row_counts[summary["table"]]}
            for summary in table_summaries
        ],
    }

    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nReport: {report_file}")


def run_import(args: argparse.Namespace) -> None:
    validate_schema_name(args.schema)

    data_dir = args.data_dir.expanduser().resolve()
    if not data_dir.is_dir():
        raise FileNotFoundError(f"Khong tim thay thu muc du lieu: {data_dir}")

    engine = create_engine(args.db_url, pool_pre_ping=True)
    try:
        # Mot transaction giup PostgreSQL rollback toan bo neu mot bang bi loi.
        with engine.begin() as connection:
            prepare_schema(connection, args.schema, args.if_exists)
            table_summaries = import_tables(
                connection=connection,
                data_dir=data_dir,
                schema=args.schema,
                if_exists=args.if_exists,
                sample_rows=args.sample_rows,
                chunksize=args.chunksize,
            )
            apply_constraints_and_indexes(
                connection,
                args.schema,
                args.if_exists,
                args.sample_rows,
            )
            row_counts = fetch_row_counts(connection, args.schema)

        print_row_counts(row_counts)
        write_import_report(
            args.report_file.expanduser().resolve(),
            engine,
            args.schema,
            args.sample_rows,
            table_summaries,
            row_counts,
        )
    finally:
        engine.dispose()


def main() -> None:
    args = parse_args()
    try:
        run_import(args)
    except (FileNotFoundError, ValueError, SQLAlchemyError, pd.errors.ParserError) as error:
        print(f"\nImport failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error

    print("\nDone.")


if __name__ == "__main__":
    main()
