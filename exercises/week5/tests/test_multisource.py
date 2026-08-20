import json

from exercises.week5.multisource import extract_multisource_batch


class FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def copy_expert(self, query, output):
        assert "olist_olap.fact_sales" in query
        output.write("product_id,order_item_count,gross_item_value\nP-1,2,100.00\n")


class FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def cursor(self):
        return FakeCursor()


class FakeDatabase:
    def connect(self):
        return FakeConnection()


def test_extract_multisource_creates_replayable_manifest(tmp_path, monkeypatch) -> None:
    csv_path = tmp_path / "sales.csv"
    csv_path.write_text("order_id,quantity,unit_price,region,category\n1,2,5,HN,Office\n", encoding="utf-8")
    monkeypatch.setattr(
        "exercises.week5.multisource.DatabaseConfig.from_env",
        lambda: FakeDatabase(),
    )
    monkeypatch.setattr(
        "exercises.week5.multisource.fetch_all_promotions",
        lambda *args, **kwargs: [{"promotion_id": "P-1", "product_id": "P-1"}],
    )

    manifest_path = extract_multisource_batch(
        batch_id="batch-001",
        output_root=tmp_path / "output",
        csv_path=csv_path,
        api_url="http://mock",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert set(manifest["sources"]) == {"csv", "postgresql", "rest_api"}
    assert {name: value["count"] for name, value in manifest["sources"].items()} == {
        "csv": 1,
        "postgresql": 1,
        "rest_api": 1,
    }
    for source in manifest["sources"].values():
        assert source["path"]


def test_spark_master_prefers_compose_environment(monkeypatch) -> None:
    from exercises.week5.spark.multisource_report import spark_master_url

    monkeypatch.delenv("WEEK5_SPARK_MASTER", raising=False)
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    assert spark_master_url() == "spark://spark-master:7077"
