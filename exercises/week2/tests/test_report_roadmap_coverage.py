from __future__ import annotations

from pathlib import Path


REPORT = Path(__file__).resolve().parents[1] / "report" / "bao_cao_tuan_2.md"


def test_report_maps_core_concepts_to_the_implementation() -> None:
    text = REPORT.read_text(encoding="utf-8")
    normalized = " ".join(text.split())

    required_sections = (
        "## 3.4. ETL và ELT trong implementation",
        "## 3.5. CAP và phạm vi áp dụng",
        "## 3.6. ACID và BASE gắn với hệ thống",
    )
    assert all(section in text for section in required_sections)
    assert "`olist_practice` → `olist_oltp`" in text
    assert "một instance đơn" in normalized
    assert "Atomicity" in text and "Eventual consistency" in text
