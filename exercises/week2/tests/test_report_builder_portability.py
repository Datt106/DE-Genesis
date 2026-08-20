from __future__ import annotations

import ast
import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BUILDER = ROOT / "exercises" / "week2" / "report" / "build_report_docx.py"


def test_report_builder_has_no_personal_or_version_pinned_skill_path() -> None:
    source = BUILDER.read_text(encoding="utf-8")

    assert r"C:\Users\ADMIN" not in source
    assert "26.812.11052" not in source
    assert "DE_GENESIS_TABLE_GEOMETRY_HELPER" in source
    assert "FallbackTableGeometry" in source
    ast.parse(source, filename=str(BUILDER))


def test_report_builder_has_cross_platform_font_fallbacks() -> None:
    source = BUILDER.read_text(encoding="utf-8")

    assert 'os.getenv("WINDIR")' in source
    assert "DejaVuSans" in source
    assert "LiberationSans" in source
    assert "ImageFont.load_default()" in source


def test_report_dependencies_are_declared() -> None:
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8").lower()

    assert "python-docx==" in requirements
    assert "pillow==" in requirements


def test_internal_table_geometry_fallback_works_without_skill() -> None:
    spec = importlib.util.spec_from_file_location("week2_builder_test", BUILDER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.table_helper_candidates = lambda: iter(())

    assert module.load_table_helper() is module.FallbackTableGeometry
    widths = module.FallbackTableGeometry.column_widths_from_weights(
        [1, 2, 3], 9360
    )
    assert widths == [1560, 3120, 4680]
