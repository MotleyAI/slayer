"""The canonical portable-SQL writer's edge cases, and the re-vendor refusal of a checkout missing a managed file."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts import ossie_schema_drift as drift_check
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.portable_sql import portable_entity_expression, portable_expression, portable_sql
from tests.test_ossie_schema_drift import CHECKOUT_FILES, _checkout, _corpus, _fake_harvest, _tree, _vendor


@pytest.mark.parametrize(("written", "stored"), [
    ("INTERVAL '1 day' + x", "DATEADD(day, 1, x)"),
    ("COALESCE(x + INTERVAL '1 hour', y)", "COALESCE(DATEADD(hour, 1, x), y)"),
    ("a * b + INTERVAL '2 weeks' > c", "DATEADD(week, 2, a * b) > c"),
    ("CASE WHEN x - INTERVAL '3 days' > y THEN 1 END", "CASE WHEN DATEADD(day, -3, x) > y THEN 1 END"),
    ("x  +  INTERVAL '7' day", "DATEADD(day, 7, x)"),
])
def test_interval_arithmetic_is_respelled(written: str, stored: str) -> None:
    assert portable_sql(written) == stored


@pytest.mark.parametrize("text", [
    "amount + 1",
    "INTERVAL '1 day' - x",
    "x + INTERVAL '1 day' * 2",
    "x + INTERVAL '1 fortnight'",
    "x +",
])
def test_text_without_a_provable_respelling_is_kept(text: str) -> None:
    assert portable_sql(text) == text


def test_only_portable_entries_are_respelled() -> None:
    expression = ExpressionObject(sql="x + INTERVAL '1 day'", dialects=[
        DialectEntry(dialect="POSTGRES", expression="x + INTERVAL '1 day'"),
        DialectEntry(dialect="OSSIE_SQL_2026", expression="y - INTERVAL '2 days'")])
    out = portable_expression(expression)
    assert isinstance(out, ExpressionObject)
    assert out.sql == "DATEADD(day, 1, x)"
    assert [d.expression for d in out.dialects or []] == ["x + INTERVAL '1 day'", "DATEADD(day, -2, y)"]
    assert portable_entity_expression({"dialects": [{"dialect": "POSTGRES", "expression": "x + INTERVAL '1 day'"}]}) \
        == {"dialects": [{"dialect": "POSTGRES", "expression": "x + INTERVAL '1 day'"}]}


def test_an_unchanged_expression_object_is_returned_as_is() -> None:
    expression = ExpressionObject(sql="x + 1")
    assert portable_expression(expression) is expression


def test_vendor_refuses_a_checkout_missing_a_managed_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    files = {p: d for p, d in CHECKOUT_FILES.items() if p != "core-spec/spec.md"}
    checkout = _checkout(tmp_path / "ossie", files)
    corpus = _corpus(tmp_path / "corpus", listed={"examples/shop.yaml": b"old\n"})
    before = _tree(corpus)
    seen = _fake_harvest(monkeypatch)
    assert _vendor(checkout, corpus) == drift_check.EXIT_VENDOR
    assert seen == []
    assert _tree(corpus) == before
