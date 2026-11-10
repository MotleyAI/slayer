"""Ossie ``source.column`` references in dataset-scoped metrics, and duplicate output labels of query sources (spec:
importers/osi-import › Source columns in dataset-scoped metrics, Duplicate output labels in query sources)."""

from pathlib import Path
from typing import Any

import pytest

from slayer.ingest_report import ConversionResult
from slayer.osi.parser import parse_osi_path
from tests._osi_helpers import (
    by_name, convert, documents, ds, f, flat, hidden, legacy, m, query, seed_db, unconverted, visible, write_yaml,
)

DDL = [
    "CREATE TABLE orders (order_id INTEGER, amount DOUBLE, quantity INTEGER, price DOUBLE, status VARCHAR, "
    "state VARCHAR)",
    "INSERT INTO orders VALUES (1, 10, 2, 10, 'completed', 'open'), (2, 5, 3, 5, 'open', 'closed'), "
    "(3, 7, NULL, NULL, 'completed', 'open')",
    "CREATE TABLE readings (reading_id INTEGER, quantity INTEGER)",
    "INSERT INTO readings VALUES (1, 1), (2, 2), (3, 3)",
    "CREATE TABLE o (id INTEGER, cid INTEGER, amount DOUBLE)",
    "CREATE TABLE c (id INTEGER)",
    "INSERT INTO o VALUES (1, 1, 10), (2, 1, 5)",
    "INSERT INTO c VALUES (1)",
]
FIELDS = [f("order_id"), f("amount")]
JOINED = "SELECT o.id, c.id, o.amount FROM o JOIN c ON o.cid = c.id"


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path, statements=DDL)


def _import(db: Path, *metrics: dict[str, Any], fields: list[dict[str, Any]] | None = None,
            name: str = "orders", source: str | None = None) -> ConversionResult:
    dataset = ds(name, source=source, fields=FIELDS if fields is None else fields, metrics=list(metrics))
    return convert(db=db, docs=documents(flat([dataset])))


async def _value(result: ConversionResult, db: Path, measure: str, *, model: str = "orders") -> Any:
    rows = await query(result=result, db=db, payload={"source_model": model, "measures": [measure]})
    assert len(rows) == 1
    return rows[0][f"{model}.{measure}"]


def _reason(result: ConversionResult, metric: str) -> str:
    return " ".join(e.message for e in result.unconverted_metrics if e.metric_name == metric)


# ─────────────────────────── values ────────────────────────────────────────

async def test_source_column_no_field_declares(db: Path) -> None:
    result = _import(db, m("units", "SUM(source.quantity)"))
    model = by_name(result)["orders"]
    assert visible(model) == {"order_id", "amount"}
    assert len(hidden(model)) == 1
    assert await _value(result, db, "units") == 5


async def test_metric_named_like_the_source_column(db: Path) -> None:
    result = _import(db, m("quantity", "SUM(source.quantity)"))
    assert visible(by_name(result)["orders"]) == {"order_id", "amount"}
    assert await _value(result, db, "quantity") == 5


async def test_source_column_a_field_passes_through(db: Path) -> None:
    result = _import(db, m("total", "SUM(source.amount)"))
    model = by_name(result)["orders"]
    assert visible(model) == {"order_id", "amount"}
    assert hidden(model) == []
    assert await _value(result, db, "total") == pytest.approx(22)


@pytest.mark.parametrize("sql", [
    "SUM(SOURCE.quantity)", "SUM(Source.quantity)", 'SUM("SOURCE".quantity)', 'SUM("source".quantity)',
])
async def test_qualifier_matches_case_insensitively(db: Path, sql: str) -> None:
    result = _import(db, m("units", sql))
    assert visible(by_name(result)["orders"]) == {"order_id", "amount"}
    assert await _value(result, db, "units") == 5


async def test_source_column_inside_an_operand(db: Path) -> None:
    result = _import(db, m("revenue", "SUM(source.quantity * price)"), fields=[*FIELDS, f("price")])
    assert visible(by_name(result)["orders"]) == {"order_id", "amount", "price"}
    assert await _value(result, db, "revenue") == pytest.approx(35)


async def test_source_column_in_a_filter_predicate(db: Path) -> None:
    result = _import(db, m("completed", "SUM(amount) FILTER (WHERE source.status = 'completed')"))
    assert visible(by_name(result)["orders"]) == {"order_id", "amount"}
    assert await _value(result, db, "completed") == pytest.approx(17)


async def test_filtered_row_count_over_a_source_column(db: Path) -> None:
    result = _import(db, m("completed_orders", "COUNT(*) FILTER (WHERE source.status = 'completed')"))
    assert visible(by_name(result)["orders"]) == {"order_id", "amount"}
    assert await _value(result, db, "completed_orders") == 2


async def test_percentile_over_a_source_column(db: Path) -> None:
    result = _import(db, m("median", "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY source.quantity)"),
                     name="readings", fields=[f("reading_id")])
    assert visible(by_name(result)["readings"]) == {"reading_id"}
    assert await _value(result, db, "median", model="readings") == pytest.approx(2)


@pytest.mark.parametrize(("source", "sql"), [
    pytest.param("SELECT order_id, quantity AS units FROM orders", "SUM(source.units)", id="alias"),
    pytest.param('SELECT order_id, quantity AS "Units" FROM orders', 'SUM(source."Units")', id="quoted-mixed-case"),
])
async def test_query_source_output_column(db: Path, source: str, sql: str) -> None:
    result = _import(db, m("units", sql), name="lines", source=source, fields=[f("order_id")])
    assert visible(by_name(result)["lines"]) == {"order_id"}
    assert await _value(result, db, "units", model="lines") == 5


# ─────────────────────────── reports ───────────────────────────────────────

@pytest.mark.parametrize(("sql", "leaf"), [
    pytest.param("SUM(source.quantty)", "quantty", id="unknown"),
    pytest.param('SUM(source."Quantity")', "Quantity", id="quoted-wrong-case"),
])
def test_unreadable_source_column_is_unconverted_and_adds_no_column(db: Path, sql: str, leaf: str) -> None:
    result = _import(db, m("units", sql))
    assert unconverted(result, "units")
    assert f"'{leaf}'" in _reason(result, "units")
    assert [c.name for c in by_name(result)["orders"].columns] == ["order_id", "amount"]


def test_metric_failing_after_its_source_column_resolves_adds_no_column(db: Path) -> None:
    result = _import(db, m("units", "SUM(source.quantity) + SUM(no_such_field)"))
    assert unconverted(result, "units")
    assert [c.name for c in by_name(result)["orders"].columns] == ["order_id", "amount"]


def test_field_named_source_takes_precedence(db: Path) -> None:
    result = _import(db, m("units", "SUM(source.quantity)"), fields=[*FIELDS, f("source", "status")])
    assert unconverted(result, "units")
    assert "'source'" in _reason(result, "units")
    assert hidden(by_name(result)["orders"]) == []


_RENAMED = [f("order_id"), f("status", "state")]
_OPEN = m("open_orders", "COUNT(*) FILTER (WHERE source.status = 'open')")


def test_source_column_behind_a_renamed_field_is_reported(db: Path) -> None:
    result = _import(db, _OPEN, fields=_RENAMED)
    assert unconverted(result, "open_orders")
    assert "'status'" in _reason(result, "open_orders")


@pytest.mark.xfail(strict=True, reason="DEV-2175 reads a physical column whose name a field has taken")
async def test_source_column_behind_a_renamed_field_reads_the_physical_column(db: Path) -> None:
    result = _import(db, _OPEN, fields=_RENAMED)
    assert await _value(result, db, "open_orders") == 1


def test_source_qualifier_in_a_model_scoped_metric(db: Path) -> None:
    document = flat([ds("orders", fields=FIELDS)], metrics=[m("units", "SUM(source.quantity)")])
    assert unconverted(convert(db=db, docs=documents(document)), "units")


def test_legacy_source_qualifier_is_not_a_source_column(db: Path, tmp_path: Path) -> None:
    model = {"name": "s", "datasets": [ds("orders", fields=FIELDS, metrics=[m("total", "SUM(source.amount)")])]}
    result = convert(db=db, docs=parse_osi_path(write_yaml(tmp_path / "legacy.yaml", legacy(model))))
    assert unconverted(result, "total")


# ──────────────────── duplicate output labels of query sources ─────────────

@pytest.mark.parametrize("sql", [pytest.param("id", id="bare"), pytest.param("id + 0", id="derived")])
def test_field_reading_a_duplicated_label_is_reported(db: Path, sql: str) -> None:
    result = _import(db, name="paid", source=JOINED,
                     fields=[f("order_ref", sql), f("amount"), f("amount_plus", "amount + 0")])
    assert visible(by_name(result)["paid"]) == {"amount", "amount_plus"}
    assert [e for e in result.warnings if e.model_name == "paid" and "'order_ref'" in e.message
            and "'id'" in e.message and e.severity == "dropped"]


@pytest.mark.parametrize("source", [
    pytest.param("SELECT * FROM o JOIN c ON o.cid = c.id", id="star"),
    pytest.param("SELECT o.*, c.* FROM o JOIN c ON o.cid = c.id", id="qualified-stars"),
    pytest.param("SELECT o.*, c.id FROM o JOIN c ON o.cid = c.id", id="star-and-column"),
    pytest.param("WITH oo AS (SELECT * FROM o) SELECT * FROM oo JOIN c ON oo.cid = c.id", id="cte"),
])
def test_star_expanding_to_a_duplicated_label_is_reported(db: Path, source: str) -> None:
    result = _import(db, m("ids", "COUNT(DISTINCT source.id)"), name="paid", source=source,
                     fields=[f("order_ref", "id"), f("amount")])
    assert visible(by_name(result)["paid"]) == {"amount"}
    assert unconverted(result, "ids")


def test_star_over_a_using_join_has_one_label(db: Path) -> None:
    result = _import(db, name="paid", source="SELECT * FROM o JOIN c USING (id)",
                     fields=[f("order_ref", "id"), f("amount")])
    assert visible(by_name(result)["paid"]) == {"order_ref", "amount"}


def test_source_column_reading_a_duplicated_label_is_unconverted(db: Path) -> None:
    result = _import(db, m("ids", "COUNT(DISTINCT source.id)"), m("total", "SUM(amount)"),
                     name="paid", source=JOINED, fields=[f("amount")])
    assert unconverted(result, "ids")
    assert "'id'" in _reason(result, "ids")
    assert [x.name for x in by_name(result)["paid"].measures] == ["total"]
