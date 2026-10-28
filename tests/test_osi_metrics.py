"""OSI metric conversion: FILTER aggregates and aggregate operands, executed on DuckDB."""

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.engine.syntax import AggCall, Ref, parse_expr
from slayer.ingest_report import ConversionResult
from tests._osi_helpers import (
    by_name, convert, documents, ds, f, flat, hidden, m, measure_home, query, rel, seed_db, unconverted,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


ORDER_FIELDS = [f("order_id", "o_orderkey"), f("customer_id", "o_custkey"), f("amount"), f("quantity"),
                f("status"), f("region"), f("is_paid"), f("first_name"), f("last_name")]


def _import(db: Path, *metrics: dict[str, Any], model_metrics: list[dict[str, Any]] | None = None
            ) -> ConversionResult:
    orders = ds("orders", fields=ORDER_FIELDS, metrics=list(metrics))
    customers = ds("customers", fields=[f("id", "c_custkey"), f("segment"), f("discount")])
    o2c = rel("o2c", src="orders", to="customers", from_columns=["customer_id"], to_columns=["id"])
    return convert(db=db, docs=documents(flat([orders, customers], relationships=[o2c], metrics=model_metrics)))


async def _value(result: ConversionResult, db: Path, measure: str) -> Any:
    home = measure_home(result, measure).name
    rows = await query(result=result, db=db, payload={"source_model": home, "measures": [measure]})
    return rows[0][f"{home}.{measure}"]


# ─────────────────────────── FILTER aggregates ─────────────────────────────

_FILTERED = [
    pytest.param("SUM(amount) FILTER (WHERE status = 'completed')", 17, id="sum"),
    pytest.param("COUNT(*) FILTER (WHERE status = 'completed')", 2, id="count-star"),
    pytest.param("AVG(amount) FILTER (WHERE status = 'completed')", 8.5, id="avg"),
    pytest.param("MIN(amount) FILTER (WHERE status = 'completed')", 7, id="min"),
    pytest.param("MAX(amount) FILTER (WHERE status = 'completed')", 10, id="max"),
    pytest.param("COUNT(amount) FILTER (WHERE status = 'open')", 1, id="count-col"),
    pytest.param("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) FILTER (WHERE status = 'completed')",
                 8.5, id="median"),
    pytest.param("PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount) FILTER (WHERE status = 'completed')",
                 9.7, id="percentile"),
    pytest.param("SUM(amount) FILTER (WHERE status = 'completed' AND amount > 8)", 10, id="and"),
    pytest.param("SUM(amount) FILTER (WHERE status = 'open' OR region = 'south')", 12, id="or"),
    pytest.param("SUM(amount) FILTER (WHERE region IS NULL)", 5, id="is-null"),
    pytest.param("SUM(amount) FILTER (WHERE region IS NOT NULL)", 17, id="is-not-null"),
    pytest.param("SUM(amount) FILTER (WHERE UPPER(status) = 'COMPLETED')", 17, id="scalar-call"),
    pytest.param("SUM(amount) FILTER (WHERE \"status\" = 'completed')", 17, id="quoted-identifier"),
    pytest.param("SUM(amount) FILTER (WHERE status = 'completed') / COUNT(*)", 17 / 3, id="in-arithmetic"),
]


@pytest.mark.parametrize(("sql", "expected"), _FILTERED)
async def test_filter_aggregate_value(db: Path, sql: str, expected: float) -> None:
    result = _import(db, m("v", sql))
    assert float(await _value(result, db, "v")) == pytest.approx(expected)


async def test_filtered_distinct_count(db: Path) -> None:
    visits = ds("visits", fields=[f("visit_id"), f("status"), f("customer_id")],
                metrics=[m("v", "COUNT(DISTINCT customer_id) FILTER (WHERE status = 'completed')")])
    result = convert(db=db, docs=documents(flat([visits])))
    assert await _value(result, db, "v") == 2


async def test_filter_does_not_remove_groups(db: Path) -> None:
    result = _import(db, m("v", "SUM(amount) FILTER (WHERE status = 'completed')"))
    rows = await query(result=result, db=db, payload={
        "source_model": "orders", "dimensions": ["status"], "measures": ["v"]})
    values = {r["orders.status"]: r["orders.v"] for r in rows}
    assert set(values) == {"completed", "open"}
    assert values["open"] is None
    assert float(values["completed"]) == pytest.approx(17)


@pytest.mark.parametrize("sql", [
    pytest.param("SUM(amount) FILTER (WHERE amount > AVG(amount))", id="aggregate"),
    pytest.param("SUM(amount) FILTER (WHERE ROW_NUMBER() OVER (ORDER BY amount) = 1)", id="window"),
    pytest.param("SUM(amount) FILTER (WHERE amount IN (SELECT amount FROM orders))", id="subquery"),
    *[pytest.param(f"{agg}(DISTINCT amount){flt}", id=f"{agg.lower()}-distinct{'-filtered' if flt else ''}")
      for agg in ("SUM", "AVG", "MIN", "MAX")
      for flt in ("", " FILTER (WHERE status = 'completed')")],
])
def test_unsupported_filter_metric_is_unconverted(db: Path, sql: str) -> None:
    assert unconverted(_import(db, m("bad", sql), m("ok", "SUM(amount)")), "bad")


def test_filter_predicate_on_another_dataset_is_unconverted(db: Path) -> None:
    result = _import(db, model_metrics=[m("bad", "SUM(orders.amount) FILTER (WHERE customers.segment = 'b2b')")])
    assert unconverted(result, "bad")


# ─────────────────────────── aggregate operands ────────────────────────────

_INLINE = [
    pytest.param("SUM(quantity * amount)", 41, id="arithmetic"),
    pytest.param("SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END)", 17, id="case"),
    pytest.param("SUM(IFF(amount > 6, amount, 0))", 17, id="iff"),
    pytest.param("COUNT(CASE WHEN NOT (status = 'open') AND (amount > 6 OR region IS NULL) THEN 1 END)", 2,
                 id="and-or-not"),
    pytest.param("SUM(CASE WHEN status <> 'open' THEN amount END)", 17, id="not-equal"),
    pytest.param("SUM(CASE WHEN status = 'open' THEN NULL ELSE amount END)", 17, id="null-literal"),
    pytest.param("SUM(CASE WHEN is_paid = TRUE THEN amount END)", 17, id="boolean-literal"),
    pytest.param("SUM(CASE WHEN NOT is_paid THEN amount END)", 5, id="boolean-column"),
]


@pytest.mark.parametrize(("sql", "expected"), _INLINE)
async def test_inline_operand(db: Path, sql: str, expected: float) -> None:
    result = _import(db, m("v", sql))
    assert hidden(by_name(result)["orders"]) == []
    assert float(await _value(result, db, "v")) == pytest.approx(expected)


@pytest.mark.parametrize(("sql", "expected"), [
    pytest.param("SUM(CAST(quantity AS DOUBLE) / 2)", 3.0, id="cast"),
    pytest.param("COUNT(first_name || last_name)", 2, id="concat-null"),
])
async def test_operand_outside_the_formula_grammar_uses_a_helper(db: Path, sql: str, expected: float) -> None:
    result = _import(db, m("v", sql))
    [helper] = hidden(by_name(result)["orders"])
    formula = next(x.formula for x in by_name(result)["orders"].measures if x.name == "v")
    parsed = parse_expr(formula)
    assert isinstance(parsed, AggCall)
    assert isinstance(parsed.source, Ref)
    assert parsed.source.name == helper.name
    assert float(await _value(result, db, "v")) == pytest.approx(expected)


async def test_cross_dataset_operand(db: Path) -> None:
    result = _import(db, model_metrics=[m("net", "SUM(orders.amount - customers.discount)")])
    assert all(hidden(model) == [] for model in result.models)
    assert float(await _value(result, db, "net")) == pytest.approx(19.5)


async def test_operand_outside_the_formula_language_falls_back_to_a_hidden_column(db: Path) -> None:
    result = _import(db, m("v", "SUM(LENGTH(LPAD(status, 12, 'x')))"))
    [helper] = hidden(by_name(result)["orders"])
    assert helper.type in (DataType.INT, DataType.DOUBLE)  # DEV-2163: tighten to INT once the probe reports integers
    assert await _value(result, db, "v") == 36


def test_cross_dataset_operand_outside_the_formula_language_is_unconverted(db: Path) -> None:
    result = _import(db, model_metrics=[m("bad", "SUM(LENGTH(LPAD(orders.status, 12, customers.segment)))")])
    assert unconverted(result, "bad")
