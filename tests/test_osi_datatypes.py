"""OSI ``datatype`` on fields and metrics, and the ``is_time`` time-dimension role."""

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from tests._osi_helpers import (
    by_name, cols, convert, disable_type_probe, documents, ds, f, flat, m, mentions, query, seed_db,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


@pytest.fixture
def failing_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    disable_type_probe(monkeypatch)


def _orders(*fields: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return flat([ds("orders", fields=[f("order_id", "o_orderkey"), *fields], **extra)])


# ─────────────────────────── field datatypes ───────────────────────────────

@pytest.mark.parametrize(("datatype", "expected"), [
    ("String", DataType.TEXT), ("Integer", DataType.INT), ("Decimal", DataType.DOUBLE),
    ("Float", DataType.DOUBLE), ("Boolean", DataType.BOOLEAN), ("Date", DataType.DATE),
    ("DateTime", DataType.TIMESTAMP), ("DateTimeTz", DataType.TIMESTAMP), ("Time", DataType.UNKNOWN),
    ("Opaque", DataType.UNKNOWN),
])
@pytest.mark.usefixtures("failing_probe")
def test_declared_type_used_when_the_database_cannot_report_one(db: Path, datatype: str, expected: DataType) -> None:
    result = convert(db=db, docs=documents(_orders(f("x", "UPPER(status)", datatype=datatype))))
    x = cols(by_name(result)["orders"])["x"]
    assert x.type == expected
    assert (x.meta or {})["osi_datatype"] == datatype


@pytest.mark.usefixtures("failing_probe")
def test_double_when_neither_database_nor_declared_type(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("x", "UPPER(status)"))))
    assert cols(by_name(result)["orders"])["x"].type == DataType.DOUBLE


def test_database_type_wins_and_disagreement_is_reported(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("qty", "quantity", datatype="String"))))
    qty = cols(by_name(result)["orders"])["qty"]
    assert qty.type == DataType.INT
    assert (qty.meta or {})["osi_datatype"] == "String"
    assert mentions(result, "qty", severity="info")


def test_probed_type_wins_over_declared_type(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("up", "UPPER(status)", datatype="Integer"))))
    assert cols(by_name(result)["orders"])["up"].type == DataType.TEXT
    assert mentions(result, "up", severity="info")


def test_agreeing_types_are_not_reported(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("qty", "quantity", datatype="Integer"))))
    assert cols(by_name(result)["orders"])["qty"].type == DataType.INT
    assert not mentions(result, "qty")


# ─────────────────────────── time-dimension role ───────────────────────────

def test_role_defaulted_from_datatype(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("ordered_on", datatype="Date"))))
    assert by_name(result)["orders"].default_time_dimension == "ordered_on"


def test_explicit_opt_out(db: Path) -> None:
    field = f("ordered_at", datatype="DateTime", dimension={"is_time": False})
    result = convert(db=db, docs=documents(_orders(field)))
    orders = by_name(result)["orders"]
    assert cols(orders)["ordered_at"].type == DataType.TIMESTAMP
    assert orders.default_time_dimension is None


def test_explicit_role_on_a_temporal_column(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("ordered_at", dimension={"is_time": True}))))
    assert by_name(result)["orders"].default_time_dimension == "ordered_at"


@pytest.mark.parametrize("field", [
    pytest.param(f("order_year", datatype="Integer", dimension={"is_time": True}), id="integer-year"),
    pytest.param(f("status", dimension={"is_time": True}), id="text-role"),
])
def test_role_on_a_non_temporal_column_keeps_its_type(db: Path, field: dict[str, Any]) -> None:
    result = convert(db=db, docs=documents(_orders(field)))
    orders = by_name(result)["orders"]
    column = cols(orders)[field["name"]]
    assert column.type == (DataType.INT if field["name"] == "order_year" else DataType.TEXT)
    assert (column.meta or {})["osi_is_time"] is True
    assert orders.default_time_dimension is None


@pytest.mark.usefixtures("failing_probe")
def test_time_of_day_is_not_the_default_time_dimension(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("at_time", "UPPER(status)", datatype="Time"))))
    orders = by_name(result)["orders"]
    at_time = cols(orders)["at_time"]
    assert at_time.type == DataType.UNKNOWN
    assert (at_time.meta or {})["osi_is_time"] is True
    assert orders.default_time_dimension is None


# ─────────────────────────── metric datatypes ──────────────────────────────

async def test_metric_datatype_is_metadata_only(db: Path) -> None:
    metrics = [m("typed", "SUM(amount)", datatype="Decimal"), m("untyped", "SUM(amount)")]
    result = convert(db=db, docs=documents(_orders(f("amount", "o_totalprice"), metrics=metrics)))
    measures = {x.name: x for x in by_name(result)["orders"].measures}
    assert (measures["typed"].meta or {})["osi_datatype"] == "Decimal"
    assert measures["typed"].type is None
    rows = await query(result=result, db=db, payload={"source_model": "orders", "measures": ["typed", "untyped"]})
    assert rows[0]["orders.typed"] == rows[0]["orders.untyped"]
