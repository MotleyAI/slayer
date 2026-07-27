"""Tests for slayer.flight.translator — Flight-shim-specific behaviour.

Most translator coverage lives in tests/facade/test_translator.py (the shared
SQL → SlayerQuery pipeline). This file pins behaviours that are specific to
the Flight shim's `_shared_translate(..., allow_column_cast=False)` call:

* DEV-1566 ``CAST(<col> AS <type>)`` projection is rejected at translate
  time (Codex round 1 — Flight has no value-coercion pass and would crash
  inside ``pa.Table.from_pylist`` if the projection were admitted).
* The time-grain ``CAST(DATE_TRUNC(...) AS DATE)`` Metabase fingerprint is
  unaffected — the time-grain unwrap runs before the column-CAST branch.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.facade.catalog import FacadeCatalog, build_catalog
from slayer.flight.translator import QueryResult, TranslationError, translate


def _catalog() -> FacadeCatalog:
    orders = SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="delivered_at", type=DataType.DATE),
            Column(name="is_paid", type=DataType.BOOLEAN),
        ],
    )
    return build_catalog(models_by_datasource={"jaffle": [orders]})


# --- DEV-1566 gate: Flight rejects CAST(<col> AS <type>) projections ---------


@pytest.mark.parametrize(
    ("col", "target"),
    [
        ("delivered_at", "TIMESTAMP"),  # DATE → TIMESTAMP (date can't fill pa.timestamp)
        ("is_paid", "TEXT"),            # BOOLEAN → TEXT  (bool can't fill pa.utf8)
        ("revenue", "TEXT"),            # DOUBLE → TEXT
        ("id", "TEXT"),                 # INT → TEXT
        ("delivered_at", "TEXT"),       # DATE → TEXT
        ("ordered_at", "TEXT"),         # TIMESTAMP → TEXT
    ],
)
def test_flight_rejects_cast_projection(col: str, target: str) -> None:
    """The Flight shim sets allow_column_cast=False so the CAST projection
    branch is skipped and the body falls through to the existing
    'Unsupported projection expression' terminal error. Without this gate
    pa.Table.from_pylist would raise ArrowTypeError at materialisation."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql=f"SELECT CAST({col} AS {target}) FROM orders",
            catalog=_catalog(),
        )
    assert "Unsupported projection expression" in str(exc_info.value)


def test_flight_admits_time_grain_cast_unwrap() -> None:
    """``CAST(DATE_TRUNC(<col>, <grain>) AS DATE)`` is the Metabase time-grain
    fingerprint, NOT a column-CAST projection. The time-grain unwrap runs
    BEFORE the column-CAST branch in _resolve_projection, so the gate must
    not regress it. (This is the pattern Flight clients actually emit.)"""
    result = translate(
        sql=(
            "SELECT CAST(date_trunc('month', ordered_at) AS DATE), revenue_sum "
            "FROM orders"
        ),
        catalog=_catalog(),
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert result.query.time_dimensions[0].granularity.value == "month"
