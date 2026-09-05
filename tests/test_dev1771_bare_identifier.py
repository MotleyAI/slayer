"""DEV-1771: schema_drift._is_bare_identifier unified on the canonical IDENTIFIER_RE.

The flip rejects non-ASCII *leading* identifiers (the old char-loop accepted
them). Non-ASCII in non-leading positions, all ASCII behavior, and whitespace
normalization are unchanged. The residual drift false-negative for a bare
non-ASCII-leading ``Column.sql`` alias is documented in the DEV-1771 decision trail (git history).
"""
from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.engine.schema_drift import (
    LiveTable,
    _CascadeState,
    _column_is_base,
    _diff_sql_table_columns,
    _first_dropped_sql_column_ref,
    _is_bare_identifier,
)


# --- Group 1: the helper directly ------------------------------------------

@pytest.mark.parametrize("value", ["год", "émile", "über", "Ω"])
def test_non_ascii_leading_rejected(value):
    # Flipped by DEV-1771: the old char-loop accepted these.
    assert _is_bare_identifier(value) is False


@pytest.mark.parametrize(
    "value",
    ["amount", "_x", "col1", "__dunder__", "café", "  amount  ", "\tamount\n"],
)
def test_bare_identifiers_accepted(value):
    # ASCII, non-ASCII *non-leading* ('café'), and surrounding whitespace unchanged.
    assert _is_bare_identifier(value) is True


@pytest.mark.parametrize("value", ["1col", "a b", "amount * 2", "", "   ", None])
def test_non_identifiers_rejected(value):
    assert _is_bare_identifier(value) is False


# --- Group 2: the immediate caller -----------------------------------------

def test_column_is_base_sql_none():
    assert _column_is_base(None) is True


def test_column_is_base_ascii_identifier():
    assert _column_is_base("amount") is True


def test_column_is_base_non_ascii_leading_now_derived():
    # Flipped: a bare non-ASCII-leading Column.sql is no longer a base column.
    assert _column_is_base("год") is False


# --- Group 3: end-to-end drift diff (the shipped false-negative) -----------

def test_diff_skips_non_ascii_leading_alias():
    """A base column aliasing a non-ASCII-leading physical name is no longer
    diffed against the live schema (DEV-1771 residual change); an ASCII alias
    still is."""
    model = SlayerModel(
        name="orders",
        data_source="db",
        sql_table="orders",
        columns=[
            Column(name="year", sql="год", type=DataType.INT),
            Column(name="year_ascii", sql="yr_phys", type=DataType.INT),
        ],
    )
    live = LiveTable(columns={"amount": DataType.INT})  # both physical names absent
    dropped, _reasons = _diff_sql_table_columns(model=model, live_table=live)
    assert dropped == ["year_ascii"]


def test_first_dropped_ref_scans_non_ascii_leading_alias():
    """The other call site: a bare non-ASCII-leading ``Column.sql`` is now
    scanned for dropped references instead of short-circuiting as base."""
    model = SlayerModel(
        name="orders",
        data_source="db",
        sql_table="orders",
        columns=[Column(name="year", sql="год", type=DataType.INT)],
    )
    state = _CascadeState(
        models_by_name={"orders": model},
        edit_entries={},
        whole_entries={},
        dropped_cols={"orders": {"год"}},
        dropped_measures={},
        dropped_joins={},
        pk_per_model={},
    )
    result = _first_dropped_sql_column_ref(
        col=model.columns[0], model=model, state=state
    )
    assert result == (model, "год")
