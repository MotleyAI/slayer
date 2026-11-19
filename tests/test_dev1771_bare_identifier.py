"""schema_drift._is_bare_identifier unified on the canonical IDENTIFIER_RE.

The flip rejects non-ASCII *leading* identifiers (the old char-loop accepted
them). Non-ASCII in non-leading positions, all ASCII behavior, and whitespace
normalization are unchanged. The residual drift false-negative for a bare
non-ASCII-leading ``DatasetField.sql`` alias is documented in the git history.
"""
from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.live_schema.schema_drift import (
    LiveTable,
    _CascadeState,
    _field_is_base,
    _diff_sql_table_columns,
    _first_dropped_sql_column_ref,
    _is_bare_identifier,
)


# --- Group 1: the helper directly ------------------------------------------

@pytest.mark.parametrize("value", ["год", "émile", "über", "Ω"])
def test_non_ascii_leading_rejected(value):
    # Flipped: the old char-loop accepted these.
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
    assert _field_is_base(None) is True


def test_column_is_base_ascii_identifier():
    assert _field_is_base("amount") is True


def test_column_is_base_non_ascii_leading_now_derived():
    # Flipped: a bare non-ASCII-leading DatasetField.sql is no longer a base field.
    assert _field_is_base("год") is False


# --- Group 3: end-to-end drift diff (the shipped false-negative) -----------

def test_diff_skips_non_ascii_leading_alias():
    """A base field aliasing a non-ASCII-leading physical name is no longer
    diffed against the live schema; an ASCII alias
    still is."""
    model = SlayerDataset(
        name="orders",
        data_source="db",
        sql_table="orders",
        fields=[
            DatasetField(name="year", sql="год", datatype=DataType.INTEGER),
            DatasetField(name="year_ascii", sql="yr_phys", datatype=DataType.INTEGER),
        ],
    )
    live = LiveTable(columns={"amount": DataType.INTEGER})  # both physical names absent
    dropped, _reasons = _diff_sql_table_columns(dataset=model, live_table=live)
    assert dropped == ["year_ascii"]


def test_first_dropped_ref_scans_non_ascii_leading_alias():
    """The other call site: a bare non-ASCII-leading ``DatasetField.sql`` is now
    scanned for dropped references instead of short-circuiting as base."""
    model = SlayerDataset(
        name="orders",
        data_source="db",
        sql_table="orders",
        fields=[DatasetField(name="year", sql="год", datatype=DataType.INTEGER)],
    )
    state = _CascadeState(
        datasets_by_name={"orders": model},
        edit_entries={},
        whole_entries={},
        dropped_cols={"orders": {"год"}},
        dropped_metrics={},
        dropped_relationships={},
        pk_per_dataset={},
    )
    result = _first_dropped_sql_column_ref(
        col=model.fields[0], dataset=model, state=state
    )
    assert result == (model, "год")
