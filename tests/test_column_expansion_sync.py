"""Unit tests for the synchronous derived-column expander
(``expand_derived_refs_sync``) used by the DEV-1450 planned-query generator.

The async ``expand_derived_refs`` (legacy enrichment path) is covered by
``test_cross_model_derived_columns.py``. These tests pin the sync twin's
behavior directly against an in-memory ``name -> SlayerModel`` resolver,
mirroring how the generator drives it via ``bundle.get_referenced_model``.
"""
from typing import Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ColumnCycleError
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.column_expansion import expand_derived_refs_sync


def _norm(sql: str) -> str:
    return " ".join(sql.split())


def _model_a() -> SlayerModel:
    return SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="bar", sql="bar", type=DataType.DOUBLE),
            Column(name="b_id", sql="b_id", type=DataType.INT),
            Column(name="raw_a", sql="raw_a", type=DataType.DOUBLE),
            # Local derived chain: c2 references sibling derived c1.
            Column(name="c1", sql="raw_a + 1", type=DataType.DOUBLE),
            Column(name="c2", sql="A.c1 * 2", type=DataType.DOUBLE),
            # Cross-model derived ref into B's derived column.
            Column(name="ratio", sql="A.bar / B.foo_normalized", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )


def _model_b() -> SlayerModel:
    return SlayerModel(
        name="B",
        data_source="ds",
        sql_table="B",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="foo_raw", sql="foo_raw", type=DataType.DOUBLE),
            Column(name="foo_normalized", sql="foo_raw / 100.0", type=DataType.DOUBLE),
        ],
    )


def _resolver(models: dict[str, SlayerModel]):
    def _get(name: str) -> Optional[SlayerModel]:
        return models.get(name)

    return _get


def test_bare_base_column_returns_name() -> None:
    a = _model_a()
    out = expand_derived_refs_sync(
        sql=None, model=a, alias_path="A",
        resolve_model=_resolver({"A": a}), dialect="sqlite",
    )
    assert out is None


def test_local_derived_chain_inlines_sibling() -> None:
    """``c2 = A.c1 * 2`` with ``c1 = raw_a + 1`` inlines c1 parenthesised and
    qualifies the bare ``raw_a`` to the host alias."""
    a = _model_a()
    out = expand_derived_refs_sync(
        sql="A.c1 * 2", model=a, alias_path="A",
        resolve_model=_resolver({"A": a}), dialect="sqlite",
    )
    assert _norm(out) == "(A.raw_a + 1) * 2"


def test_cross_table_derived_ref_inlines_joined_derived() -> None:
    """``A.bar / B.foo_normalized`` inlines B's derived ``foo_normalized``
    (qualified to the single-hop alias ``B``)."""
    a, b = _model_a(), _model_b()
    out = expand_derived_refs_sync(
        sql="A.bar / B.foo_normalized", model=a, alias_path="A",
        resolve_model=_resolver({"A": a, "B": b}), dialect="sqlite",
    )
    assert _norm(out) == "A.bar / (B.foo_raw / 100.0)"


def test_bare_local_ref_qualifies_to_alias() -> None:
    a = _model_a()
    out = expand_derived_refs_sync(
        sql="raw_a + 1", model=a, alias_path="A",
        resolve_model=_resolver({"A": a}), dialect="sqlite",
    )
    assert _norm(out) == "A.raw_a + 1"


def test_cycle_raises_column_cycle_error() -> None:
    cyclic = SlayerModel(
        name="C",
        data_source="ds",
        sql_table="C",
        columns=[
            Column(name="x", sql="x", type=DataType.DOUBLE),
            Column(name="c1", sql="c2 + 1", type=DataType.DOUBLE),
            Column(name="c2", sql="c1 - 1", type=DataType.DOUBLE),
        ],
    )
    with pytest.raises(ColumnCycleError):
        expand_derived_refs_sync(
            sql="c1", model=cyclic, alias_path="C",
            resolve_model=_resolver({"C": cyclic}), dialect="sqlite",
        )


def test_unknown_alias_left_untouched() -> None:
    """A ``<alias>.<col>`` whose alias is not a join target is left alone
    (likely a CTE / subquery alias the user wired up)."""
    a = _model_a()
    out = expand_derived_refs_sync(
        sql="cte_x.value + A.bar", model=a, alias_path="A",
        resolve_model=_resolver({"A": a}), dialect="sqlite",
    )
    # cte_x.value untouched; A.bar stays qualified to the host alias.
    assert "cte_x.value" in _norm(out)
    assert "A.bar" in _norm(out)
