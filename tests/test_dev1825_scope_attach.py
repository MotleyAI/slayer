"""DEV-1825 — ``ScopeFrame.attached_columns`` fail-closed anchor (Codex F4).

A reserved-leaf placeholder resolves by EXACT registry membership first; a
non-prefixed column is unaffected by the registry; a prefixed leaf that misses
the registry RAISES rather than emitting ``orders."__regroup__…"``.
"""

from __future__ import annotations

import pytest
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.keys import ColumnKey
from slayer.core.models import Column, SlayerModel
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.naming import AliasAllocator
from slayer.sql.scope import ScopeFrame


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
        ],
    )


def _scope(*, attached=None) -> ScopeFrame:
    host = _orders()
    alloc = AliasAllocator()
    bundle = ResolvedSourceBundle(source_model=host, referenced_models=[host])
    return ScopeFrame(
        scope_id=alloc.next_scope_id(host.name),
        root_model=host, root_relation=host.name,
        bundle=bundle, dialect=get_dialect("postgres"), allocator=alloc,
        attached_columns=attached or {},
    )


PLACEHOLDER = ColumnKey(path=(), leaf="__regroup__0__amount_sum_partition_by_city")


class TestAttachedColumnAnchor:
    def test_registered_placeholder_resolves_to_registered_expression(self) -> None:
        attached_expr = exp.column("orders.amount_sum_partition_by_city", table="_cm_x")
        scope = _scope(attached={PLACEHOLDER: attached_expr})
        out = scope.resolve(PLACEHOLDER)
        assert "_cm_x" in out.sql(dialect="postgres")

    def test_ordinary_column_unaffected_by_nonempty_registry(self) -> None:
        scope = _scope(attached={PLACEHOLDER: exp.column("x", table="_cm_x")})
        out = scope.resolve(ColumnKey(path=(), leaf="amount"))
        sql = out.sql(dialect="postgres")
        assert "amount" in sql
        assert "__regroup__" not in sql

    def test_unregistered_prefixed_leaf_raises(self) -> None:
        scope = _scope(attached={PLACEHOLDER: exp.column("x", table="_cm_x")})
        missing = ColumnKey(path=(), leaf="__regroup__9__not_registered")
        with pytest.raises(Exception, match=r"__regroup__"):
            scope.resolve(missing)

    def test_prefixed_leaf_without_active_regroup_anchors_normally(self) -> None:
        # Codex F4: with no regroup active (empty registry) a column that only
        # collides with the reserved prefix is NOT a placeholder — it anchors as
        # an ordinary column, never fail-closed. The prefix is fenced at plan
        # time only when a regroup is planned; here nothing is attached.
        scope = _scope()
        ref = ColumnKey(path=(), leaf="__regroup__legacy_col")
        out = scope.resolve(ref)
        assert "__regroup__legacy_col" in out.sql(dialect="postgres")
