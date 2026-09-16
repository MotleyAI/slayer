"""DEV-1871 — time-transform traversal reaches an InKey nested in scalar-call args."""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.keys import (
    ColumnKey,
    InKey,
    LiteralKey,
    ScalarCallKey,
    TimeTruncKey,
    TransformKey,
)
from slayer.engine.bind_inputs import _attach_time_keys
from slayer.engine.elaborate_env import check_time_transforms_resolved

_TD = TimeTruncKey(column=ColumnKey(leaf="ordered_at"), granularity="month")


def _in_scalar_call() -> ScalarCallKey:
    axisless = TransformKey(op="cumsum", input=ColumnKey(leaf="amount"))
    membership = InKey(column=axisless, values=(LiteralKey(value=Decimal("1")),))
    return ScalarCallKey(
        name="iif",
        args=(
            membership,
            LiteralKey(value=Decimal("1")),
            LiteralKey(value=Decimal("0")),
        ),
    )


class TestInKeyTimeTraversal:
    def test_attach_reaches_the_in_lhs(self) -> None:
        out = _attach_time_keys(_in_scalar_call(), td_key=_TD)
        assert isinstance(out, ScalarCallKey)
        membership = out.args[0]
        assert isinstance(membership, InKey)
        assert isinstance(membership.column, TransformKey)
        assert membership.column.time_key == _TD

    def test_checker_detects_the_unresolved_op(self) -> None:
        roots = [_in_scalar_call()]
        with pytest.raises(ValueError, match="cumsum"):
            check_time_transforms_resolved(roots=roots)

    def test_checker_passes_once_attached(self) -> None:
        attached = _attach_time_keys(_in_scalar_call(), td_key=_TD)
        check_time_transforms_resolved(roots=[attached])
