"""DEV-1706 Stage 2 — golden freeze of ``agg_kwarg_canonical_str`` (Codex H2 / D-H).

Stage 2 deletes only the EMISSION round-trip of ``agg_kwarg_canonical_str``
(``generator.py:9055-9057``). The function survives for the one-way
result-key / cross-model-alias sites (``planning.py``, ``stage_planner.py``,
``generator.py``), where
``ColumnSqlKey`` → bare-name collapse is *correct*. These goldens freeze its
output for every kwarg variant so removing the emission use cannot silently
shift a result key. Green now; must stay green through the refactor.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.keys import ColumnKey, ColumnSqlKey
from slayer.core.refs import agg_kwarg_canonical_str


def test_columnkey_local() -> None:
    assert agg_kwarg_canonical_str(ColumnKey(leaf="net")) == "net"


def test_columnkey_joined() -> None:
    assert agg_kwarg_canonical_str(
        ColumnKey(path=("customers",), leaf="net")) == "customers.net"


def test_columnkey_multi_hop() -> None:
    assert agg_kwarg_canonical_str(
        ColumnKey(path=("customers", "regions"), leaf="net")
    ) == "customers.regions.net"


def test_columnsqlkey_local() -> None:
    assert agg_kwarg_canonical_str(
        ColumnSqlKey(model="orders", column_name="net")) == "net"


def test_columnsqlkey_joined() -> None:
    assert agg_kwarg_canonical_str(
        ColumnSqlKey(path=("customers",), model="customers", column_name="net")
    ) == "customers.net"


def test_decimal_plain_notation() -> None:
    assert agg_kwarg_canonical_str(Decimal("0.95")) == "0.95"


def test_int() -> None:
    assert agg_kwarg_canonical_str(3) == "3"


def test_str_passthrough() -> None:
    assert agg_kwarg_canonical_str("quantity") == "quantity"


def test_bool_raises() -> None:
    with pytest.raises(TypeError):
        agg_kwarg_canonical_str(True)


def test_none_raises() -> None:
    with pytest.raises(TypeError):
        agg_kwarg_canonical_str(None)
