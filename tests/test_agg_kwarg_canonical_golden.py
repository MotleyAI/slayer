"""Golden freeze of ``agg_kwarg_canonical_str``.

Only the EMISSION round-trip of ``agg_kwarg_canonical_str`` is deleted
(``generator.py:9055-9057``). The function survives for the one-way
result-key / cross-dataset-alias sites (``planning.py``, ``stage_planner.py``,
``generator.py``), where
``FieldSqlKey`` → bare-name collapse is *correct*. These goldens freeze its
output for every kwarg variant so removing the emission use cannot silently
shift a result key. Green now; must stay green through the refactor.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.keys import AggregateKey, FieldKey, FieldSqlKey, Grain
from slayer.core.refs import agg_kwarg_canonical_str


def test_columnkey_local() -> None:
    assert agg_kwarg_canonical_str(FieldKey(leaf="net")) == "net"


def test_columnkey_joined() -> None:
    assert agg_kwarg_canonical_str(
        FieldKey(path=("customers",), leaf="net")) == "customers.net"


def test_columnkey_multi_hop() -> None:
    assert agg_kwarg_canonical_str(
        FieldKey(path=("customers", "regions"), leaf="net")
    ) == "customers.regions.net"


def test_columnsqlkey_local() -> None:
    assert agg_kwarg_canonical_str(
        FieldSqlKey(dataset="orders", field_name="net")) == "net"


def test_columnsqlkey_joined() -> None:
    assert agg_kwarg_canonical_str(
        FieldSqlKey(path=("customers",), dataset="customers", field_name="net")
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


def test_aggregate_key_carries_source_kwargs_and_partition() -> None:
    key = AggregateKey(
        source=FieldKey(leaf="w"), agg="sum",
        partition_keys=Grain.of([FieldKey(leaf="city")]),
    )
    frag = agg_kwarg_canonical_str(key)
    assert frag.startswith("sum_w")
    assert "by_city" in frag


def test_aggregate_key_positional_args_disambiguate() -> None:
    # Positional args must enter the fragment, else nested aggs alias-collide.
    first_ts1 = AggregateKey(
        source=FieldKey(leaf="x"), agg="first", args=(FieldKey(leaf="ts1"),),
    )
    first_ts2 = AggregateKey(
        source=FieldKey(leaf="x"), agg="first", args=(FieldKey(leaf="ts2"),),
    )
    assert "ts1" in agg_kwarg_canonical_str(first_ts1)
    assert agg_kwarg_canonical_str(first_ts1) != agg_kwarg_canonical_str(first_ts2)
