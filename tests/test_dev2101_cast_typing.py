"""A cast's inferred result type (spec: queries/casts › A cast's result type is inferred)."""

from __future__ import annotations

from typing import Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import SlayerDataset
from slayer.core.keys import CastKey, ValueKey, walk_value_keys
from slayer.core.scope import DatasetScope
from slayer.core.syntax import parse_expr
from slayer.engine.binding import bind_expr
from slayer.engine.key_metadata import SlotTyping
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev2101_fixtures import customers_dataset, orders_dataset

ORDERS = orders_dataset()
BUNDLE = ResolvedSourceBundle(dialect="postgres", population=ORDERS, referenced_datasets=[customers_dataset()])


def _key_type(*, dataset: SlayerDataset, key: ValueKey, bundle: Optional[ResolvedSourceBundle] = None) -> Optional[DataType]:
    """The one derivation's type of ``key`` over ``dataset``."""
    bundle = bundle or ResolvedSourceBundle(dialect="postgres", population=dataset)
    return SlotTyping(scope=DatasetScope(population=dataset), bundle=bundle).value_type(key)

TYPE_CASES = [
    ("INT", DataType.INTEGER), ("INTEGER", DataType.INTEGER), ("BIGINT", DataType.INTEGER), ("SMALLINT", DataType.INTEGER),
    ("TINYINT", DataType.INTEGER),
    ("FLOAT", DataType.FLOAT), ("DOUBLE", DataType.FLOAT), ("DOUBLE PRECISION", DataType.FLOAT),
    ("REAL", DataType.FLOAT), ("DECIMAL(10,2)", DataType.FLOAT), ("NUMERIC", DataType.FLOAT),
    ("TEXT", DataType.STRING), ("VARCHAR(3)", DataType.STRING), ("CHAR(2)", DataType.STRING),
    ("BOOLEAN", DataType.BOOLEAN),
    ("DATE", DataType.DATE),
    ("TIMESTAMP", DataType.DATETIME), ("DATETIME", DataType.DATETIME),
    ("JSON", DataType.OPAQUE), ("UUID", DataType.OPAQUE), ("BYTEA", DataType.OPAQUE),
]


def _bind(text: str) -> ValueKey:
    return bind_expr(parse_expr(text), scope=DatasetScope(population=ORDERS), bundle=BUNDLE,
                     allow_metrics=True).value_key


class TestCastKey:
    @pytest.mark.parametrize("text", ["CAST(amount AS INT)", "amount::int", "TRY_CAST(code AS INT)"])
    def test_cast_binds_to_its_own_key_kind(self, text: str) -> None:
        key = _bind(text)
        assert isinstance(key, CastKey), key
        assert key.try_ is text.startswith("TRY_")

    def test_spellings_share_one_key(self) -> None:
        assert _bind("CAST(amount AS INT)") == _bind("amount::int")

    def test_try_cast_is_a_different_value(self) -> None:
        assert _bind("TRY_CAST(code AS INT)") != _bind("CAST(code AS INT)")

    def test_traversal_reaches_the_source(self) -> None:
        keys = list(walk_value_keys(_bind("amount::int::text")))
        assert sum(isinstance(k, CastKey) for k in keys) == 2
        assert any(getattr(k, "leaf", None) == "amount" for k in keys)


class TestInferredType:
    @pytest.mark.parametrize(("target", "expected"), TYPE_CASES, ids=[t for t, _ in TYPE_CASES])
    def test_dimension_type(self, target: str, expected: DataType) -> None:
        dim_type = _key_type(dataset=ORDERS, key=_bind(f"CAST(code AS {target})"),
                                                       bundle=BUNDLE)
        assert dim_type == expected

    @pytest.mark.parametrize(("target", "expected"), [c for c in TYPE_CASES if c[1] is not DataType.OPAQUE],
                             ids=[t for t, e in TYPE_CASES if e is not DataType.OPAQUE])
    def test_measure_type(self, target: str, expected: DataType) -> None:
        assert _key_type(dataset=ORDERS, key=_bind(f"max(CAST(code AS {target}))")) == expected

    def test_try_cast_types_like_cast(self) -> None:
        dim_type = _key_type(dataset=ORDERS, key=_bind("TRY_CAST(code AS DATE)"),
                                                       bundle=BUNDLE)
        assert dim_type == DataType.DATE

    def test_cast_to_date_feeds_a_date_function(self) -> None:
        key = _bind("date_diff('day', CAST(created_txt AS DATE), DATE '2025-01-01')")
        dim_type = _key_type(dataset=ORDERS, key=key, bundle=BUNDLE)
        assert dim_type == DataType.INTEGER
