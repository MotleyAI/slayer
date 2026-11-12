"""A cast's inferred result type (spec: queries/casts › A cast's result type is inferred)."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import CastKey, ValueKey, walk_value_keys
from slayer.core.scope import ModelScope
from slayer.core.syntax import parse_expr
from slayer.engine.binding import bind_expr
from slayer.engine.key_metadata import dimension_key_metadata, measure_key_type
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev2101_fixtures import customers_model, orders_model

ORDERS = orders_model()
BUNDLE = ResolvedSourceBundle(dialect="postgres", source_model=ORDERS, referenced_models=[customers_model()])

TYPE_CASES = [
    ("INT", DataType.INT), ("INTEGER", DataType.INT), ("BIGINT", DataType.INT), ("SMALLINT", DataType.INT),
    ("TINYINT", DataType.INT),
    ("FLOAT", DataType.DOUBLE), ("DOUBLE", DataType.DOUBLE), ("DOUBLE PRECISION", DataType.DOUBLE),
    ("REAL", DataType.DOUBLE), ("DECIMAL(10,2)", DataType.DOUBLE), ("NUMERIC", DataType.DOUBLE),
    ("TEXT", DataType.TEXT), ("VARCHAR(3)", DataType.TEXT), ("CHAR(2)", DataType.TEXT),
    ("BOOLEAN", DataType.BOOLEAN),
    ("DATE", DataType.DATE),
    ("TIMESTAMP", DataType.TIMESTAMP), ("DATETIME", DataType.TIMESTAMP),
    ("JSON", DataType.UNKNOWN), ("UUID", DataType.UNKNOWN), ("BYTEA", DataType.UNKNOWN),
]


def _bind(text: str) -> ValueKey:
    return bind_expr(parse_expr(text), scope=ModelScope(source_model=ORDERS), bundle=BUNDLE,
                     allow_measures=True).value_key


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
        dim_type, _fmt, _desc = dimension_key_metadata(model=ORDERS, key=_bind(f"CAST(code AS {target})"),
                                                       bundle=BUNDLE)
        assert dim_type == expected

    @pytest.mark.parametrize(("target", "expected"), [c for c in TYPE_CASES if c[1] is not DataType.UNKNOWN],
                             ids=[t for t, e in TYPE_CASES if e is not DataType.UNKNOWN])
    def test_measure_type(self, target: str, expected: DataType) -> None:
        assert measure_key_type(model=ORDERS, key=_bind(f"max(CAST(code AS {target}))")) == expected

    def test_try_cast_types_like_cast(self) -> None:
        dim_type, _fmt, _desc = dimension_key_metadata(model=ORDERS, key=_bind("TRY_CAST(code AS DATE)"),
                                                       bundle=BUNDLE)
        assert dim_type == DataType.DATE

    def test_cast_to_date_feeds_a_date_function(self) -> None:
        key = _bind("date_diff('day', CAST(created_txt AS DATE), DATE '2025-01-01')")
        dim_type, _fmt, _desc = dimension_key_metadata(model=ORDERS, key=key, bundle=BUNDLE)
        assert dim_type == DataType.INT
