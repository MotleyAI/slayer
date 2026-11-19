"""The expression-dimension query surface: ``{"field", "alias"}`` entries, and a string that is not a bare
identifier or dotted path parsed as an expression. Construction-time only."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slayer.core.query import QueryDimension, SlayerQuery


def _dims(dimensions):
    return SlayerQuery.model_validate({
        "population": "orders",
        "dimensions": dimensions,
        "metrics": [{"metric": "sum(orders.amount)"}],
    }).dimensions


class TestDictForm:
    def test_expression_and_name_preserved(self) -> None:
        (dim,) = _dims([{"field": "round(amount)", "alias": "r"}])
        assert isinstance(dim, QueryDimension)
        assert dim.ref is None
        assert dim.field == "round(amount)"
        assert dim.alias == "r"

    def test_extra_key_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            _dims([{"field": "round(amount)", "alias": "r", "bogus": 1}])

    def test_explicit_name_wins_over_autoname(self) -> None:
        (dim,) = _dims([{"field": "lower(city)", "alias": "chosen"}])
        assert dim.alias == "chosen"


class TestWidenedString:
    def test_non_identifier_string_becomes_expression(self) -> None:
        (dim,) = _dims(["round(amount)"])
        assert dim.ref is None
        assert dim.field == "round(amount)"

    def test_case_expression_string(self) -> None:
        (dim,) = _dims(["CASE WHEN amount > 5000 THEN 1 ELSE 0 END"])
        assert dim.ref is None


class TestBareIdentifierPathUnchanged:
    def test_bare_identifier_stays_columnref(self) -> None:
        (dim,) = _dims(["region"])
        assert dim.ref is not None
        assert dim.ref.name == "region"

    def test_dotted_path_stays_columnref(self) -> None:
        (dim,) = _dims(["customers.tier"])
        assert dim.ref is not None
        assert dim.ref.dataset == "customers"
        assert dim.ref.name == "tier"

    def test_order_of_mixed_dims_preserved(self) -> None:
        dims = _dims(["region", {"field": "lower(city)", "alias": "lc"}, "channel"])
        assert [(d.field, d.ref is None) for d in dims] == [
            ("region", False), ("lower(city)", True), ("channel", False),
        ]


class TestAutoName:
    def test_unnamed_gets_a_valid_identifier(self) -> None:
        (dim,) = _dims(["round(amount)"])
        assert dim.name
        assert dim.name.replace("_", "a").isalnum()

    def test_distinct_expressions_get_distinct_names(self) -> None:
        a, b = _dims(["round(amount)", "lower(city)"])
        assert a.name != b.name

    def test_autoname_is_deterministic(self) -> None:
        n1 = _dims(["round(amount)"])[0].name
        n2 = _dims(["round(amount)"])[0].name
        assert n1 == n2

    def test_long_expressions_get_distinct_names_after_truncation(self) -> None:
        # Two long expressions that could collide under a naïve length-capped
        # sanitizer must still get distinct, deterministic identifiers.
        a = "CASE WHEN lower(concat(city, channel, region)) = 'parisweeeu' THEN 1 ELSE 0 END"
        b = "CASE WHEN lower(concat(city, channel, region)) = 'berlinapeu' THEN 1 ELSE 0 END"
        d1, d2 = _dims([{"field": a}, {"field": b}])
        assert d1.name
        assert d2.name
        assert d1.name != d2.name
        # Deterministic across constructions.
        assert _dims([{"field": a}])[0].name == d1.name


class TestNeitherRefNorExpression:
    def test_unparseable_string_names_both_readings(self) -> None:
        with pytest.raises(ValidationError) as ei:
            _dims(["!!! not valid @@@"])
        msg = str(ei.value).lower()
        assert "column" in msg or "reference" in msg
        assert "expression" in msg
