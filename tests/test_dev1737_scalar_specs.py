"""One canonical scalar spec table: derived allowlist / arity / pass-through sets, legacy parser, OSI, reserved names."""

from __future__ import annotations

import pydantic
import pytest

from slayer.core import formula as legacy_formula
from slayer.core import keys
from slayer.core.formula import MixedArithmeticField, parse_formula
from slayer.core.keys import (
    SCALAR_FUNCTION_ARITY,
    SCALAR_FUNCTIONS,
    SCALAR_PASSTHROUGH,
    SCALAR_SPECS,
    ScalarSpec,
)
from slayer.core.models import Aggregation, SlayerModel
from slayer.osi import expression as osi_expression
from slayer.osi.expression import convert_expression

DATE_NAMES = {"date_part", "date_diff", "date_add", "interval", "current_date", "now"}


class TestSpecTable:
    def test_allowlist_is_the_table(self) -> None:
        assert SCALAR_FUNCTIONS == frozenset(SCALAR_SPECS)

    def test_arity_is_derived(self) -> None:
        assert SCALAR_FUNCTION_ARITY == {
            name: (spec.min_args, spec.max_args) for name, spec in SCALAR_SPECS.items()
        }

    def test_passthrough_is_derived(self) -> None:
        assert SCALAR_PASSTHROUGH == frozenset(
            name for name, spec in SCALAR_SPECS.items() if spec.sql_passthrough
        )

    @pytest.mark.parametrize("name", sorted(DATE_NAMES | {"like", "iif"}))
    def test_non_passthrough_names(self, name: str) -> None:
        assert SCALAR_SPECS[name].sql_passthrough is False
        assert name not in SCALAR_PASSTHROUGH

    def test_specs_are_frozen(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            SCALAR_SPECS["now"].min_args = 1  # type: ignore[misc]

    def test_spec_is_a_pydantic_model(self) -> None:
        assert all(isinstance(s, ScalarSpec) for s in SCALAR_SPECS.values())
        assert issubclass(ScalarSpec, pydantic.BaseModel)

    def test_one_canonical_passthrough_set(self) -> None:
        assert getattr(legacy_formula, "SCALAR_PASSTHROUGH", keys.SCALAR_PASSTHROUGH) is keys.SCALAR_PASSTHROUGH
        assert osi_expression.SCALAR_PASSTHROUGH is keys.SCALAR_PASSTHROUGH


class TestLegacyFormulaParser:
    @pytest.mark.parametrize("text", [
        "date_diff('day', created_at:min, created_at:max)",
        "revenue:sum / date_part('day', created_at:max)",
        "date_part('year', date_add(created_at:max, 1, 'month'))",
        "date_diff('day', created_at:max, now())",
        "DATE_DIFF('day', created_at:min, current_date())",
    ])
    def test_recognises_date_functions(self, text: str) -> None:
        assert isinstance(parse_formula(text), MixedArithmeticField)

    def test_unknown_call_lists_date_functions(self) -> None:
        with pytest.raises(ValueError, match="date_diff"):
            parse_formula("bogus_fn(revenue:sum)")


class TestOsiDoesNotCarryDateFunctions:
    @pytest.mark.parametrize("expr", [
        "DATE_ADD(MAX(created_at), INTERVAL 1 DAY)",
        "DATEDIFF(MAX(created_at), MIN(created_at))",
        "DATE_DIFF(MAX(created_at), MIN(created_at))",
        "EXTRACT(YEAR FROM MAX(created_at))",
        "SUM(amount) / NOW()",
    ])
    def test_clean_fails(self, expr: str) -> None:
        result = convert_expression(
            expr, entity_name="m", owner_of=lambda q, c: "orders", ref_of=lambda m, c: c,
        )
        assert not result.ok, result.formula


class TestReservedAggregationNames:
    @pytest.mark.parametrize("name", [
        "date_part", "date_diff", "DATE_DIFF", "date_add", "interval", "Interval",
        "current_date", "now", "NOW",
    ])
    def test_rejected(self, name: str) -> None:
        with pytest.raises(pydantic.ValidationError, match="scalar"):
            Aggregation(name=name, formula="SUM({value})")

    def test_model_with_shadowing_aggregation_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError, match="date_diff"):
            SlayerModel.model_validate({
                "name": "orders", "sql_table": "orders", "data_source": "test",
                "columns": [{"name": "amount", "type": "DOUBLE"}],
                "aggregations": [{"name": "date_diff", "formula": "SUM({value})"}],
            })
