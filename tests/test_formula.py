"""Tests for the formula parser and unified fields."""

import pytest

from slayer.core.formula import (
    AggregatedMeasureRef,
    ArithmeticField,
    TransformField,
    parse_formula,
)


class TestFormulaParser:
    def test_bare_measure_raises(self) -> None:
        with pytest.raises(ValueError, match="Bare measure name"):
            parse_formula("count")

    def test_bare_measure_in_arithmetic_raises(self) -> None:
        with pytest.raises(ValueError, match="Bare measure name"):
            parse_formula("revenue / count")

    def test_aggregated_measure(self) -> None:
        result = parse_formula("*:count")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "*"
        assert result.aggregation_name == "count"

    def test_aggregated_measure_sum(self) -> None:
        result = parse_formula("revenue:sum")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "revenue"
        assert result.aggregation_name == "sum"

    def test_arithmetic(self) -> None:
        result = parse_formula("revenue:sum / *:count")
        assert isinstance(result, ArithmeticField)

    def test_arithmetic_complex(self) -> None:
        result = parse_formula("(revenue:sum - cost:sum) / *:count")
        assert isinstance(result, ArithmeticField)

    def test_transform_cumsum(self) -> None:
        result = parse_formula("cumsum(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "cumsum"
        assert isinstance(result.inner, AggregatedMeasureRef)
        assert result.inner.measure_name == "revenue"

    def test_time_shift_row_based(self) -> None:
        result = parse_formula("time_shift(revenue:sum, -1)")
        assert isinstance(result, TransformField)
        assert result.transform == "time_shift"
        assert result.args == [-1]

    def test_time_shift_calendar_based(self) -> None:
        result = parse_formula("time_shift(revenue:sum, -1, 'year')")
        assert isinstance(result, TransformField)
        assert result.transform == "time_shift"
        assert result.args == [-1, "year"]

    def test_transform_last(self) -> None:
        result = parse_formula("last(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "last"
        assert isinstance(result.inner, AggregatedMeasureRef)
        assert result.inner.measure_name == "revenue"

    def test_transform_change(self) -> None:
        result = parse_formula("change(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "change"

    def test_nested_transform_with_arithmetic(self) -> None:
        result = parse_formula("cumsum(revenue:sum / *:count)")
        assert isinstance(result, TransformField)
        assert result.transform == "cumsum"
        assert isinstance(result.inner, ArithmeticField)

    def test_rank(self) -> None:
        result = parse_formula("rank(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "rank"

    def test_change_pct(self) -> None:
        result = parse_formula("change_pct(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "change_pct"

    def test_unknown_function_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown transform"):
            parse_formula("unknown_func(revenue)")

    def test_invalid_syntax_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid formula"):
            parse_formula("revenue +")

    def test_no_args_raises(self) -> None:
        with pytest.raises(ValueError, match="requires at least one argument"):
            parse_formula("cumsum()")

    def test_nested_transforms(self) -> None:
        """change(cumsum(revenue:sum)) → TransformField wrapping TransformField."""
        result = parse_formula("change(cumsum(revenue:sum))")
        assert isinstance(result, TransformField)
        assert result.transform == "change"
        assert isinstance(result.inner, TransformField)
        assert result.inner.transform == "cumsum"
        assert isinstance(result.inner.inner, AggregatedMeasureRef)
        assert result.inner.inner.measure_name == "revenue"

    def test_mixed_arithmetic_with_transform(self) -> None:
        """cumsum(revenue:sum) / *:count → MixedArithmeticField."""
        from slayer.core.formula import MixedArithmeticField
        result = parse_formula("cumsum(revenue:sum) / *:count")
        assert isinstance(result, MixedArithmeticField)
        assert len(result.sub_transforms) == 1
        placeholder, transform = result.sub_transforms[0]
        assert isinstance(transform, TransformField)
        assert transform.transform == "cumsum"

    def test_triple_nesting(self) -> None:
        """last(change(cumsum(revenue:sum))) → three levels deep."""
        result = parse_formula("last(change(cumsum(revenue:sum)))")
        assert isinstance(result, TransformField)
        assert result.transform == "last"
        assert isinstance(result.inner, TransformField)
        assert result.inner.transform == "change"
        assert isinstance(result.inner.inner, TransformField)
        assert result.inner.inner.transform == "cumsum"
