"""Tests for the formula parser and unified fields."""

import pytest

from slayer.core.formula import (
    ArithmeticField,
    MeasureRef,
    TransformField,
    parse_formula,
)


class TestFormulaParser:
    def test_bare_measure(self) -> None:
        result = parse_formula("count")
        assert isinstance(result, MeasureRef)
        assert result.name == "count"

    def test_arithmetic(self) -> None:
        result = parse_formula("revenue / count")
        assert isinstance(result, ArithmeticField)
        assert "revenue" in result.measure_names
        assert "count" in result.measure_names

    def test_arithmetic_complex(self) -> None:
        result = parse_formula("(revenue - cost) / count")
        assert isinstance(result, ArithmeticField)
        assert set(result.measure_names) == {"revenue", "cost", "count"}

    def test_transform_cumsum(self) -> None:
        result = parse_formula("cumsum(revenue)")
        assert isinstance(result, TransformField)
        assert result.transform == "cumsum"
        assert isinstance(result.inner, MeasureRef)
        assert result.inner.name == "revenue"

    def test_transform_lag(self) -> None:
        result = parse_formula("lag(revenue, 2)")
        assert isinstance(result, TransformField)
        assert result.transform == "lag"
        assert result.args == [2]

    def test_transform_time_shift(self) -> None:
        result = parse_formula("time_shift(revenue, -1, 'year')")
        assert isinstance(result, TransformField)
        assert result.transform == "time_shift"
        assert result.args == [-1, "year"]

    def test_transform_last(self) -> None:
        result = parse_formula("last(revenue)")
        assert isinstance(result, TransformField)
        assert result.transform == "last"
        assert isinstance(result.inner, MeasureRef)
        assert result.inner.name == "revenue"

    def test_transform_change(self) -> None:
        result = parse_formula("change(revenue)")
        assert isinstance(result, TransformField)
        assert result.transform == "change"

    def test_nested_transform_with_arithmetic(self) -> None:
        result = parse_formula("cumsum(revenue / count)")
        assert isinstance(result, TransformField)
        assert result.transform == "cumsum"
        assert isinstance(result.inner, ArithmeticField)
        assert "revenue" in result.inner.measure_names

    def test_rank(self) -> None:
        result = parse_formula("rank(revenue)")
        assert isinstance(result, TransformField)
        assert result.transform == "rank"

    def test_change_pct(self) -> None:
        result = parse_formula("change_pct(revenue)")
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
        """change(cumsum(revenue)) → TransformField wrapping TransformField."""
        result = parse_formula("change(cumsum(revenue))")
        assert isinstance(result, TransformField)
        assert result.transform == "change"
        assert isinstance(result.inner, TransformField)
        assert result.inner.transform == "cumsum"
        assert isinstance(result.inner.inner, MeasureRef)
        assert result.inner.inner.name == "revenue"

    def test_mixed_arithmetic_with_transform(self) -> None:
        """cumsum(revenue) / count → MixedArithmeticField."""
        from slayer.core.formula import MixedArithmeticField
        result = parse_formula("cumsum(revenue) / count")
        assert isinstance(result, MixedArithmeticField)
        assert "count" in result.measure_names
        assert len(result.sub_transforms) == 1
        placeholder, transform = result.sub_transforms[0]
        assert isinstance(transform, TransformField)
        assert transform.transform == "cumsum"

    def test_triple_nesting(self) -> None:
        """last(change(cumsum(revenue))) → three levels deep."""
        result = parse_formula("last(change(cumsum(revenue)))")
        assert isinstance(result, TransformField)
        assert result.transform == "last"
        assert isinstance(result.inner, TransformField)
        assert result.inner.transform == "change"
        assert isinstance(result.inner.inner, TransformField)
        assert result.inner.inner.transform == "cumsum"
