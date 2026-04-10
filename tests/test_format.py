"""Tests for slayer.core.format — number formatting."""

import pytest

from slayer.core.format import NumberFormat, NumberFormatType, format_number


class TestNumberFormatType:
    def test_values(self):
        assert NumberFormatType.PERCENT.value == "percent"
        assert NumberFormatType.CURRENCY.value == "currency"
        assert NumberFormatType.INTEGER.value == "integer"
        assert NumberFormatType.FLOAT.value == "float"


class TestNumberFormat:
    def test_default(self):
        fmt = NumberFormat()
        assert fmt.type == NumberFormatType.FLOAT
        assert fmt.precision is None
        assert fmt.symbol is None

    def test_integer(self):
        fmt = NumberFormat(type=NumberFormatType.INTEGER)
        assert fmt.type == NumberFormatType.INTEGER

    def test_currency_default_symbol(self):
        fmt = NumberFormat(type=NumberFormatType.CURRENCY)
        assert fmt.symbol == "$"

    def test_currency_custom_symbol(self):
        fmt = NumberFormat(type=NumberFormatType.CURRENCY, symbol="€")
        assert fmt.symbol == "€"

    def test_non_currency_rejects_symbol(self):
        with pytest.raises(ValueError, match="Currency symbol must be None"):
            NumberFormat(type=NumberFormatType.FLOAT, symbol="$")

    def test_serialization_roundtrip(self):
        fmt = NumberFormat(type=NumberFormatType.CURRENCY, precision=2, symbol="£")
        data = fmt.model_dump(mode="json")
        restored = NumberFormat.model_validate(data)
        assert restored == fmt


class TestFormatNumber:
    # --- INTEGER ---
    def test_integer_basic(self):
        fmt = NumberFormat(type=NumberFormatType.INTEGER)
        assert format_number(value=42, format_spec=fmt) == "42"
        assert format_number(value=0, format_spec=fmt) == "0"
        assert format_number(value=999, format_spec=fmt) == "999"

    def test_integer_truncates_decimals(self):
        fmt = NumberFormat(type=NumberFormatType.INTEGER)
        assert format_number(value=42.7, format_spec=fmt) == "43"

    def test_integer_k_notation(self):
        fmt = NumberFormat(type=NumberFormatType.INTEGER)
        assert format_number(value=10000, format_spec=fmt) == "10K"
        assert format_number(value=15000, format_spec=fmt) == "15K"

    def test_integer_m_notation(self):
        fmt = NumberFormat(type=NumberFormatType.INTEGER)
        assert format_number(value=1000000, format_spec=fmt) == "1M"
        assert format_number(value=2500000, format_spec=fmt) == "2M"

    # --- FLOAT ---
    def test_float_basic(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=42.123, format_spec=fmt) == "42.1"

    def test_float_explicit_precision(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT, precision=2)
        assert format_number(value=42.126, format_spec=fmt) == "42.13"

    def test_float_small_value(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=0.1234, format_spec=fmt) == "0.123"

    def test_float_k_notation(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=12345.6, format_spec=fmt) == "12.3K"

    def test_float_m_notation(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=1234567.0, format_spec=fmt) == "1.23M"

    # --- PERCENT ---
    def test_percent_basic(self):
        fmt = NumberFormat(type=NumberFormatType.PERCENT)
        assert format_number(value=0.1234, format_spec=fmt) == "12%"

    def test_percent_with_precision(self):
        fmt = NumberFormat(type=NumberFormatType.PERCENT, precision=1)
        assert format_number(value=0.1234, format_spec=fmt) == "12.3%"

    # --- CURRENCY ---
    def test_currency_basic(self):
        fmt = NumberFormat(type=NumberFormatType.CURRENCY)
        assert format_number(value=42.50, format_spec=fmt) == "$42.5"

    def test_currency_negative(self):
        fmt = NumberFormat(type=NumberFormatType.CURRENCY)
        result = format_number(value=-42.50, format_spec=fmt)
        assert result == "-$42.5"

    def test_currency_long_symbol(self):
        fmt = NumberFormat(type=NumberFormatType.CURRENCY, symbol="EUR")
        result = format_number(value=1000.0, format_spec=fmt)
        assert result == "1000 EUR"

    def test_currency_k_notation(self):
        fmt = NumberFormat(type=NumberFormatType.CURRENCY)
        result = format_number(value=15000.0, format_spec=fmt)
        assert result == "$15.0K"

    # --- Edge cases ---
    def test_nan(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        result = format_number(value=float("nan"), format_spec=fmt)
        assert result == "nan"

    def test_zero(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=0, format_spec=fmt) == "0.00"

    def test_negative(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=-42.1, format_spec=fmt) == "-42.1"

    def test_non_numeric(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value="hello", format_spec=fmt) == "hello"
