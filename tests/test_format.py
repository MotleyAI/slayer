"""Tests for slayer.core.format — number formatting."""

import math
from decimal import Decimal

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

    def test_infinity(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=float("inf"), format_spec=fmt) == "inf"
        assert format_number(value=float("-inf"), format_spec=fmt) == "-inf"

    def test_zero(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=0, format_spec=fmt) == "0.00"

    def test_negative(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=-42.1, format_spec=fmt) == "-42.1"

    def test_non_numeric(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value="hello", format_spec=fmt) == "hello"

    # --- Negative precision ---
    def test_negative_precision_rejected(self):
        with pytest.raises(ValueError):
            NumberFormat(type=NumberFormatType.FLOAT, precision=-1)

    # --- decimal.Decimal support ---
    def test_decimal_float(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=Decimal("42.123"), format_spec=fmt) == "42.1"

    def test_decimal_currency(self):
        fmt = NumberFormat(type=NumberFormatType.CURRENCY)
        assert format_number(value=Decimal("1500.50"), format_spec=fmt) == "$1500"

    @pytest.mark.parametrize("fmt_type", list(NumberFormatType))
    def test_decimal_m_notation_matches_float(self, fmt_type):
        fmt = NumberFormat(type=fmt_type)
        expected = format_number(value=2500000.0, format_spec=fmt)
        assert expected.rstrip("%").endswith("M")
        assert format_number(value=Decimal("2500000"), format_spec=fmt) == expected

    @pytest.mark.parametrize(("fmt_type", "expected"), [
        (NumberFormatType.FLOAT, "1" + "0" * 309 + "M"),
        (NumberFormatType.INTEGER, "1" + "0" * 309 + "M"),
        (NumberFormatType.CURRENCY, "$1" + "0" * 309 + "M"),
        (NumberFormatType.PERCENT, "1" + "0" * 311 + "M%"),
    ])
    def test_decimal_beyond_float_range(self, fmt_type, expected):
        fmt = NumberFormat(type=fmt_type)
        assert format_number(value=Decimal("1e315"), format_spec=fmt) == expected

    def test_float_percent_overflow(self):
        fmt = NumberFormat(type=NumberFormatType.PERCENT)
        assert format_number(value=1.7e308, format_spec=fmt) == "inf%"

    @pytest.mark.parametrize(("value", "expected"), [("9e999999", "Infinity%"), ("-9e999999", "-Infinity%")])
    def test_decimal_percent_overflow(self, value, expected):
        fmt = NumberFormat(type=NumberFormatType.PERCENT)
        assert format_number(value=Decimal(value), format_spec=fmt) == expected

    @pytest.mark.parametrize("fmt_type", list(NumberFormatType))
    @pytest.mark.parametrize("text", [
        "0", "0.00", "0.5", "0.0012", "1", "9.99", "10", "999.9999999999", "1000", "9999.5", "123456.789",
    ])
    def test_decimal_digit_counts_match_float(self, fmt_type, text):
        fmt = NumberFormat(type=fmt_type)
        assert format_number(value=Decimal(text), format_spec=fmt) == format_number(value=float(text), format_spec=fmt)

    @pytest.mark.parametrize("power", [10.0, 100.0, 1000.0])
    def test_float_just_below_power_of_ten_matches_decimal(self, power):
        below = math.nextafter(power, 0.0)
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=below, format_spec=fmt) == format_number(value=Decimal(below), format_spec=fmt)

    @pytest.mark.parametrize("fmt_type", list(NumberFormatType))
    def test_numpy_int64_min_matches_python_int(self, fmt_type):
        np = pytest.importorskip("numpy")
        fmt = NumberFormat(type=fmt_type)
        expected = format_number(value=-(2**63), format_spec=fmt)
        assert format_number(value=np.int64(-(2**63)), format_spec=fmt) == expected

    @pytest.mark.parametrize("fmt_type", list(NumberFormatType))
    @pytest.mark.parametrize("text", ["inf", "-inf", "nan"])
    def test_numpy_non_finite_float_renders_verbatim(self, fmt_type, text):
        np = pytest.importorskip("numpy")
        value = np.float32(text)
        assert format_number(value=value, format_spec=NumberFormat(type=fmt_type)) == str(value)

    def test_huge_int_is_finite(self):
        fmt = NumberFormat(type=NumberFormatType.INTEGER)
        assert format_number(value=10**400, format_spec=fmt) == "1" + "0" * 394 + "M"

    def test_decimal_integer(self):
        fmt = NumberFormat(type=NumberFormatType.INTEGER)
        assert format_number(value=Decimal("42"), format_spec=fmt) == "42"

    def test_decimal_nan(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        result = format_number(value=Decimal("NaN"), format_spec=fmt)
        assert result == "NaN"

    def test_decimal_infinity(self):
        fmt = NumberFormat(type=NumberFormatType.FLOAT)
        assert format_number(value=Decimal("Infinity"), format_spec=fmt) == "Infinity"
        assert format_number(value=Decimal("-Infinity"), format_spec=fmt) == "-Infinity"
