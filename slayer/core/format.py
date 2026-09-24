"""Number formatting for SLayer query results."""

import decimal
import math
import numbers
from enum import Enum

from pydantic import BaseModel, Field, model_validator


class NumberFormatType(str, Enum):
    """Format types for number display."""

    PERCENT = "percent"
    CURRENCY = "currency"
    INTEGER = "integer"
    FLOAT = "float"

    def __str__(self) -> str:
        return self.value


class NumberFormat(BaseModel):
    """Number format specification for measures and dimensions."""

    type: NumberFormatType = Field(
        default=NumberFormatType.FLOAT,
        description="The format type for number display",
    )
    precision: int | None = Field(
        default=None,
        ge=0,
        description="Number of decimal places to show",
    )
    symbol: str | None = Field(
        default=None,
        description="Currency symbol (defaults to $ for CURRENCY type, must be None otherwise)",
    )

    @model_validator(mode="after")
    def validate_symbol(self) -> "NumberFormat":
        """Validate symbol: default to $ for CURRENCY type, forbidden otherwise."""
        if self.type == NumberFormatType.CURRENCY and self.symbol is None:
            self.symbol = "$"
        if self.type != NumberFormatType.CURRENCY and self.symbol is not None:
            raise ValueError("Currency symbol must be None for non-CURRENCY types")
        return self


def _format_with_notation(
    value: float | decimal.Decimal,
    default_precision: int,
    explicit_precision: int | None = None,
    max_precision: int | None = None,
) -> tuple[float | decimal.Decimal, str, int]:
    """Core formatting logic with K/M notation and dynamic precision calculation.

    Args:
        value: The numeric value to format
        default_precision: Default number of significant figures to aim for
        explicit_precision: If provided, use this precision instead of calculating
        max_precision: Maximum precision to use when calculating dynamically

    Returns:
        Tuple of (scaled_value, suffix, precision_to_use)
    """
    # Determine suffix and scale value
    # Integer divisors: ``Decimal / float`` raises.
    if abs(value) >= 1_000_000:
        formatted_value = value / 1_000_000
        suffix = "M"
    elif abs(value) >= 10000:
        formatted_value = value / 1000
        suffix = "K"
    else:
        formatted_value = value
        suffix = ""

    # Calculate dynamic precision if not specified
    if explicit_precision is None:
        precision = max(0, default_precision - _integer_digits(abs(formatted_value)))
        if max_precision is not None:
            precision = min(precision, max_precision)
    else:
        precision = explicit_precision

    return formatted_value, suffix, precision


def _integer_digits(abs_value: float | decimal.Decimal) -> int:
    """Digits before the decimal point: 1 for zero, 0 below one; exact for Decimals beyond float range."""
    if abs_value == 0:
        return 1
    if abs_value < 1:
        return 0
    if isinstance(abs_value, decimal.Decimal):
        return abs_value.adjusted() + 1
    # ``log10`` rounds near powers of ten; exact int comparisons correct it.
    digits = math.floor(math.log10(abs_value)) + 1
    if 10 ** (digits - 1) > abs_value:
        return digits - 1
    if 10 ** digits <= abs_value:
        return digits + 1
    return digits


def _times_hundred(value: float | decimal.Decimal) -> float | decimal.Decimal:
    """``value * 100``; a Decimal past the context's ``Emax`` becomes ±Infinity, as a float does."""
    try:
        return value * 100
    except decimal.Overflow:
        return decimal.Decimal("Infinity") if value > 0 else decimal.Decimal("-Infinity")


def _is_non_finite(value: float | decimal.Decimal) -> bool:
    if isinstance(value, decimal.Decimal):
        return not value.is_finite()
    if isinstance(value, numbers.Integral):
        return False
    return not math.isfinite(value)


def _format_currency(*, value: float | decimal.Decimal, precision: int | None, symbol: str) -> str:
    formatted_value, suffix, calc_precision = _format_with_notation(
        value=abs(value), default_precision=3, explicit_precision=precision, max_precision=2
    )
    formatted_str = f"{formatted_value:.{calc_precision}f}{suffix}"
    # Short symbols lead, long ones trail.
    result = symbol + formatted_str if len(symbol) == 1 else formatted_str + " " + symbol
    return "-" + result if value < 0 else result


def format_number(value: float | decimal.Decimal, format_spec: NumberFormat) -> str:
    """Format number with type-specific rules (currency/percent/integer/float).

    Args:
        value: The numeric value to format
        format_spec: NumberFormat specifying how to format

    Returns:
        Formatted string representation of the value
    """
    # numbers.Real covers numpy scalars; NaN / ±Infinity render verbatim.
    if not isinstance(value, (numbers.Real, decimal.Decimal)) or _is_non_finite(value):
        return str(value)
    if isinstance(value, numbers.Integral):
        value = int(value)  # fixed-width (numpy) ints wrap on abs()
        if value.bit_length() > 1023:  # int / int overflows float
            value = decimal.Decimal(value)

    format_type = format_spec.type
    precision = format_spec.precision

    if format_type == NumberFormatType.CURRENCY:
        return _format_currency(value=value, precision=precision, symbol=format_spec.symbol or "$")

    elif format_type == NumberFormatType.PERCENT:
        percent_value = _times_hundred(value)
        if _is_non_finite(percent_value):  # overflow
            return f"{percent_value}%"
        formatted_value, suffix, calc_precision = _format_with_notation(
            value=percent_value, default_precision=2, explicit_precision=precision
        )
        return f"{formatted_value:.{calc_precision}f}{suffix}%"

    elif format_type == NumberFormatType.INTEGER:
        formatted_value, suffix, calc_precision = _format_with_notation(
            value=value, default_precision=0, explicit_precision=0, max_precision=0
        )
        return f"{formatted_value:.{calc_precision}f}{suffix}"

    else:  # FLOAT or fallback
        formatted_value, suffix, calc_precision = _format_with_notation(
            value=value, default_precision=3, explicit_precision=precision
        )
        return f"{formatted_value:.{calc_precision}f}{suffix}"
