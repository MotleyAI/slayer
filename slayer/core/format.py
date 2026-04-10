"""Number formatting for SLayer query results.

Ported from Storyline's storyline/domain/format.py.
"""

import math
import numbers
from enum import Enum
from typing import Optional

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
    precision: Optional[int] = Field(
        default=None,
        description="Number of decimal places to show",
    )
    symbol: Optional[str] = Field(
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
    value: float,
    default_precision: int,
    explicit_precision: Optional[int] = None,
    max_precision: Optional[int] = None,
) -> tuple[float, str, int]:
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
    if abs(value) >= 1e6:
        formatted_value = value / 1e6
        suffix = "M"
    elif abs(value) >= 10000:
        formatted_value = value / 1000
        suffix = "K"
    else:
        formatted_value = value
        suffix = ""

    # Calculate dynamic precision if not specified
    if explicit_precision is None:
        abs_value = abs(formatted_value)
        if abs_value == 0:
            digits = 1
        elif abs_value >= 1:
            digits = math.floor(math.log10(abs_value)) + 1
        else:
            digits = 0

        precision = max(0, default_precision - digits)
        if max_precision is not None:
            precision = min(precision, max_precision)
    else:
        precision = explicit_precision

    return formatted_value, suffix, precision


def format_number(value: float, format_spec: NumberFormat) -> str:
    """Format number with type-specific rules (currency/percent/integer/float).

    Args:
        value: The numeric value to format
        format_spec: NumberFormat specifying how to format

    Returns:
        Formatted string representation of the value
    """
    # Check if value is numeric (includes numpy types via numbers.Real)
    if not isinstance(value, numbers.Real):
        return str(value)

    # Check for NaN after confirming it's numeric
    if isinstance(value, float) and math.isnan(value):
        return str(value)

    format_type = format_spec.type
    precision = format_spec.precision
    symbol = format_spec.symbol

    if format_type == NumberFormatType.CURRENCY:
        currency_symbol = symbol or "$"
        is_negative = value < 0
        abs_value = abs(value)
        formatted_value, suffix, calc_precision = _format_with_notation(
            abs_value, default_precision=3, explicit_precision=precision, max_precision=2
        )
        formatted_str = f"{formatted_value:.{calc_precision}f}{suffix}"

        # Currency symbol positioning: short symbols before, long after
        if len(currency_symbol) == 1:
            result = currency_symbol + formatted_str
        else:
            result = formatted_str + " " + currency_symbol

        return "-" + result if is_negative else result

    elif format_type == NumberFormatType.PERCENT:
        percent_value = value * 100
        formatted_value, suffix, calc_precision = _format_with_notation(
            percent_value, default_precision=2, explicit_precision=precision
        )
        return f"{formatted_value:.{calc_precision}f}{suffix}%"

    elif format_type == NumberFormatType.INTEGER:
        formatted_value, suffix, calc_precision = _format_with_notation(
            value, default_precision=0, explicit_precision=0, max_precision=0
        )
        return f"{formatted_value:.{calc_precision}f}{suffix}"

    else:  # FLOAT or fallback
        formatted_value, suffix, calc_precision = _format_with_notation(
            value, default_precision=3, explicit_precision=precision
        )
        return f"{formatted_value:.{calc_precision}f}{suffix}"
