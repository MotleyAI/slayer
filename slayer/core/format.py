"""Number formatting and the result-value codec."""

import datetime as dt
import decimal
import json
import math
import numbers
import uuid
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


# --------------------------------------------------------------------------- #
# The result-value codec: one JSON form and one text form per value.
# --------------------------------------------------------------------------- #
def _duration(value: dt.timedelta) -> str:
    """ISO-8601 duration; a negative interval is ``-`` + its magnitude's duration."""
    sign = "-" if value < dt.timedelta(0) else ""
    value = abs(value)
    hours, rest = divmod(value.seconds, 3600)
    minutes, seconds = divmod(rest, 60)
    secs = f"{seconds}.{value.microseconds:06d}".rstrip("0") if value.microseconds else str(seconds)
    time = "".join(f"{n}{u}" for n, u in ((hours, "H"), (minutes, "M")) if n)
    if seconds or value.microseconds:
        time += f"{secs}S"
    days = f"{value.days}D" if value.days else ""
    if not days and not time:
        time = "0S"
    return f"{sign}P{days}" + (f"T{time}" if time else "")


def _scalar_string(value: object) -> str | None:
    """The shared string spelling of a UUID, bytes, interval or temporal value; ``None`` for other types."""
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(value).hex()
    if isinstance(value, dt.timedelta):
        return _duration(value)
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    return None


def _json_number(value: numbers.Number | decimal.Decimal) -> str:
    if isinstance(value, decimal.Decimal):
        return str(value) if value.is_finite() else "null"
    if isinstance(value, numbers.Integral):
        return str(int(value))
    as_float = float(value)  # pyright: ignore[reportArgumentType]
    return repr(as_float) if math.isfinite(as_float) else "null"


def _json_key(key: object) -> str:
    return key if isinstance(key, str) else encode_text(key)


def _write_json(value: object, *, indent: int | None, depth: int) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (numbers.Number, decimal.Decimal)):
        return _json_number(value)
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    if isinstance(value, dict):
        items = [(json.dumps(_json_key(k), ensure_ascii=False), v) for k, v in value.items()]
        return _join([f"{k}: {_write_json(v, indent=indent, depth=depth + 1)}" for k, v in items],
                     brackets="{}", indent=indent, depth=depth)
    if isinstance(value, (list, tuple, set, frozenset)):
        return _join([_write_json(v, indent=indent, depth=depth + 1) for v in value],
                     brackets="[]", indent=indent, depth=depth)
    text = _scalar_string(value)
    return json.dumps(str(value) if text is None else text, ensure_ascii=False)


def _join(parts: list[str], *, brackets: str, indent: int | None, depth: int) -> str:
    if not parts:
        return brackets
    if indent is None:
        return brackets[0] + ", ".join(parts) + brackets[1]
    pad = "\n" + " " * (indent * (depth + 1))
    return brackets[0] + pad + ("," + pad).join(parts) + "\n" + " " * (indent * depth) + brackets[1]


def encode_json(value: object, *, indent: int | None = None) -> str:
    """The JSON form of a result value, or of a payload holding result values; always valid JSON."""
    return _write_json(value, indent=indent, depth=0)


def encode_text(value: object) -> str:
    """The text form of one result cell; NULL is the empty string."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple, dict)):
        return encode_json(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (float, decimal.Decimal)) and _is_non_finite(value):
        if value.is_nan() if isinstance(value, decimal.Decimal) else math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ")
    text = _scalar_string(value)
    return str(value) if text is None else text
