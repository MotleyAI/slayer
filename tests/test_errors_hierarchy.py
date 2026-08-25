"""Pin the ValueError inheritance contract for binding-time errors.

DEV-1484 Stage C fix-up: the typed-pipeline migration (DEV-1452 Stage A/B)
introduced dedicated ``SlayerError`` subclasses for binding-time failures
that the legacy enrichment pipeline raised as bare ``ValueError``. Callers —
notably the REST ``/query`` endpoint, which maps ``ValueError -> HTTP 400`` —
rely on these being ``ValueError`` to surface user-correctable failures as
400s rather than 500s. This test guards that contract so a future error
class can't silently drop the ``ValueError`` base.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import (
    AggregationNotAllowedError,
    AmbiguousReferenceError,
    CanonicalAliasShadowsColumnError,
    ColumnCycleError,
    DuplicateMeasureNameError,
    IllegalScopeReferenceError,
    IllegalWindowInFilterError,
    MeasureCycleError,
    MeasureNameCollidesWithColumnError,
    MeasureRecursionLimitError,
    SlayerError,
    UnknownFunctionError,
    UnknownReferenceError,
)

# Every binding-/resolution-time error a user can trigger by submitting a
# malformed query or formula. All must be ``ValueError`` so the REST 400
# mapping and ``except ValueError`` callers keep catching them.
_VALUE_ERROR_BINDING_ERRORS = [
    AggregationNotAllowedError,
    AmbiguousReferenceError,
    CanonicalAliasShadowsColumnError,
    ColumnCycleError,
    DuplicateMeasureNameError,
    IllegalScopeReferenceError,
    IllegalWindowInFilterError,
    MeasureCycleError,
    MeasureNameCollidesWithColumnError,
    MeasureRecursionLimitError,
    UnknownFunctionError,
    UnknownReferenceError,
]


@pytest.mark.parametrize(argnames="cls", argvalues=_VALUE_ERROR_BINDING_ERRORS)
def test_binding_error_is_value_error(cls: type) -> None:
    assert issubclass(cls, ValueError), (
        f"{cls.__name__} must subclass ValueError so the REST 'ValueError -> 400' "
        f"mapping and 'except ValueError' callers keep catching it"
    )


@pytest.mark.parametrize(argnames="cls", argvalues=_VALUE_ERROR_BINDING_ERRORS)
def test_binding_error_is_slayer_error(cls: type) -> None:
    assert issubclass(cls, SlayerError)
