"""A deprecation's Python warning carries its structured payload and the same text.

Spec: cli/commands › The CLI prints each deprecation once (A spelling deprecation has one wording).
"""

from __future__ import annotations

import pytest

from slayer.core import deprecation
from slayer.core.legacy_spellings import LegacyKind, LegacySpan, emit_deprecations
from slayer.core.warnings import DeprecatedSpellingWarning
from slayer.engine.query_engine import SlayerQueryEngine
from tests._deprecated_input_helpers import CLEAN, orders_engine, recorded

SPANS: list[tuple[LegacyKind, str, str]] = [
    ("==", "==", "="),
    ("is None", "is None", "IS NULL"),
    ("is not None", "is not None", "IS NOT NULL"),
    ("bare None", "None", "NULL"),
    ("list after IN", "['a', 'b']", "('a', 'b')"),
    ("method-call", "customers.count_distinct(status)", "count_distinct(customers.status)"),
]


@pytest.mark.parametrize(("kind", "written", "twin"), SPANS, ids=[s[0] for s in SPANS])
def test_each_spelling_warns_with_its_payload(kind: LegacyKind, written: str, twin: str) -> None:
    span = LegacySpan(kind=kind, start=0, end=len(written), written=written, twin=twin)
    with recorded() as caught:
        [payload] = emit_deprecations({kind: span})
    [w] = caught
    assert isinstance(w.message, deprecation.DeprecatedSpellingFutureWarning)
    assert isinstance(w.message, deprecation.DeprecationFutureWarning)
    assert isinstance(w.message.payload, DeprecatedSpellingWarning)
    assert w.message.payload == payload
    assert str(w.message) == payload.human_message()
    assert (payload.written, payload.canonical) == (written, twin)


def test_input_carrier_is_a_deprecation_carrier() -> None:
    assert issubclass(deprecation.DeprecatedInputFutureWarning, deprecation.DeprecationFutureWarning)


@pytest.fixture
async def engine():
    async with orders_engine() as e:
        yield e


async def test_a_spelling_deprecation_has_one_wording(engine: SlayerQueryEngine) -> None:
    with recorded() as caught:
        response = await engine.execute({**CLEAN, "filter": ["orders.status == 'paid'"]})
    [payload] = [w for w in response.warnings if isinstance(w, DeprecatedSpellingWarning)]
    futures = [str(w.message) for w in caught if issubclass(w.category, FutureWarning)]
    assert futures == [payload.human_message()]
