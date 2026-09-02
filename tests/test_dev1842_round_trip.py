"""DEV-1842 task 1.6 — round-trip expansions are rejected.

A saved measure whose re-anchored path crosses back to a model already on the
host→target chain (``customers.order_total`` = ``orders.amount:sum`` from an
``orders`` query) errors naming the measure and the revisited model.
"""

from __future__ import annotations

import pytest

from tests._dev1842_fixtures import gen, q


async def test_round_trip_errors_naming_measure_and_revisited_model() -> None:
    query = q(measures=[{"formula": "customers.order_total", "name": "x"}])
    with pytest.raises(ValueError) as ei:
        await gen(query)
    message = str(ei.value)
    assert "order_total" in message
    assert "orders" in message
    low = message.lower()
    assert "circular" in low or "round" in low or "revisit" in low or "again" in low


async def test_round_trip_does_not_emit_a_value() -> None:
    """The failure is loud: no SQL is produced for the round-trip spelling."""
    query = q(
        dimensions=["customers.tier"],
        measures=[{"formula": "customers.order_total", "name": "x"}],
    )
    with pytest.raises(ValueError):
        await gen(query)
