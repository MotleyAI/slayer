"""DEV-1842 task 1.6 — round-trip expansions are rejected.

``customers.order_total`` is saved as ``orders.amount:sum`` (via the declared
reverse join). Referenced from an ``orders``-rooted query it re-anchors to a join
path that crosses back to ``orders`` — the host already on the host→target
chain. That is rejected with an error naming the saved measure and the revisited
model, rather than emitting a wrong / double-counted value.

Today the dotted reference raises ``UnknownReferenceError`` (no measure
fall-through), so this fails for the right reason.
"""

from __future__ import annotations

import pytest

from tests._dev1842_fixtures import gen, q


async def test_round_trip_errors_naming_measure_and_revisited_model() -> None:
    with pytest.raises(ValueError) as ei:
        await gen(q(measures=[{"formula": "customers.order_total", "name": "x"}]))
    message = str(ei.value)
    assert "order_total" in message
    assert "orders" in message
    # It is rejected as circular / a revisit — not a plain unknown column.
    low = message.lower()
    assert "circular" in low or "round" in low or "revisit" in low or "again" in low


async def test_round_trip_does_not_emit_a_value() -> None:
    """The failure is loud: no SQL is produced for the round-trip spelling."""
    with pytest.raises(ValueError):
        await gen(q(dimensions=["customers.tier"],
                    measures=[{"formula": "customers.order_total", "name": "x"}]))
