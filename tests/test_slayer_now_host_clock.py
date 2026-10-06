"""``host_clock()`` interprets ``SLAYER_NOW``: a naive pin, midnight for a date, the wall clock when blank."""

from __future__ import annotations

from datetime import datetime

import pytest

from slayer.core.errors import SlayerError
from slayer.core.time_points import host_clock


@pytest.mark.parametrize(("value", "expected"), [
    ("2025-07-15T12:00:00", datetime(2025, 7, 15, 12)),
    ("2025-07-15 12:00:00.250", datetime(2025, 7, 15, 12, 0, 0, 250000)),
    ("  2025-07-15T12:00:00  ", datetime(2025, 7, 15, 12)),
    ("2025-07-15", datetime(2025, 7, 15)),
], ids=["datetime", "space-fractional", "padded", "date-only"])
def test_pinned_value(monkeypatch, value, expected) -> None:
    monkeypatch.setenv("SLAYER_NOW", value)
    clock = host_clock()
    assert clock() == expected
    assert clock() == expected


@pytest.mark.parametrize("value", [None, "", "   "], ids=["unset", "empty", "whitespace"])
def test_blank_is_the_host_wall_clock(monkeypatch, value) -> None:
    if value is None:
        monkeypatch.delenv("SLAYER_NOW", raising=False)
    else:
        monkeypatch.setenv("SLAYER_NOW", value)
    clock = host_clock()
    before = datetime.now()
    reading = clock()
    after = datetime.now()
    assert reading.tzinfo is None
    assert before <= reading <= after


@pytest.mark.parametrize("value", ["2025-07-15T12:00:00Z", "2025-07-15T12:00:00+02:00"], ids=["Z", "offset"])
def test_timezone_aware_is_rejected(monkeypatch, value) -> None:
    monkeypatch.setenv("SLAYER_NOW", value)
    with pytest.raises(SlayerError) as exc:
        host_clock()
    msg = str(exc.value)
    assert "SLAYER_NOW" in msg
    assert value in msg
    assert "naive" in msg.lower()


def test_unparseable_is_rejected(monkeypatch) -> None:
    monkeypatch.setenv("SLAYER_NOW", "yesterday-ish")
    with pytest.raises(SlayerError) as exc:
        host_clock()
    assert "SLAYER_NOW" in str(exc.value)
    assert "yesterday-ish" in str(exc.value)
