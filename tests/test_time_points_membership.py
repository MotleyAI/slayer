"""A single-string ``in`` whose string is not a time point fails with the typed time-literal error."""

from __future__ import annotations

import pytest

from slayer.core.errors import TimeLiteralError

from tests._time_points_fixtures import ids, tp_engine


@pytest.mark.parametrize("filt", ["ts in 'last fortnight'", "ts not in '2025/01'"])
async def test_non_time_point_membership_lists_the_forms(filt) -> None:
    async with tp_engine("sqlite") as engine:
        with pytest.raises(TimeLiteralError) as ei:
            await ids(engine, filt)
    for form in ("YYYY-Qn", "YYYY-MM", "YYYY-Www", "last N"):
        assert form in str(ei.value)
