"""A time-point comparison the checker did not resolve fails closed at compilation."""

from __future__ import annotations

import pytest

import slayer.engine.bind_inputs as bind_inputs
from slayer.core.query import SlayerQuery

from tests._time_points_fixtures import tp_engine


async def test_unresolved_time_point_is_refused(monkeypatch) -> None:
    monkeypatch.setattr(bind_inputs, "resolve_time_points", lambda vk, **_: vk)
    query = SlayerQuery.model_validate({
        "source_model": "ev", "measures": [{"formula": "count(*)", "name": "n"}],
        "filters": ["ts >= 'last month'"],
    })
    async with tp_engine("sqlite") as engine:
        with pytest.raises(RuntimeError, match="unresolved time-point"):
            await engine.execute(query, dry_run=True)
