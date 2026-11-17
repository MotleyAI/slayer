"""The non-nesting ``whole_periods_only`` warning is judged per resolved column, whatever its spelling."""

from __future__ import annotations

from slayer.core.models import DatasetRelationship
from slayer.core.query import SlayerQuery

from tests._time_points_fixtures import ev_model, tp_engine


async def test_two_spellings_of_one_column_warn() -> None:
    model = ev_model(name="ev_buyer", table="ev").model_copy(update={"joins": [
        DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]], name="buyer"),
    ]})
    query = SlayerQuery.model_validate({
        "population": "ev_buyer", "metrics": [{"expression": "count(*)", "name": "n"}],
        "time_dimensions": [
            {"field": "buyer.created_at", "granularity": "week"},
            {"field": "customers.created_at", "granularity": "month"},
        ],
        "whole_periods_only": True,
    })
    async with tp_engine("sqlite") as engine:
        await engine.save_dataset(model)
        resp = await engine.execute(query, dry_run=True)
    [warning] = [w for w in resp.warnings if w.kind == "whole_periods_non_nesting"]
    assert warning.model_dump()["column"] == "buyer.created_at"
    assert warning.model_dump()["granularities"] == ["week", "month"]
