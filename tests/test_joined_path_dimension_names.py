"""A computed dimension named over a bare joined path keys by its name.

Specs: ``queries/computed-dimensions`` "A named computed dimension keys by its
name" and ``queries/multi-stage`` "Stage errors name the stage".

Values from the ``tests/_dev1739_fixtures.py`` dataset: amount by customer
1/2/3 = 90/70/50; by ``customers.region_id`` 1/2 = 160/50 (RegN/RegS).
"""

from __future__ import annotations

from typing import Any, List

import pytest
from sqlglot import exp

from slayer.core.errors import NameCollisionError
from slayer.core.models import SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.response_meta import expected_columns_from_sql, projection_result_keys
from slayer.sql.dialects import get_dialect
from slayer.sql.stage_wrapper import build_flat_rename_wrapper
from tests._dev1739_fixtures import make_exec_engine

REVENUE = {"formula": "sum(amount)", "name": "revenue"}
REGION_ID = {"name": "region_id", "expression": "customers.region_id"}
REGION_NAME = {"name": "region_name", "expression": "customers.regions.name"}
RID = {"name": "rid", "expression": "customers.region_id"}
UNNAMED_PATH = {"expression": "customers.region_id"}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _stage(*, dimensions: list, measures: list | None = None, **kwargs: Any) -> dict:
    return {
        "name": "cr", "source_model": "orders", "dimensions": dimensions,
        "measures": measures if measures is not None else [REVENUE], **kwargs,
    }


def _outer(*, dimensions: list, measures: list | None = None, **kwargs: Any) -> dict:
    return {"source_model": "cr", "dimensions": dimensions, "measures": measures or [], **kwargs}


def _rows(resp, *keys: str) -> List[tuple]:
    return sorted(
        tuple(float(r[k]) if isinstance(r[k], float) else r[k] for k in keys)
        for r in resp.data
    )


SINGLE_NAMED_ONE_HOP = {"source_model": "orders", "dimensions": [REGION_ID], "measures": [REVENUE]}
STAGE_NAMED_ONE_HOP = [
    _stage(dimensions=[REGION_ID, "customer_id"]),
    _outer(dimensions=["region_id", "customer_id", "revenue"]),
]
STAGE_NAMED_TWO_HOP = [
    _stage(dimensions=[REGION_NAME]),
    _outer(dimensions=["region_name", "revenue"]),
]
STAGE_DOWNSTREAM_AGG = [
    _stage(dimensions=[RID, "customer_id"]),
    _outer(dimensions=["rid"], measures=[{"formula": "sum(revenue)", "name": "tot"}]),
]
SINGLE_UNNAMED_PATH = {"source_model": "orders", "dimensions": [UNNAMED_PATH], "measures": [REVENUE]}
STAGE_UNNAMED_PATH = [
    _stage(dimensions=[UNNAMED_PATH]),
    _outer(dimensions=["customers__region_id", "revenue"]),
]
SINGLE_PLAIN_AND_NAMED = {
    "source_model": "orders",
    "dimensions": ["customers.region_id", "status", RID],
    "measures": [REVENUE],
}
STAGE_PLAIN_AND_NAMED = [
    _stage(dimensions=["customers.region_id", "status", RID]),
    _outer(dimensions=["customers__region_id", "status", "rid", "revenue"]),
]
SINGLE_ORDER_BY_NAME = {
    **SINGLE_NAMED_ONE_HOP, "dimensions": [RID],
    "order": [{"column": "rid", "direction": "desc"}],
}
SINGLE_FILTER_BY_NAME = {**SINGLE_NAMED_ONE_HOP, "dimensions": [RID], "filters": ["rid = 1"]}
STAGE_PLAIN_PATH = [
    _stage(dimensions=["customers.region_id"]),
    _outer(dimensions=["customers__region_id", "revenue"]),
]
STAGE_LOCAL_RENAME = [
    _stage(dimensions=[{"name": "cust", "expression": "customer_id"}]),
    _outer(dimensions=["cust", "revenue"]),
]
QUERY_BACKED_OUTER = {"source_model": "cr", "dimensions": ["region_id", "revenue"]}


async def _save_query_backed_cr(engine: SlayerQueryEngine) -> None:
    await engine.save_model(SlayerModel.model_validate({
        "name": "cr",
        "source_queries": [{"source_model": "orders", "dimensions": [REGION_ID], "measures": [REVENUE]}],
    }))


class TestNamedJoinedPathSingleQuery:
    async def test_named_one_hop_keys_by_name(self, exec_engine) -> None:
        resp = await exec_engine.execute(SINGLE_NAMED_ONE_HOP)
        assert resp.columns == ["orders.region_id", "orders.revenue"]
        assert _rows(resp, "orders.region_id", "orders.revenue") == [(1, 160.0), (2, 50.0)]
        attribute_keys = set(resp.attributes.dimensions) | set(resp.attributes.measures)
        assert attribute_keys <= set(resp.columns)
        assert "orders.region_id" in resp.attributes.dimensions

    async def test_order_by_name(self, exec_engine) -> None:
        resp = await exec_engine.execute(SINGLE_ORDER_BY_NAME)
        assert resp.columns == ["orders.rid", "orders.revenue"]
        got = [(r["orders.rid"], float(r["orders.revenue"])) for r in resp.data]
        assert got == [(2, 50.0), (1, 160.0)]

    async def test_filter_by_name(self, exec_engine) -> None:
        resp = await exec_engine.execute(SINGLE_FILTER_BY_NAME)
        assert resp.columns == ["orders.rid", "orders.revenue"]
        assert _rows(resp, "orders.rid", "orders.revenue") == [(1, 160.0)]

    async def test_name_colliding_with_host_column_rejected(self, exec_engine) -> None:
        with pytest.raises(NameCollisionError, match="computed dimension name collides"):
            await exec_engine.execute({
                "source_model": "orders",
                "dimensions": [{"name": "region", "expression": "customers.regions.name"}],
                "measures": [REVENUE],
            })


class TestNamedJoinedPathCrossesStage:
    async def test_named_one_hop(self, exec_engine) -> None:
        resp = await exec_engine.execute(STAGE_NAMED_ONE_HOP)
        assert _rows(resp, "cr.region_id", "cr.customer_id", "cr.revenue") == [
            (1, 1, 90.0), (1, 2, 70.0), (2, 3, 50.0),
        ]

    async def test_named_two_hop(self, exec_engine) -> None:
        resp = await exec_engine.execute(STAGE_NAMED_TWO_HOP)
        assert _rows(resp, "cr.region_name", "cr.revenue") == [("RegN", 160.0), ("RegS", 50.0)]

    async def test_downstream_aggregates_by_name(self, exec_engine) -> None:
        resp = await exec_engine.execute(STAGE_DOWNSTREAM_AGG)
        assert _rows(resp, "cr.rid", "cr.tot") == [(1, 160.0), (2, 50.0)]

    async def test_query_backed_model(self, exec_engine) -> None:
        await _save_query_backed_cr(exec_engine)
        resp = await exec_engine.execute(QUERY_BACKED_OUTER)
        assert _rows(resp, "cr.region_id", "cr.revenue") == [(1, 160.0), (2, 50.0)]


class TestUnnamedComputedPathEqualsPlain:
    async def test_single_query_keys_by_path(self, exec_engine) -> None:
        resp = await exec_engine.execute(SINGLE_UNNAMED_PATH)
        assert resp.columns == ["orders.customers.region_id", "orders.revenue"]
        assert _rows(resp, "orders.customers.region_id", "orders.revenue") == [(1, 160.0), (2, 50.0)]

    async def test_stage_reads_flattened_path(self, exec_engine) -> None:
        resp = await exec_engine.execute(STAGE_UNNAMED_PATH)
        assert _rows(resp, "cr.customers__region_id", "cr.revenue") == [(1, 160.0), (2, 50.0)]


class TestPlainAndNamedOverOnePath:
    async def test_single_query_projects_both(self, exec_engine) -> None:
        resp = await exec_engine.execute(SINGLE_PLAIN_AND_NAMED)
        assert resp.columns == [
            "orders.customers.region_id", "orders.status", "orders.rid", "orders.revenue",
        ]
        assert resp.data
        assert all(r["orders.rid"] == r["orders.customers.region_id"] for r in resp.data)

    async def test_stage_projects_both(self, exec_engine) -> None:
        resp = await exec_engine.execute(STAGE_PLAIN_AND_NAMED)
        assert resp.columns == ["cr.customers__region_id", "cr.status", "cr.rid", "cr.revenue"]
        assert resp.data
        assert all(r["cr.rid"] == r["cr.customers__region_id"] for r in resp.data)


class TestControlsUnchanged:
    async def test_plain_dotted_dimension_in_stage(self, exec_engine) -> None:
        resp = await exec_engine.execute(STAGE_PLAIN_PATH)
        assert _rows(resp, "cr.customers__region_id", "cr.revenue") == [(1, 160.0), (2, 50.0)]

    async def test_local_rename_in_stage(self, exec_engine) -> None:
        resp = await exec_engine.execute(STAGE_LOCAL_RENAME)
        assert _rows(resp, "cr.cust", "cr.revenue") == [(1, 90.0), (2, 70.0), (3, 50.0)]


async def _plan(engine: SlayerQueryEngine, payload: Any):
    main, named, ds, chain = await engine._normalize_input(
        payload, runtime_kwarg={}, prefer_data_source=None,
    )
    return await engine._plan_and_render(
        query=main, named_queries=named, runtime_kwarg={}, prefer_data_source=ds,
        splice_chain=chain,
    )


PARITY_PAYLOADS = {
    "single_named_one_hop": SINGLE_NAMED_ONE_HOP,
    "stage_named_one_hop": STAGE_NAMED_ONE_HOP,
    "stage_named_two_hop": STAGE_NAMED_TWO_HOP,
    "stage_downstream_agg": STAGE_DOWNSTREAM_AGG,
    "single_unnamed_path": SINGLE_UNNAMED_PATH,
    "stage_unnamed_path": STAGE_UNNAMED_PATH,
    "single_plain_and_named": SINGLE_PLAIN_AND_NAMED,
    "stage_plain_and_named": STAGE_PLAIN_AND_NAMED,
    "single_order_by_name": SINGLE_ORDER_BY_NAME,
    "single_filter_by_name": SINGLE_FILTER_BY_NAME,
    "stage_plain_path": STAGE_PLAIN_PATH,
    "stage_local_rename": STAGE_LOCAL_RENAME,
}


@pytest.mark.parametrize("payload", list(PARITY_PAYLOADS.values()), ids=list(PARITY_PAYLOADS))
async def test_planned_keys_equal_rendered_keys(exec_engine, payload) -> None:
    await _assert_parity(exec_engine, payload)


async def test_planned_keys_equal_rendered_keys_query_backed(exec_engine) -> None:
    await _save_query_backed_cr(exec_engine)
    await _assert_parity(exec_engine, QUERY_BACKED_OUTER)


async def _assert_parity(engine: SlayerQueryEngine, payload: Any) -> None:
    """Planned result keys equal the rendered SQL's decoded outer aliases, in order."""
    rendered = await _plan(engine, payload)
    assert rendered.sql is not None
    planned = projection_result_keys(root_planned=rendered.planned_list[-1])
    emitted = expected_columns_from_sql(sql=rendered.sql, dialect=rendered.dialect)
    decoded = list(get_dialect(rendered.dialect).decode_result_keys(
        [dict.fromkeys(emitted)], aliases=planned,
    )[0])
    assert planned == decoded


@pytest.mark.parametrize(
    ("dimension", "flat", "public"),
    [
        (REGION_ID, "region_id", "region_id"),
        (REGION_NAME, "region_name", "region_name"),
        (UNNAMED_PATH, "customers__region_id", "customers.region_id"),
    ],
    ids=["named_one_hop", "named_two_hop", "unnamed_path"],
)
async def test_stage_column_names(exec_engine, dimension, flat, public) -> None:
    rendered = await _plan(exec_engine, [
        _stage(dimensions=[dimension]),
        _outer(dimensions=[flat, "revenue"]),
    ])
    schema = next(
        p.stage_schema for p in rendered.planned_list
        if p.stage_schema is not None and p.stage_schema.display_name == "cr"
    )
    by_public = {c.public_alias: c for c in schema.columns}
    column = by_public[public]
    assert column.name == column.sql_alias == flat


def test_flat_rename_wrapper_error_names_the_stage() -> None:
    inner = exp.select(
        exp.alias_(exp.column("x"), exp.to_identifier("orders.customers.region_id", quoted=True)),
    ).from_("orders")
    with pytest.raises(ValueError, match="stage 'cr'") as info:
        build_flat_rename_wrapper(
            stage="cr", source_relation="orders", inner=inner,
            expected_columns=["region_id"], dialect="duckdb",
        )
    assert "stage 'orders'" not in str(info.value)
