"""DEV-1850 — keyless-grain dual-role cross-model partitioned aggregate.

Exact local/cross-model parity for a partitioned aggregate whose partition key
is absent from the query dimensions. A combined-position consumer (measure,
arithmetic/scalar composite, transform input, raw ORDER BY target, filter-only
reference) fails at plan time with the SAME ``not a query dimension`` ValueError
the local twin raises — never the internal ``missing a host / producer``
RuntimeError. A row-scope reference (filter over the computed dimension's own
aggregate, ORDER BY the dimension's name) executes row-routed exactly as it does
locally, with one producer relation and no combined twin.

Feature-missing today: the cross-model combined-consumer and row-scope shapes
raise the internal RuntimeError (the retired DEV-1838 pin
``test_dual_role_without_partition_key_in_grain_unsupported`` documented it).
The local twins, the already-clean cross-model measure-only / filter-only
errors, and the keyless banded-only shape are green today and stay green.
"""

from __future__ import annotations

import pytest

from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import (
    _plan_regroups,
    _resolve_scope,
    bind_query_inputs,
)

from tests._dev1838_fixtures import (
    ModelMeasure,
    SPEND_BAND,
    cte_aliases,
    dev1838_models,
    gen,
    make_exec_engine,
    q,
)

LOCAL_AGG = "amount:sum(partition_by=city)"
CM_AGG = "customers.spend:sum(partition_by=customers.tier)"
LOCAL_BAND = "CASE WHEN amount:sum(partition_by=city) > 25 THEN 'hi' ELSE 'lo' END"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _cm_count(sql: str, dialect: str) -> int:
    return len(cte_aliases(sql, "_cm_", dialect=dialect))


def _dim(expression: str, name: str) -> dict:
    return {"expression": expression, "name": name}


# --------------------------------------------------------------------------- #
# Combined-position consumers reject the keyless grain — local == cross-model.
# --------------------------------------------------------------------------- #
_COMBINED_CONSUMER_CASES = [
    # A computed dimension banding the aggregate AND a combined consumer of it,
    # with the partition key absent from the dimensions. Cross-model RuntimeError
    # today; local twin already clean. (dims, measures, order, filters, key)
    pytest.param(
        [_dim(LOCAL_BAND, "cband")],
        [ModelMeasure(formula=LOCAL_AGG, name="rt")],
        None, None, "city", id="dual_role_measure-local",
    ),
    pytest.param(
        [_dim(SPEND_BAND, "sband")],
        [ModelMeasure(formula=CM_AGG, name="rt")],
        None, None, "customers.tier", id="dual_role_measure-cross_model",
    ),
    pytest.param(
        [_dim(LOCAL_BAND, "cband")],
        [ModelMeasure(formula="amount:sum", name="m")],
        [{"column": LOCAL_AGG, "direction": "asc"}], None, "city",
        id="raw_order_target-local",
    ),
    pytest.param(
        [_dim(SPEND_BAND, "sband")],
        [ModelMeasure(formula="amount:sum", name="m")],
        [{"column": CM_AGG, "direction": "asc"}], None, "customers.tier",
        id="raw_order_target-cross_model",
    ),
    pytest.param(
        [_dim(LOCAL_BAND, "cband")],
        [ModelMeasure(formula=f"{LOCAL_AGG} + 1", name="rt")],
        None, None, "city", id="composite_operand-local",
    ),
    pytest.param(
        [_dim(SPEND_BAND, "sband")],
        [ModelMeasure(formula=f"{CM_AGG} + 1", name="rt")],
        None, None, "customers.tier", id="composite_operand-cross_model",
    ),
    pytest.param(
        [_dim(LOCAL_BAND, "cband")],
        [ModelMeasure(formula=f"coalesce({LOCAL_AGG}, 0)", name="rt")],
        None, None, "city", id="scalar_call_operand-local",
    ),
    pytest.param(
        [_dim(SPEND_BAND, "sband")],
        [ModelMeasure(formula=f"coalesce({CM_AGG}, 0)", name="rt")],
        None, None, "customers.tier", id="scalar_call_operand-cross_model",
    ),
    pytest.param(
        [_dim(LOCAL_BAND, "cband")],
        [ModelMeasure(formula=f"rank({LOCAL_AGG})", name="rt")],
        None, None, "city", id="transform_input-local",
    ),
    pytest.param(
        [_dim(SPEND_BAND, "sband")],
        [ModelMeasure(formula=f"rank({CM_AGG})", name="rt")],
        None, None, "customers.tier", id="transform_input-cross_model",
    ),
    # Already-clean today (no computed dimension): the aggregate is consumed only
    # as a measure, or only in a filter — a combined position, so it stays rejected.
    pytest.param(
        ["status"], [ModelMeasure(formula=CM_AGG, name="rt")],
        None, None, "customers.tier", id="measure_only-cross_model",
    ),
    pytest.param(
        ["status"], [ModelMeasure(formula="amount:sum", name="m")],
        None, [f"{CM_AGG} > 100"], "customers.tier", id="filter_only-cross_model",
    ),
]


@pytest.mark.parametrize("dims,measures,order,filters,key", _COMBINED_CONSUMER_CASES)
async def test_combined_consumer_keyless_grain_rejected(
    dims, measures, order, filters, key,
) -> None:
    kw = {"dimensions": dims, "measures": measures}
    if order:
        kw["order"] = order
    if filters:
        kw["filters"] = filters
    with pytest.raises(ValueError, match=r"not a query dimension") as ei:
        await gen(q(**kw))
    msg = str(ei.value)
    assert key in msg, msg
    assert "Add it to dimensions/time_dimensions" in msg, msg  # the remedy
    assert "missing a host / producer" not in msg  # never the internal RuntimeError
    assert "__regroup__" not in msg


_ERROR_TEMPLATE = (
    "Aggregation 'sum': partition_by column '{key}' is not a query dimension. "
    "Add it to dimensions/time_dimensions, or choose one of: {dims}."
)


async def test_keyless_dual_role_error_shape_matches_local() -> None:
    """The cross-model clean error is identical IN SHAPE to the local twin's —
    same wording, differing only in the offending key and the available dims."""
    with pytest.raises(ValueError) as local_ei:
        await gen(q(
            dimensions=[_dim(LOCAL_BAND, "cband")],
            measures=[ModelMeasure(formula=LOCAL_AGG, name="rt")],
        ))
    with pytest.raises(ValueError) as cm_ei:
        await gen(q(
            dimensions=[_dim(SPEND_BAND, "sband")],
            measures=[ModelMeasure(formula=CM_AGG, name="rt")],
        ))
    assert str(local_ei.value) == _ERROR_TEMPLATE.format(key="city", dims="cband")
    assert str(cm_ei.value) == _ERROR_TEMPLATE.format(
        key="customers.tier", dims="sband",
    )


# --------------------------------------------------------------------------- #
# Dimension-only consumption keeps the finer-grain exemption (executes keyless).
# --------------------------------------------------------------------------- #
async def test_keyless_banded_only_executes(exec_backend) -> None:
    dialect, engine = exec_backend
    query = q(
        dimensions=[_dim(SPEND_BAND, "sband")],
        measures=[ModelMeasure(formula="amount:sum", name="m")],
    )
    resp = await engine.execute(query)
    got = {r["orders.sband"]: r["orders.m"] for r in resp.data}
    assert got == {"hi": pytest.approx(40.0), "lo": pytest.approx(77.0)}
    dry = await engine.execute(query, dry_run=True)
    assert _cm_count(dry.sql, dialect) == 1, cte_aliases(
        dry.sql, "_cm_", dialect=dialect,
    )


# --------------------------------------------------------------------------- #
# Row-scope references execute row-routed — cross-model mirrors local exactly.
# --------------------------------------------------------------------------- #
async def test_keyless_filter_over_own_cross_model_aggregate_row_routes(
    exec_backend,
) -> None:
    """The filter references the same aggregate the dimension bands: row-routed,
    the predicate applies per base row against the attached per-tier value before
    aggregation. Only silver rows (spend 210 > 100) survive → the 'lo' group is
    removed and 'hi' keeps its value. One producer, no combined twin."""
    dialect, engine = exec_backend
    query = q(
        dimensions=[_dim(SPEND_BAND, "sband")],
        measures=[ModelMeasure(formula="amount:sum", name="m")],
        filters=[f"{CM_AGG} > 100"],
    )
    resp = await engine.execute(query)
    got = {r["orders.sband"]: r["orders.m"] for r in resp.data}
    assert got == {"hi": pytest.approx(40.0)}
    dry = await engine.execute(query, dry_run=True)
    assert _cm_count(dry.sql, dialect) == 1, cte_aliases(
        dry.sql, "_cm_", dialect=dialect,
    )


async def test_keyless_order_by_dimension_name_cross_model_row_routes(
    exec_backend,
) -> None:
    """ORDER BY the computed dimension's NAME sorts by the banded value and stays
    row-routed — no combined attach synthesized for the order reference."""
    dialect, engine = exec_backend
    query = q(
        dimensions=[_dim(SPEND_BAND, "sband")],
        measures=[ModelMeasure(formula="amount:sum", name="m")],
        order=[{"column": "sband", "direction": "asc"}],
    )
    resp = await engine.execute(query)
    rows = [(r["orders.sband"], r["orders.m"]) for r in resp.data]
    assert rows == [("hi", pytest.approx(40.0)), ("lo", pytest.approx(77.0))]
    dry = await engine.execute(query, dry_run=True)
    assert _cm_count(dry.sql, dialect) == 1, cte_aliases(
        dry.sql, "_cm_", dialect=dialect,
    )


# Local twins — the mirror the cross-model shapes must match (green today).
async def test_keyless_filter_over_own_local_aggregate_row_routes(
    exec_backend,
) -> None:
    _, engine = exec_backend
    resp = await engine.execute(q(
        dimensions=[_dim(LOCAL_BAND, "cband")],
        measures=[ModelMeasure(formula="amount:sum", name="m")],
        filters=[f"{LOCAL_AGG} > 25"],
    ))
    got = {r["orders.cband"]: r["orders.m"] for r in resp.data}
    assert got == {"hi": pytest.approx(107.0)}


async def test_keyless_order_by_dimension_name_local_row_routes(
    exec_backend,
) -> None:
    _, engine = exec_backend
    resp = await engine.execute(q(
        dimensions=[_dim(LOCAL_BAND, "cband")],
        measures=[ModelMeasure(formula="amount:sum", name="m")],
        order=[{"column": "cband", "direction": "asc"}],
    ))
    rows = [(r["orders.cband"], r["orders.m"]) for r in resp.data]
    assert rows == [("hi", pytest.approx(107.0)), ("lo", pytest.approx(10.0))]


# --------------------------------------------------------------------------- #
# D4 — the producer-recursion contract of the unified seam: with
# ``local_discovery=False`` the local combined bucket is suppressed (the recursion
# guard) while cross-model discovery still runs. Pinned directly on _plan_regroups.
# --------------------------------------------------------------------------- #
def test_local_discovery_false_suppresses_local_keeps_cross_model() -> None:
    models = dev1838_models()
    bundle = ResolvedSourceBundle(
        source_model=models[0], referenced_models=list(models[1:]),
    )
    query = q(
        dimensions=["status", "customers.tier"],
        measures=[
            ModelMeasure(formula="amount:sum(partition_by=status)", name="lt"),
            ModelMeasure(formula="customers.spend:sum", name="cm"),
        ],
    )
    scope = _resolve_scope(query=query, bundle=bundle, stage_schemas={})
    prebound = bind_query_inputs(query=query, bundle=bundle, scope=scope)

    def _attaches(local_discovery: bool):
        result = _plan_regroups(
            prebound=prebound, scope=scope, bundle=bundle, stage_schemas={},
            producer_source_model="orders", local_discovery=local_discovery,
        )
        assert result is not None
        return result[1]

    # A LOCAL combined attach has no producer_root_model; a cross-model one names its root.
    on = _attaches(local_discovery=True)
    assert [a.producer_root_model for a in on] == [None, "customers"]

    off = _attaches(local_discovery=False)
    assert [a.producer_root_model for a in off] == ["customers"]
