"""A stage is read as the dataset its schema defines, through the one dataset scope.

Spec: openspec …/specs/queries/multi-stage — "A stage is read as the dataset its schema defines";
…/specs/queries/dotted-dimension-routing — "Datasource-scoped and deferred past stage boundaries",
"A dotted spelling of a flattened column is refused with its flat name".
"""

from __future__ import annotations

import pytest

from slayer.core.errors import PopulationInferenceError, UnknownReferenceError, UnrelatedDatasetError

from tests._dev2219_fixtures import (
    DIALECTS,
    REVENUE_BY_CUSTOMER,
    REVENUE_BY_REGION,
    by,
    m,
    outcome,
    over,
    per_customer,
    q,
    shop,
)


@pytest.fixture(params=DIALECTS)
async def engine(request):
    async with shop(request.param) as e:
        yield e


def _hint(err: BaseException) -> str:
    return (getattr(err, "suggestion", None) or "") + (getattr(err, "suggested_path", None) or "")


def _flattened(*extra: str):
    """Stage ``s`` over ``orders`` projecting ``customers.region`` (and ``extra``)."""
    return q(name="s", population="orders", dimensions=[*extra, "customers.region"], metrics=[m("sum(amount)", "revenue")])


# --------------------------------------------------------------------------- #
# Edge-less stages read as before.
# --------------------------------------------------------------------------- #
class TestEdgeLessStage:
    async def test_flat_names(self, engine) -> None:
        resp = await engine.execute([_flattened(), over("s", dimensions=["customers__region"])])
        assert list(resp.data[0]) == ["s.customers__region", "s.total"]
        assert by(resp, key="customers__region", value="total") == REVENUE_BY_REGION

    async def test_root_qualifier(self, engine) -> None:
        resp = await engine.execute([per_customer(), over("per_customer", dimensions=["per_customer.customer_id"])])
        assert list(resp.data[0]) == ["per_customer.customer_id", "per_customer.total"]
        assert by(resp, key="customer_id", value="total") == REVENUE_BY_CUSTOMER

    async def test_time_dimension_re_bucketing(self, engine) -> None:
        daily = q(name="d", population="orders", time_dimensions=[{"field": "ordered_at", "granularity": "day"}],
                  metrics=[m("sum(amount)", "revenue")])
        root = q(population="d", time_dimensions=[{"field": "ordered_at", "granularity": "month"}],
                 metrics=[m("sum(revenue)", "total")])
        resp = await engine.execute([daily, root])
        got = {str(r["d.ordered_at"])[:10]: r["d.total"] for r in resp.data}
        assert got == {"2024-01-01": 30.0, "2024-02-01": 35.0, "2024-03-01": 62.0}

    async def test_population_inference_over_a_stage_root(self, engine) -> None:
        rootless = q(dimensions=["per_customer.customer_id"], metrics=[m("sum(per_customer.revenue)", "total")])
        stages = [per_customer(), rootless]
        with pytest.raises(PopulationInferenceError, match="per_customer"):
            await engine.execute(stages)

    async def test_list_elements(self) -> None:
        pytest.importorskip("duckdb")
        stage = q(name="s", population="orders", dimensions=["customer_id"], metrics=[m("collect(status)", "sts")])
        root = q(population="s", dimensions=["sts.value"], metrics=[m("count(sts.*)", "n")])
        async with shop("duckdb") as engine:
            resp = await engine.execute([stage, root])
        assert {r["s.sts.value"]: r["s.n"] for r in resp.data} == {"ok": 5, "new": 2}


# --------------------------------------------------------------------------- #
# Dotted references follow the stored-root rules.
# --------------------------------------------------------------------------- #
class TestDottedReferences:
    async def test_dotted_spelling_of_a_flattened_column_names_the_flat_column(self, engine) -> None:
        stages = [_flattened(), over("s", dimensions=["s.customers.region"])]
        with pytest.raises(UnknownReferenceError) as ei:
            await engine.execute(stages)
        assert "customers__region" in _hint(ei.value)

    async def test_unrelated_short_form_suggests_the_flat_column_first(self, engine) -> None:
        stages = [_flattened(), over("s", dimensions=["customers.region"])]
        with pytest.raises(UnrelatedDatasetError) as ei:
            await engine.execute(stages)
        hint = _hint(ei.value)
        assert "customers__region" in hint
        assert hint.index("customers__region") < hint.index("max(")

    async def test_with_the_targets_key_the_dotted_reference_resolves(self, engine) -> None:
        stage = _flattened("customers.id")
        resp = await engine.execute([stage, over("s", dimensions=["s.customers.region"])])
        assert by(resp, key="customers.region", value="total") == REVENUE_BY_REGION

    async def test_routing_through_an_inherited_edge(self, engine) -> None:
        resp = await engine.execute([per_customer(), over("per_customer", dimensions=["regions.name"])])
        assert "per_customer.customers.regions.name" in resp.data[0]
        assert by(resp, key="customers.regions.name", value="total") == REVENUE_BY_REGION

    async def test_a_dotted_path_no_edge_reaches_fails_as_from_a_stored_root(self, engine) -> None:
        stored = await outcome(engine, over("orders", dimensions=["customers.nope.name"], metric="sum(amount)"))
        staged = await outcome(engine, [per_customer(), over("per_customer", dimensions=["customers.nope.name"])])
        assert stored[0] == staged[0] == "error"
        assert staged[1] == stored[1]
