"""DEV-1892 task 1.2 — the ill-typed residue: a parameter the home dataset's
grain does not determine fails at plan time with one ref-free typed error naming
the parameter, the grain, and the remedy; an attached parameter on a row-level
source is a named typed error; the REST surface maps the residue to 400. All
assert the NEW one-rule contract, so they fail against today's three gates
(whose messages name neither the grain nor partition_by).

Spec: queries/semantics — "Parameter not determined by the grain fails closed";
queries/partitioned-aggregates — "Column-reference outer parameter fails
closed"; aggregations/expression-aggregation — "Attached parameter on a
row-level source rejected".
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.errors import SlayerError
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1841_fixtures import assoc_q, bcast_q, error_q, make_exec_engine
from tests._dev1847_fixtures import (
    INNER_CR,
    _engine_for,
    _seed_sqlite,
    dev1847_models,
    gen,
    sales_q,
)
from tests._dev1892_fixtures import (
    ModelMeasure,
    assert_grain_residue,
    assert_ref_free,
)

_ATTACHED_ON_ROW = (
    "customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))"
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def assoc_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestOperandGrainResidue:
    """A re-aggregation parameter the operand grain does not determine — plan
    time, ref-free, naming the parameter / grain / partition_by remedy."""

    async def test_column_reference_parameter(self):
        query = sales_q(dimensions=["region"], measures=[ModelMeasure(
            formula=f"wavg({INNER_CR}, weight=id)", name="w")])
        with pytest.raises(SlayerError) as ei:
            await gen(query)
        assert_grain_residue(ei.value, param="weight")

    async def test_definition_default_column_parameter(self):
        query = sales_q(dimensions=["region"], measures=[ModelMeasure(
            formula=f"wavg({INNER_CR})", name="w")])
        with pytest.raises(SlayerError) as ei:
            await gen(query)
        # wavg's weight defaults to the ``amount`` column — undetermined at the
        # [city, region] grain.
        assert_grain_residue(ei.value, param="weight")

    async def test_aggregate_grained_outside_operand(self):
        query = sales_q(dimensions=["region"], measures=[ModelMeasure(
            formula=f"weighted_avg({INNER_CR}, weight=sum(amount, partition_by=product))",
            name="w")])
        with pytest.raises(SlayerError) as ei:
            await gen(query)
        assert_grain_residue(ei.value, param="weight")


class TestResidueFiresAtPlanTime:
    """Design D7: the definition-default typing is a plan-time check, not the
    old render-time gate — ``plan_query`` (no SQL generation) already raises."""

    def test_default_column_parameter_rejected_by_plan_query(self):
        models = dev1847_models()
        bundle = ResolvedSourceBundle(
            source_model=models[0], referenced_models=models[1:])
        query = sales_q(dimensions=["region"], measures=[ModelMeasure(
            formula=f"wavg({INNER_CR})", name="w")])
        with pytest.raises(SlayerError) as ei:
            plan_query(query=query, bundle=bundle)
        assert_grain_residue(ei.value, param="weight")


class TestAttachedParameterOnRowLevelSource:
    """An aggregate-valued parameter on a row-level source is a typed error in
    every mode — the parameter would need the attached value on the aggregation's
    own input rows."""

    @pytest.mark.parametrize("make_q", [assoc_q, bcast_q, error_q])
    async def test_rejected_in_every_mode(self, assoc_engine, make_q):
        query = make_q(dimensions=["status"],
                       measures=[ModelMeasure(formula=_ATTACHED_ON_ROW, name="w")])
        with pytest.raises(SlayerError) as ei:
            await assoc_engine.execute(query)
        msg = str(ei.value)
        assert "weight" in msg  # names the parameter
        assert "row" in msg.lower()  # the remedy: use a row-level parameter
        assert_ref_free(msg)


class TestRestSurfaceResidue:
    @pytest.fixture
    async def sales_storage(self) -> AsyncIterator[object]:
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "data.sqlite")
            _seed_sqlite(db_path)
            engine = await _engine_for(
                dialect="sqlite", db_path=db_path, models=dev1847_models())
            yield engine.storage

    async def test_residue_maps_to_400(self, sales_storage):
        """A re-aggregation column-parameter residue is a client error (400)."""
        client = TestClient(create_app(storage=sales_storage))
        body = {"source_model": "sales", "dimensions": ["region"],
                "measures": [{"formula": f"wavg({INNER_CR}, weight=id)", "name": "w"}]}
        resp = client.post("/query", json=body)
        assert resp.status_code == 400, resp.text
