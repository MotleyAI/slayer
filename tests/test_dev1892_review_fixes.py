"""DEV-1892 review-round regressions: CR4 renamed-key seeding, CR5 picked-param
filter, CR3 parse-based expression-default extraction."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.keys import ColumnKey, Grain
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.join_safety import grain_determines

from tests._dev1841_fixtures import (
    ModelMeasure,
    assoc_q,
    make_exec_engine as make_assoc_engine,
    status_key,
)
from tests._dev1847_fixtures import (
    chain_q,
    make_exec_engine as make_reagg_engine,
    region_key,
    sales_q,
)
from tests._dev1892_fixtures import (
    ASSOC_WPHYS_BY_STATUS,
    ASSOC_WSUM_BY_STATUS,
    ASSOC_WSUM6_BY_STATUS,
    ASSOC_WSUM7_BY_STATUS,
    ASSOC_WSUM9_BY_STATUS,
    CORDERS_GLOBAL_UNWEIGHTED,
    CORDERS_GLOBAL_WAVG,
    SlayerError,
    UNWEIGHTED_CITY_BY_REGION,
    assert_grain_residue,
    derived_local_expr_default_models,
    root_named_expr_default_models,
    literal_collision_reagg_models,
    literal_only_reagg_models,
    mixed_expr_default_models,
    opaque_expr_default_models,
    qualified_expr_default_models,
    qualified_reagg_default_models,
    self_qualified_expr_default_models,
    toone_filter_models,
    unmodeled_physical_expr_default_models,
    unparseable_expr_default_models,
)
from tests._engine_helpers import _engine_generate


def _customers_model(*, key_sql: str | None) -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="prod", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, sql=key_sql, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def _fk_hop_models(*, fk_sql: str | None) -> tuple[SlayerModel, dict[str, SlayerModel]]:
    """``customers`` (FK ``region_id``, renamable) → ``regions`` (m:1); the join's
    physical source column is ``fk_sql or "region_id"``."""
    regions = SlayerModel(
        name="regions", data_source="prod", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="pop", type=DataType.INT),
        ],
    )
    customers = SlayerModel(
        name="customers", data_source="prod", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT, sql=fk_sql),
        ],
        joins=[ModelJoin(
            target_model="regions", join_pairs=[[fk_sql or "region_id", "id"]],
            cardinality=JoinCardinality.MANY_TO_ONE,
        )],
    )
    return customers, {"customers": customers, "regions": regions}


class TestGrainDeterminesRenamedKey:
    def test_renamed_unique_key_seeds_entity(self) -> None:
        # logical grain leaf ``id`` must match the physical PK ``customer_id``.
        model = _customers_model(key_sql="customer_id")
        assert grain_determines(
            key=ColumnKey(leaf="name"), grain=Grain.of([ColumnKey(leaf="id")]),
            host_model=model, models_by_name={"customers": model},
        )

    def test_plain_key_unaffected(self) -> None:
        model = _customers_model(key_sql=None)
        assert grain_determines(
            key=ColumnKey(leaf="name"), grain=Grain.of([ColumnKey(leaf="id")]),
            host_model=model, models_by_name={"customers": model},
        )

    def test_renamed_fk_seeds_to_one_hop(self) -> None:
        # logical grain leaf ``region_id`` must seed the m:1 hop whose physical
        # ``join_pairs`` source column is the rename ``region_fk`` — else a valid
        # lifted parameter over ``regions.pop`` is wrongly rejected.
        customers, models = _fk_hop_models(fk_sql="region_fk")
        assert grain_determines(
            key=ColumnKey(path=("regions",), leaf="pop"),
            grain=Grain.of([ColumnKey(leaf="region_id")]),
            host_model=customers, models_by_name=models,
        )

    def test_plain_fk_unaffected(self) -> None:
        customers, models = _fk_hop_models(fk_sql=None)
        assert grain_determines(
            key=ColumnKey(path=("regions",), leaf="pop"),
            grain=Grain.of([ColumnKey(leaf="region_id")]),
            host_model=customers, models_by_name=models,
        )


class TestPickedParamDoesNotInheritSourceFilter:
    async def test_picked_weight_not_masked_by_source_filter(self) -> None:
        # ``north_spend`` is filtered on regions.name='North'. Only the VALUE (_v)
        # carries that CASE mask; the picked weight ``customers.spend`` (_p0) is NOT
        # masked by the source's filter (DEV-1832: a parameter is masked only by its
        # own column's filter, never the source's).
        models = toone_filter_models()
        orders = next(m for m in models if m.name == "orders")
        extra = [m for m in models if m.name != "orders"]
        sql = await _engine_generate(
            query=assoc_q(
                dimensions=["channel"],
                measures=[ModelMeasure(
                    formula="customers.north_spend:weighted_avg(weight=customers.spend)",
                    name="w")]),
            model=orders, extra_models=extra, dialect="sqlite", validate=False,
        )
        p0_line = next(line for line in sql.splitlines() if "AS _p0" in line)
        assert "CASE WHEN" not in p0_line, sql
        v_line = next(line for line in sql.splitlines() if "AS _v" in line)
        assert "CASE WHEN" in v_line, sql
        assert "'North'" in v_line, sql


@pytest.fixture(params=["sqlite", "duckdb"])
async def qual_expr_engine(request):
    async for engine in make_assoc_engine(request, models=qualified_expr_default_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def mixed_expr_engine(request):
    async for engine in make_assoc_engine(request, models=mixed_expr_default_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def root_named_expr_engine(request):
    async for engine in make_assoc_engine(request, models=root_named_expr_default_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def opaque_expr_engine(request):
    async for engine in make_assoc_engine(request, models=opaque_expr_default_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def self_qual_engine(request):
    async for engine in make_assoc_engine(
            request, models=self_qualified_expr_default_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def derived_local_engine(request):
    async for engine in make_assoc_engine(
            request, models=derived_local_expr_default_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def phys_expr_engine(request):
    async for engine in make_assoc_engine(
            request, models=unmodeled_physical_expr_default_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparseable_engine(request):
    async for engine in make_assoc_engine(
            request, models=unparseable_expr_default_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def lit_reagg_engine(request):
    async for engine in make_reagg_engine(request, models=literal_collision_reagg_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def lit_only_engine(request):
    async for engine in make_reagg_engine(request, models=literal_only_reagg_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def qual_reagg_engine(request):
    async for engine in make_reagg_engine(request, models=qualified_reagg_default_models()):
        yield engine


def _status_vals(resp, measure):
    return {k[0]: v[measure] for k, v in status_key(resp).items()}


class TestExprDefaultQualifiedRefs:
    """CR3: a qualified ref inside an expression default is a real dependency —
    lifted when determined, never silently dropped."""

    async def test_qualified_expr_default_lifts(self, qual_expr_engine):
        # Was silent 0.0: ``regions.pop * 2`` extracted no owner token, so the
        # default was neither lifted nor rejected.
        resp = await qual_expr_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wsum6", name="w")]))
        vals = _status_vals(resp, "orders.w")
        for status, expected in ASSOC_WSUM6_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_mixed_expr_default(self, mixed_expr_engine):
        # ``spend * regions.pop``: owner and qualified refs in one expression.
        resp = await mixed_expr_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wsum7", name="w")]))
        vals = _status_vals(resp, "orders.w")
        for status, expected in ASSOC_WSUM7_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_self_qualified_ref_is_local(self, self_qual_engine):
        # ``customers.spend * 1`` on customers: the owner-name qualifier is a
        # self-reference, not a hop (and not opaque) — SUM(spend * spend).
        resp = await self_qual_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wsum8", name="w")]))
        vals = _status_vals(resp, "orders.w")
        for status, expected in ASSOC_WSUM_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_derived_local_ref_in_expr_default(self, derived_local_engine):
        # ``double_spend * regions.pop``: a DERIVED local column expands through
        # the owner-anchored entry alongside the qualified ref.
        resp = await derived_local_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wsum9", name="w")]))
        vals = _status_vals(resp, "orders.w")
        for status, expected in ASSOC_WSUM9_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_unmodeled_physical_ref_lifts(self, phys_expr_engine):
        # ``tier`` is physical-only (removed from the model): a bare ref in an
        # expr default is owner-anchored like a bare default, never dropped.
        resp = await phys_expr_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wphys", name="w")]))
        vals = _status_vals(resp, "orders.w")
        for status, expected in ASSOC_WPHYS_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_reagg_qualified_expr_default(self, qual_reagg_engine):
        # Was silent NULL: ``customers.region_id * 1`` is FK-seeded by the
        # operand grain (customer_id pins the to-one customers row).
        resp = await qual_reagg_engine.execute(chain_q(
            measures=[ModelMeasure(
                formula="cwavg(sum(amount, partition_by=customer_id))", name="w")]))
        got = float(resp.data[0]["corders.w"])
        assert got == pytest.approx(CORDERS_GLOBAL_WAVG)
        assert got != pytest.approx(CORDERS_GLOBAL_UNWEIGHTED)


class TestExprDefaultLiteralsAndResidue:
    async def test_string_literal_is_not_a_dependency(self, lit_reagg_engine):
        # Was a spurious typed error: the literal ``'amount'`` read as the
        # ``amount`` column. Weight is constant 2.0 -> the unweighted average.
        resp = await lit_reagg_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula="wlit(sum(amount, partition_by=[city, region]))",
                name="w")]))
        vals = {k[0]: v["sales.w"] for k, v in region_key(resp).items()}
        for region, expected in UNWEIGHTED_CITY_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)
        assert vals["Void"] is None  # its only cell total is NULL

    async def test_literal_only_default_rides_plain_machinery(self, lit_only_engine):
        # Zero column refs (parsed, not opaque) -> not lifted, constant weight
        # 2.0 -> the unweighted average. Was a spurious ``amount`` dependency.
        resp = await lit_only_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula="wconst(sum(amount, partition_by=[city, region]))",
                name="w")]))
        vals = {k[0]: v["sales.w"] for k, v in region_key(resp).items()}
        for region, expected in UNWEIGHTED_CITY_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)
        assert vals["Void"] is None

    async def test_undetermined_qualified_expr_default_rejected(self, qual_reagg_engine):
        # An operand grain that pins nothing about customers -> the one-rule
        # typed error (was silent invalid SQL).
        q = chain_q(measures=[ModelMeasure(
            formula="cwavg(sum(amount, partition_by=amount))", name="w")])
        with pytest.raises(SlayerError) as ei:
            await qual_reagg_engine.execute(q)
        assert_grain_residue(ei.value, param="weight")

    async def test_opaque_qualifier_fails_closed(self, opaque_expr_engine):
        # ``nosuch.col`` resolves no join walk -> the input closure is
        # unanalysable with no nameable column, so the analyzability guard
        # fails it closed (preempts the grain-residue diagnosis).
        q = assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wopq", name="w")])
        with pytest.raises(ValueError, match="no supported dialect can analyse"):
            await opaque_expr_engine.execute(q)

    async def test_unparseable_default_fails_closed(self, unparseable_engine):
        # No dialect parses ``)((( bad``: an unanalysable input with no nameable
        # column fails closed via the analyzability guard, never a raw
        # level-2 render.
        q = assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wugly", name="w")])
        with pytest.raises(ValueError, match="no supported dialect can analyse"):
            await unparseable_engine.execute(q)

    async def test_root_named_expr_default_widens_to_root(self, root_named_expr_engine):
        # DEV-1931 (Reading A): the default ``orders.amount + 0`` names the query
        # root, so it resolves root-local and widens the home to orders —
        # identical to spelling the weight explicitly. A genuinely fanning
        # definition default still fails closed
        # (tests/test_dev1931_default_home.py::TestFanningDefinitionDefaultFailsClosed).
        resp = await root_named_expr_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[
                ModelMeasure(formula="customers.spend:wroot", name="w"),
                ModelMeasure(
                    formula="sum(customers.spend * (orders.amount + 0))", name="oracle"),
            ]))
        default_vals = _status_vals(resp, "orders.w")
        oracle_vals = _status_vals(resp, "orders.oracle")
        assert default_vals
        for status, expected in oracle_vals.items():
            assert float(default_vals[status]) == pytest.approx(float(expected))
