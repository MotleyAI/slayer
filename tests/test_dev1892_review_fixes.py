"""Review-round regressions: CR4 renamed-key seeding, CR5 picked-param
filter, CR3 parse-based expression-default extraction."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import (
    UnanalyzableAggregationParameterError,
    UnknownReferenceError,
    UnresolvableDimensionRelationshipError,
)
from slayer.core.keys import FieldKey, Grain
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.cardinality.relationship_safety import grain_determines

from slayer.core.query import QueryMetric

from tests._dev1841_fixtures import (
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
    ASSOC_WSUM_BY_STATUS,
    ASSOC_WSUM6_BY_STATUS,
    ASSOC_WSUM7_BY_STATUS,
    ASSOC_WSUM9_BY_STATUS,
    CORDERS_GLOBAL_UNWEIGHTED,
    CORDERS_GLOBAL_WAVG,
    SlayerError,
    UNWEIGHTED_CITY_BY_REGION,
    assert_grain_residue,
    derived_local_expr_default_datasets,
    root_named_expr_default_datasets,
    literal_collision_reagg_datasets,
    literal_only_reagg_datasets,
    mixed_expr_default_datasets,
    opaque_expr_default_datasets,
    qualified_expr_default_datasets,
    qualified_reagg_default_datasets,
    self_qualified_expr_default_datasets,
    toone_filter_datasets,
    unmodeled_physical_expr_default_datasets,
    unparseable_expr_default_datasets,
)
from tests._engine_helpers import _engine_generate, datasets_bundle


def _customers_dataset(*, key_sql: str | None) -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source="prod", sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, sql=key_sql, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
        ],
    )


def _fk_hop_datasets(*, fk_sql: str | None) -> tuple[SlayerDataset, dict[str, SlayerDataset]]:
    """``customers`` (FK ``region_id``, renamable) → ``regions`` (m:1), keyed by
    the logical ``region_id`` whatever its physical spelling."""
    regions = SlayerDataset(
        name="regions", data_source="prod", sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="pop", datatype=DataType.INTEGER),
        ],
    )
    customers = SlayerDataset(
        name="customers", data_source="prod", sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER, sql=fk_sql),
        ],
        relationships=[DatasetRelationship(
            to="regions", from_columns=["region_id"], to_columns=["id"],
            cardinality=RelationshipCardinality.MANY_TO_ONE,
        )],
    )
    return customers, {"customers": customers, "regions": regions}


class TestGrainDeterminesRenamedKey:
    def test_renamed_unique_key_seeds_entity(self) -> None:
        # logical grain leaf ``id`` must match the physical PK ``customer_id``.
        model = _customers_dataset(key_sql="customer_id")
        assert grain_determines(
            key=FieldKey(leaf="name"), grain=Grain.of([FieldKey(leaf="id")]),
            host_dataset=model, datasets_by_name={"customers": model},
            bundle=datasets_bundle({"customers": model}),
        )

    def test_plain_key_unaffected(self) -> None:
        model = _customers_dataset(key_sql=None)
        assert grain_determines(
            key=FieldKey(leaf="name"), grain=Grain.of([FieldKey(leaf="id")]),
            host_dataset=model, datasets_by_name={"customers": model},
            bundle=datasets_bundle({"customers": model}),
        )

    def test_renamed_fk_seeds_to_one_hop(self) -> None:
        # grain leaf ``region_id`` seeds the m:1 hop keyed on the renamed FK
        # (physical ``region_fk``) — else a valid lifted parameter is rejected.
        customers, models = _fk_hop_datasets(fk_sql="region_fk")
        assert grain_determines(
            key=FieldKey(path=("regions",), leaf="pop"),
            grain=Grain.of([FieldKey(leaf="region_id")]),
            host_dataset=customers, datasets_by_name=models, bundle=datasets_bundle(models),
        )

    def test_plain_fk_unaffected(self) -> None:
        customers, models = _fk_hop_datasets(fk_sql=None)
        assert grain_determines(
            key=FieldKey(path=("regions",), leaf="pop"),
            grain=Grain.of([FieldKey(leaf="region_id")]),
            host_dataset=customers, datasets_by_name=models, bundle=datasets_bundle(models),
        )


class TestPickedParamDoesNotInheritSourceFilter:
    async def test_picked_weight_not_masked_by_source_filter(self) -> None:
        # ``north_spend`` is filtered on regions.name='North'. Only the VALUE (_v)
        # carries that CASE mask; the picked weight ``customers.spend`` (_p0) is NOT
        # masked by the source's filter (a parameter is masked only by its
        # own field's filter, never the source's).
        models = toone_filter_datasets()
        orders = next(m for m in models if m.name == "orders")
        extra = [m for m in models if m.name != "orders"]
        sql = await _engine_generate(
            query=assoc_q(
                dimensions=["orders.channel"],
                metrics=[QueryMetric(
                    metric="weighted_avg(customers.north_spend, weight=customers.spend)",
                    alias="w")]),
            dataset=orders, extra_datasets=extra, dialect="sqlite", validate=False,
        )
        p0_line = next(line for line in sql.splitlines() if "AS _p0" in line)
        assert "CASE WHEN" not in p0_line, sql
        v_line = next(line for line in sql.splitlines() if "AS _v" in line)
        assert "CASE WHEN" in v_line, sql
        assert "'North'" in v_line, sql


@pytest.fixture(params=["sqlite", "duckdb"])
async def qual_expr_engine(request):
    async for engine in make_assoc_engine(request, datasets=qualified_expr_default_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def mixed_expr_engine(request):
    async for engine in make_assoc_engine(request, datasets=mixed_expr_default_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def root_named_expr_engine(request):
    async for engine in make_assoc_engine(request, datasets=root_named_expr_default_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def opaque_expr_engine(request):
    async for engine in make_assoc_engine(request, datasets=opaque_expr_default_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def self_qual_engine(request):
    async for engine in make_assoc_engine(
            request, datasets=self_qualified_expr_default_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def derived_local_engine(request):
    async for engine in make_assoc_engine(
            request, datasets=derived_local_expr_default_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def phys_expr_engine(request):
    async for engine in make_assoc_engine(
            request, datasets=unmodeled_physical_expr_default_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparseable_engine(request):
    async for engine in make_assoc_engine(
            request, datasets=unparseable_expr_default_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def lit_reagg_engine(request):
    async for engine in make_reagg_engine(request, datasets=literal_collision_reagg_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def lit_only_engine(request):
    async for engine in make_reagg_engine(request, datasets=literal_only_reagg_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def qual_reagg_engine(request):
    async for engine in make_reagg_engine(request, datasets=qualified_reagg_default_datasets()):
        yield engine


def _status_vals(resp, metric):
    return {k[0]: v[metric] for k, v in status_key(resp).items()}


class TestExprDefaultQualifiedRefs:
    """CR3: a qualified ref inside an expression default is a real dependency —
    lifted when determined, never silently dropped."""

    async def test_qualified_expr_default_lifts(self, qual_expr_engine):
        # Was silent 0.0: ``regions.pop * 2`` extracted no owner token, so the
        # default was neither lifted nor rejected.
        resp = await qual_expr_engine.execute(assoc_q(
            dimensions=["orders.status"],
            metrics=[QueryMetric(metric="wsum6(customers.spend)", alias="w")]))
        vals = _status_vals(resp, "w")
        for status, expected in ASSOC_WSUM6_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_mixed_expr_default(self, mixed_expr_engine):
        # ``spend * regions.pop``: owner and qualified refs in one expression.
        resp = await mixed_expr_engine.execute(assoc_q(
            dimensions=["orders.status"],
            metrics=[QueryMetric(metric="wsum7(customers.spend)", alias="w")]))
        vals = _status_vals(resp, "w")
        for status, expected in ASSOC_WSUM7_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_self_qualified_ref_is_local(self, self_qual_engine):
        # ``customers.spend * 1`` on customers: the owner-name qualifier is a
        # self-reference, not a hop (and not opaque) — SUM(spend * spend).
        resp = await self_qual_engine.execute(assoc_q(
            dimensions=["orders.status"],
            metrics=[QueryMetric(metric="wsum8(customers.spend)", alias="w")]))
        vals = _status_vals(resp, "w")
        for status, expected in ASSOC_WSUM_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_derived_local_ref_in_expr_default(self, derived_local_engine):
        # ``double_spend * regions.pop``: a DERIVED local field expands through
        # the owner-anchored entry alongside the qualified ref.
        resp = await derived_local_engine.execute(assoc_q(
            dimensions=["orders.status"],
            metrics=[QueryMetric(metric="wsum9(customers.spend)", alias="w")]))
        vals = _status_vals(resp, "w")
        for status, expected in ASSOC_WSUM9_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_unmodeled_physical_ref_fails_closed(self, phys_expr_engine):
        # ``tier`` is physical-only (removed from the dataset): a default names only
        # dataset fields, exactly as its explicit spelling must.
        q = assoc_q(
            dimensions=["orders.status"],
            metrics=[QueryMetric(metric="wphys(customers.spend)", alias="w")])
        with pytest.raises(UnknownReferenceError, match="tier"):
            await phys_expr_engine.execute(q)

    async def test_reagg_qualified_expr_default(self, qual_reagg_engine):
        # Was silent NULL: ``customers.region_id * 1`` is FK-seeded by the
        # operand grain (customer_id pins the to-one customers row).
        resp = await qual_reagg_engine.execute(chain_q(
            metrics=[QueryMetric(
                metric="cwavg(sum(corders.amount, partition_by=corders.customer_id))", alias="w")]))
        got = float(resp.data[0]["w"])
        assert got == pytest.approx(CORDERS_GLOBAL_WAVG)
        assert got != pytest.approx(CORDERS_GLOBAL_UNWEIGHTED)


class TestExprDefaultLiteralsAndResidue:
    async def test_string_literal_is_not_a_dependency(self, lit_reagg_engine):
        # Was a spurious typed error: the literal ``'amount'`` read as the
        # ``amount`` field. Weight is constant 2.0 -> the unweighted average.
        resp = await lit_reagg_engine.execute(sales_q(
            dimensions=["sales.region"],
            metrics=[QueryMetric(
                metric="wlit(sum(sales.amount, partition_by=[sales.city, sales.region]))",
                alias="w")]))
        vals = {k[0]: v["w"] for k, v in region_key(resp).items()}
        for region, expected in UNWEIGHTED_CITY_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)
        assert vals["Void"] is None  # its only cell total is NULL

    async def test_literal_only_default_rides_plain_machinery(self, lit_only_engine):
        # Zero field refs (parsed, not opaque) -> not lifted, constant weight
        # 2.0 -> the unweighted average. Was a spurious ``amount`` dependency.
        resp = await lit_only_engine.execute(sales_q(
            dimensions=["sales.region"],
            metrics=[QueryMetric(
                metric="wconst(sum(sales.amount, partition_by=[sales.city, sales.region]))",
                alias="w")]))
        vals = {k[0]: v["w"] for k, v in region_key(resp).items()}
        for region, expected in UNWEIGHTED_CITY_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)
        assert vals["Void"] is None

    async def test_undetermined_qualified_expr_default_rejected(self, qual_reagg_engine):
        # An operand grain that pins nothing about customers -> the one-rule
        # typed error (was silent invalid SQL).
        q = chain_q(metrics=[QueryMetric(
            metric="cwavg(sum(corders.amount, partition_by=corders.amount))", alias="w")])
        with pytest.raises(SlayerError) as ei:
            await qual_reagg_engine.execute(q)
        assert_grain_residue(ei.value, param="weight")

    async def test_opaque_qualifier_fails_closed(self, opaque_expr_engine):
        # ``nosuch.col`` resolves no relationship walk -> the default fails at binding.
        q = assoc_q(
            dimensions=["orders.status"],
            metrics=[QueryMetric(metric="wopq(customers.spend)", alias="w")])
        with pytest.raises(UnresolvableDimensionRelationshipError, match="nosuch.col"):
            await opaque_expr_engine.execute(q)

    async def test_unparseable_default_fails_closed(self, unparseable_engine):
        # ``)((( bad`` does not parse: a typed error at binding, never a raw
        # level-2 render, and never the parser's own diagnostics.
        q = assoc_q(
            dimensions=["orders.status"],
            metrics=[QueryMetric(metric="wugly(customers.spend)", alias="w")])
        with pytest.raises(UnanalyzableAggregationParameterError, match="weight") as ei:
            await unparseable_engine.execute(q)
        assert "\x1b" not in str(ei.value)

    async def test_root_named_expr_default_widens_to_root(self, root_named_expr_engine):
        # Reading A: the default ``orders.amount + 0`` names the query
        # root, so it resolves root-local and widens the home to orders —
        # identical to spelling the weight explicitly. A genuinely fanning
        # definition default still fails closed
        # (tests/test_dev1931_default_home.py::TestFanningDefinitionDefaultFailsClosed).
        resp = await root_named_expr_engine.execute(assoc_q(
            dimensions=["orders.status"],
            metrics=[
                QueryMetric(metric="wroot(customers.spend)", alias="w"),
                QueryMetric(
                    metric="sum(customers.spend * (orders.amount + 0))", alias="oracle"),
            ]))
        default_vals = _status_vals(resp=resp, metric="w")
        oracle_vals = _status_vals(resp=resp, metric="oracle")
        assert default_vals
        for status, expected in oracle_vals.items():
            assert float(default_vals[status]) == pytest.approx(float(expected))
