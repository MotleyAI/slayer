"""One aggregate expression consumed by a computed dimension AND another position
(spec: queries/computed-dimensions, queries/partitioned-aggregates)."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Callable, List

import pytest

from slayer.core.enums import DataType, RelationshipCardinality, TimeGranularity
from slayer.core.errors import PartitionKeyError
from slayer.core.keys import (
    REGROUP_LEAF_PREFIX,
    AggregateKey,
    ArithmeticKey,
    FieldKey,
    Grain,
    LiteralKey,
    ScalarCallKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
)
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery, TimeDimension
from slayer.engine.compile.discovery import RootDisposition
from slayer.engine.compile.stages import _regroup_producer_prebound
from slayer.engine.plan import plan_query
from slayer.ir.prebound import ModePolicy
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed
from slayer.storage.sqlite_conn import transaction

from tests._dev1847_fixtures import (
    chain_q,
    dev1847_datasets,
    gen,
    make_exec_engine,
    sales_q,
)
from tests._engine_helpers import _extract_cte_body, seeded_exec_engine
from tests.test_dev1903_discovery import _discover
from slayer.engine.query_engine import SlayerQueryEngine

R = "avg(sum(sales.amount, partition_by=[sales.city, sales.region]), partition_by=sales.region)"
RLEVEL = {"field": f"CASE WHEN {R} > 50 THEN 'hi' ELSE 'lo' END", "alias": "rlevel"}
TLEVEL = {"field": f"CASE WHEN rank({R}, direction='desc') > 1 THEN 'top' ELSE 'rest' END",
          "alias": "tlevel"}
TOT = QueryMetric(metric="sum(sales.amount)", alias="tot")

R_BY_REGION = {"North": 45.0, "South": 70.0, "East": 60.0, "Gap": 10.0, "Void": None}
TOT_BY_REGION = {"North": 90.0, "South": 140.0, "East": 180.0, "Gap": 20.0, "Void": None}
RLEVEL_BY_REGION = {"North": "lo", "South": "hi", "East": "hi", "Gap": "lo", "Void": "lo"}
RANK_BY_REGION = {"South": 1, "East": 2, "North": 3, "Gap": 4, "Void": None}
TLEVEL_BY_REGION = {"South": "rest", "East": "top", "North": "top", "Gap": "top",
                    "Void": "rest"}

PLAIN_RANK = "rank(sum(sales.amount, partition_by=sales.region), direction='desc')"
PBAND = {"field": f"CASE WHEN {PLAIN_RANK} > 1 THEN 'top' ELSE 'rest' END",
         "alias": "pband"}

# Finer-grained re-aggregation: outer grain [city, region] under dimensions [region].
FINE = "avg(sum(sales.amount, partition_by=[sales.city, sales.region, sales.product]), partition_by=[sales.city, sales.region])"
FINE_DIM = {"field": f"CASE WHEN {FINE} > 30 THEN 'hi' ELSE 'lo' END", "alias": "x"}
FINE_TOT = {("North", "lo"): 30.0, ("North", "hi"): 60.0, ("South", "hi"): 140.0,
            ("East", "hi"): 180.0, ("Gap", "lo"): 20.0, ("Void", "lo"): None}

CITY_BAND = {"field": "CASE WHEN sum(sales.amount, partition_by=[sales.city, sales.region]) > 45 "
                           "THEN 'hi' ELSE 'lo' END", "alias": "band"}

CHAIN_R = "avg(sum(corders.amount, partition_by=corders.customer_id), partition_by=customers.regions.name)"
CL = {"field": f"CASE WHEN {CHAIN_R} > 40 THEN 'hi' ELSE 'lo' END", "alias": "cl"}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _bundle(source: str = "sales") -> ResolvedSourceBundle:
    models = dev1847_datasets()
    [host] = [m for m in models if m.name == source]
    return ResolvedSourceBundle(dialect="duckdb", population=host,
                                referenced_datasets=[m for m in models if m is not host])


def _col(resp, name: str) -> dict:
    """``{region: value}`` for result column ``sales.<name>``."""
    return {r["sales.region"]: r[name] for r in resp.data}


def _regions(resp) -> List[str]:
    return [r["sales.region"] for r in resp.data]


def _approx(d: dict) -> dict:
    return {k: (None if v is None else pytest.approx(v)) for k, v in d.items()}


# --------------------------------------------------------------------------- #
# queries/computed-dimensions — per-position evaluation, executed.
# --------------------------------------------------------------------------- #
class TestReaggregationInDimensionAndElsewhere:
    async def test_measure_typed_filter(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", RLEVEL], metrics=[TOT], filter=[f"{R} < sum(sales.amount)"]))
        got = {(r["sales.region"], r["rlevel"]): r["tot"] for r in resp.data}
        assert got == _approx({("East", "hi"): 180.0, ("Gap", "lo"): 20.0,
                               ("North", "lo"): 90.0, ("South", "hi"): 140.0})

    async def test_measure_typed_filter_keeping_nothing(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", RLEVEL], metrics=[TOT], filter=[f"{R} > sum(sales.amount)"]))
        assert resp.data == []

    async def test_order_target(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", RLEVEL], metrics=[TOT],
            order=[{"field": R, "direction": "desc"}]))
        assert _regions(resp) == ["South", "East", "North", "Gap", "Void"]

    async def test_measure(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", RLEVEL], metrics=[TOT, QueryMetric(metric=R, alias="r")]))
        assert _col(resp, "r") == _approx(R_BY_REGION)
        assert _col(resp, "rlevel") == RLEVEL_BY_REGION

    async def test_filter_by_dimension_name_uses_band(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", RLEVEL], metrics=[TOT, QueryMetric(metric=R, alias="r")],
            filter=["rlevel = 'hi'"]))
        assert _col(resp, "r") == _approx({"South": 70.0, "East": 60.0})


class TestTransformOverReaggregationInDimension:
    async def test_dimension_only(self, exec_engine):
        resp = await exec_engine.execute(sales_q(dimensions=["sales.region", TLEVEL], metrics=[TOT]))
        assert _col(resp, "tlevel") == TLEVEL_BY_REGION
        assert _col(resp, "tot") == _approx(TOT_BY_REGION)

    async def test_rank_as_measure(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", TLEVEL],
            metrics=[TOT, QueryMetric(metric=f"rank({R}, direction='desc')", alias="rk")]))
        assert _col(resp, "rk") == RANK_BY_REGION
        assert _col(resp, "tlevel") == TLEVEL_BY_REGION

    @pytest.mark.parametrize(("flt", "kept"), [
        pytest.param(f"rank({R}, direction='desc') > 1", {"East", "North", "Gap"}, id="gt"),
        pytest.param(f"rank({R}, direction='desc') < 4", {"South", "East", "North"}, id="lt"),
    ])
    async def test_rank_in_filter(self, exec_engine, flt, kept):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", TLEVEL], metrics=[TOT], filter=[flt]))
        assert set(_regions(resp)) == kept
        assert _col(resp, "tot") == _approx({k: TOT_BY_REGION[k] for k in kept})

    async def test_rank_as_order_target(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", TLEVEL], metrics=[TOT],
            order=[{"field": f"rank({R}, direction='desc')", "direction": "asc"}]))
        assert _regions(resp) == ["South", "East", "North", "Gap", "Void"]

    async def test_two_dimensions_share_the_reaggregation(self, exec_engine: SlayerQueryEngine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", TLEVEL, RLEVEL],
            metrics=[TOT, QueryMetric(metric=R, alias="r")]))
        got = {r["sales.region"]: (r["tlevel"], r["rlevel"]) for r in resp.data}
        assert got == {"South": ("rest", "hi"), "East": ("top", "hi"),
                       "North": ("top", "lo"), "Gap": ("top", "lo"), "Void": ("rest", "lo")}
        assert _col(resp, "tot") == _approx(TOT_BY_REGION)
        assert _col(resp, "r") == _approx(R_BY_REGION)
        assert not any(REGROUP_LEAF_PREFIX in c or c.endswith(".grain") for c in resp.columns)


class TestOrderByTransformSharedWithDimension:
    async def test_order_by_expression(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", PBAND], metrics=[TOT],
            order=[{"field": PLAIN_RANK, "direction": "asc"}]))
        assert _regions(resp) == ["East", "South", "North", "Gap", "Void"]

    async def test_order_by_dimension_name_sorts_band(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", PBAND], metrics=[TOT],
            order=[{"field": "pband", "direction": "asc"}]))
        bands = [r["pband"] for r in resp.data]
        assert bands == ["rest", "rest", "top", "top", "top"]
        assert set(_regions(resp)[:2]) == {"East", "Void"}


class TestFilterOnAggregateOwnedByDimensionTransform:
    """The aggregate inside a dimension's transform is not itself row-attached, so a
    filter on it is a metric-typed combined consumer."""

    async def test_executes_at_query_grain(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", PBAND], metrics=[TOT],
            filter=["sum(sales.amount, partition_by=sales.region) > 100"]))
        assert _col(resp, "tot") == _approx({"South": 140.0, "East": 180.0})

    def test_finer_partition_key_is_a_typed_error(self):
        band = {"field": "CASE WHEN rank(sum(sales.amount, partition_by=[sales.city, sales.region]), direction='desc') > 1 "
                              "THEN 'top' ELSE 'rest' END", "alias": "cband"}
        query = sales_q(dimensions=["sales.region", band], metrics=[TOT],
                        filter=["sum(sales.amount, partition_by=[sales.city, sales.region]) > 50"])
        bundle = _bundle()
        with pytest.raises(PartitionKeyError) as ei:
            plan_query(query=query, bundle=bundle)
        _assert_consumer_error(ei.value, key="city", location=r"filter\b")


class TestCrossModelReaggregationInDimension:
    async def test_measure_typed_filter(self, exec_engine):
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.regions.name", CL],
            metrics=[QueryMetric(metric="sum(corders.amount)", alias="tot")],
            filter=[f"{CHAIN_R} < sum(corders.amount)"]))
        got = [(r["customers.regions.name"], r["cl"], r["tot"])
               for r in resp.data]
        assert got == [("North", "lo", pytest.approx(70.0))]

    @pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
    async def test_an_associated_dimension_reads_its_reaggregation(self, backend):
        # customers → regions unproven: the dimension associates, its re-aggregation attached inside the producer.
        datasets = dev1847_datasets()
        for d in datasets:
            d.relationships = [r.model_copy(update={"cardinality": None}) if (d.name, r.to) == ("customers", "regions")
                               else r for r in d.relationships]

        class _Request:
            param = backend

        rows = []
        async for engine in make_exec_engine(_Request(), datasets=datasets):
            resp = await engine.execute(chain_q(
                dimensions=["customers.regions.name", CL], metrics=[QueryMetric(metric="sum(corders.amount)", alias="tot")],
                filter=[f"{CHAIN_R} < sum(corders.amount)"]))
            rows = [(r["customers.regions.name"], r["cl"], r["tot"]) for r in resp.data]
        assert rows == [("North", "lo", pytest.approx(70.0))]


# --------------------------------------------------------------------------- #
# queries/partitioned-aggregates — combined-consumer partition keys.
# --------------------------------------------------------------------------- #
class TestFinerGrainedReaggregationDimension:
    async def test_executes_at_its_declared_grain(self, exec_engine):
        resp = await exec_engine.execute(sales_q(dimensions=["sales.region", FINE_DIM], metrics=[TOT]))
        got = {(r["sales.region"], r["x"]): r["tot"] for r in resp.data}
        assert got == _approx(FINE_TOT)

    async def test_order_by_name_of_the_reaggregation_dimension(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.region", {"field": FINE, "alias": "x"}], metrics=[TOT],
            order=[{"field": "x", "direction": "desc"}]))
        xs = [r["x"] for r in resp.data if r["x"] is not None]
        assert xs == pytest.approx([100.0, 80.0, 60.0, 50.0, 40.0, 15.0, 12.0, 8.0])


class TestPlainMeasureTypedFilterOverDimensionAggregate:
    def test_same_partition_key_error_as_without_dimension(self):
        flt = "sum(sales.amount, partition_by=[sales.city, sales.region]) < sum(sales.amount)"
        bare_q = sales_q(dimensions=["sales.region"], metrics=[TOT], filter=[flt])
        banded_q = sales_q(dimensions=["sales.region", CITY_BAND], metrics=[TOT], filter=[flt])
        bundle = _bundle()
        with pytest.raises(PartitionKeyError) as bare:
            plan_query(query=bare_q, bundle=bundle)
        with pytest.raises(PartitionKeyError) as banded:
            plan_query(query=banded_q, bundle=bundle)
        assert "'city'" in banded.value.summary
        assert (banded.value.summary, banded.value.location) == (
            bare.value.summary, bare.value.location)


def _fine_cases():
    """(id, query kwargs, expected location pattern) for combined consumers of FINE."""
    fm = QueryMetric(metric=FINE, alias="f")
    return [
        ("measure", {"metrics": [fm]}, r"metric 'f'"),
        ("order", {"metrics": [TOT], "order": [{"field": FINE, "direction": "asc"}]},
         r"order item\b"),
        ("arithmetic", {"metrics": [QueryMetric(metric=f"{FINE} + 1", alias="f")]},
         r"metric 'f'"),
        ("transform", {"metrics": [QueryMetric(metric=f"rank({FINE}, direction='desc')", alias="f")]},
         r"metric 'f'"),
        ("filter", {"metrics": [TOT], "filter": [f"{FINE} < sum(sales.amount)"]}, r"filter\b"),
        ("split-filter",
         {"metrics": [TOT], "filter": [f"{FINE} < sum(sales.amount) and sales.region <> 'Gap'"]},
         r"filter\b"),
    ]


def _assert_consumer_error(exc: PartitionKeyError, *, key: str, location: str) -> None:
    assert re.search(rf"\b{re.escape(key)}\b", exc.summary), exc.summary
    assert exc.location is not None, str(exc)
    assert re.match(location, exc.location), str(exc)
    assert "dimension" not in exc.location
    assert REGROUP_LEAF_PREFIX not in str(exc)


class TestReaggregationOuterKeyCarriesTheRule:
    @pytest.mark.parametrize("with_dim", [False, True], ids=["no-dim", "with-dim"])
    @pytest.mark.parametrize(("kw", "location"),
                             [pytest.param(kw, loc, id=i) for i, kw, loc in _fine_cases()])
    def test_combined_consumer_rejected(self, kw, location, with_dim):
        dims = ["sales.region", FINE_DIM] if with_dim else ["sales.region"]
        query, bundle = sales_q(dimensions=dims, **kw), _bundle()
        with pytest.raises(PartitionKeyError) as ei:
            plan_query(query=query, bundle=bundle)
        _assert_consumer_error(ei.value, key="city", location=location)

    @pytest.mark.parametrize(("dim", "agg"), [(CITY_BAND, "sum(sales.amount, partition_by=[sales.city, sales.region])"),
                                              (FINE_DIM, FINE)], ids=["plain", "reagg"])
    @pytest.mark.parametrize("template", ["rank({}, direction='desc')", "{} + 1"], ids=["transform", "arithmetic"])
    def test_order_expression_over_dimension_aggregate(self, dim, agg, template):
        query = sales_q(dimensions=["sales.region", dim], metrics=[TOT],
                        order=[{"field": template.format(agg), "direction": "asc"}])
        bundle = _bundle()
        with pytest.raises(PartitionKeyError) as ei:
            plan_query(query=query, bundle=bundle)
        _assert_consumer_error(ei.value, key="city", location=r"order item\b")

    def test_cross_model_outer_key(self):
        formula = "avg(sum(corders.amount, partition_by=corders.customer_id), partition_by=customers.region_id)"
        query = chain_q(dimensions=["customers.regions.name"],
                        metrics=[QueryMetric(metric=formula, alias="c")])
        bundle = _bundle("corders")
        with pytest.raises(PartitionKeyError) as ei:
            plan_query(query=query, bundle=bundle)
        _assert_consumer_error(ei.value, key="customers.region_id", location=r"metric 'c'")


# --------------------------------------------------------------------------- #
# Plan structure — one producer, one attach per (root, phase).
# --------------------------------------------------------------------------- #
def _is_r(key: ValueKey) -> bool:
    return isinstance(key, AggregateKey) and key.agg == "avg"


_R_CONSUMERS = [
    pytest.param({"filter": [f"{R} < sum(sales.amount)"]}, id="filter"),
    pytest.param({"order": [{"field": R, "direction": "desc"}]}, id="order"),
    pytest.param({"metrics": [TOT, QueryMetric(metric=R, alias="r")]}, id="measure"),
]


def _r_query(kw: dict):
    return sales_q(**{"dimensions": ["sales.region", RLEVEL], "metrics": [TOT], **kw})


class TestDualPhasePlan:
    @pytest.mark.parametrize("kw", _R_CONSUMERS)
    def test_one_attach_per_phase_sharing_one_placeholder(self, kw):
        plan = plan_query(query=_r_query(kw), bundle=_bundle())
        attaches = [a for a in plan.regroup_attach_plans
                    if any(_is_r(s.original_key) for s in a.substitutions)]
        assert sorted(a.attach_phase for a in attaches) == ["combined", "row"]
        assert len({s.placeholder for a in attaches for s in a.substitutions}) == 1

    @pytest.mark.parametrize("kw", _R_CONSUMERS)
    async def test_one_producer_cte_joined_once_per_phase(self, kw):
        sql = await gen(_r_query(kw), dialect="duckdb")
        assert REGROUP_LEAF_PREFIX not in sql
        [cte] = [c for c in re.findall(r"(_cm_\w+) AS \(", sql)
                 if "AVG(" in _extract_cte_body(sql, re.escape(c))]
        assert len(re.findall(rf"JOIN {cte}\b", sql)) == 2, sql
        assert_scope_closed(sql, dialect="duckdb")


def _roots(ds: List[RootDisposition], match: Callable[[ValueKey], bool]) -> set:
    return {(d.phase, d.routing) for d in ds if match(d.root)}


def _is_rank(key: ValueKey) -> bool:
    return isinstance(key, TransformKey) and key.op == "rank"


def _is_inner_cr(key: ValueKey) -> bool:
    return (isinstance(key, AggregateKey) and key.agg == "sum"
            and key.partition_keys is not None and len(key.partition_keys) == 2)


class TestDimensionTransformOwnsReaggregation:
    def test_rank_dimension_is_the_only_root(self):
        ds = _discover(sales_q(dimensions=["sales.region", TLEVEL], metrics=[TOT]), _bundle())
        assert {d.phase for d in ds if _is_rank(d.root)} == {"row"}
        assert not _roots(ds, _is_r)
        assert not _roots(ds, _is_inner_cr)

    def test_measure_occurrence_is_combined_only(self):
        ds = _discover(sales_q(dimensions=["sales.region", TLEVEL],
                               metrics=[TOT, QueryMetric(metric=R, alias="r")]),
                       _bundle())
        assert {d.phase for d in ds if _is_rank(d.root)} == {"row"}
        assert {d.phase for d in ds if _is_r(d.root)} == {"combined"}
        assert not _roots(ds, _is_inner_cr)


# --------------------------------------------------------------------------- #
# Synthesized producer grain names — injective, key-derived.
# --------------------------------------------------------------------------- #
def _flag(field: str, value: str) -> ScalarCallKey:
    cond = ArithmeticKey(op="=", operands=(FieldKey(leaf=field), LiteralKey(value=value)))
    return ScalarCallKey(name="iif", args=(cond, LiteralKey(value=Decimal(1)),
                                           LiteralKey(value=Decimal(0))))


def _grain_names(keys) -> dict:
    """``{key: declared grain name}`` of a producer grouped by ``keys``."""
    models = dev1847_datasets()
    prebound, _ = _regroup_producer_prebound(
        pks=Grain(keys=frozenset(keys)),
        aggs=[AggregateKey(source=FieldKey(leaf="amount"), agg="sum")],
        dataset=models[0], bundle=_bundle(), inherited=[], n_date_range=0, mode_policy=ModePolicy(),
        ranking_candidates=(),
    )
    dims = [dm for dm in prebound.declared_metrics if dm.is_dimension]
    return {dm.bound.value_key: dm.public_name for dm in dims}


class TestSynthesizedGrainNames:
    def test_expression_keys_get_distinct_names(self):
        a, b = _flag("product", "P"), _flag("region", "North")
        names = _grain_names([a, b, FieldKey(leaf="city")])
        assert len(set(names.values())) == 3
        assert names[FieldKey(leaf="city")] == "city"

    def test_name_is_key_derived_not_encounter_order(self):
        a = _flag("product", "P")
        with_later = _grain_names([a, _flag("region", "North")])
        with_earlier = _grain_names([_flag("city", "Alpha"), a])
        assert len(set(with_later.values())) == len(set(with_earlier.values())) == 2
        assert with_later[a] == with_earlier[a]

    def test_time_bucket_spelled_like_a_column(self):
        bucket = TimeTruncKey(source=FieldKey(leaf="city"), granularity=TimeGranularity.MONTH)
        names = _grain_names([bucket, FieldKey(leaf="city_month")])
        assert names[FieldKey(leaf="city_month")] == "city_month"
        assert len(set(names.values())) == 2

    async def test_same_leaf_time_keys_on_different_paths(self):
        # orders.created_at and customers.created_at, both by month.
        rows = [(1, 1, "N", "2024-01-05", 10.0), (2, 1, "S", "2024-01-06", 20.0),
                (3, 2, "N", "2024-02-05", 30.0), (4, 2, "S", "2024-02-07", 40.0)]
        custs = [(1, "2023-01-01"), (2, "2023-02-01")]

        def seed(path: str) -> None:
            with transaction(path) as conn:
                conn.execute("CREATE TABLE orders (id INT, customer_id INT, region TEXT, "
                             "created_at TEXT, amount REAL)")
                conn.execute("CREATE TABLE customers (id INT, created_at TEXT)")
                conn.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", rows)
                conn.executemany("INSERT INTO customers VALUES (?,?)", custs)

        models = [
            SlayerDataset(name="orders", data_source="test", sql_table="orders", fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="region", datatype=DataType.STRING),
                DatasetField(name="created_at", datatype=DataType.DATETIME),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ], relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"],
                                cardinality=RelationshipCardinality.MANY_TO_ONE)]),
            SlayerDataset(name="customers", data_source="test", sql_table="customers", fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="created_at", datatype=DataType.DATETIME),
            ]),
        ]
        months = [TimeDimension(field=n, granularity=TimeGranularity.MONTH)
                  for n in ("orders.created_at", "customers.created_at")]
        q = SlayerQuery(
            population="orders", dimensions=[QueryDimension(field="orders.region")], time_dimensions=months,
            metrics=[QueryMetric(
                metric="avg(sum(orders.amount, partition_by=[orders.region, orders.created_at, customers.created_at]),"
                        " partition_by=[orders.created_at, customers.created_at])", alias="m")],
        )
        async with seeded_exec_engine(dialect="sqlite", seed=seed, datasets=models) as (eng, _):
            resp = await eng.execute(q)
        got = {(r["orders.region"], r["orders.created_at"][:7]): r["m"] for r in resp.data}
        assert got == {("N", "2024-01"): 15.0, ("S", "2024-01"): 15.0,
                       ("N", "2024-02"): 35.0, ("S", "2024-02"): 35.0}
