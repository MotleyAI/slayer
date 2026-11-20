"""Route-emitting surfaces agree: canonical tokens, one tie-break, the spine rule, unaddressable-pair diagnostics."""

from __future__ import annotations

import logging
import re
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import PopulationErrorReason, PopulationInferenceError, UnresolvableDimensionRelationshipError
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.time_spine import spine_dataset
from slayer.engine.population.dimension_routing import route_dotted_target
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.facade.catalog import build_catalog
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import (
    OSIDataset,
    OSIDialectExpression,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSIMetric,
    OSIRelationship,
)
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import disposable_engine, seeded_exec_engine
from tests._osi_helpers import run_converter

EngineBuilder = Callable[[list[SlayerDataset]], Awaitable[SlayerQueryEngine]]

DS = "ds"
M2O = RelationshipCardinality.MANY_TO_ONE
_REMEDY = re.compile(r"name (the|its) edges", re.IGNORECASE)


def _names_pair(text: str, *, a: str, b: str) -> bool:
    return a in text and b in text and bool(_REMEDY.search(text))


def _m(name: str, *, cols: tuple[str, ...] = (), joins: list[DatasetRelationship] | None = None,
       metrics: list[DatasetMetric] | None = None) -> SlayerDataset:
    return SlayerDataset(
        name=name, data_source=DS, sql_table=name,
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                 *(DatasetField(name=c, datatype=DataType.INTEGER if c.endswith("_id") else DataType.STRING) for c in cols)],
        relationships=joins or [], metrics=metrics or [],
    )


def _to(target: str, src: str, *, tgt: str = "id", name: str | None = None,
        cardinality: RelationshipCardinality | None = M2O) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[src], to_columns=[tgt], name=name, cardinality=cardinality)


def _by(*models: SlayerDataset) -> dict[str, SlayerDataset]:
    return {m.name: m for m in models}


def _unnamed_pair() -> list[SlayerDataset]:
    """``orders`` declares two unnamed relationships onto ``addresses``."""
    return [
        _m("orders", cols=("status", "billing_id", "shipping_id"),
           joins=[_to("addresses", "billing_id"), _to("addresses", "shipping_id")]),
        _m("addresses", cols=("city",)),
    ]


@pytest.fixture
async def make_engine() -> AsyncIterator[EngineBuilder]:
    with tempfile.TemporaryDirectory() as tmp:
        async def build(models: list[SlayerDataset]) -> SlayerQueryEngine:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite", database=":memory:"))
            for model in models:
                await storage.save_dataset(model)
            return SlayerQueryEngine(storage=storage)
        yield build


class TestRecommendation:
    @pytest.mark.parametrize("item", ["regions.rname", "regions.n"])
    async def test_single_named_edge_is_emitted_as_written(self, make_engine: EngineBuilder, item: str) -> None:
        engine = await make_engine([
            _m("orders", cols=("customer_id",), joins=[_to("customers", "customer_id")]),
            _m("customers", cols=("region_id",), joins=[_to("regions", "region_id", name="hr")]),
            _m("regions", cols=("rname",), metrics=[DatasetMetric(name="n", expression="count(rname)")]),
        ])
        rec = await engine.recommend_population(["orders.id", item])
        assert rec.population == "orders"
        paths = {ip.input_item: ip.path for ip in rec.item_paths}
        assert paths[item] == item

    async def test_equal_length_routes_prefer_the_to_one_hop_is_emitted_as_written(self, make_engine: EngineBuilder) -> None:
        engine = await make_engine([
            _m("a", cols=("b_id", "x"), joins=[_to("b", "b_id", name="to_b")]),
            _m("b", cols=("a_id", "y"), joins=[_to("a", "a_id", name="to_a")],
               metrics=[DatasetMetric(name="total", expression="count(y)")]),
        ])
        rec = await engine.recommend_population(["a.x", "b.total"])
        assert rec.population == "a"
        paths = {ip.input_item: ip.path for ip in rec.item_paths}
        assert paths["b.total"] == "b.total"

    async def test_unaddressable_pair_is_named_in_the_warnings(self, make_engine: EngineBuilder) -> None:
        engine = await make_engine(_unnamed_pair())
        rec = await engine.recommend_population(["orders.id", "addresses.city"])
        assert not any("addresses" in ip.path.split(".")[:-1] for ip in rec.item_paths)
        assert any(_names_pair(w, a="orders", b="addresses") for w in rec.warnings), rec.warnings


class TestShortFormRouting:
    def test_unaddressable_pair_on_the_only_route_is_named(self) -> None:
        models = _by(
            _m("shipments", cols=("order_id",), joins=[_to("orders", "order_id")]),
            *_unnamed_pair(),
        )
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            route_dotted_target(root=models["shipments"], target_dataset="addresses", leaf="city",
                                datasets_by_name=models)
        assert ei.value.suggested_path is None
        assert _names_pair(str(ei.value), a="orders", b="addresses"), str(ei.value)

    def test_suggestion_through_a_single_named_edge_uses_its_name(self) -> None:
        models = _by(
            _m("orders", cols=("customer_id", "store_id"),
               joins=[_to("customers", "customer_id"), _to("stores", "store_id")]),
            _m("customers", cols=("region_id",), joins=[_to("regions", "region_id", name="hr")]),
            _m("stores", cols=("region_id",), joins=[_to("regions", "region_id")]),
            _m("regions", cols=("name",)),
        )
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            route_dotted_target(root=models["orders"], target_dataset="regions", leaf="name",
                                datasets_by_name=models)
        assert ei.value.suggested_path == "customers.hr.name"

    def test_equal_length_suggestion_prefers_fewer_fanning_hops(self) -> None:
        # Both 2-hop routes fan out somewhere; via "aaa" twice, via "zzz" once.
        models = _by(
            _m("root", cols=("grp", "zzz_id"),
               joins=[_to("aaa", "grp", tgt="grp", cardinality=None), _to("zzz", "zzz_id")]),
            _m("aaa", cols=("grp", "grp2"), joins=[_to("d", "grp2", tgt="grp", cardinality=None)]),
            _m("zzz", cols=("grp",), joins=[_to("d", "grp", tgt="grp", cardinality=None)]),
            _m("d", cols=("grp", "name")),
        )
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            route_dotted_target(root=models["root"], target_dataset="d", leaf="name", datasets_by_name=models)
        assert ei.value.suggested_path == "zzz.d.name"

    def test_routes_never_cross_the_spine(self) -> None:
        ts = DatasetField(name="at", datatype=DataType.DATETIME)
        orders = _m("orders").model_copy(update={"fields": [*_m("orders").fields, ts],
                                                  "default_time_dimension": "at"})
        returns = _m("returns").model_copy(update={"fields": [*_m("returns").fields, ts],
                                                    "default_time_dimension": "at"})
        models = _by(orders, returns, spine_dataset(data_source=DS))
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            route_dotted_target(root=models["orders"], target_dataset="returns", leaf="id",
                                datasets_by_name=models)
        assert ei.value.suggested_path is None


class TestFacadeCatalog:
    def test_unaddressable_pair_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="slayer.facade.catalog"):
            build_catalog(datasets_by_datasource={DS: _unnamed_pair()})
        assert any(_names_pair(r.getMessage(), a="orders", b="addresses") for r in caplog.records), [
            r.getMessage() for r in caplog.records]

    def test_paths_never_cross_the_spine(self) -> None:
        ts = DatasetField(name="at", datatype=DataType.DATETIME)
        orders = _m("orders").model_copy(update={"fields": [*_m("orders").fields, ts],
                                                  "default_time_dimension": "at"})
        returns = _m("returns", cols=("kind",)).model_copy(update={
            "fields": [*_m("returns", cols=("kind",)).fields, ts], "default_time_dimension": "at"})
        catalog = build_catalog(datasets_by_datasource={DS: [orders, returns, spine_dataset(data_source=DS)]})
        table = next(t for s in catalog.schemas for t in s.tables if t.name == "orders")
        assert not any("returns" in d.name for d in table.dimensions)


_OSI_SCHEMA = [
    "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)",
    "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, region_id INTEGER)",
]


@pytest.fixture
def osi_engine(tmp_path: Path) -> Iterator[sa.Engine]:
    with disposable_engine(f"sqlite:///{tmp_path}/osi.db") as engine:
        with engine.connect() as conn:
            for ddl in _OSI_SCHEMA:
                conn.execute(sa.text(ddl))
            conn.commit()
        yield engine


def _expr(sql: str) -> OSIExpression:
    return OSIExpression(dialects=[OSIDialectExpression.model_validate({"dialect": "ANSI_SQL", "expression": sql})])


class TestOsiConversion:
    def test_unaddressable_pair_in_the_no_join_path_report(self, osi_engine: sa.Engine) -> None:
        def rel(name: str, src: str, dst: str) -> OSIRelationship:
            return OSIRelationship.model_validate({"name": name, "from": src, "to": dst,
                                                   "from_columns": ["customer_id"], "to_columns": ["customer_id"]})
        doc = OSIDocument(
            version="0.2.0.dev0", name="s", legacy=True,
            datasets=[
                OSIDataset(name="orders", source="orders",
                           fields=[OSIField(name="amount", expression=_expr("amount"))]),
                OSIDataset(name="customers", source="customers",
                           fields=[OSIField(name="region_id", expression=_expr("region_id"))]),
            ],
            relationships=[rel("o2c", "orders", "customers"), rel("c2o", "customers", "orders")],
            metrics=[OSIMetric(name="cross",
                               expression=_expr("SUM(orders.amount) + SUM(customers.region_id)"))],
        )
        result = run_converter(
            OsiToSlayerConverter(documents=[doc], data_source=DS),
            datasource=DatasourceConfig(name=DS, type="sqlite", database=osi_engine.url.database),
        )
        report = [w for w in result.unconverted_metrics if w.category == "no_join_path"]
        assert report, result.unconverted_metrics
        assert any(_names_pair(f"{w.message} {w.suggestion or ''}", a="orders", b="customers") for w in report)


class TestPopulationInference:
    async def test_unaddressable_pair_in_the_candidate_reason(self, make_engine: EngineBuilder) -> None:
        engine = await make_engine(_unnamed_pair())
        query = SlayerQuery.model_validate({"dimensions": ["orders.status", "addresses.city"]})
        with pytest.raises(PopulationInferenceError) as ei:
            await engine.execute(query)
        assert _names_pair(str(ei.value), a="orders", b="addresses"), str(ei.value)


def _cycle_datasets() -> list[SlayerDataset]:
    """``x → y → z → x``, every edge declared many-to-one."""
    return [
        _m("x", cols=("y_id", "v"), joins=[_to("y", "y_id")]),
        _m("y", cols=("z_id", "v"), joins=[_to("z", "z_id")]),
        _m("z", cols=("x_id", "v"), joins=[_to("x", "x_id")]),
    ]


def _seed_cycle(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        for table, fk in (("x", "y_id"), ("y", "z_id"), ("z", "x_id")):
            cur.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, {fk} INTEGER, v TEXT)")
            cur.execute(f"INSERT INTO {table} VALUES (1, 1, '{table}1')")


@pytest.fixture
async def cycle_engine() -> AsyncIterator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect="sqlite", seed=_seed_cycle, datasets=_cycle_datasets(),
                                  datasource=DS, validate=True) as (engine, _db):
        yield engine


class TestDirectedCycle:
    async def test_inference_ties(self, cycle_engine) -> None:
        query = SlayerQuery.model_validate({"dimensions": ["x.v", "y.v", "z.v"]})
        with pytest.raises(PopulationInferenceError) as ei:
            await cycle_engine.execute(query)
        assert ei.value.reason == PopulationErrorReason.TIE

    async def test_the_named_population_executes(self, cycle_engine) -> None:
        resp = await cycle_engine.execute(SlayerQuery.model_validate({"population": "x", "dimensions": ["x.v", "y.v", "y.z.v"]}))
        assert len(resp.data) == 1
        assert sorted(resp.data[0].values()) == ["x1", "y1", "z1"]

    async def test_recommendation_agrees_with_inference(self, cycle_engine: SlayerQueryEngine) -> None:
        rec = await cycle_engine.recommend_population(["x.v", "y.v", "z.v"])
        assert rec.reachable is False
        assert rec.population is None
        coverage = {c.dataset_name: set(c.reachable_items) for c in rec.coverage}
        assert coverage == {name: {"x.v", "y.v", "z.v"} for name in ("x", "y", "z")}
