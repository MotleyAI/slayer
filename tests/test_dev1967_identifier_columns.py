"""Only a model's sole primary-key column is an identifier; composite-key members are ordinary columns."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from slayer.core.enums import DataType
from slayer.core.errors import AggregationNotAllowedError
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.plan import plan_query
from slayer.engine.profiling import (
    _is_sample_cached,
    ensure_samples_fresh,
    refresh_table_backed_dataset_sampled,
)
from slayer.facade.catalog import _eligible_aggregations
from slayer.inspect.dataset_render import (
    _build_sample_query_args,
    _choose_sample_dims,
    render_dataset_inspection,
)
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._dev1836_fixtures import make_exec_engine, orders_dataset
from tests._stored_upgrade_fixtures import DS, raw_store
from slayer.engine.query_engine import SlayerQueryEngine

COMPOSITE_MEMBERS = {"order_id", "line_no", "status"}


def order_lines(*, line_no_aggs=None) -> SlayerDataset:
    """Composite primary key ``(order_id, line_no, status)`` over the seeded ``orders`` table."""
    return SlayerDataset(
        name="order_lines", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="order_id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="line_no", sql="customer_id", datatype=DataType.INTEGER, primary_key=True,
                   allowed_aggregations=line_no_aggs),
            DatasetField(name="status", datatype=DataType.STRING, primary_key=True),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
    )


def codes() -> SlayerDataset:
    """Sole TEXT primary key ``code``."""
    return SlayerDataset(
        name="codes", data_source="test", sql_table="segments",
        fields=[
            DatasetField(name="code", datatype=DataType.STRING, primary_key=True),
            DatasetField(name="label", datatype=DataType.STRING),
        ],
    )


def _plan(*, dataset: SlayerDataset, expression: str) -> None:
    query = SlayerQuery.model_validate({
        "population": dataset.name, "metrics": [{"metric": expression, "alias": "m"}],
    })
    plan_query(query=query, bundle=ResolvedSourceBundle(population=dataset, referenced_datasets=[dataset], dialect="postgres"))


def _col(*, dataset: SlayerDataset, name: str) -> DatasetField:
    return next(c for c in dataset.fields if c.name == name)


async def _sampled(*, dataset: SlayerDataset, engine, names: tuple[str, ...] | None = None) -> dict[str, str | None]:
    """Profile ``model`` (or just ``names``) through the owner; ``{column: sampled}``."""
    cols = [c for c in dataset.fields if names is None or c.name in names]
    outcome = await ensure_samples_fresh(dataset=dataset, fields=cols, engine=engine, storage=engine.storage)
    return {c.name: c.sampled for c in outcome.fields}


class TestAggregationGate:
    @pytest.mark.parametrize("formula", [
        "count(orders.id)", "count_distinct(orders.id)", "count_distinct_approx(orders.id)", "min(orders.id)", "max(orders.id)",
    ])
    def test_sole_primary_key_accepts_count_family_and_min_max(self, formula) -> None:
        _plan(dataset=orders_dataset(), expression=formula)

    def test_sole_primary_key_refuses_sum(self) -> None:
        model = orders_dataset()
        with pytest.raises(AggregationNotAllowedError):
            _plan(dataset=model, expression="sum(orders.id)")

    def test_sole_primary_key_accepts_collect(self) -> None:
        _plan(dataset=orders_dataset(), expression="collect(orders.id)")

    @pytest.mark.parametrize("formula", ["max(order_lines.line_no)", "sum(order_lines.line_no)"])
    def test_composite_member_aggregates_by_type(self, formula) -> None:
        _plan(dataset=order_lines(), expression=formula)


class TestAllowedAggregationsValidator:
    def test_composite_member_checked_against_type_defaults(self) -> None:
        assert _col(dataset=order_lines(line_no_aggs=["sum"]), name="line_no").allowed_aggregations == ["sum"]

    def test_sole_primary_key_may_declare_max(self) -> None:
        model = SlayerDataset(
            name="t", data_source="test", sql_table="t",
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True, allowed_aggregations=["max"])],
        )
        assert _col(dataset=model, name="id").allowed_aggregations == ["max"]

    def test_sole_primary_key_still_refuses_sum(self) -> None:
        columns = [DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True, allowed_aggregations=["sum"])]
        with pytest.raises(ValidationError):
            SlayerDataset(name="t", data_source="test", sql_table="t", fields=columns)

    @pytest.mark.parametrize("backend", ["yaml", "sqlite"])
    async def test_sole_primary_key_may_declare_collect(self, tmp_path, backend: str) -> None:
        doc = {"version": CURRENT_VERSIONS["SlayerModel"], "name": "stored", "sql_table": "t", "data_source": DS,
               "fields": [{"name": "id", "datatype": "Integer", "primary_key": True,
                            "allowed_aggregations": ["count", "collect"]}]}
        storage = await raw_store(backend=backend, base=str(tmp_path), models=[doc],
                                  datasource=DatasourceConfig(name=DS, type="duckdb", database=":memory:"))
        loaded = await storage.get_dataset("stored", data_source=DS)
        assert loaded is not None
        assert _col(dataset=loaded, name="id").allowed_aggregations == ["count", "collect"]
        saved = SlayerDataset(name="saved", data_source=DS, sql_table="t", fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True, allowed_aggregations=["count", "collect"])])
        await storage.save_dataset(saved)
        assert await storage.get_dataset("saved", data_source=DS) is not None


class TestCatalogEligibility:
    def test_composite_member_gets_type_defaults(self) -> None:
        model = order_lines()
        assert "sum" in _eligible_aggregations(field=_col(dataset=model, name="line_no"), dataset=model)

    def test_sole_primary_key_gets_count_family_and_min_max(self) -> None:
        model = orders_dataset()
        eligible = _eligible_aggregations(field=_col(dataset=model, name="id"), dataset=model)
        assert {"count", "count_distinct", "min", "max"} <= eligible
        assert "sum" not in eligible

    def test_sole_primary_key_gets_collect(self) -> None:
        model = orders_dataset()
        assert "collect" in _eligible_aggregations(field=_col(dataset=model, name="id"), dataset=model)


class TestInspectSampling:
    def test_composite_member_is_a_sample_dimension(self) -> None:
        dims, _ = _choose_sample_dims(order_lines())
        assert "order_lines.status" in dims

    def test_sole_primary_key_is_not_a_sample_dimension(self) -> None:
        dims, _ = _choose_sample_dims(codes())
        assert dims == ["codes.label"]

    def test_composite_members_are_sample_measures(self) -> None:
        formulas = [m["metric"] for m in _build_sample_query_args(dataset=order_lines(), num_rows=3)["metrics"]]
        assert any("(order_lines.order_id)" in f for f in formulas)
        assert any("(order_lines.line_no)" in f for f in formulas)

    def test_sole_primary_key_is_not_a_sample_measure(self) -> None:
        formulas = [m["metric"] for m in _build_sample_query_args(dataset=orders_dataset(), num_rows=3)["metrics"]]
        assert not any("(orders.id)" in f for f in formulas)

    def test_composite_member_sample_cache_is_checked(self) -> None:
        model = order_lines()
        assert not _is_sample_cached(field=_col(dataset=model, name="status"), dataset=model)

    def test_sole_primary_key_counts_as_cached(self) -> None:
        model = codes()
        assert _is_sample_cached(field=_col(dataset=model, name="code"), dataset=model)


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for eng in make_exec_engine(request):
        await eng.storage.save_dataset(order_lines(), _validate=False)
        yield eng


class TestProbingAndProfiling:
    async def test_type_probe_includes_composite_members(self, engine) -> None:
        types = await engine.get_result_column_types("order_lines")
        assert COMPOSITE_MEMBERS <= set(types)

    async def test_type_probe_skips_sole_primary_key(self, engine) -> None:
        types = await engine.get_result_column_types("orders")
        assert "id" not in types
        assert "amount" in types

    async def test_measure_profile_includes_composite_members(self, engine) -> None:
        profile = await _sampled(dataset=order_lines(), engine=engine)
        assert all(profile[n] is not None for n in ("order_id", "line_no"))

    async def test_measure_profile_skips_sole_primary_key(self, engine) -> None:
        profile = await _sampled(dataset=orders_dataset(), engine=engine)
        assert profile["id"] is None

    async def test_profile_column_profiles_composite_members(self, engine) -> None:
        model = order_lines()
        for name in ("line_no", "status"):
            assert (await _sampled(dataset=model, engine=engine, names=(name,)))[name] is not None

    async def test_profile_column_skips_sole_primary_key(self, engine) -> None:
        assert (await _sampled(dataset=orders_dataset(), engine=engine, names=("id",)))["id"] is None

    async def test_batched_profile_includes_composite_members(self, engine) -> None:
        profile = await _sampled(dataset=order_lines(), engine=engine)
        assert all(profile[n] is not None for n in COMPOSITE_MEMBERS)

    async def test_batched_profile_skips_sole_primary_key(self, engine) -> None:
        profile = await _sampled(dataset=orders_dataset(), engine=engine)
        assert profile["id"] is None

    async def test_refresh_samples_composite_members(self, engine: SlayerQueryEngine) -> None:
        errors = await refresh_table_backed_dataset_sampled(
            dataset=order_lines(), engine=engine, storage=engine.storage,
        )
        assert errors == []
        stored = await engine.storage.get_dataset("order_lines")
        assert stored is not None
        assert all(_col(dataset=stored, name=n).sampled is not None for n in COMPOSITE_MEMBERS)

    async def test_refresh_skips_sole_primary_key(self, engine: SlayerQueryEngine) -> None:
        errors = await refresh_table_backed_dataset_sampled(
            dataset=orders_dataset(), engine=engine, storage=engine.storage,
        )
        assert errors == []
        stored = await engine.storage.get_dataset("orders")
        assert stored is not None
        assert _col(dataset=stored, name="id").sampled is None
        assert _col(dataset=stored, name="amount").sampled is not None

    async def test_inspect_samples_composite_members_not_sole_primary_key(self, engine) -> None:
        rendered = {}
        for model in (order_lines(), orders_dataset()):
            out = await render_dataset_inspection(
                dataset=model, storage=engine.storage, engine=engine, format="json",
            )
            rendered[model.name] = {c["name"]: c["sampled"] for c in json.loads(out)["fields"]}
        assert all(rendered["order_lines"][n] is not None for n in COMPOSITE_MEMBERS)
        assert rendered["orders"]["id"] is None
