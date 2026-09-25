"""Only a model's sole primary-key column is an identifier; composite-key members are ordinary columns."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from slayer.core.enums import DataType
from slayer.core.errors import AggregationNotAllowedError
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.plan import plan_query
from slayer.engine.profiling import (
    _collect_dim_profile,
    _is_sample_cached,
    profile_column,
    refresh_table_backed_model_sampled,
)
from slayer.facade.catalog import _eligible_aggregations
from slayer.inspect.model_render import (
    _build_sample_query_args,
    _choose_sample_dims,
    _collect_measure_profile,
    render_model_inspection,
)
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev1836_fixtures import make_exec_engine, orders_model

COMPOSITE_MEMBERS = {"order_id", "line_no", "status"}


def order_lines(*, line_no_aggs=None) -> SlayerModel:
    """Composite primary key ``(order_id, line_no, status)`` over the seeded ``orders`` table."""
    return SlayerModel(
        name="order_lines", data_source="test", sql_table="orders",
        columns=[
            Column(name="order_id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="line_no", sql="customer_id", type=DataType.INT, primary_key=True,
                   allowed_aggregations=line_no_aggs),
            Column(name="status", type=DataType.TEXT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
        ],
    )


def codes() -> SlayerModel:
    """Sole TEXT primary key ``code``."""
    return SlayerModel(
        name="codes", data_source="test", sql_table="segments",
        columns=[
            Column(name="code", type=DataType.TEXT, primary_key=True),
            Column(name="label", type=DataType.TEXT),
        ],
    )


def _plan(*, model: SlayerModel, formula: str) -> None:
    query = SlayerQuery.model_validate({
        "source_model": model.name, "measures": [{"formula": formula, "name": "m"}],
    })
    plan_query(query=query, bundle=ResolvedSourceBundle(source_model=model, referenced_models=[model], dialect="postgres"))


def _col(*, model: SlayerModel, name: str) -> Column:
    return next(c for c in model.columns if c.name == name)


class TestAggregationGate:
    @pytest.mark.parametrize("formula", [
        "count(id)", "count_distinct(id)", "count_distinct_approx(id)", "min(id)", "max(id)",
    ])
    def test_sole_primary_key_accepts_count_family_and_min_max(self, formula) -> None:
        _plan(model=orders_model(), formula=formula)

    def test_sole_primary_key_refuses_sum(self) -> None:
        model = orders_model()
        with pytest.raises(AggregationNotAllowedError):
            _plan(model=model, formula="sum(id)")

    @pytest.mark.parametrize("formula", ["max(line_no)", "sum(line_no)"])
    def test_composite_member_aggregates_by_type(self, formula) -> None:
        _plan(model=order_lines(), formula=formula)


class TestAllowedAggregationsValidator:
    def test_composite_member_checked_against_type_defaults(self) -> None:
        assert _col(model=order_lines(line_no_aggs=["sum"]), name="line_no").allowed_aggregations == ["sum"]

    def test_sole_primary_key_may_declare_max(self) -> None:
        model = SlayerModel(
            name="t", data_source="test", sql_table="t",
            columns=[Column(name="id", type=DataType.INT, primary_key=True, allowed_aggregations=["max"])],
        )
        assert _col(model=model, name="id").allowed_aggregations == ["max"]

    def test_sole_primary_key_still_refuses_sum(self) -> None:
        columns = [Column(name="id", type=DataType.INT, primary_key=True, allowed_aggregations=["sum"])]
        with pytest.raises(ValidationError):
            SlayerModel(name="t", data_source="test", sql_table="t", columns=columns)


class TestCatalogEligibility:
    def test_composite_member_gets_type_defaults(self) -> None:
        model = order_lines()
        assert "sum" in _eligible_aggregations(column=_col(model=model, name="line_no"), model=model)

    def test_sole_primary_key_gets_count_family_and_min_max(self) -> None:
        model = orders_model()
        eligible = _eligible_aggregations(column=_col(model=model, name="id"), model=model)
        assert {"count", "count_distinct", "min", "max"} <= eligible
        assert "sum" not in eligible


class TestInspectSampling:
    def test_composite_member_is_a_sample_dimension(self) -> None:
        dims, _ = _choose_sample_dims(order_lines())
        assert {"name": "status"} in dims

    def test_sole_primary_key_is_not_a_sample_dimension(self) -> None:
        dims, _ = _choose_sample_dims(codes())
        assert dims == [{"name": "label"}]

    def test_composite_members_are_sample_measures(self) -> None:
        formulas = [m["formula"] for m in _build_sample_query_args(model=order_lines(), num_rows=3)["measures"]]
        assert any("(order_id)" in f for f in formulas)
        assert any("(line_no)" in f for f in formulas)

    def test_sole_primary_key_is_not_a_sample_measure(self) -> None:
        formulas = [m["formula"] for m in _build_sample_query_args(model=orders_model(), num_rows=3)["measures"]]
        assert not any("(id)" in f for f in formulas)

    def test_composite_member_sample_cache_is_checked(self) -> None:
        model = order_lines()
        assert not _is_sample_cached(column=_col(model=model, name="status"), model=model)

    def test_sole_primary_key_counts_as_cached(self) -> None:
        model = codes()
        assert _is_sample_cached(column=_col(model=model, name="code"), model=model)


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for eng in make_exec_engine(request):
        await eng.storage.save_model(order_lines(), _validate=False)
        yield eng


class TestProbingAndProfiling:
    async def test_type_probe_includes_composite_members(self, engine) -> None:
        types = await engine.get_column_types("order_lines")
        assert COMPOSITE_MEMBERS <= set(types)

    async def test_type_probe_skips_sole_primary_key(self, engine) -> None:
        types = await engine.get_column_types("orders")
        assert "id" not in types
        assert "amount" in types

    async def test_measure_profile_includes_composite_members(self, engine) -> None:
        profile = await _collect_measure_profile(model=order_lines(), engine=engine)
        assert {"order_id", "line_no"} <= set(profile)

    async def test_measure_profile_skips_sole_primary_key(self, engine) -> None:
        profile = await _collect_measure_profile(model=orders_model(), engine=engine)
        assert "id" not in profile

    async def test_profile_column_profiles_composite_members(self, engine) -> None:
        model = order_lines()
        for name in ("line_no", "status"):
            assert await profile_column(model=model, column=_col(model=model, name=name), engine=engine) is not None

    async def test_profile_column_skips_sole_primary_key(self, engine) -> None:
        model = orders_model()
        assert await profile_column(model=model, column=_col(model=model, name="id"), engine=engine) is None

    async def test_batched_profile_includes_composite_members(self, engine) -> None:
        entries = await _collect_dim_profile(model=order_lines(), engine=engine)
        assert COMPOSITE_MEMBERS <= {e.name for e in entries}

    async def test_batched_profile_skips_sole_primary_key(self, engine) -> None:
        entries = await _collect_dim_profile(model=orders_model(), engine=engine)
        assert "id" not in {e.name for e in entries}

    async def test_refresh_samples_composite_members(self, engine) -> None:
        errors = await refresh_table_backed_model_sampled(
            model=order_lines(), engine=engine, storage=engine.storage,
        )
        assert errors == []
        stored = await engine.storage.get_model("order_lines")
        assert all(_col(model=stored, name=n).sampled is not None for n in COMPOSITE_MEMBERS)

    async def test_refresh_skips_sole_primary_key(self, engine) -> None:
        errors = await refresh_table_backed_model_sampled(
            model=orders_model(), engine=engine, storage=engine.storage,
        )
        assert errors == []
        stored = await engine.storage.get_model("orders")
        assert _col(model=stored, name="id").sampled is None
        assert _col(model=stored, name="amount").sampled is not None

    async def test_inspect_samples_composite_members_not_sole_primary_key(self, engine) -> None:
        rendered = {}
        for model in (order_lines(), orders_model()):
            out = await render_model_inspection(
                model=model, storage=engine.storage, engine=engine, format="json",
            )
            rendered[model.name] = {c["name"]: c["sampled"] for c in json.loads(out)["columns"]}
        assert all(rendered["order_lines"][n] is not None for n in COMPOSITE_MEMBERS)
        assert rendered["orders"]["id"] is None
