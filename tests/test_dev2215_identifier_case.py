"""Unquoted names fold, quoted names match exactly, the exact spelling wins, and saves never introduce a case
collision (spec: queries/identifier-case)."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import (
    AmbiguousReferenceError,
    IllegalScopeReferenceError,
    SlayerError,
    UnknownReferenceError,
)
from slayer.core.models import Aggregation, DatasetField, DatasourceConfig, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.names import match_path
from slayer.core.query import FieldRef, ComputedDimension, QueryRefinement, SlayerQuery, refine_query
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.facade.catalog import build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2215_fixtures import (
    BACKENDS,
    TIER1,
    dev2215_engine,
    dev2215_datasets,
    ledger_dataset,
    m,
    messages,
    num,
    orders_dataset,
    q,
    recorded_warnings,
    single,
    sorted_rows,
    warnings_mentioning,
)
from tests._engine_helpers import _engine_generate

STORAGES = ("yaml", "sqlite")


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev2215_engine(request.param) as eng:
        yield eng


@pytest.fixture(params=STORAGES)
async def storage(request, tmp_path) -> StorageBackend:
    store: StorageBackend = (YAMLStorage(base_dir=str(tmp_path)) if request.param == "yaml"
                             else SQLiteStorage(db_path=os.path.join(tmp_path, "store.db")))
    await store.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
    return store


# --------------------------------------------------------------------------- #
# Unquoted names fold, quoted names match exactly
# --------------------------------------------------------------------------- #
class TestFolding:
    async def test_any_case_reaches_a_column(self, engine: SlayerQueryEngine) -> None:
        folded = await engine.execute(q(dimensions=["ORDERS.Status"], metrics=[m("SUM(Amount)")]))
        plain = await engine.execute(q(dimensions=["status"], metrics=[m("sum(amount)")]))
        assert folded.columns == plain.columns
        assert sorted_rows(folded) == sorted_rows(plain)

    async def test_quoted_names_are_exact(self, engine: SlayerQueryEngine) -> None:
        query = q(metrics=[m('sum("Amount")')])
        with pytest.raises(UnknownReferenceError):
            await engine.execute(query)
        assert num(single(await engine.execute(q(metrics=[m('sum("amount")')])))) == 160.0

    @pytest.mark.parametrize("dimension", ['"customers".REGION', 'CUSTOMERS."region"'])
    async def test_quoting_per_segment(self, engine: SlayerQueryEngine, dimension: str) -> None:
        resp = await engine.execute(q(dimensions=[dimension], metrics=[m("count(*)", "n")]))
        assert "orders.customers.region" in resp.columns

    async def test_quoted_segment_with_wrong_case_fails(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=['"Customers".region'], metrics=[m("count(*)", "n")])
        with pytest.raises(UnknownReferenceError):
            await engine.execute(query)

    @pytest.mark.parametrize("query", [
        q(dimensions=['"ORDERS".status'], metrics=[m("count(*)", "n")]),
        q(metrics=[m('count("ORDERS".*)', "n")]),
        q(time_dimensions=[{"field": '"ORDERS".created_at', "granularity": "month"}], metrics=[m("count(*)", "n")]),
    ], ids=["dimension", "star", "time-dimension"])
    async def test_a_quoted_root_qualifier_is_exact(self, engine: SlayerQueryEngine, query: Any) -> None:
        with pytest.raises(UnknownReferenceError):
            await engine.execute(query)

    @pytest.mark.parametrize("dimension", ['"status"', '"orders".status', 'orders."status"', '"orders"."status"'])
    async def test_a_quoted_dimension_is_its_column(self, engine: SlayerQueryEngine, dimension: str) -> None:
        quoted = await engine.execute(q(dimensions=[dimension], metrics=[m("count(*)", "n")]))
        plain = await engine.execute(q(dimensions=["status"], metrics=[m("count(*)", "n")]))
        assert quoted.columns == plain.columns
        assert sorted_rows(quoted) == sorted_rows(plain)

    async def test_a_quoted_time_dimension_is_its_column(self, engine: SlayerQueryEngine) -> None:
        quoted = await engine.execute(q(time_dimensions=[{"field": '"orders"."created_at"', "granularity": "month"}],
                                        metrics=[m("count(*)", "n")]))
        plain = await engine.execute(q(time_dimensions=[{"field": "created_at", "granularity": "month"}],
                                       metrics=[m("count(*)", "n")]))
        assert quoted.columns == plain.columns
        assert sorted_rows(quoted) == sorted_rows(plain)

    async def test_a_quoted_column_object_time_dimension_is_exact(self, engine: SlayerQueryEngine) -> None:
        query = q(time_dimensions=[{"field": {"name": "CREATED_AT", "quoted": [True]}, "granularity": "month"}],
                  metrics=[m("count(*)", "n")])
        with pytest.raises(UnknownReferenceError):
            await engine.execute(query)

    @pytest.mark.parametrize(("column", "error"), [('"status"', None), ('"STATUS"', UnknownReferenceError)])
    async def test_a_quoted_order_column_is_exact(self, engine: SlayerQueryEngine, column: str,
                                                  error: type[SlayerError] | None) -> None:
        query = q(metrics=[m("count(*)", "n")], dimensions=["region"], order=[{"field": column, "direction": "desc"}])
        if error is None:
            assert (await engine.execute(query)).data
        else:
            with pytest.raises(error):
                await engine.execute(query)

    async def test_a_quoted_root_qualifier_in_its_case_is_dropped(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(metrics=[m('count("orders".status)', "n")]))
        assert num(single(resp, "n")) == 6.0

    @pytest.mark.parametrize(("ref", "error"), [
        ("ITEMS.value", None), ('"items".value', None), ('"ITEMS".value', UnknownReferenceError),
        ("items.VALUE", None), ('items."value"', None), ('items."VALUE"', IllegalScopeReferenceError),
    ])
    async def test_a_list_element_reference_follows_quoting(self, ref: str, error: type[SlayerError] | None) -> None:
        stages: list[Any] = [
            {"name": "s", "population": "orders", "dimensions": ["status"], "metrics": [m("collect(note)", "items")]},
            {"population": "s", "metrics": [m(f"count({ref})", "c")]},
        ]
        async with dev2215_engine("duckdb") as eng:
            if error is None:
                assert num(single(await eng.execute(stages), "c", dataset="s")) == 5.0
            else:
                with pytest.raises(error):
                    await eng.execute(stages)

    async def test_join_names_fold(self, engine: SlayerQueryEngine) -> None:
        folded = await engine.execute(q(dimensions=["CUSTOMERS.region"], metrics=[m("count(*)", "n")]))
        plain = await engine.execute(q(dimensions=["customers.region"], metrics=[m("count(*)", "n")]))
        assert folded.columns == plain.columns
        assert sorted_rows(folded) == sorted_rows(plain)

    async def test_stage_names_fold(self, engine: SlayerQueryEngine) -> None:
        stages: list[Any] = [
            {"name": "Monthly", "population": "orders",
             "time_dimensions": [{"field": "created_at", "granularity": "month"}], "metrics": [m("count(*)", "n")]},
            {"population": "monthly", "metrics": [m("sum(n)", "t")]},
        ]
        resp = await engine.execute(stages)
        assert [num(v) for r in resp.data for k, v in r.items() if k.endswith(".t")] == [6.0]

    async def test_model_names_fold(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(population="ORDERS", metrics=[m("count(*)", "n")]))
        assert resp.columns == ["orders.n"]

    @pytest.mark.parametrize(("folded", "plain"), [
        ("RANK(SUM(amount), direction = 'desc')", "rank(sum(amount), direction = 'desc')"),
        ("CumSum(sum(amount))", "cumsum(sum(amount))"),
    ])
    async def test_transform_names_fold(self, engine: SlayerQueryEngine, folded: str, plain: str) -> None:
        td = [{"field": "created_at", "granularity": "month"}]
        a = await engine.execute(q(time_dimensions=td, metrics=[m(folded)]))
        b = await engine.execute(q(time_dimensions=td, metrics=[m(plain)]))
        assert a.sql == b.sql
        assert sorted_rows(a) == sorted_rows(b)

    async def test_model_aggregation_parameter_names_fold(self, engine: SlayerQueryEngine) -> None:
        a = await engine.execute(q(metrics=[m("TRIMMED(amount, cutoff = 25)")]))
        b = await engine.execute(q(metrics=[m("trimmed(amount, cutoff = 25)")]))
        assert num(single(a)) == num(single(b))


# --------------------------------------------------------------------------- #
# The exact spelling wins; otherwise one match or a typed ambiguity
# --------------------------------------------------------------------------- #
class TestExactSpellingWins:
    async def test_exact_spelling_in_a_colliding_model(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(population="ledger", metrics=[m("sum(Revenue)", "a"), m("sum(revenue)", "b")]))
        assert (num(single(resp, "a", dataset="ledger")), num(single(resp, "b", dataset="ledger"))) == (3.0, 30.0)

    async def test_ambiguous_spelling(self, engine: SlayerQueryEngine) -> None:
        query = q(population="ledger", metrics=[m("sum(REVENUE)")])
        with pytest.raises(AmbiguousReferenceError) as err:
            await engine.execute(query)
        text = str(err.value)
        assert "Revenue" in text
        assert "revenue" in text
        assert '"Revenue"' in text
        assert '"revenue"' in text

    async def test_quoting_resolves_the_ambiguity(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(population="ledger", metrics=[m('sum("Revenue")')]))
        assert num(single(resp, dataset="ledger")) == 3.0


# --------------------------------------------------------------------------- #
# Declared spellings are canonical
# --------------------------------------------------------------------------- #
class TestCanonicalKeys:
    async def test_case_variant_references_share_one_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(metrics=["sum(AMOUNT)", "sum(amount)"]))
        assert resp.columns == ["orders.amount_sum"]
        assert num(resp.data[0]["orders.amount_sum"]) == 160.0

    async def test_dimension_key_uses_the_declared_spelling(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["CUSTOMERS.REGION"], metrics=[m("count(*)", "n")]))
        assert "orders.customers.region" in resp.columns


# --------------------------------------------------------------------------- #
# Saves never introduce a case collision
# --------------------------------------------------------------------------- #
def _with_metric(dataset: SlayerDataset, name: str) -> SlayerDataset:
    return dataset.model_copy(update={"metrics": [*dataset.metrics, DatasetMetric(name=name, expression="sum(amount)")]})


class TestSaveCollisions:
    async def test_a_new_column_measure_collision_is_refused(self, storage: StorageBackend) -> None:
        await storage.save_dataset(orders_dataset())
        model = _with_metric(orders_dataset(), "Amount")
        with pytest.raises(SlayerError) as err:
            await storage.save_dataset(model)
        assert "amount" in str(err.value)
        assert "Amount" in str(err.value)

    async def test_a_new_aggregation_collision_is_refused(self, storage: StorageBackend) -> None:
        model = orders_dataset(aggregations=[Aggregation(name="Share", expression="AVG({value})")])
        with pytest.raises(SlayerError) as err:
            await storage.save_dataset(model)
        assert "Share" in str(err.value)
        assert "share" in str(err.value)

    async def test_a_new_join_name_collision_is_refused(self, storage: StorageBackend) -> None:
        await storage.save_dataset(dev2215_datasets()[1])
        model = orders_dataset().model_copy(update={"relationships": [
            DatasetRelationship(to="customers", name="buyer", from_columns=["customer_id"], to_columns=["id"]),
            DatasetRelationship(to="customers", name="Buyer", from_columns=["customer_id"], to_columns=["id"]),
        ]})
        with pytest.raises(SlayerError) as err:
            await storage.save_dataset(model)
        assert "buyer" in str(err.value)
        assert "Buyer" in str(err.value)

    @pytest.mark.parametrize("source_queries", [
        [{"name": "Monthly", "population": "orders", "metrics": [{"metric": "count(*)", "alias": "n"}]},
         {"name": "monthly", "population": "Monthly", "metrics": [{"metric": "sum(n)", "alias": "t"}]},
         {"population": "monthly", "metrics": [{"metric": "sum(t)", "alias": "u"}]}],
        [{"population": "orders", "metrics": [{"metric": "sum(amount)", "alias": "Total"},
                                                 {"metric": "count(*)", "alias": "total"}]}],
    ], ids=["stage-names", "result-names"])
    async def test_a_saved_query_collision_is_refused(self, storage: StorageBackend,
                                                      source_queries: list[dict[str, Any]]) -> None:
        await storage.save_dataset(orders_dataset())
        model = SlayerDataset.model_validate({"name": "saved", "data_source": "test", "source_queries": source_queries})
        with pytest.raises(SlayerError):
            await storage.save_dataset(model)

    async def test_model_names_in_a_datasource(self, storage: StorageBackend) -> None:
        await storage.save_dataset(orders_dataset())
        model = orders_dataset().model_copy(update={"name": "Orders"})
        with pytest.raises(SlayerError) as err:
            await storage.save_dataset(model)
        assert "orders" in str(err.value)
        assert "Orders" in str(err.value)

    async def test_a_grandfathered_collision_keeps_working(self, storage: StorageBackend) -> None:
        await storage.save_dataset(ledger_dataset(), _validate=False)
        with recorded_warnings() as caught:
            await storage.save_dataset(ledger_dataset())
            await storage.save_dataset(ledger_dataset().model_copy(update={"description": "edited"}))
        assert len(messages(caught, "Revenue", "revenue")) >= 2

    async def test_removing_and_reintroducing_a_collision(self, storage: StorageBackend) -> None:
        await storage.save_dataset(ledger_dataset(), _validate=False)
        without = ledger_dataset().model_copy(update={"fields": [c for c in ledger_dataset().fields if c.name != "revenue"]})
        await storage.save_dataset(without)
        model = ledger_dataset()
        with pytest.raises(SlayerError) as err:
            await storage.save_dataset(model)
        assert "Revenue" in str(err.value)
        assert "revenue" in str(err.value)

    async def test_a_grandfathered_collision_warns_on_load(self, storage: StorageBackend) -> None:
        await storage.save_dataset(ledger_dataset(), _validate=False)
        with recorded_warnings() as caught:
            await storage.get_dataset("ledger", data_source="test")
        assert messages(caught, "Revenue", "revenue")

    async def test_a_query_reading_the_model_returns_the_warning(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(population="ledger", metrics=[m('sum("Revenue")')]))
        assert warnings_mentioning(resp, "Revenue", "revenue")

    async def test_a_facade_query_reading_the_model_returns_the_warning(self, engine: SlayerQueryEngine) -> None:
        catalog = build_catalog(datasets_by_datasource={"test": dev2215_datasets()})
        result = translate("SELECT id FROM ledger", catalog)
        assert isinstance(result, QueryResult)
        assert warnings_mentioning(await engine.execute(result.query), "Revenue", "revenue")

    async def test_a_query_not_reading_the_model_is_silent(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(metrics=[m("count(*)", "n")]))
        assert not warnings_mentioning(resp, "Revenue", "revenue")


# --------------------------------------------------------------------------- #
# A query's result names never collide by case
# --------------------------------------------------------------------------- #
COLLIDING = [m("sum(amount)", "Total"), m("count(*)", "total")]


async def test_result_names_colliding_by_case_are_refused(engine: SlayerQueryEngine) -> None:
    query = q(metrics=COLLIDING)
    with pytest.raises(SlayerError) as err:
        await engine.execute(query)
    assert "Total" in str(err.value)
    assert "total" in str(err.value)


@pytest.mark.parametrize("dialect", TIER1)
async def test_result_names_colliding_by_case_are_refused_on_every_dialect(dialect: str) -> None:
    first, *rest = dev2215_datasets()
    query = q(metrics=COLLIDING)
    with pytest.raises(SlayerError):
        await _engine_generate(query=query, dataset=first, extra_datasets=rest, dialect=dialect, validate=False)


async def test_a_stored_query_with_colliding_names_keeps_running() -> None:
    saved = SlayerDataset.model_validate({"name": "saved", "data_source": "test", "source_queries": [
        {"population": "orders", "metrics": [{"metric": "sum(amount)", "alias": "Total"},
                                                {"metric": "count(*)", "alias": "total"}]}]})
    async with dev2215_engine("sqlite", datasets=[*dev2215_datasets(), saved]) as eng:
        resp = await eng.execute(q(population="saved", metrics=[m("sum(Total)", "a")]))
    assert num(single(resp, "a", dataset="saved")) == 160.0


@pytest.mark.parametrize(("parts", "quoted", "expected"), [
    (("Foo", "BAR"), (True, False), "Foo.bar"),
    (("FOO", "bar"), (True, False), None),
    (("foo", "Bar"), (False, True), None),
    (("FOO", "BAR"), (), "Foo.bar"),
])
def test_dotted_names_match_per_segment(parts: tuple[str, ...], quoted: tuple[bool, ...], expected: str | None) -> None:
    assert match_path(parts=parts, declared=["Foo.bar", "other"], quoted=quoted) == expected


def test_dotted_names_colliding_by_case_are_ambiguous() -> None:
    with pytest.raises(AmbiguousReferenceError):
        match_path(parts=("a", "b"), declared=["A.b", "a.B"], quoted=())


@pytest.mark.parametrize(("dimension", "expected"), [
    ('"Revenue"', {"name": "Revenue", "quoted": [True]}),
    ('ledger."Revenue"', {"name": "Revenue", "dataset": "ledger", "quoted": [False, True]}),
    ("Revenue", {"name": "Revenue"}),
])
def test_a_reference_dimension_is_a_column_ref_keeping_its_quoting(dimension: str, expected: dict[str, Any]) -> None:
    query = q(population="ledger", dimensions=[dimension], metrics=[m("count(*)", "n")])
    dumped = query.model_dump(mode="json", exclude_none=True)["dimensions"]
    assert dumped == [expected]
    assert SlayerQuery.model_validate(query.model_dump(mode="json")).dimensions == query.dimensions


def test_an_explicitly_named_reference_stays_computed() -> None:
    (dim,) = q(dimensions=[{"field": '"status"', "alias": "st"}]).dimensions or []
    assert isinstance(dim, ComputedDimension)


def test_a_refinement_dedupes_a_quoted_and_a_plain_reference() -> None:
    saved = q(dimensions=['"orders".status'], metrics=[m("count(*)", "n")])
    refined = refine_query(saved=saved, refinement=QueryRefinement.model_validate({"dimensions": ["status"]}))
    assert [d.full_name for d in refined.dimensions or [] if isinstance(d, FieldRef)] == ["orders.status"]


def test_a_quoted_dimension_takes_its_column_name() -> None:
    with pytest.raises(ValueError, match="used twice"):
        q(dimensions=['"status"'], time_dimensions=[{"field": "created_at", "granularity": "month", "alias": "status"}])
    q(dimensions=['"orders".status'],
      time_dimensions=[{"field": "created_at", "granularity": "month", "alias": "orders_status"}])


def test_model_lookups_fold() -> None:
    model = SlayerDataset(name="x", sql_table="orders", data_source="test",
                        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
                        metrics=[DatasetMetric(name="Total", expression="count(*)")])
    column, measure = model.get_field("ID"), model.get_metric("total")
    assert column is not None
    assert column.name == "id"
    assert measure is not None
    assert measure.name == "Total"
