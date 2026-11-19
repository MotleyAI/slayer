"""A field's or metric's one ``expression``: its forms, which runs where, and the row-definition name rule.

Spec: models/field-expressions.
"""

from __future__ import annotations

import json
import os
import tempfile

import pytest

from slayer.core.errors import (
    DatasetFieldCycleError,
    DerivedFieldFanningError,
    FieldSqlStatementError,
    RelationshipKeyError,
    SlayerError,
    SpellingConflictError,
)
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.warnings import SlayerWarning
from slayer.storage.yaml_storage import YAMLStorage
from tests._deprecated_input_helpers import deprecated_in, deprecations, recorded
from tests._dev2175_fixtures import (
    BACKENDS,
    TOKEN,
    assert_refused,
    by_id,
    counts_by,
    customers_doc,
    dataset,
    datasource,
    dumped_field,
    engine_with,
    entry,
    fld,
    metric_value,
    orders_doc,
    rows,
    seed,
    to_customers,
)
from tests._engine_helpers import owned_query_engine

BACKEND = pytest.mark.parametrize("dialect", BACKENDS)
STATUS_IS_STATE = fld("status", "state")


# ── one expression ────────────────────────────────────────────────────────

class TestOneExpression:
    @BACKEND
    async def test_shorthand_and_object_are_stored_as_written(self, dialect: str) -> None:
        triple = {"sql": "amount * 3", "dialects": [entry("SNOWFLAKE", "amount * 3")]}
        doc = orders_doc(fld("amount"), fld("double", "amount * 2"), fld("triple", triple))
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, storage, _):
            stored = await storage.get_dataset("orders")
            assert stored is not None
            assert dumped_field(stored, "double")["expression"] == "amount * 2"
            assert dumped_field(stored, "triple")["expression"] == triple
            assert await by_id(engine, field="double") == [20, 40, 10]
            assert await by_id(engine, field="triple") == [30, 60, 15]

    @BACKEND
    async def test_a_field_without_an_expression_reads_its_own_column(self, dialect: str) -> None:
        async with engine_with(dialect=dialect, docs=[orders_doc(fld("amount"))]) as (engine, _, _):
            assert await by_id(engine, field="amount") == [10, 20, 5]

    def test_an_object_with_neither_form_is_refused(self) -> None:
        doc = orders_doc(fld("hollow", {}))
        with pytest.raises(ValueError) as caught:
            dataset(doc)
        assert_refused(caught.value, "hollow")


# ── which form runs ───────────────────────────────────────────────────────

class TestDialectSelection:
    @BACKEND
    async def test_the_datasource_dialect_entry_wins(self, dialect: str) -> None:
        n = {"sql": "amount * 2", "dialects": [entry("DUCKDB", "amount * 3")]}
        async with engine_with(dialect=dialect, docs=[orders_doc(fld("n", n))]) as (engine, _, _):
            expected = [30, 60, 15] if dialect == "duckdb" else [20, 40, 10]
            assert await by_id(engine, field="n") == expected

    @BACKEND
    async def test_an_ossie_dialect_entry_stands_in_for_sql(self, dialect: str) -> None:
        n = {"dialects": [entry("OSSIE_SQL_2026", "amount * 2")]}
        async with engine_with(dialect=dialect, docs=[orders_doc(fld("n", n))]) as (engine, _, _):
            assert await by_id(engine, field="n") == [20, 40, 10]

    async def test_a_token_matches_whatever_its_case(self) -> None:
        n = {"dialects": [entry("duckdb", "amount + 1")]}
        async with engine_with(dialect="duckdb", docs=[orders_doc(fld("n", n))]) as (engine, _, _):
            assert await by_id(engine, field="n") == [11, 21, 6]

    async def test_two_entries_for_one_dialect_are_refused(self) -> None:
        n = dataset(orders_doc(fld("n", {"dialects": [entry("DUCKDB", "amount + 1"), entry("duckdb", "amount + 2")]})))
        async with engine_with(dialect="duckdb") as (engine, _, _):
            with pytest.raises(SlayerError) as caught:
                await engine.save_dataset(n)
        assert_refused(caught.value, "n", "DUCKDB")

    async def test_no_runnable_form_is_refused(self) -> None:
        n = dataset(orders_doc(fld("n", {"dialects": [entry("SNOWFLAKE", "amount::NUMBER")]})))
        async with engine_with(dialect="duckdb") as (engine, _, _):
            with pytest.raises(SlayerError) as caught:
                await engine.save_dataset(n)
        assert_refused(caught.value, "n", "SNOWFLAKE")


# ── portable vs dialect meaning ───────────────────────────────────────────

class TestFormMeaning:
    @BACKEND
    async def test_true_division_in_portable_sql(self, dialect: str) -> None:
        async with engine_with(dialect=dialect, docs=[orders_doc(fld("q", "amount / 4"))]) as (engine, _, _):
            assert await by_id(engine, field="q") == [2.5, 5, 1.25]

    @BACKEND
    async def test_two_argument_round_breaks_ties_away_from_zero(self, dialect: str) -> None:
        doc = orders_doc(fld("r", "ROUND(amount / 4, 0)"), fld("neg", "ROUND(0 - amount / 4, 0)"))
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await by_id(engine, field="r") == [3, 5, 1]
            assert await by_id(engine, field="neg") == [-3, -5, -1]

    async def test_the_databases_division_in_a_dialect_entry(self) -> None:
        q = {"dialects": [entry("SQLITE", "amount / 4")]}
        async with engine_with(dialect="sqlite", docs=[orders_doc(fld("q", q))]) as (engine, _, _):
            assert await by_id(engine, field="q") == [2, 5, 1]

    def test_a_statement_is_refused(self) -> None:
        leak = {"name": "leak", "expression": {"dialects": [entry("DUCKDB", "SELECT pw FROM secrets")]}}
        with pytest.raises(ValueError) as caught:
            DatasetField.model_validate(leak)
        assert_refused(caught.value, "leak", kind=FieldSqlStatementError)


# ── the row-definition name rule ──────────────────────────────────────────

class TestRowDefinitionNames:
    @BACKEND
    async def test_a_self_named_field_reads_its_source_column(self, dialect: str) -> None:
        async with engine_with(dialect=dialect, docs=[orders_doc(fld("status", "UPPER(status)"))]) as (engine, _, _):
            assert await counts_by(engine, field="status") == {"PAID": 2, "OPEN": 1}

    @BACKEND
    async def test_a_source_column_whose_name_another_field_has_taken(self, dialect: str) -> None:
        doc = orders_doc(STATUS_IS_STATE, fld("status_upper", "UPPER(status)"))
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await counts_by(engine, field="status") == {"open": 1, "paid": 1, "x": 1}
            assert await by_id(engine, field="status_upper") == ["PAID", "PAID", "OPEN"]

    @BACKEND
    async def test_a_valid_ossie_dataset_keeps_ossie_meaning(self, dialect: str) -> None:
        doc = {"name": "lines", "sql_table": "lines", "data_source": "shop", "fields": [
            fld("id", primary_key=True, datatype="Integer"),
            fld("amount", {"sql": "o_totalprice"}), fld("total", {"sql": "amount * 2"})]}
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await by_id(engine, field="amount", ds="lines") == [100, 200]
            assert await by_id(engine, field="total", ds="lines") == [2, 4]

    @BACKEND
    async def test_a_path_reads_the_field(self, dialect: str) -> None:
        doc = orders_doc(STATUS_IS_STATE, fld("shout", "orders.status || '!'"))
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await by_id(engine, field="shout") == ["open!", "paid!", "x!"]

    @BACKEND
    async def test_the_source_qualifier(self, dialect: str) -> None:
        doc = orders_doc(STATUS_IS_STATE, fld("raw", "source.status"))
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await by_id(engine, field="raw") == ["paid", "paid", "open"]

    @BACKEND
    async def test_a_field_across_a_relationship(self, dialect: str) -> None:
        orders = orders_doc(fld("customer_id", datatype="Integer"), fld("cust_name", "customers.name"),
                            relationships=[to_customers()])
        async with engine_with(dialect=dialect, docs=[customers_doc(), orders]) as (engine, _, _):
            assert await by_id(engine, field="cust_name") == ["Ann", "Ann", "Bo"]

    async def test_a_field_named_source_takes_the_qualifier(self) -> None:
        ds = dataset(orders_doc(fld("source", "state"), fld("raw", "source.status")))
        async with engine_with(dialect="duckdb") as (engine, _, _):
            with pytest.raises(SlayerError) as caught:
                await engine.save_dataset(ds)
        assert_refused(caught.value, "source")

    async def test_a_field_reading_itself_by_path_is_a_cycle(self) -> None:
        ds = dataset(orders_doc(fld("status", "orders.status || ''")))
        async with engine_with(dialect="duckdb") as (engine, _, _):
            with pytest.raises(SlayerError) as caught:
                await engine.save_dataset(ds)
        assert_refused(caught.value, "status", kind=DatasetFieldCycleError)

    async def test_a_subquery_in_a_dialect_entry_is_left_as_written(self) -> None:
        min_amount = {"dialects": [entry("DUCKDB", "(SELECT MIN(o.amount) FROM orders o)")]}
        doc = orders_doc(fld("amount", "amount * 2"), fld("min_amount", min_amount))
        async with engine_with(dialect="duckdb", docs=[doc]) as (engine, _, _):
            assert await by_id(engine, field="min_amount") == [5, 5, 5]

    async def test_an_unknown_qualifier_is_left_verbatim(self) -> None:
        top = {"dialects": [entry("DUCKDB", "(SELECT MAX(main.orders.amount) FROM main.orders)")]}
        doc = orders_doc(fld("amount", "amount * 2"), fld("top", top))
        async with engine_with(dialect="duckdb", docs=[doc]) as (engine, _, _):
            assert await by_id(engine, field="top") == [20, 20, 20]
            dry = await engine.execute({"population": "orders", "dimensions": ["top"]}, dry_run=True)
            assert "main.orders.amount" in (dry.sql or "")

    @BACKEND
    async def test_a_filter_reads_source_columns(self, dialect: str) -> None:
        doc = orders_doc(STATUS_IS_STATE, fld("paid_amount", "amount", filter="status = 'paid'"))
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await by_id(engine, field="paid_amount") == [10, 20, None]


# ── arity of paths ────────────────────────────────────────────────────────

class TestPathArity:
    async def test_a_fanning_path_is_refused(self) -> None:
        orders = orders_doc(fld("customer_id", datatype="Integer"), fld("amount"))
        customers = customers_doc(fld("spend", "orders.amount"))
        customers["relationships"] = [{"to": "orders", "from_columns": ["id"], "to_columns": ["customer_id"],
                                       "cardinality": "one_to_many"}]
        ds = dataset(customers)
        async with engine_with(dialect="duckdb", docs=[orders]) as (engine, _, _):
            with pytest.raises(SlayerError) as caught:
                await engine.save_dataset(ds)
        assert_refused(caught.value, "spend", "orders.amount", kind=DerivedFieldFanningError)

    async def test_a_cycle_through_both_forms(self) -> None:
        doc = orders_doc(fld("amount"), fld("a", "orders.b + 1"),
                         fld("b", {"dialects": [entry("DUCKDB", "orders.a * 2")]}))
        ds = dataset(doc)
        async with engine_with(dialect="duckdb") as (engine, _, _):
            with pytest.raises(SlayerError) as caught:
                await engine.save_dataset(ds)
        assert_refused(caught.value, "a", kind=DatasetFieldCycleError)
        async with engine_with(dialect="duckdb", docs=[doc], validate=False) as (engine, _, _):
            with pytest.raises(SlayerError) as at_query:
                await by_id(engine, field="a")
        assert_refused(at_query.value, "a", kind=DatasetFieldCycleError)


# ── physical readers ──────────────────────────────────────────────────────

class TestPhysicalReader:
    @BACKEND
    async def test_a_renamed_reader_is_a_base_field(self, dialect: str) -> None:
        orders = orders_doc(fld("customer_ref", "customer_id", datatype="Integer"),
                            fld("ghost", "ghost_col", datatype="Integer"),
                            relationships=[to_customers(from_columns=["customer_ref"])])
        async with engine_with(dialect=dialect, docs=[customers_doc(), orders], validate=False) as (engine, _, _):
            out = await rows(engine, {"population": "orders", "dimensions": ["customers.name"],
                                      "metrics": [{"metric": "count(*)", "alias": "n"}]})
            assert {r["orders.customers.name"]: r["orders.n"] for r in out} == {"Ann": 2, "Bo": 1}
            drift = json.dumps([e.model_dump(mode="json") for e in await engine.validate_datasets(data_source="shop")])
            assert "ghost_col" in drift
            assert "customer_ref" not in drift

    async def test_a_filtered_reader_is_not_a_base_field(self) -> None:
        orders = orders_doc(fld("paid_ref", "customer_id", datatype="Integer", filter="status = 'paid'"),
                            relationships=[to_customers(from_columns=["paid_ref"])])
        with pytest.raises(ValueError) as caught:
            dataset(orders)
        assert_refused(caught.value, "paid_ref", kind=RelationshipKeyError)


# ── typing ────────────────────────────────────────────────────────────────

@BACKEND
async def test_a_source_column_is_typed_from_the_live_column(dialect: str) -> None:
    async with engine_with(dialect=dialect, docs=[orders_doc(fld("q", "amount / 4"))]) as (engine, storage, _):
        stored = await storage.get_dataset("orders")
        assert stored is not None
        assert stored.get_field("q") is not None
        values = await by_id(engine, field="q")
    assert values == [2.5, 5, 1.25]
    assert all(isinstance(v, float) for v in values)


# ── metric expressions ────────────────────────────────────────────────────

class TestMetricExpression:
    @BACKEND
    async def test_the_source_qualifier_past_a_renamed_field(self, dialect: str) -> None:
        doc = orders_doc(STATUS_IS_STATE, metrics=[
            {"name": "paid_rows", "expression": "count(*) FILTER (WHERE source.status = 'paid')"},
            {"name": "paid_state", "expression": "count(*) FILTER (WHERE status = 'paid')"}])
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await metric_value(engine, metric="paid_rows") == 2
            assert await metric_value(engine, metric="paid_state") == 1

    async def test_a_metrics_own_dialect_entry_is_refused(self) -> None:
        p = {"sql": "sum(amount)", "dialects": [entry("DUCKDB", "sum(amount)")]}
        ds = dataset(orders_doc(fld("amount"), metrics=[{"name": "p", "expression": p}]))
        async with engine_with(dialect="duckdb") as (engine, _, _):
            with pytest.raises(SlayerError) as caught:
                await engine.save_dataset(ds)
        assert_refused(caught.value, "p", "sql")

    async def test_another_dialects_entry_is_kept(self) -> None:
        p = {"sql": "sum(amount)", "dialects": [entry("SNOWFLAKE", "SUM(amount)")]}
        doc = orders_doc(fld("amount", datatype="Integer"), metrics=[{"name": "p", "expression": p}])
        async with engine_with(dialect="duckdb", docs=[doc]) as (engine, storage, _):
            assert await metric_value(engine, metric="p") == 35
            stored = await storage.get_dataset("orders")
        assert stored is not None
        (metric,) = stored.model_dump(mode="json", exclude_none=True)["metrics"]
        assert metric["expression"] == p


# ── checks that need the dialect ──────────────────────────────────────────

async def test_saved_without_its_datasource_refused_when_queried() -> None:
    doc = orders_doc(fld("n", {"dialects": [entry("DUCKDB", "amount + 1")]}))
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "shop.sqlite")
        seed(db_path, dialect="sqlite")
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        await storage.save_dataset(dataset(doc))
        async with owned_query_engine(storage=storage, datasources=[datasource(dialect="sqlite", db_path=db_path)]) as engine:
            with pytest.raises(SlayerError) as caught:
                await by_id(engine, field="n")
    assert_refused(caught.value, "n", "DUCKDB")


# ── the deprecated field sql ──────────────────────────────────────────────

class TestFieldSqlConverts:
    async def test_a_bare_column(self) -> None:
        out: list[SlayerWarning] = []
        async with engine_with(dialect="duckdb") as (engine, storage, _):
            with recorded() as caught:
                await engine.save_dataset(dataset(orders_doc({"name": "total", "sql": "amount"})), warnings_out=out)
            stored = await storage.get_dataset("orders")
        assert stored is not None
        assert dumped_field(stored, "total")["expression"] == "amount"
        assert "sql" not in dumped_field(stored, "total")
        (warning,) = deprecated_in(out)
        assert "sql" in warning.old
        assert "expression" in warning.new
        assert warning.removal == "3.0"
        assert len(deprecations(caught)) == 1

    @BACKEND
    async def test_a_sibling_reference_is_qualified(self, dialect: str) -> None:
        doc = orders_doc(fld("amount"), {"name": "doubled", "sql": "amount * 2"}, {"name": "ratio", "sql": "doubled / 4"})
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, storage, _):
            stored = await storage.get_dataset("orders")
            assert stored is not None
            assert dumped_field(stored, "ratio")["expression"] == {
                "dialects": [entry(TOKEN[dialect], "orders.doubled / 4")]}
            expected = [5.0, 10.0, 2.5] if dialect == "duckdb" else [5, 10, 2]
            assert await by_id(engine, field="ratio") == expected

    @BACKEND
    async def test_a_bare_alias_of_a_sibling_field(self, dialect: str) -> None:
        doc = orders_doc(fld("amount"), {"name": "doubled", "sql": "amount * 2"}, {"name": "alias", "sql": "doubled"})
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, storage, _):
            stored = await storage.get_dataset("orders")
            assert stored is not None
            assert dumped_field(stored, "alias")["expression"] == "orders.doubled"
            assert await by_id(engine, field="alias") == [20, 40, 10]

    def test_both_keys_are_refused(self) -> None:
        doc = orders_doc({"name": "total", "sql": "amount", "expression": "amount"})
        with pytest.raises(ValueError) as caught:
            dataset(doc)
        assert_refused(caught.value, "sql", "expression", kind=SpellingConflictError)


class TestFieldSqlWaitsForItsDialect:
    async def test_constructed_in_python(self) -> None:
        with recorded() as caught:
            doubled = DatasetField(name="doubled", sql="amount * 2")
        assert len([w for w in caught if issubclass(w.category, FutureWarning)]) == 1
        assert doubled.sql == "amount * 2"
        ds = SlayerDataset(name="orders", sql_table="orders", data_source="shop",
                           fields=[DatasetField(name="id", primary_key=True), doubled])
        async with engine_with(dialect="duckdb") as (engine, storage, _):
            await engine.save_dataset(ds)
            stored = await storage.get_dataset("orders")
            assert stored is not None
            assert dumped_field(stored, "doubled")["expression"] == {"dialects": [entry("DUCKDB", "amount * 2")]}
            assert await by_id(engine, field="doubled") == [20, 40, 10]

    async def test_saved_where_no_dialect_is_known(self) -> None:
        with recorded():
            doubled = DatasetField(name="doubled", sql="amount * 2")
        ds = SlayerDataset(name="orders", sql_table="orders", data_source="shop",
                           fields=[DatasetField(name="id", primary_key=True), doubled])
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "shop.duckdb")
            seed(db_path, dialect="duckdb")
            storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
            await storage.save_dataset(ds)
            stored = await storage.get_dataset("orders")
            assert stored is not None
            assert dumped_field(stored, "doubled").get("sql") == "amount * 2"
            assert "expression" not in dumped_field(stored, "doubled")
            async with owned_query_engine(
                    storage=storage, datasources=[datasource(dialect="duckdb", db_path=db_path)]) as engine:
                assert await by_id(engine, field="doubled") == [20, 40, 10]
