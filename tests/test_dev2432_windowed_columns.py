"""A windowed field is a column of its dataset: computed over the dataset's filtered rows, then read as a column everywhere.

Spec: models/field-expressions › A windowed field is a column of its dataset; › A portable windowed field is refused
at both save doors; models/model-filters › Filters whose dataset reaches itself are refused; › Model filters are
validated wherever read and at save time.

orders (id, region, amount, ordered_at, customer_id, slot):  slot is a stored copy of rn
  1 EU 10 2024-01-01 1 1 | 2 EU 20 2024-01-02 1 2 | 3 EU 30 2024-01-03 2 3 | 4 US 40 2024-01-01 2 1
  5 US 50 2024-01-02 3 2 | 6 US 60 2024-01-03 3 3 | 7 AS 70 2024-01-01 1 1
customers (id, tier): 1 gold | 2 silver | 3 gold
slots (pos, label): 1 first | 2 second | 3 third
slot_src (label, sort_key): first 10 | second 20 | third 30
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.errors import (
    DatasetFieldCycleError,
    DatasetFilterCycleError,
    IllegalWindowInFilterError,
    UnevaluableEntityError,
    UnsafeRelationshipInputError,
)
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.core.query import SlayerQuery
from slayer.engine.cardinality.inference import CardinalityVerdict
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.ir.variables import substitute_dataset_sql_surfaces
from slayer.sql.generator import (
    _UNSAFE_WINDOWS,
    _WINDOW_LAYER,
    _prune_window_layers,
    _raiser,
    _refuse_unsafe_window_reads,
)
from slayer.sql.untrusted_projection import UNTRUSTED_HOP, mark_untrusted
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2210_fixtures import DDL as FILTER_DDL
from tests._dev2210_fixtures import seed, table_datasets
from tests._engine_helpers import seeded_exec_engine

DDL = (
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, region VARCHAR(5), amount INTEGER, ordered_at DATE, "
    "customer_id INTEGER, slot INTEGER)",
    "INSERT INTO orders VALUES (1, 'EU', 10, '2024-01-01', 1, 1), (2, 'EU', 20, '2024-01-02', 1, 2), "
    "(3, 'EU', 30, '2024-01-03', 2, 3), (4, 'US', 40, '2024-01-01', 2, 1), (5, 'US', 50, '2024-01-02', 3, 2), "
    "(6, 'US', 60, '2024-01-03', 3, 3), (7, 'AS', 70, '2024-01-01', 1, 1)",
    "CREATE TABLE customers (id INTEGER PRIMARY KEY, tier VARCHAR(10))",
    "INSERT INTO customers VALUES (1, 'gold'), (2, 'silver'), (3, 'gold')",
    "CREATE TABLE slots (pos INTEGER PRIMARY KEY, label VARCHAR(10))",
    "INSERT INTO slots VALUES (1, 'first'), (2, 'second'), (3, 'third')",
    "CREATE TABLE slot_src (label VARCHAR(10), sort_key INTEGER)",
    "INSERT INTO slot_src VALUES ('first', 10), ('second', 20), ('third', 30)",
)

RN = "ROW_NUMBER() OVER (PARTITION BY region ORDER BY ordered_at)"
RN_OF_ID = {1: 1, 2: 2, 3: 3, 4: 1, 5: 2, 6: 3, 7: 1}
#: An Ossie document whose dataset carries an OSSIE_SQL_2026 window field.
DOC_WINDOW = """version: "0.3.0.dev0"
name: m_doc
datasets:
  - name: doc_orders
    source: orders
    fields:
      - name: id
        expression:
          dialects:
            - dialect: OSSIE_SQL_2026
              expression: id
      - name: rn
        expression:
          dialects:
            - dialect: OSSIE_SQL_2026
              expression: ROW_NUMBER() OVER (ORDER BY id)
"""
#: Window inside an uncorrelated scalar subquery: 2 for every order.
SUBQUERY_WINDOW = "(SELECT COUNT(*) FROM (SELECT ROW_NUMBER() OVER (ORDER BY id) AS r FROM customers) t WHERE t.r <= 2)"


def native(name: str, text: str, *, datatype: DataType = DataType.INTEGER, **kw: Any) -> DatasetField:
    """A field whose definition is the SQLite and DuckDB dialect entry ``text``."""
    return DatasetField(name=name, datatype=datatype, expression=ExpressionObject(dialects=[
        DialectEntry(dialect="SQLITE", expression=text), DialectEntry(dialect="DUCKDB", expression=text)]), **kw)


def _col(name: str, datatype: DataType = DataType.INTEGER, **kw: Any) -> DatasetField:
    return DatasetField(name=name, datatype=datatype, **kw)


def _order_columns() -> list[DatasetField]:
    return [_col("id", primary_key=True), _col("region", DataType.STRING), _col("amount"),
            _col("ordered_at", DataType.DATE), _col("customer_id"), _col("slot")]


def _orders_like(name: str, *, fields: Sequence[DatasetField] = (), relationships: Sequence[DatasetRelationship] = (),
                 **kw: Any) -> SlayerDataset:
    kw.setdefault("sql_table", "orders")
    return SlayerDataset(name=name, data_source="test", fields=[*_order_columns(), *fields],
                         relationships=list(relationships), **kw)


def _to_customers() -> DatasetRelationship:
    return DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"], cardinality=RelationshipCardinality.MANY_TO_ONE)


def _datasets() -> list[SlayerDataset]:
    orders = _orders_like("orders", relationships=[_to_customers()], fields=[
        native("rn", RN),
        native("prev_at", "LAG(ordered_at) OVER (PARTITION BY region ORDER BY ordered_at)", datatype=DataType.DATE),
        DatasetField(name="rn10", datatype=DataType.INTEGER, expression="orders.rn * 10"),
        native("ww", "SUM(orders.rn) OVER ()"),
        native("w3", "MAX(orders.ww) OVER (PARTITION BY region)"),
        native("lvl1", "ROW_NUMBER() OVER (ORDER BY id)"),
        native("lvl2", "SUM(orders.lvl1) OVER (PARTITION BY region)"),
        native("lvl3", "RANK() OVER (ORDER BY orders.lvl2 DESC)"),
        native("tier_rn", "ROW_NUMBER() OVER (PARTITION BY customers.tier ORDER BY id)"),
        native("rn_eu", RN, filter="region = 'EU'"),
        _col("slot_eu", filter="region = 'EU'", expression="slot"),
        native("seq", "ROW_NUMBER() OVER (ORDER BY id)"),
        native("note", "'over (x)' || region", datatype=DataType.STRING),
        native("n_cust", SUBQUERY_WINDOW),
    ])
    customers = SlayerDataset(name="customers", data_source="test", sql_table="customers", fields=[
        _col("id", primary_key=True), _col("tier", DataType.STRING), native("crn", "ROW_NUMBER() OVER (ORDER BY id)")])
    slots = SlayerDataset(name="slots", data_source="test", sql_table="slots",
                          fields=[_col("pos", primary_key=True), _col("label", DataType.STRING)])
    slots_w = SlayerDataset(name="slots_w", data_source="test", sql_table="slot_src", fields=[
        _col("label", DataType.STRING), _col("sort_key"), native("pos", "ROW_NUMBER() OVER (ORDER BY sort_key)")])
    return [
        orders, customers, slots, slots_w,
        _orders_like("big_orders", fields=[native("rn", RN)], filters=["amount >= 20"]),
        _orders_like("sql_orders", fields=[native("rn", RN)], sql_table=None, sql="SELECT * FROM orders"),
        _orders_like("orders_s", fields=[
            native("rn", RN), native("by_label", "ROW_NUMBER() OVER (PARTITION BY slots.label ORDER BY orders_s.id)")],
                     relationships=[
            DatasetRelationship(to="slots", from_columns=["rn"], to_columns=["pos"], cardinality=RelationshipCardinality.MANY_TO_ONE)]),
        _orders_like("orders_wt", relationships=[
            DatasetRelationship(to="slots_w", from_columns=["slot"], to_columns=["pos"], cardinality=RelationshipCardinality.MANY_TO_ONE)]),
        _orders_like("orders_ws", fields=[native("rn", RN)], relationships=[
            DatasetRelationship(to="slots_w", from_columns=["rn"], to_columns=["pos"], cardinality=RelationshipCardinality.MANY_TO_ONE)]),
        SlayerDataset(name="slots_wc", data_source="test", sql_table="slot_src", fields=[
            _col("label", DataType.STRING), _col("sort_key"), native("pos", "ROW_NUMBER() OVER (ORDER BY sort_key)"),
            native("zero", "sort_key * 0")]),
        _orders_like("orders_wc", fields=[native("rn", RN), native("zero", "amount * 0")],
                     relationships=[DatasetRelationship(
                         to="slots_wc", from_columns=["rn", "zero"], to_columns=["pos", "zero"],
                         cardinality=RelationshipCardinality.MANY_TO_ONE)]),
        _orders_like("orders_2j", fields=[native("rn", RN), native("both", "ROW_NUMBER() OVER (PARTITION BY customers.tier, slots.label ORDER BY id)")],
                     relationships=[_to_customers(), DatasetRelationship(
                         to="slots", from_columns=["slot"], to_columns=["pos"], cardinality=RelationshipCardinality.MANY_TO_ONE)]),
        _orders_like("orders_inner", fields=[native("w", "ROW_NUMBER() OVER (PARTITION BY nomatch.tier ORDER BY id)")],
                     relationships=[DatasetRelationship(
                         to="customers", name="nomatch", from_columns=["amount"], to_columns=["id"],
                         cardinality=RelationshipCardinality.MANY_TO_ONE, join_type=JoinType.INNER)]),
    ]


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request: pytest.FixtureRequest) -> AsyncIterator[SlayerQueryEngine]:
    dialect = request.param
    async with seeded_exec_engine(dialect=dialect, seed=lambda p: seed(dialect, p, DDL), datasets=_datasets()) as (eng, _db):
        yield eng


async def _run(engine: SlayerQueryEngine, body: dict[str, Any]) -> SlayerResponse:
    return await engine.execute(SlayerQuery.model_validate(body))


def _by(resp: SlayerResponse, key: str, value: str) -> dict[Any, Any]:
    got = {row[key]: row[value] for row in resp.data}
    assert len(got) == len(resp.data), f"duplicate {key} rows: {resp.data}"
    return got


def _by_id(resp: SlayerResponse, source: str, column: str) -> dict[int, Any]:
    return {int(k): v for k, v in _by(resp, f"{source}.id", column).items()}


def _num(values: dict[Any, Any]) -> dict[Any, Any]:
    return {k: (None if v is None else int(v)) for k, v in values.items()}


def _day(value: Any) -> str | None:
    return None if value is None else str(value)[:10]


def _grouped(population: str, dimension: str, metric: str, **extra: Any) -> dict[str, Any]:
    return {"population": population, "dimensions": [dimension], "metrics": [{"metric": metric, "alias": "m"}], **extra}


def _raw(population: str, *columns: str, **extra: Any) -> dict[str, Any]:
    return {"population": population, "dimensions": [f"{population}.{c}" for c in columns],
            "distinct_dimension_values": False, **extra}


class TestPositions:
    async def test_as_a_dimension(self, engine):
        resp = await _run(engine, _grouped("orders", "orders.rn", "sum(orders.amount)"))
        assert _num(_by(resp, "orders.rn", "m")) == {1: 120, 2: 70, 3: 90}

    async def test_as_an_aggregation_source(self, engine):
        resp = await _run(engine, {"population": "orders", "metrics": [{"metric": "max(orders.rn)", "alias": "m"}]})
        assert [int(r["m"]) for r in resp.data] == [3]

    async def test_inside_an_aggregate_filter(self, engine):
        resp = await _run(engine, {"population": "orders", "metrics": [
            {"metric": "sum(orders.amount) FILTER (WHERE orders.rn = 1)", "alias": "m"}]})
        assert [int(r["m"]) for r in resp.data] == [120]

    async def test_inside_a_case(self, engine):
        resp = await _run(engine, {"population": "orders", "metrics": [
            {"metric": "sum(CASE WHEN orders.rn = 1 THEN orders.amount ELSE 0 END)", "alias": "m"}]})
        assert [int(r["m"]) for r in resp.data] == [120]

    async def test_as_a_partition_key(self, engine):
        resp = await _run(engine, {"population": "orders", "dimensions": ["orders.region", "orders.rn"],
                                   "metrics": [{"metric": "sum(orders.amount, partition_by=orders.rn)", "alias": "m"}]})
        assert len(resp.data) == 7
        assert {(int(r["orders.rn"]), int(r["m"])) for r in resp.data} == {(1, 120), (2, 70), (3, 90)}

    async def test_as_a_ranking_key(self, engine):
        resp = await _run(engine, _grouped("orders", "orders.region", "last(orders.amount, orders.rn)"))
        assert _num(_by(resp, "orders.region", "m")) == {"EU": 30, "US": 60, "AS": 70}

    async def test_as_an_order_key_of_a_grouped_query(self, engine):
        def body(key: str) -> dict[str, Any]:
            return _grouped("orders", "orders.region", "sum(orders.amount)", order=[
                {"field": key, "direction": "desc"}, {"field": "orders.region", "direction": "asc"}])

        windowed = [r["orders.region"] for r in (await _run(engine, body("orders.rn"))).data]
        stored = [r["orders.region"] for r in (await _run(engine, body("orders.slot"))).data]
        assert windowed == stored == ["EU", "US", "AS"]

    async def test_as_a_query_filter(self, engine):
        resp = await _run(engine, _grouped("orders", "orders.region", "sum(orders.amount)", filter=["orders.rn = 1"]))
        assert _num(_by(resp, "orders.region", "m")) == {"EU": 10, "US": 40, "AS": 70}

    async def test_raw_rows_under_a_query_filter_keep_their_values(self, engine):
        resp = await _run(engine, _raw("orders", "id", "rn", filter=["orders.id > 1"],
                                       order=[{"field": "orders.rn", "direction": "asc"}]))
        assert _num(_by_id(resp, "orders", "orders.rn")) == {i: RN_OF_ID[i] for i in range(2, 8)}
        assert [int(r["orders.rn"]) for r in resp.data] == [1, 1, 2, 2, 3, 3]

    async def test_as_a_time_dimension(self, engine):
        resp = await _run(engine, {"population": "orders", "metrics": [{"metric": "count(orders.*)", "alias": "m"}],
                                   "time_dimensions": [{"field": "orders.prev_at", "granularity": "day"}]})
        assert {_day(r["orders.prev_at"]): int(r["m"]) for r in resp.data} == {None: 3, "2024-01-01": 2, "2024-01-02": 2}


class TestReaders:
    async def test_a_join_targets_windowed_field_is_computed_over_its_own_rows(self, engine):
        grouped = await _run(engine, _grouped("orders", "orders.customers.crn", "sum(orders.amount)"))
        assert _num(_by(grouped, "orders.customers.crn", "m")) == {1: 100, 2: 70, 3: 110}
        raw = await _run(engine, _raw("orders", "id", "customers.crn"))
        assert _num(_by_id(raw, "orders", "orders.customers.crn")) == {1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 1}

    async def test_a_derived_field_over_a_windowed_field(self, engine):
        resp = await _run(engine, _grouped("orders", "orders.rn10", "count(orders.*)"))
        assert _num(_by(resp, "orders.rn10", "m")) == {10: 3, 20: 2, 30: 2}

    async def test_a_window_over_a_windowed_field(self, engine):
        raw = await _run(engine, _raw("orders", "id", "ww", "w3"))
        assert _num(_by_id(raw, "orders", "orders.ww")) == dict.fromkeys(range(1, 8), 13)
        assert _num(_by_id(raw, "orders", "orders.w3")) == dict.fromkeys(range(1, 8), 13)
        grouped = await _run(engine, _grouped("orders", "orders.ww", "count(orders.*)"))
        assert _num(_by(grouped, "orders.ww", "m")) == {13: 7}

    async def test_a_three_level_window_chain(self, engine):
        raw = await _run(engine, _raw("orders", "id", "lvl3"))
        assert _num(_by_id(raw, "orders", "orders.lvl3")) == {1: 5, 2: 5, 3: 5, 4: 1, 5: 1, 6: 1, 7: 4}
        grouped = await _run(engine, _grouped("orders", "orders.lvl3", "count(orders.*)"))
        assert _num(_by(grouped, "orders.lvl3", "m")) == {1: 3, 4: 1, 5: 3}

    async def test_a_window_reading_a_to_one_joined_field(self, engine):
        raw = await _run(engine, _raw("orders", "id", "tier_rn"))
        assert _num(_by_id(raw, "orders", "orders.tier_rn")) == {1: 1, 2: 2, 3: 1, 4: 2, 5: 3, 6: 4, 7: 5}
        grouped = await _run(engine, _grouped("orders", "orders.tier_rn", "count(orders.*)"))
        assert _num(_by(grouped, "orders.tier_rn", "m")) == {1: 2, 2: 2, 3: 1, 4: 1, 5: 1}

    async def test_a_window_across_an_inner_relationship_keeps_every_row(self, engine):
        raw = await _run(engine, _raw("orders_inner", "id", "w"))
        assert _num(_by_id(raw, "orders_inner", "orders_inner.w")) == {i: i for i in range(1, 8)}

    async def test_the_window_runs_over_the_datasets_filtered_rows(self, engine):
        resp = await _run(engine, _grouped("big_orders", "big_orders.rn", "sum(big_orders.amount)"))
        assert _num(_by(resp, "big_orders.rn", "m")) == {1: 130, 2: 80, 3: 60}


class TestMask:
    async def test_as_an_aggregation_source_and_a_dimension(self, engine):
        total = await _run(engine, {"population": "orders", "metrics": [{"metric": "sum(orders.rn_eu)", "alias": "m"}]})
        assert [int(r["m"]) for r in total.data] == [6]
        grouped = await _run(engine, _grouped("orders", "orders.rn_eu", "count(orders.*)"))
        assert _num(_by(grouped, "orders.rn_eu", "m")) == {1: 1, 2: 1, 3: 1, None: 4}

    async def test_as_a_query_filter(self, engine):
        resp = await _run(engine, _grouped("orders", "orders.region", "sum(orders.amount)", filter=["orders.rn_eu = 1"]))
        assert _num(_by(resp, "orders.region", "m")) == {"EU": 10}

    async def test_as_an_order_key(self, engine):
        def body(key: str) -> dict[str, Any]:
            return _grouped("orders", "orders.region", "sum(orders.amount)", order=[
                {"field": key, "direction": "desc"}, {"field": "orders.region", "direction": "asc"}])

        windowed = [r["orders.region"] for r in (await _run(engine, body("orders.rn_eu"))).data]
        stored = [r["orders.region"] for r in (await _run(engine, body("orders.slot_eu"))).data]
        assert windowed == stored
        assert windowed[0] == "EU"


class TestRelationshipKeys:
    @pytest.mark.parametrize("source", ["orders_s", "orders_wt", "orders_ws"],
                             ids=["windowed-source", "windowed-target", "windowed-both"])
    async def test_a_windowed_relationship_key(self, engine, source):
        target = "slots" if source == "orders_s" else "slots_w"
        resp = await _run(engine, _grouped(source, f"{source}.{target}.label", f"sum({source}.amount)"))
        assert _num(_by(resp, f"{source}.{target}.label", "m")) == {"first": 120, "second": 70, "third": 90}

    async def test_a_filter_across_a_fanning_hop_reads_the_computed_key(self, engine):
        resp = await _run(engine, _grouped("slots", "slots.label", "count(slots.*)", filter=["orders_s.amount >= 60"]))
        assert _num(_by(resp, "slots.label", "m")) == {"first": 1, "third": 1}

    @pytest.mark.filterwarnings("ignore")
    async def test_an_unread_window_across_an_unproven_hop_blocks_no_query(self, engine):
        await engine.save_dataset(_orders_like("orders_u", fields=[
            native("w", "ROW_NUMBER() OVER (PARTITION BY ss.label ORDER BY orders_u.id)")], relationships=[
            DatasetRelationship(name="ss", to="slots_w", from_columns=["slot"], to_columns=["sort_key"])]))
        resp = await _run(engine, {"population": "orders_u", "metrics": [{"metric": "count(orders_u.*)", "alias": "m"}]})
        assert [int(r["m"]) for r in resp.data] == [7]
        query = SlayerQuery.model_validate(_grouped("orders_u", "orders_u.w", "count(orders_u.*)"))
        with pytest.raises(UnsafeRelationshipInputError):
            await engine.execute(query)

    async def test_a_window_across_a_windowed_key_runs_a_layer_above_it(self, engine):
        resp = await _run(engine, _grouped("orders_s", "orders_s.by_label", "count(orders_s.*)"))
        assert _num(_by(resp, "orders_s.by_label", "m")) == {1: 3, 2: 3, 3: 1}


@pytest.mark.parametrize("source, target", [
    ("orders_s", "slots"), ("orders_wt", "slots_w"), ("orders_ws", "slots_w"), ("orders_wc", "slots_wc")],
    ids=["windowed-source", "windowed-target", "windowed-both", "windowed-and-computed-composite"])
async def test_cardinality_profiling_reads_a_windowed_key_as_a_column(engine, source, target):
    report = await engine.detect_relationship_cardinality(dataset=source)
    finding = next(f for f in report.findings if f.dataset == source and f.to == target)
    assert finding.detected is RelationshipCardinality.MANY_TO_ONE, finding


async def test_cardinality_profiling_skips_a_windowed_key_of_a_filtered_dataset(engine):
    await engine.storage.save_dataset(_orders_like("orders_fk", fields=[native("rn", RN)], filters=["amount >= 20"],
                                                   relationships=[DatasetRelationship(to="slots", from_columns=["rn"],
                                                                                      to_columns=["pos"])]))
    report = await engine.detect_relationship_cardinality(dataset="orders_fk")
    finding = next(f for f in report.findings if f.dataset == "orders_fk" and f.to == "slots")
    assert finding.verdict is CardinalityVerdict.SKIPPED_UNSUPPORTED, finding
    assert finding.detected is None
    assert "windowed" in (finding.note or "")


async def test_cardinality_profiling_reads_the_key_form_running_on_the_datasource(engine, request):
    own = request.node.callspec.params["engine"].upper()
    other = "DUCKDB" if own == "SQLITE" else "SQLITE"
    key = DatasetField(name="k", datatype=DataType.INTEGER, expression=ExpressionObject(dialects=[
        DialectEntry(dialect=other, expression=RN), DialectEntry(dialect=own, expression="slot")]))
    elsewhere = DatasetField(name="elsewhere", datatype=DataType.INTEGER,
                             expression=ExpressionObject(dialects=[DialectEntry(dialect=other, expression="amount")]))
    await engine.storage.save_dataset(_orders_like("orders_dk", fields=[key, elsewhere], filters=["amount >= 20"],
                                                   relationships=[DatasetRelationship(to="slots", from_columns=["k"],
                                                                                      to_columns=["pos"])]),
                                      _validate=False)
    report = await engine.detect_relationship_cardinality(dataset="orders_dk")
    finding = next(f for f in report.findings if f.dataset == "orders_dk" and f.to == "slots")
    assert finding.detected is RelationshipCardinality.MANY_TO_ONE, finding


class TestBacking:
    async def test_sql_backed_dataset(self, engine):
        grouped = await _run(engine, _grouped("sql_orders", "sql_orders.rn", "sum(sql_orders.amount)"))
        assert _num(_by(grouped, "sql_orders.rn", "m")) == {1: 120, 2: 70, 3: 90}
        filtered = await _run(engine, _grouped("sql_orders", "sql_orders.region", "sum(sql_orders.amount)",
                                               filter=["sql_orders.rn = 1"]))
        assert _num(_by(filtered, "sql_orders.region", "m")) == {"EU": 10, "US": 40, "AS": 70}
        raw = await _run(engine, _raw("sql_orders", "id", "rn", filter=["sql_orders.id > 1"]))
        assert _num(_by_id(raw, "sql_orders", "sql_orders.rn")) == {i: RN_OF_ID[i] for i in range(2, 8)}


async def _shape(engine: SlayerQueryEngine, *, field: DatasetField, swap: tuple[str, str]) -> tuple[str, str]:
    """Dry-run SQL of a grouped query on ``field`` and on its twin with ``swap`` applied to the definition."""
    text = field.expression.dialects[0].expression  # type: ignore[union-attr]
    twin = native(field.name, text.replace(*swap), datatype=field.datatype or DataType.INTEGER)
    out = []
    for f in (field, twin):
        await engine.storage.save_dataset(_orders_like("orders_shape", fields=[f]), _validate=False)
        resp = await engine.execute(SlayerQuery.model_validate(
            _grouped("orders_shape", f"orders_shape.{f.name}", "count(orders_shape.*)")), dry_run=True)
        assert resp.sql is not None
        out.append(resp.sql.replace(*swap))
    return out[0], out[1]


class TestClassification:
    async def test_over_inside_a_string_literal_is_not_a_window(self, engine):
        grouped = await _run(engine, _grouped("orders", "orders.note", "count(orders.*)"))
        assert _num(_by(grouped, "orders.note", "m")) == {"over (x)EU": 3, "over (x)US": 3, "over (x)AS": 1}
        filtered = await _run(engine, _grouped("orders", "orders.region", "count(orders.*)", filter=["orders.note LIKE '%EU'"]))
        assert _num(_by(filtered, "orders.region", "m")) == {"EU": 3}
        literal, plain = await _shape(engine, field=native("note", "'over (x)' || region", datatype=DataType.STRING),
                                      swap=("over (x)", "plain"))
        assert literal == plain

    async def test_a_window_inside_a_scalar_subquery(self, engine):
        grouped = await _run(engine, _grouped("orders", "orders.n_cust", "count(orders.*)"))
        assert _num(_by(grouped, "orders.n_cust", "m")) == {2: 7}
        filtered = await _run(engine, _grouped("orders", "orders.region", "count(orders.*)", filter=["orders.n_cust = 2"]))
        assert _num(_by(filtered, "orders.region", "m")) == {"EU": 3, "US": 3, "AS": 1}
        windowed, plain = await _shape(engine, field=native("n_cust", SUBQUERY_WINDOW),
                                       swap=("ROW_NUMBER() OVER (ORDER BY id)", "id"))
        assert windowed != plain


async def _windows(engine: SlayerQueryEngine, body: dict[str, Any]) -> list[str]:
    """The window columns the dry-run SQL of ``body`` computes."""
    resp = await engine.execute(SlayerQuery.model_validate(body), dry_run=True)
    assert resp.sql is not None
    return sorted(a.alias for a in sqlglot.parse_one(resp.sql).find_all(exp.Alias) if a.alias.startswith("__slayer_w_"))


class TestOnlyReadWindowsAreComputed:
    async def test_a_query_reading_no_windowed_field_computes_no_window(self, engine):
        assert await _windows(engine, _grouped("orders", "orders.region", "sum(orders.amount)")) == []

    async def test_a_window_over_a_windowed_field_computes_only_its_chain(self, engine):
        assert await _windows(engine, _grouped("orders", "orders.ww", "count(orders.*)")) == [
            "__slayer_w_rn", "__slayer_w_ww"]

    async def test_an_unread_windows_join_is_dropped(self, engine):
        resp = await engine.execute(SlayerQuery.model_validate(_grouped("orders", "orders.rn", "count(orders.*)")),
                                    dry_run=True)
        assert resp.sql is not None
        assert sqlglot.parse_one(resp.sql).find(exp.Join) is None, resp.sql

    async def test_every_unread_join_is_dropped_and_read_joins_are_kept(self, engine):
        for dimension, joined in (("orders_2j.rn", set()), ("orders_2j.both", {"customers", "slots"})):
            resp = await engine.execute(SlayerQuery.model_validate(_grouped("orders_2j", dimension, "count(orders_2j.*)")),
                                        dry_run=True)
            assert resp.sql is not None
            aliases = {j.this.alias_or_name for j in sqlglot.parse_one(resp.sql).find_all(exp.Join)}
            assert aliases & {"customers", "slots"} == joined, resp.sql
        both = await _run(engine, _grouped("orders_2j", "orders_2j.both", "count(orders_2j.*)"))
        assert _num(_by(both, "orders_2j.both", "m")) == {1: 5, 2: 2}


async def test_a_dataset_with_windowed_fields_saves_with_validation(engine):
    for dataset in _datasets():
        await engine.save_dataset(dataset)


async def test_a_cyclic_windowed_field_chain_is_refused(engine):
    dataset = _orders_like("orders_cyc", fields=[native("cyc_a", "SUM(orders_cyc.cyc_b) OVER ()"),
                                                 native("cyc_b", "SUM(orders_cyc.cyc_a) OVER ()")])
    with pytest.raises(DatasetFieldCycleError):
        await engine.save_dataset(dataset)
    assert await engine.storage.get_dataset("orders_cyc", data_source="test") is None


async def test_the_session_policy_restricts_the_rows_before_the_window(engine):
    policy = SessionPolicy(ruleset=ColumnFilterRuleset(column="region", value="US", on_unapplicable="pass"))
    scoped = SlayerQueryEngine(storage=engine.storage, policy=policy)
    try:
        raw = await _run(scoped, _raw("orders", "id", "seq"))
        grouped = await _run(scoped, _grouped("orders", "orders.seq", "sum(orders.amount)"))
    finally:
        scoped.close()
    assert _num(_by_id(raw, "orders", "orders.seq")) == {4: 1, 5: 2, 6: 3}
    assert _num(_by(grouped, "orders.seq", "m")) == {1: 40, 2: 50, 3: 60}


class TestPortableWindowedField:
    @staticmethod
    def _portable(**expression: Any) -> SlayerDataset:
        return _orders_like("orders_portable", fields=[DatasetField(
            name="rn", datatype=DataType.INTEGER, expression=ExpressionObject(**expression))])

    @pytest.mark.parametrize("door", ["engine", "storage"])
    async def test_both_doors_refuse_a_portable_window(self, engine, door):
        dataset = self._portable(sql=RN)
        save = engine.save_dataset if door == "engine" else engine.storage.save_dataset
        with pytest.raises(IllegalWindowInFilterError, match="rn"):
            await save(dataset)
        assert await engine.storage.get_dataset("orders_portable", data_source="test") is None

    async def test_a_stored_portable_window_fails_the_query(self, engine):
        await engine.storage.save_dataset(self._portable(sql=RN), _validate=False)
        query = SlayerQuery.model_validate(_raw("orders_portable", "id", "rn"))
        with pytest.raises(IllegalWindowInFilterError, match="rn"):
            await engine.execute(query)

    @pytest.mark.parametrize("door", ["engine", "storage"])
    async def test_a_windowed_dialect_entry_is_accepted_beside_a_portable_window(self, engine, door):
        dataset = self._portable(sql=RN, dialects=[DialectEntry(dialect="SQLITE", expression=RN),
                                                   DialectEntry(dialect="DUCKDB", expression=RN)])
        save = engine.save_dataset if door == "engine" else engine.storage.save_dataset
        await save(dataset)
        resp = await _run(engine, _grouped("orders_portable", "orders_portable.rn", "sum(orders_portable.amount)"))
        assert _num(_by(resp, "orders_portable.rn", "m")) == {1: 120, 2: 70, 3: 90}

    async def test_a_document_window_saves_and_its_query_is_unevaluable(self, engine):
        await engine.storage.save_semantic_model_text(data_source="test", name="m_doc", text=DOC_WINDOW)
        dataset = await engine.storage.get_dataset("doc_orders", data_source="test")
        assert dataset is not None
        await engine.storage.save_dataset(dataset)
        query = SlayerQuery.model_validate(_raw("doc_orders", "rn"))
        with pytest.raises(UnevaluableEntityError, match="doc_orders.rn"):
            await engine.execute(query)

    @pytest.mark.parametrize("door", ["engine", "storage"])
    async def test_an_authored_document_window_is_refused(self, engine, door):
        await engine.storage.save_semantic_model_text(data_source="test", name="m_doc", text=DOC_WINDOW)
        dataset = await engine.storage.get_dataset("doc_orders", data_source="test")
        assert dataset is not None
        added = DatasetField(name="rn2", datatype=DataType.INTEGER, expression=ExpressionObject(
            dialects=[DialectEntry(dialect="OSSIE_SQL_2026", expression="RANK() OVER (ORDER BY id)")]))
        changed = dataset.model_copy(update={"fields": [*dataset.fields, added]})
        save = engine.save_dataset if door == "engine" else engine.storage.save_dataset
        with pytest.raises(IllegalWindowInFilterError, match="rn2"):
            await save(changed)


RANKED = "rank() over (order by amount)"
CYCLE_DDL = (
    "CREATE TABLE a (id INTEGER PRIMARY KEY, flag INTEGER, b_id INTEGER)",
    "INSERT INTO a VALUES (1, 1, 1), (2, 0, 2)",
    "CREATE TABLE b (id INTEGER PRIMARY KEY, flag INTEGER, a_id INTEGER)",
    "INSERT INTO b VALUES (1, 1, 1), (2, 0, 2)",
)


def _ranked(name: str, *, fields: Sequence[DatasetField] = (), **kw: Any) -> SlayerDataset:
    return SlayerDataset(name=name, data_source="test", sql_table="orders", fields=[
        _col("id", primary_key=True), _col("status", DataType.STRING), _col("amount"), native("ranked", RANKED), *fields], **kw)


def _returns_ranked(**kw: Any) -> SlayerDataset:
    return SlayerDataset(name="returns_ranked", data_source="test", sql_table="returns",
                         fields=[_col("id", primary_key=True), _col("order_id")],
                         relationships=[DatasetRelationship(to="ranked_orders", from_columns=["order_id"], to_columns=["id"])],
                         **kw)


def _cycle_dataset(name: str, other: str, *, filters: Sequence[str] = (), extra: Sequence[DatasetField] = ()) -> SlayerDataset:
    return SlayerDataset(
        name=name, data_source="test", sql_table=name,
        fields=[_col("id", primary_key=True), _col("flag"), _col(f"{other}_id"), *extra],
        relationships=[DatasetRelationship(to=other, name=f"{name}_to_{other}", from_columns=[f"{other}_id"],
                                           to_columns=["id"], cardinality=RelationshipCardinality.MANY_TO_ONE)],
        filters=list(filters),
    )


@pytest.fixture
async def filter_engine() -> AsyncIterator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect="sqlite", seed=lambda p: seed("sqlite", p, (*FILTER_DDL, *CYCLE_DDL)),
        datasets=[*table_datasets(), _cycle_dataset("a", "b"), _cycle_dataset("b", "a")],
    ) as (eng, _db):
        yield eng


async def _save(engine: SlayerQueryEngine, dataset: SlayerDataset, *, door: str) -> None:
    if door == "engine":
        await engine.save_dataset(dataset)
    else:
        await engine.storage.save_dataset(dataset)


def _reaching_back() -> tuple[SlayerDataset, SlayerDataset]:
    """``c.cw`` partitions by ``a.flag``; ``b.w`` reads ``c.cw``: a filter of ``a`` reading ``b`` reaches back to ``a``."""
    to_one = RelationshipCardinality.MANY_TO_ONE
    c = SlayerDataset(name="c", data_source="test", sql_table="b", fields=[
        _col("id", primary_key=True), _col("flag"), _col("a_id"),
        native("cw", "ROW_NUMBER() OVER (PARTITION BY c_to_a.flag ORDER BY id)")], relationships=[
        DatasetRelationship(to="a", name="c_to_a", from_columns=["a_id"], to_columns=["id"], cardinality=to_one)])
    b = SlayerDataset(name="b", data_source="test", sql_table="b", fields=[
        _col("id", primary_key=True), _col("flag"), _col("a_id"), native("w", "SUM(b_to_c.cw) OVER ()")],
        relationships=[DatasetRelationship(to="c", name="b_to_c", from_columns=["id"], to_columns=["id"],
                                           cardinality=to_one)])
    return c, b


@pytest.mark.parametrize("door", ["engine", "storage"])
class TestModelFilterOnWindowedColumn:
    @pytest.mark.parametrize("filters, extra", [
        (["orders_win.ranked > 1"], []),
        (["orders_win.rank_plus > 1"], [native("rank_plus", "orders_win.ranked + 0")]),
    ], ids=["direct", "through-derived"])
    async def test_own_windowed_column_is_a_cycle(self, filter_engine, door, filters, extra):
        dataset = _ranked("orders_win", fields=extra, filters=filters)
        with pytest.raises(DatasetFilterCycleError) as exc:
            await _save(filter_engine, dataset, door=door)
        assert exc.value.cycle == ["orders_win", "orders_win"]
        assert "orders_win → orders_win" in str(exc.value)
        assert "ranked" in str(exc.value)
        assert await filter_engine.storage.get_dataset("orders_win", data_source="test") is None

    @pytest.mark.parametrize("reference", ["orders_win.ranked", 'orders_win."Ranked"'], ids=["unquoted", "quoted"])
    async def test_own_windowed_column_is_a_cycle_whatever_the_casing(self, filter_engine, door, reference):
        dataset = SlayerDataset(name="orders_win", data_source="test", sql_table="orders", filters=[f"{reference} > 1"],
                                fields=[_col("id", primary_key=True), _col("status", DataType.STRING), _col("amount"),
                                        native("Ranked", RANKED)])
        with pytest.raises(DatasetFilterCycleError):
            await _save(filter_engine, dataset, door=door)

    async def test_cycle_through_a_joined_windowed_columns_definition(self, filter_engine, door):
        await _save(filter_engine, _cycle_dataset("b", "a", extra=[
            native("rn", "ROW_NUMBER() OVER (PARTITION BY b_to_a.flag ORDER BY id)")]), door=door)
        dataset = _cycle_dataset("a", "b", filters=["a_to_b.rn = 1"])
        with pytest.raises(DatasetFilterCycleError) as exc:
            await _save(filter_engine, dataset, door=door)
        assert exc.value.cycle == ["a", "b", "a"]
        stored = await filter_engine.storage.get_dataset("a", data_source="test")
        assert stored is not None
        assert stored.filters == []

    async def test_cycle_through_a_chain_of_joined_windowed_columns(self, filter_engine, door):
        await _save(filter_engine, _cycle_dataset("b", "a", extra=[
            native("w1", "ROW_NUMBER() OVER (PARTITION BY b_to_a.flag ORDER BY id)"), native("w2", "SUM(b.w1) OVER ()")]),
            door=door)
        dataset = _cycle_dataset("a", "b", filters=["a_to_b.w2 >= 1"])
        with pytest.raises(DatasetFilterCycleError) as exc:
            await _save(filter_engine, dataset, door=door)
        assert exc.value.cycle[0] == "a"
        assert exc.value.cycle[-1] == "a"

    async def test_another_dialects_window_reading_back_is_not_a_cycle(self, filter_engine, door):
        w = DatasetField(name="w", datatype=DataType.INTEGER, expression=ExpressionObject(dialects=[
            DialectEntry(dialect="SQLITE", expression="ROW_NUMBER() OVER (ORDER BY id)"),
            DialectEntry(dialect="DUCKDB", expression="ROW_NUMBER() OVER (PARTITION BY b_to_a.flag ORDER BY id)")]))
        await _save(filter_engine, _cycle_dataset("b", "a", extra=[w]), door=door)
        await _save(filter_engine, _cycle_dataset("a", "b", filters=["a_to_b.w = 1"]), door=door)
        resp = await _run(filter_engine, {"population": "a", "dimensions": ["a.id"]})
        assert [int(r["a.id"]) for r in resp.data] == [1]

    async def test_an_unread_window_reading_back_blocks_no_filter(self, filter_engine, door):
        await _save(filter_engine, _cycle_dataset("b", "a", extra=[
            native("w", "ROW_NUMBER() OVER (PARTITION BY b_to_a.flag ORDER BY id)")]), door=door)
        await _save(filter_engine, _cycle_dataset("a", "b", filters=["a_to_b.flag = 1"]), door=door)
        resp = await _run(filter_engine, {"population": "a", "dimensions": ["a.id"]})
        assert [int(r["a.id"]) for r in resp.data] == [1]

    async def test_a_sibling_sharing_the_hop_of_a_refused_window_stays_readable(self, filter_engine, door):
        c, b = _reaching_back()
        b = b.model_copy(update={"fields": [*b.fields, native("safe", "SUM(b_to_c.flag) OVER ()")]})
        await _save(filter_engine, c, door=door)
        await _save(filter_engine, b, door=door)
        await _save(filter_engine, _cycle_dataset("a", "b", filters=["a_to_b.safe = 1"]), door=door)
        resp = await _run(filter_engine, {"population": "a", "dimensions": ["a.id"]})
        assert sorted(int(r["a.id"]) for r in resp.data) == [1, 2]

    async def test_an_unread_window_reaching_back_through_another_window_blocks_no_filter(self, filter_engine, door):
        c, b = _reaching_back()
        await _save(filter_engine, c, door=door)
        await _save(filter_engine, b, door=door)
        await _save(filter_engine, _cycle_dataset("a", "b", filters=["a_to_b.flag = 1"]), door=door)
        resp = await _run(filter_engine, {"population": "a", "dimensions": ["a.id"]})
        assert [int(r["a.id"]) for r in resp.data] == [1]

    async def test_a_hop_keyed_by_its_own_window_is_a_cycle(self, filter_engine, door):
        keyed = SlayerDataset(
            name="a", data_source="test", sql_table="a", filters=["a_rn.flag = 1"],
            fields=[_col("id", primary_key=True), _col("flag"), _col("b_id"), native("rn", "ROW_NUMBER() OVER (ORDER BY id)")],
            relationships=[DatasetRelationship(to="b", name="a_rn", from_columns=["rn"], to_columns=["id"],
                                               cardinality=RelationshipCardinality.MANY_TO_ONE)])
        with pytest.raises(DatasetFilterCycleError) as exc:
            await _save(filter_engine, keyed, door=door)
        assert exc.value.cycle == ["a", "a"]
        await _save(filter_engine, _cycle_dataset("a", "b", filters=["a_to_b.flag = 1"]), door=door)

    async def test_a_window_in_another_dialects_entry_is_not_a_cycle(self, filter_engine, door):
        flag = DatasetField(name="flag2", datatype=DataType.INTEGER, expression=ExpressionObject(dialects=[
            DialectEntry(dialect="DUCKDB", expression="ROW_NUMBER() OVER (ORDER BY id)"),
            DialectEntry(dialect="SQLITE", expression="flag")]))
        await _save(filter_engine, _cycle_dataset("a", "b", extra=[flag], filters=["a.flag2 = 1"]), door=door)
        resp = await _run(filter_engine, {"population": "a", "dimensions": ["a.id"]})
        assert [int(r["a.id"]) for r in resp.data] == [1]

    async def test_joined_windowed_column_is_accepted_and_evaluated(self, filter_engine, door):
        await filter_engine.storage.save_dataset(_ranked("ranked_orders"))
        await _save(filter_engine, _returns_ranked(filters=["ranked_orders.ranked > 1"]), door=door)
        resp = await _run(filter_engine, {"population": "returns_ranked",
                                          "metrics": [{"metric": "count(returns_ranked.*)", "alias": "m"}]})
        assert [int(r["m"]) for r in resp.data] == [2]


async def test_windows_of_two_datasets_reading_each_other_across_to_one_hops(filter_engine):
    await filter_engine.save_dataset(_cycle_dataset("a", "b", extra=[
        native("w", "ROW_NUMBER() OVER (PARTITION BY a_to_b.flag ORDER BY id)")]))
    await filter_engine.save_dataset(_cycle_dataset("b", "a", extra=[
        native("w", "ROW_NUMBER() OVER (PARTITION BY b_to_a.flag ORDER BY id)")]))
    ids = await _run(filter_engine, {"population": "a", "dimensions": ["a.id"]})
    assert sorted(int(r["a.id"]) for r in ids.data) == [1, 2]
    windows = await _run(filter_engine, _grouped("a", "a.w", "count(a.*)"))
    assert _num(_by(windows, "a.w", "m")) == {1: 2}


def test_variable_substitution_keeps_the_window_flag():
    dataset = _orders_like("orders_v", fields=[native("rn", RN).with_native(RN, windowed=True)], filters=["region <> '{r}'"])
    bound = substitute_dataset_sql_surfaces(dataset=dataset, variables={"r": "AS"}, bind_mode_a=lambda text, _v, **_kw: text)
    field = bound.get_field("rn")
    assert field is not None
    assert field.windowed


def test_collapsing_an_unread_window_layer_keeps_the_untrusted_mark():
    layer = exp.select("t.*").from_("tbl AS t").subquery("t")
    layer.meta[_WINDOW_LAYER] = True
    mark_untrusted(layer)
    statement = exp.select("x.id").from_("x").join(layer, on="x.id = t.id", join_type="LEFT")
    _prune_window_layers(statement)
    joined = next(statement.find_all(exp.Join)).this
    assert isinstance(joined, exp.Table)
    assert joined.meta.get(UNTRUSTED_HOP)


def _scoped_reads(*, read_from_marked: bool, error: Exception | None = None) -> exp.Select:
    """Two derived tables each joining an alias ``t``; only the first one's ``t`` leaves ``__slayer_w_w`` unmaterialised."""
    marked = exp.select("t.*").from_("tbl AS t").subquery("t")
    marked.meta[_UNSAFE_WINDOWS] = {"__slayer_w_w": _raiser(error or ValueError("unsafe"))}
    safe = exp.select("t.*").from_("tbl AS t").subquery("t")
    first = exp.select("t.id" if not read_from_marked else "t.__slayer_w_w").from_(marked).subquery("one")
    second = exp.select("t.__slayer_w_w").from_(safe).subquery("two")
    return exp.select("one.*").from_(first).join(second, join_type="CROSS")


def test_an_unsafe_window_is_refused_only_when_its_own_relation_is_read():
    _refuse_unsafe_window_reads(_scoped_reads(read_from_marked=False))
    statement = _scoped_reads(read_from_marked=True)
    with pytest.raises(ValueError, match="unsafe"):
        _refuse_unsafe_window_reads(statement)


def test_a_refusal_survives_a_copy_of_the_statement():
    copied = _scoped_reads(read_from_marked=True, error=DatasetFilterCycleError(["a", "b", "a"])).copy()
    with pytest.raises(DatasetFilterCycleError) as exc:
        _refuse_unsafe_window_reads(copied)
    assert exc.value.cycle == ["a", "b", "a"]


LONG_A = "first_relationship_with_a_rather_long_descriptive_name"
LONG_B = "second_relationship_with_a_rather_long_descriptive_name"


@pytest.mark.parametrize("leaf, read", [("dv", "v"), ("wv", "__slayer_w_wv")], ids=["derived", "windowed"])
async def test_a_field_across_a_long_joined_path_reads_the_fitted_alias(tmp_path, leaf, read):
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(DatasourceConfig(name="pg", type="postgres", host="localhost", database="x"))
    to_one = RelationshipCardinality.MANY_TO_ONE
    pg = [DialectEntry(dialect="POSTGRES", expression="ROW_NUMBER() OVER (ORDER BY v)")]
    await storage.save_dataset(SlayerDataset(name="c", data_source="pg", sql_table="c", fields=[
        _col("id", primary_key=True), _col("v"), DatasetField(name="dv", datatype=DataType.INTEGER, expression="c.v * 2"),
        DatasetField(name="wv", datatype=DataType.INTEGER, expression=ExpressionObject(dialects=pg))]), _validate=False)
    await storage.save_dataset(SlayerDataset(name="b", data_source="pg", sql_table="b", fields=[
        _col("id", primary_key=True), _col("c_id")], relationships=[DatasetRelationship(
            name=LONG_B, to="c", from_columns=["c_id"], to_columns=["id"], cardinality=to_one)]), _validate=False)
    await storage.save_dataset(SlayerDataset(name="a", data_source="pg", sql_table="a", fields=[
        _col("id", primary_key=True), _col("b_id")], relationships=[DatasetRelationship(
            name=LONG_A, to="b", from_columns=["b_id"], to_columns=["id"], cardinality=to_one)]), _validate=False)
    engine = SlayerQueryEngine(storage=storage)
    try:
        resp = await engine.execute(SlayerQuery.model_validate(
            {"population": "a", "dimensions": [f"a.{LONG_A}.{LONG_B}.{leaf}"], "metrics": ["count(a.*)"]}), dry_run=True)
    finally:
        engine.close()
    assert resp.sql is not None
    tree = sqlglot.parse_one(resp.sql, dialect="postgres")
    terminal = next(j.this.alias_or_name for j in tree.find_all(exp.Join)
                    if any(t.name == "c" for t in j.this.find_all(exp.Table)))
    projected = {c.table for s in tree.find_all(exp.Select) for p in s.expressions for c in p.find_all(exp.Column)
                 if c.name == read}
    assert projected == {terminal}, resp.sql


async def test_a_stored_own_windowed_filter_fails_the_query(filter_engine):
    await filter_engine.storage.save_dataset(_ranked("orders_win", filters=["orders_win.ranked > 1"]), _validate=False)
    with pytest.raises(DatasetFilterCycleError, match="ranked"):
        await _run(filter_engine, {"population": "orders_win", "dimensions": ["orders_win.id"]})
