"""Row-level expressions in value positions are homed or refused (spec: queries/semantics › Row-level expressions
are homed or refused; queries/time-dimensions › Time dimensions accept row-level expressions).

Appendix B seed of ``tests/_dev2212_fixtures.py`` plus ``orders.shipped_at``, ``orders.eff_col`` (=
``coalesce(shipped_at, customers.signup_ts)``), ``returns.returned_at`` and ``customers.signup_ts``::

    orders.shipped_at: O1 2025-01-12 | O2 NULL | O3 2025-01-25 | O4 2025-03-01
    returns.returned_at: R1 2025-02-05 | R2 2025-03-10
    customers.signup_ts: C1 2024-11-01 | C2 NULL | C3 2024-12-15 | C4 2025-02-01
"""

from __future__ import annotations

from typing import Any, Optional

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import (
    ModeASqlParseError,
    SlayerError,
    UnrelatedDatasetError,
    UnresolvableDimensionRelationshipError,
    UnsafeRelationshipInputError,
)
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1945_fixtures import UNPARSEABLE_SQL
from tests._dev2119_fixtures import bucket
from tests._dev2212_fixtures import (
    BACKENDS,
    customers_dataset,
    kinds,
    orders_dataset,
    q,
    returns_dataset,
    rows,
    run,
    seed_engine,
    table,
    tickets_dataset,
    to_customers,
    visits_dataset,
)

R = "customers.region"
SIBLINGS = "orders.amount * returns.amount"
COUNT = {"metric": "count(customers.*)", "alias": "m"}
REMEDY = "orders.amount * sum(returns.amount, partition_by=[customers.customer_id])"
PARENT_CHILD = "orders.amount * (customers.region = 'EAST')"

EXTRA_STATEMENTS = (
    "ALTER TABLE orders ADD COLUMN shipped_at DATE",
    "ALTER TABLE orders ADD COLUMN eff_col DATE",
    "ALTER TABLE returns ADD COLUMN returned_at DATE",
    "ALTER TABLE customers ADD COLUMN signup_ts DATE",
    "UPDATE orders SET shipped_at = '2025-01-12', eff_col = '2025-01-12' WHERE order_id = 'O1'",
    "UPDATE orders SET eff_col = '2024-11-01' WHERE order_id = 'O2'",
    "UPDATE orders SET shipped_at = '2025-01-25', eff_col = '2025-01-25' WHERE order_id = 'O3'",
    "UPDATE orders SET shipped_at = '2025-03-01', eff_col = '2025-03-01' WHERE order_id = 'O4'",
    "UPDATE returns SET returned_at = '2025-02-05' WHERE return_id = 'R1'",
    "UPDATE returns SET returned_at = '2025-03-10' WHERE return_id = 'R2'",
    "UPDATE customers SET signup_ts = '2024-11-01' WHERE customer_id = 'C1'",
    "UPDATE customers SET signup_ts = '2024-12-15' WHERE customer_id = 'C3'",
    "UPDATE customers SET signup_ts = '2025-02-01' WHERE customer_id = 'C4'",
)


def _plus(ds: SlayerDataset, *fields: DatasetField) -> SlayerDataset:
    return ds.model_copy(update={"fields": [*ds.fields, *fields]})


def _date(name: str) -> DatasetField:
    return DatasetField(name=name, datatype=DataType.DATE)


def _datasets(*, customers: Optional[SlayerDataset] = None, orders: Optional[SlayerDataset] = None,
              ) -> list[SlayerDataset]:
    customers = _plus(customers or customers_dataset(), _date("signup_ts"))
    orders = _plus(orders or orders_dataset(), _date("shipped_at"), _date("eff_col"),
                   DatasetField(name="amount_x2", datatype=DataType.FLOAT, expression="amount * 2"))
    returns = _plus(returns_dataset(), _date("returned_at"))
    return [customers, orders, returns, visits_dataset(), tickets_dataset()]


def _engine(backend: str, **datasets: Any):
    return seed_engine(backend, datasets=_datasets(**datasets), extra_statements=EXTRA_STATEMENTS)


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


async def _refused(eng: SlayerQueryEngine, query: SlayerQuery | list[SlayerQuery | dict], *,
                   error: type[Exception] = UnsafeRelationshipInputError) -> Any:
    with pytest.raises(error) as ei:
        await eng.execute(query, dry_run=True)
    return ei.value


def _names(err: UnsafeRelationshipInputError, *, position: str, operands: tuple[str, ...],
           hops: tuple[str, ...]) -> None:
    """The error names the position, the operands, their hops and a remedy."""
    assert position in (err.location or ""), err
    for name in (*operands, *hops):
        assert name in err.summary, (name, err.summary)
    assert err.suggestion, err


def _siblings_x(**kw: Any) -> SlayerQuery:
    return q(**{"population": "customers", "dimensions": [{"field": SIBLINGS, "alias": "x"}],
                "metrics": [COUNT], **kw})


def _bucketed(resp, key: str, value: str = "m", *, width: int = 7) -> dict:
    out = {bucket(r[key], width=width): float(r[value]) for r in resp.data}
    assert len(out) == len(resp.data), resp.data
    return out


# --------------------------------------------------------------------------- #
# Refusals
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("mode", [None, "associate", "error", "broadcast"])
async def test_sibling_operands_in_a_dimension_are_refused(backend: str, mode: Optional[str]) -> None:
    async with _engine(backend) as eng:
        err = await _refused(eng, _siblings_x(**({"to_many_handling": mode} if mode else {})))
    _names(err, position="dimension 'x'", operands=("orders.amount", "returns.amount"),
           hops=("to orders", "to returns", "customers"))


def _stage_list() -> list[SlayerQuery | dict]:
    return [q(name="per_customer", population="customers", dimensions=["customers.customer_id"], metrics=[COUNT]),
            q(population="per_customer", dimensions=[{"field": SIBLINGS, "alias": "x"}],
              metrics=[{"metric": "count(per_customer.*)", "alias": "n"}])]


@pytest.mark.parametrize("query", [
    pytest.param(lambda: q(dimensions=[{"field": SIBLINGS, "alias": "x"}], metrics=[COUNT]), id="inferred"),
    pytest.param(lambda: q(population="customers", dimensions=[{"field": SIBLINGS, "alias": "x"}]), id="no-metrics"),
    pytest.param(lambda: q(population="customers", dimensions=[{"field": SIBLINGS, "alias": "x"}],
                           distinct_dimension_values=False), id="raw-rows"),
    pytest.param(_stage_list, id="second-stage"),
])
async def test_refused_whatever_the_querys_other_parts(backend: str, query) -> None:
    async with _engine(backend) as eng:
        err = await _refused(eng, query())
    _names(err, position="dimension 'x'", operands=("orders.amount", "returns.amount"), hops=())


async def test_sibling_operands_in_an_order_key_are_refused_before_the_wrap(backend: str) -> None:
    async with _engine(backend) as eng:
        err = await _refused(eng, q(population="customers", dimensions=[R], metrics=[COUNT],
                                    order=[{"field": SIBLINGS, "direction": "desc"}]))
    _names(err, position="order", operands=("orders.amount", "returns.amount"), hops=())
    assert "min(" not in str(err).lower()
    assert "max(" not in str(err).lower()


@pytest.mark.parametrize("constituent", [
    "sum(returns.amount, partition_by=[returns.return_id])",
    "rank(sum(returns.amount, partition_by=[returns.return_id]), direction='desc')",
], ids=["aggregate", "transform"])
async def test_a_constituent_grained_on_a_sibling_is_refused(backend: str, constituent: str) -> None:
    async with _engine(backend) as eng:
        err = await _refused(eng, q(population="customers", metrics=[COUNT],
                                    dimensions=[{"field": f"orders.amount * {constituent}", "alias": "x"}]))
    _names(err, position="dimension 'x'", operands=("orders.amount", "returns.return_id"), hops=())


async def test_a_derived_operand_is_judged_by_its_closure(backend: str) -> None:
    async with _engine(backend) as eng:
        err = await _refused(eng, q(population="customers", metrics=[COUNT],
                                    dimensions=[{"field": "orders.amount_x2 * returns.amount", "alias": "x"}]))
    _names(err, position="dimension 'x'", operands=("amount_x2", "returns.amount"), hops=())


def _customers_reading_orders() -> SlayerDataset:
    return _plus(customers_dataset(), DatasetField(name="ord_amt", datatype=DataType.FLOAT, expression="orders.amount"))


@pytest.mark.parametrize("field", ["customers.ord_amt", "customers.region || customers.ord_amt"],
                         ids=["alone", "in-an-expression"])
async def test_a_derived_operand_reading_across_an_unproven_hop_is_refused(backend: str, field: str) -> None:
    orders = orders_dataset(relationship=to_customers(cardinality=None))
    async with _engine(backend, customers=_customers_reading_orders(), orders=orders) as eng:
        err = await _refused(eng, q(population="customers", metrics=[COUNT],
                                    dimensions=[{"field": field, "alias": "x"}]))
    _names(err, position="dimension 'x'", operands=("ord_amt",), hops=("to orders",))


def _customers_with_tags(*fields: DatasetField) -> SlayerDataset:
    """``customers`` many-to-many to ``tags`` on the non-unique ``segment``."""
    rel = DatasetRelationship(to="tags", from_columns=["segment"], to_columns=["segment"],
                              cardinality=RelationshipCardinality.MANY_TO_MANY)
    return _plus(customers_dataset(relationships=[rel]), *fields)


TAGS = SlayerDataset(name="tags", sql_table="tags", data_source=customers_dataset().data_source, fields=[
    DatasetField(name="segment", datatype=DataType.STRING), DatasetField(name="tag", datatype=DataType.STRING)])
TAG_STATEMENTS = (
    "CREATE TABLE tags (segment VARCHAR, tag VARCHAR)",
    "INSERT INTO tags SELECT DISTINCT segment, 'a' FROM customers WHERE segment IS NOT NULL",
    "INSERT INTO tags SELECT DISTINCT segment, 'b' FROM customers WHERE segment IS NOT NULL",
)


def _tags_engine(backend: str, customers: SlayerDataset):
    return seed_engine(backend, datasets=[*_datasets(customers=customers), TAGS],
                       extra_statements=(*EXTRA_STATEMENTS, *TAG_STATEMENTS))


async def test_a_derived_operand_reading_locally_and_across_a_many_to_many_hop_is_refused(backend: str) -> None:
    customers = _customers_with_tags(DatasetField(name="mix", datatype=DataType.STRING, expression="region || tags.tag"))
    async with _tags_engine(backend, customers) as eng:
        err = await _refused(eng, q(population="customers", metrics=[COUNT],
                                    dimensions=[{"field": "customers.mix", "alias": "x"}]))
    _names(err, position="dimension 'x'", operands=("mix",), hops=("to tags",))


async def test_a_derived_operand_reading_only_across_a_many_to_many_hop_is_homed_there(backend: str) -> None:
    customers = _customers_with_tags(DatasetField(name="only", datatype=DataType.STRING, expression="tags.tag"))
    async with _tags_engine(backend, customers) as eng:
        resp = await run(eng, population="customers", metrics=[COUNT],
                         dimensions=[{"field": "customers.only", "alias": "x"}])
    assert kinds(resp, "associated")


@pytest.mark.parametrize("extra", [
    pytest.param({"dimensions": [{"field": "orders.bad_ts", "alias": "x"}]}, id="dimension"),
    pytest.param({"time_dimensions": [{"field": "coalesce(orders.bad_ts, orders.ordered_at)", "granularity": "month",
                                       "alias": "x"}]}, id="time-dimension"),
])
async def test_an_unanalysable_derived_operand_keeps_its_resolution_error(backend: str, extra: dict) -> None:
    bad = DatasetField(name="bad_ts", datatype=DataType.DATE, expression=ExpressionObject(dialects=[
        DialectEntry(dialect=d, expression=UNPARSEABLE_SQL) for d in ("DUCKDB", "SQLITE")]))
    async with _engine(backend, orders=_plus(orders_dataset(), bad)) as eng:
        err = await _refused(eng, q(population="customers", metrics=[COUNT], **extra), error=ModeASqlParseError)
    assert UNPARSEABLE_SQL in str(err)


async def test_an_unproven_hop_determines_nothing(backend: str) -> None:
    async with _engine(backend, customers=customers_dataset(keyed=False),
                       orders=orders_dataset(relationship=to_customers(cardinality=None))) as eng:
        err = await _refused(eng, q(population="customers", metrics=[COUNT],
                                    dimensions=[{"field": PARENT_CHILD, "alias": "x"}]))
    _names(err, position="dimension 'x'", operands=("orders.amount", "customers.region"), hops=("to orders",))


async def test_sibling_leaves_in_a_time_dimension_fail_closed(backend: str) -> None:
    async with _engine(backend) as eng:
        err = await _refused(eng, q(population="customers", metrics=[COUNT], time_dimensions=[
            {"field": "coalesce(orders.shipped_at, returns.returned_at)", "granularity": "month", "alias": "t"}]))
    _names(err, position="time dimension", operands=("orders.shipped_at", "returns.returned_at"),
           hops=("to orders", "to returns"))


# --------------------------------------------------------------------------- #
# Product hop first
# --------------------------------------------------------------------------- #
def _orders_reading_tickets() -> SlayerDataset:
    return _plus(orders_dataset(),
                 DatasetField(name="tix_len", datatype=DataType.INTEGER, expression="length(tickets.priority)"))


@pytest.mark.parametrize("extra", [
    pytest.param({"dimensions": [{"field": "orders.amount * length(tickets.priority)", "alias": "x"}]},
                 id="dimension"),
    pytest.param({"time_dimensions": [{"field": "case when tickets.priority = 'high' then orders.ordered_at end",
                                       "granularity": "month", "alias": "t"}]}, id="time-dimension"),
    pytest.param({"dimensions": [R], "order": [{"field": "orders.amount * length(tickets.priority)"}]},
                 id="order-key"),
])
async def test_an_unrelated_operand_keeps_the_product_hop_error(backend: str, extra: dict) -> None:
    async with _engine(backend) as eng:
        err = await _refused(eng, q(population="customers", metrics=[COUNT], **extra), error=UnrelatedDatasetError)
    assert "tickets" in str(err)
    assert "customers" in str(err)


@pytest.mark.xfail(strict=True, raises=UnresolvableDimensionRelationshipError,
                   reason="DEV-2445 will refuse a field reading an unreachable dataset at save time")
async def test_an_unrelated_derived_field_keeps_the_product_hop_error(backend: str) -> None:
    async with _engine(backend, orders=_orders_reading_tickets()) as eng:
        err = await _refused(eng, q(population="customers", metrics=[COUNT],
                                    dimensions=[{"field": "orders.tix_len * returns.amount", "alias": "x"}]),
                             error=UnrelatedDatasetError)
    assert "tickets" in str(err)
    assert "customers" in str(err)


# --------------------------------------------------------------------------- #
# Legal shapes
# --------------------------------------------------------------------------- #
async def test_the_remedy_is_accepted(backend: str) -> None:
    async with _engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=[{"field": REMEDY, "alias": "x"}], metrics=[COUNT])
    assert table(resp, "x", "m") == rows((2500, 1), (5000, 1), (None, 3))
    assert kinds(resp, "associated")


async def test_parent_and_child_operands_in_a_dimension_are_homed_on_the_child(backend: str) -> None:
    async with _engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=[{"field": PARENT_CHILD, "alias": "x"}],
                         metrics=[COUNT])
    assert table(resp, "x", "m") == rows((0, 1), (50, 1), (100, 1), (200, 1), (None, 1))
    assert kinds(resp, "associated")


async def test_a_parent_and_child_expression_reads_null_without_a_home_row(backend: str) -> None:
    def query(field: str) -> SlayerQuery:
        return q(population="customers", metrics=[COUNT], dimensions=[{"field": field, "alias": "t"}])
    async with _engine(backend) as eng:
        by_expr = await eng.execute(query("coalesce(orders.shipped_at, customers.signup_ts)"))
        by_col = await eng.execute(query("orders.eff_col"))
    assert _bucketed(by_expr, "t", width=10) == {
        "2024-11-01": 1, "2025-01-12": 1, "2025-01-25": 1, "2025-03-01": 1, None: 1}
    assert _bucketed(by_expr, "t", width=10) == _bucketed(by_col, "t", width=10)
    assert kinds(by_expr, "associated")


async def test_a_derived_child_operand_reads_null_without_a_home_row(backend: str) -> None:
    # Declared on customers, but its definition reaches orders: the home is orders.
    customers = _plus(customers_dataset(),
                      DatasetField(name="ord_shipped", datatype=DataType.DATE, expression="orders.shipped_at"))

    def query(field: str) -> SlayerQuery:
        return q(population="customers", metrics=[COUNT], dimensions=[{"field": field, "alias": "t"}])
    async with _engine(backend, customers=customers) as eng:
        by_expr = await eng.execute(query("coalesce(customers.ord_shipped, customers.signup_ts)"))
        by_col = await eng.execute(query("orders.eff_col"))
    assert _bucketed(by_expr, "t", width=10) == _bucketed(by_col, "t", width=10)


async def test_a_parent_and_child_dimension_broadcasts_under_broadcast(backend: str) -> None:
    async with _engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=[{"field": PARENT_CHILD, "alias": "x"}],
                         metrics=[COUNT], to_many_handling="broadcast")
    assert table(resp, "x", "m") == rows((0, 4), (50, 4), (100, 4), (200, 4), (None, 4))
    assert kinds(resp, "broadcast")


async def test_a_parent_and_child_dimension_is_refused_by_error_mode_not_as_homeless(backend: str) -> None:
    async with _engine(backend) as eng:
        err = await _refused(eng, q(population="customers", dimensions=[{"field": PARENT_CHILD, "alias": "x"}],
                                    metrics=[COUNT], to_many_handling="error"), error=SlayerError)
    assert not isinstance(err, UnsafeRelationshipInputError)
    assert "error mode" in str(err)


async def test_parent_and_child_operands_in_a_time_dimension_are_homed_on_the_child(backend: str) -> None:
    td = {"field": "case when customers.region = 'EAST' then orders.ordered_at end", "granularity": "month",
          "alias": "t"}
    async with _engine(backend) as eng:
        resp = await run(eng, population="customers", time_dimensions=[td], metrics=[COUNT])
    assert _bucketed(resp, "t") == {"2025-01": 2, "2025-02": 1, None: 2}
    assert kinds(resp, "associated")


async def test_a_parent_and_a_child_time_leaf_equal_a_child_column(backend: str) -> None:
    def query(field: str) -> SlayerQuery:
        return q(population="customers", metrics=[COUNT],
                 time_dimensions=[{"field": field, "granularity": "month", "alias": "t"}])
    async with _engine(backend) as eng:
        by_expr = await eng.execute(query("coalesce(orders.shipped_at, customers.signup_ts)"))
        by_col = await eng.execute(query("orders.eff_col"))
    assert _bucketed(by_expr, "t") == {"2024-11": 1, "2025-01": 2, "2025-03": 1, None: 1}
    assert _bucketed(by_expr, "t") == _bucketed(by_col, "t")
    assert kinds(by_expr, "associated")


@pytest.mark.parametrize("pk", ["x", PARENT_CHILD], ids=["alias", "text"])
async def test_a_partition_key_names_a_homed_dimension_by_alias_or_text(backend: str, pk: str) -> None:
    async with _engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=[R, {"field": PARENT_CHILD, "alias": "x"}],
                         metrics=[COUNT, {"metric": f"sum(count(customers.*), partition_by=[{pk}])", "alias": "s"}])
    assert table(resp, "x", "s") == rows((0, 1), (50, 1), (100, 1), (200, 1), (None, 1))


async def test_a_respelled_bucket_order_key_matches_a_homed_time_dimension(backend: str) -> None:
    td = "case when customers.region = 'EAST' then orders.ordered_at end"
    async with _engine(backend) as eng:
        resp = await run(eng, population="customers", metrics=[COUNT],
                         time_dimensions=[{"field": td, "granularity": "month", "alias": "t"}],
                         order=[{"field": "date_trunc('month', CASE WHEN customers.region = 'EAST' "
                                          "THEN orders.ordered_at END)", "nulls": "last"}])
    assert [bucket(r["t"]) for r in resp.data] == ["2025-01", "2025-02", None]


async def test_a_homed_time_dimensions_date_range_reads_its_home(backend: str) -> None:
    # C4 has no orders but signs up in 2025-02: the range must not admit it through signup_ts.
    def query(field: str) -> SlayerQuery:
        return q(population="customers", metrics=[COUNT], time_dimensions=[
            {"field": field, "granularity": "month", "alias": "t", "date_range": ["2025-02-01", "2025-02-28"]}])
    async with _engine(backend) as eng:
        by_expr = await eng.execute(query("coalesce(orders.shipped_at, customers.signup_ts)"))
        by_col = await eng.execute(query("orders.eff_col"))
    assert by_expr.data == by_col.data == []


async def test_a_homed_time_dimension_checks_only_its_own_leaves(backend: str) -> None:
    # The NULL guard reads the join column orders.customer_id; recorded as month-grained, it must not refuse a day bucket.
    orders = orders_dataset().model_copy(update={"fields": [
        f.model_copy(update={"datatype": DataType.DATE, "granularity": "month"}) if f.name == "customer_id" else f
        for f in orders_dataset().fields]})
    td = {"field": "coalesce(orders.shipped_at, customers.signup_ts)", "granularity": "day", "alias": "t"}
    async with _engine(backend, orders=orders) as eng:
        await eng.execute(q(population="customers", metrics=[COUNT], time_dimensions=[td]), dry_run=True)


@pytest.mark.parametrize("key, direction, expected", [
    pytest.param(PARENT_CHILD, "desc", ["EAST", "WEST", "NORTH"], id="spec"),
    # Per-region min/max of the key: EAST 50/200, WEST 100/100 — only the direction's wrap yields this order.
    pytest.param("orders.amount - 200 * (customers.region = 'WEST')", "desc", ["EAST", "WEST", "NORTH"],
                 id="desc-max"),
    pytest.param("orders.amount - 200 * (customers.region = 'WEST')", "asc", ["EAST", "WEST", "NORTH"],
                 id="asc-min"),
])
async def test_a_parent_and_child_order_key_is_still_wrapped_by_direction(
        backend: str, key: str, direction: str, expected: list[str]) -> None:
    async with _engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=[R], metrics=[COUNT],
                         order=[{"field": key, "direction": direction, "nulls": "last"}])
    assert [r[R] for r in resp.data] == expected


async def test_a_field_filter_with_sibling_operands_still_restricts_by_association(backend: str) -> None:
    async with _engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=[R], metrics=[COUNT],
                         filter=[f"{SIBLINGS} > 100"])
    assert table(resp, R, "m") == rows(("EAST", 1))
    assert kinds(resp, "semi_join_pushed")
