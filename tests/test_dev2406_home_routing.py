"""An aggregation homed below the query root is computed at its home (spec: queries/semantics › The home, not
the query root, decides where an aggregation is computed)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, Optional, Union

import pytest

from slayer.core.errors import UnsafeRelationshipInputError
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._engine_helpers import seed_tables, seeded_exec_engine

Outcome = Union[dict[tuple[Any, ...], Optional[float]], type]


def _seed(db_path: str) -> None:
    seed_tables(db_path, {
        "accounts": ("id INTEGER", [(1,), (2,)]),
        "invoices": ("id INTEGER, account_id INTEGER, state VARCHAR, fee DOUBLE, issued_at TIMESTAMP", [
            (1, 1, "paid", 2.0, "2024-01-10 00:00:00"), (2, 1, "sent", 3.0, "2024-01-20 00:00:00"),
            (3, 2, "draft", 5.0, "2024-02-25 00:00:00"), (4, 2, "paid", 7.0, "2024-02-15 00:00:00"),
            (5, 1, "paid", 11.0, "2024-03-01 00:00:00"),  # no lines
        ]),
        "lines": ("id INTEGER, invoice_id INTEGER, amount DOUBLE, shipped_at TIMESTAMP", [
            (1, 1, 10.0, "2024-01-11 00:00:00"), (2, 1, 20.0, "2024-01-12 00:00:00"),
            (3, 2, 5.0, "2024-01-21 00:00:00"), (4, 3, 7.0, "2024-02-06 00:00:00"),
            (5, 4, 3.0, "2024-02-16 00:00:00"), (6, 4, 4.0, "2024-02-17 00:00:00"),
            (7, 4, 1.0, "2024-02-18 00:00:00"),
        ]),
        "payments": ("id INTEGER, invoice_id INTEGER, paid DOUBLE", [(1, 1, 30.0), (2, 2, 5.0), (3, 4, 8.0), (4, 4, 0.0)]),
    })


def _rel(to: str, *, to_column: str) -> dict[str, Any]:
    return {"to": to, "from_columns": ["id"], "to_columns": [to_column], "cardinality": "one_to_many"}


def _models() -> list[SlayerDataset]:
    return [SlayerDataset.model_validate(m) for m in [
        {"name": "accounts", "sql_table": "accounts", "data_source": "test",
         "fields": [{"name": "id", "datatype": "Integer", "primary_key": True}],
         "relationships": [_rel("invoices", to_column="account_id")]},
        {"name": "invoices", "sql_table": "invoices", "data_source": "test", "default_time_dimension": "issued_at",
         "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                    {"name": "account_id", "datatype": "Integer"},
                    {"name": "state", "datatype": "String"},
                    {"name": "fee", "datatype": "Float"},
                    {"name": "issued_at", "datatype": "DateTime"}],
         "relationships": [_rel("lines", to_column="invoice_id"), _rel("payments", to_column="invoice_id")],
         "aggregations": [{"name": "lw", "expression": "SUM({value} * {f})",
                           "params": [{"name": "f", "sql": "lines.amount"}]}]},
        {"name": "lines", "sql_table": "lines", "data_source": "test",
         "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                    {"name": "invoice_id", "datatype": "Integer"},
                    {"name": "amount", "datatype": "Float"},
                    {"name": "shipped_at", "datatype": "DateTime"}]},
        {"name": "payments", "sql_table": "payments", "data_source": "test",
         "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                    {"name": "invoice_id", "datatype": "Integer"},
                    {"name": "paid", "datatype": "Float"}]},
    ]]


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect=request.param, seed=_seed, datasets=_models()) as (e, _):
        yield e


def _body(root: str, formula: str, *, dimensions: Optional[list[str]] = None,
          month: Optional[str] = None) -> dict[str, Any]:
    body: dict[str, Any] = {"population": root, "metrics": [{"metric": formula, "alias": "v"}],
                            "dimensions": dimensions or []}
    if month is not None:
        body["time_dimensions"] = [{"field": month, "granularity": "month"}]
    return body


def _num(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def _col(resp: SlayerResponse, name: str) -> str:
    hits = [k for k in resp.columns if k == name or k.endswith(f".{name}")]
    assert len(hits) == 1, (name, resp.columns)
    return hits[0]


async def _outcome(engine: SlayerQueryEngine, body: dict[str, Any]) -> Outcome:
    """``{dimension values: v}`` (each value's first 10 characters, so a month reads alike on every backend), or the
    class of the typed error."""
    try:
        resp = await engine.execute(SlayerQuery.model_validate(body))
    except ValueError as exc:
        return type(exc)
    v = _col(resp, "v")
    keys = [k for k in resp.columns if k != v]
    return {tuple(str(r[k])[:10] for k in keys): _num(r[v]) for r in resp.data}


def _common(home: Outcome, other: Outcome) -> None:
    """Both roots agree on every cell ``other`` attributes (the root may also yield cells the child never reaches)."""
    assert isinstance(other, dict), other
    assert isinstance(home, dict), home
    assert other
    assert {k: home.get(k) for k in other} == other


PAID_OR_SENT = "sum(iif(invoices.state in ('paid', 'sent'), lines.amount, 0))"


class TestRootColumnReadPerChildRow:
    async def test_total(self, engine: SlayerQueryEngine) -> None:
        home = await _outcome(engine, _body("invoices", PAID_OR_SENT))
        other = await _outcome(engine, _body("lines", PAID_OR_SENT))
        assert home == other == {(): 43.0}

    async def test_by_state(self, engine: SlayerQueryEngine) -> None:
        home = await _outcome(engine, _body("invoices", PAID_OR_SENT, dimensions=["invoices.state"]))
        other = await _outcome(engine, _body("lines", PAID_OR_SENT, dimensions=["invoices.state"]))
        assert other == {("draft",): 0.0, ("paid",): 38.0, ("sent",): 5.0}
        _common(home, other)


async def test_root_column_and_reverse_to_many_column(engine: SlayerQueryEngine) -> None:
    formula = "sum(iif(lines.amount > 0, invoices.fee, 0))"
    home = await _outcome(engine, _body("invoices", formula))
    other = await _outcome(engine, _body("lines", formula))
    assert home == other == {(): 33.0}


#: kind → (formula, dimensions, time dimension) read the same from ``invoices`` and ``lines``, and the cells (or
#: typed error) from both roots.
PRODUCER_KINDS: dict[str, tuple[str, list[str], Optional[str], Outcome]] = {
    "filter": ("sum(lines.amount) FILTER (WHERE invoices.state = 'paid')", [], None, {(): 38.0}),
    "ranked": (f"rank({PAID_OR_SENT}, direction='desc')", ["invoices.account_id"], None,
               {("1",): 1.0, ("2",): 2.0}),
    # A ranking key never moves the home: ``fee`` homes on ``invoices`` and ``shipped_at`` fans from it.
    "first": ("first(invoices.fee, lines.shipped_at)", ["invoices.account_id"], None, UnsafeRelationshipInputError),
    "last": ("last(lines.amount, invoices.issued_at)", [], "invoices.issued_at",
             {("2024-01-01",): 5.0, ("2024-02-01",): 7.0}),
    "windowed": ("sum(iif(invoices.state = 'paid', lines.amount, 0), window='1y')", [], "invoices.issued_at",
                 {("2024-01-01",): 30.0, ("2024-02-01",): 38.0}),
    "attached-constituent": ("sum(iif(invoices.state = 'paid', lines.amount, 0)"
                             " * max(lines.amount, partition_by=invoices.account_id))",
                             ["invoices.account_id"], None, {("1",): 600.0, ("2",): 56.0}),
    "explicit-partition": ("sum(iif(invoices.state in ('paid', 'sent'), lines.amount, 0),"
                           " partition_by=invoices.account_id)",
                           ["invoices.account_id", "invoices.state"], None,
                           {("1", "paid"): 35.0, ("1", "sent"): 35.0, ("2", "draft"): 8.0, ("2", "paid"): 8.0}),
}


@pytest.mark.parametrize("kind", sorted(PRODUCER_KINDS))
async def test_every_producer_kind_is_independent_of_the_root(engine: SlayerQueryEngine, kind: str) -> None:
    formula, dims, month, expected = PRODUCER_KINDS[kind]
    home = await _outcome(engine, _body("invoices", formula, dimensions=dims, month=month))
    other = await _outcome(engine, _body("lines", formula, dimensions=dims, month=month))
    if isinstance(expected, type):
        assert home == other == expected
        return
    assert other == expected
    _common(home, other)


async def test_undetermined_inputs_refused_from_every_population(engine: SlayerQueryEngine) -> None:
    formula = "sum(lines.amount * payments.paid)"
    outcomes = [await _outcome(engine, _body(root, formula)) for root in ("invoices", "lines", "payments")]
    assert outcomes == [UnsafeRelationshipInputError] * 3


async def test_unresolved_aggregation_with_a_cross_model_anchor_is_judged_from_it(engine: SlayerQueryEngine) -> None:
    query = SlayerQuery.model_validate(_body("accounts", "sum(invoices.lines.amount * invoices.payments.paid)"))
    with pytest.raises(UnsafeRelationshipInputError, match="lines|payments"):
        await engine.execute(query)


async def test_reverse_hop_definition_default_homes_on_the_child(engine: SlayerQueryEngine) -> None:
    # ``lines`` is not on the path from ``invoices``: nothing cancels, and ``lines → invoices`` is to-one.
    default = await _outcome(engine, _body("invoices", "lw(invoices.fee)"))
    explicit = await _outcome(engine, _body("invoices", "lw(invoices.fee, f=lines.amount)"))
    other = await _outcome(engine, _body("lines", "sum(invoices.fee * lines.amount)"))
    assert default == explicit == other == {(): 166.0}


@pytest.mark.parametrize("root", ["invoices", "lines"])
async def test_redundant_partition_with_a_home_changing_default(engine: SlayerQueryEngine, root: str) -> None:
    dims = ["invoices.account_id"]
    plain = await _outcome(engine, _body(root, "lw(invoices.fee)", dimensions=dims))
    redundant = await _outcome(engine, _body(root, "lw(invoices.fee, partition_by=invoices.account_id)",
                                             dimensions=dims))
    assert isinstance(plain, dict), plain
    assert redundant == plain
    assert {k: plain[k] for k in (("1",), ("2",))} == {("1",): 75.0, ("2",): 91.0}


async def test_nested_redundant_partitions_keep_every_rebuilt_home(engine: SlayerQueryEngine) -> None:
    # Stripping the middle aggregate's partition rebuilds the outer one too.
    outcome = await _outcome(engine, _body(
        "invoices",
        "sum(invoices.fee * sum(invoices.fee * avg(invoices.fee, partition_by=invoices.state),"
        " partition_by=invoices.state))",
        dimensions=["invoices.state"]))
    assert outcome == {("draft",): 125.0, ("paid",): pytest.approx(8000 / 3), ("sent",): 27.0}


def _seed_one_to_one(db_path: str) -> None:
    seed_tables(db_path, {
        "orders": ("id INTEGER, price DOUBLE", [(1, 10.0), (2, 20.0), (3, 30.0)]),
        "customers": ("id INTEGER, weight DOUBLE", [(1, 2.0), (2, 4.0)]),
    })


@pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
async def test_a_to_one_input_keeps_the_aggregation_on_its_anchor(dialect: str) -> None:
    # One-to-one by primary keys: order 3 has no customer and still counts in SUM(price).
    models = [SlayerDataset.model_validate(m) for m in [
        {"name": "orders", "sql_table": "orders", "data_source": "test",
         "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                    {"name": "price", "datatype": "Float"}],
         "relationships": [{"to": "customers", "from_columns": ["id"], "to_columns": ["id"]}],
         "aggregations": [{"name": "scaled", "expression": "SUM({value}) / MAX({scale})"}]},
        {"name": "customers", "sql_table": "customers", "data_source": "test",
         "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                    {"name": "weight", "datatype": "Float"}]},
    ]]
    async with seeded_exec_engine(dialect=dialect, seed=_seed_one_to_one, datasets=models) as (e, _):
        outcome = await _outcome(e, _body("orders", "scaled(orders.price, scale='customers.weight')"))
    assert outcome == {(): 15.0}
