"""An aggregation homed below the query root is computed at its home (spec: queries/semantics › The home, not
the query root, decides where an aggregation is computed)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, Optional, Union

import pytest

from slayer.core.errors import UnsafeJoinInputError
from slayer.core.keys import AggregateKey, ColumnKey, Grain
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.compile.stages import _rebuilt_aggregates
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


def _models() -> list[SlayerModel]:
    many = "one_to_many"
    return [SlayerModel.model_validate(m) for m in [
        {"name": "accounts", "sql_table": "accounts", "data_source": "test",
         "columns": [{"name": "id", "type": "number", "primary_key": True}],
         "joins": [{"target_model": "invoices", "join_pairs": [["id", "account_id"]], "cardinality": many}]},
        {"name": "invoices", "sql_table": "invoices", "data_source": "test", "default_time_dimension": "issued_at",
         "columns": [{"name": "id", "type": "number", "primary_key": True},
                     {"name": "account_id", "type": "number"},
                     {"name": "state", "type": "string"},
                     {"name": "fee", "type": "number"},
                     {"name": "issued_at", "type": "time"}],
         "joins": [{"target_model": "lines", "join_pairs": [["id", "invoice_id"]], "cardinality": many},
                   {"target_model": "payments", "join_pairs": [["id", "invoice_id"]], "cardinality": many}],
         "aggregations": [{"name": "lw", "formula": "SUM({value} * {f})",
                           "params": [{"name": "f", "sql": "lines.amount"}]}]},
        {"name": "lines", "sql_table": "lines", "data_source": "test",
         "columns": [{"name": "id", "type": "number", "primary_key": True},
                     {"name": "invoice_id", "type": "number"},
                     {"name": "amount", "type": "number"},
                     {"name": "shipped_at", "type": "time"}]},
        {"name": "payments", "sql_table": "payments", "data_source": "test",
         "columns": [{"name": "id", "type": "number", "primary_key": True},
                     {"name": "invoice_id", "type": "number"},
                     {"name": "paid", "type": "number"}]},
    ]]


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect=request.param, seed=_seed, models=_models()) as (e, _):
        yield e


def _body(root: str, formula: str, *, dimensions: Optional[list[str]] = None,
          month: Optional[str] = None) -> dict[str, Any]:
    body: dict[str, Any] = {"source_model": root, "measures": [{"formula": formula, "name": "v"}],
                            "dimensions": dimensions or []}
    if month is not None:
        body["time_dimensions"] = [{"dimension": month, "granularity": "month"}]
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


class TestRootColumnReadPerChildRow:
    async def test_total(self, engine: SlayerQueryEngine) -> None:
        home = await _outcome(engine, _body("invoices", "sum(iif(state in ('paid', 'sent'), lines.amount, 0))"))
        other = await _outcome(engine, _body("lines", "sum(iif(invoices.state in ('paid', 'sent'), amount, 0))"))
        assert home == other == {(): 43.0}

    async def test_by_state(self, engine: SlayerQueryEngine) -> None:
        home = await _outcome(engine, _body(
            "invoices", "sum(iif(state in ('paid', 'sent'), lines.amount, 0))", dimensions=["state"]))
        other = await _outcome(engine, _body(
            "lines", "sum(iif(invoices.state in ('paid', 'sent'), amount, 0))", dimensions=["invoices.state"]))
        assert other == {("draft",): 0.0, ("paid",): 38.0, ("sent",): 5.0}
        _common(home, other)


async def test_root_column_and_reverse_to_many_column(engine: SlayerQueryEngine) -> None:
    home = await _outcome(engine, _body("invoices", "sum(iif(lines.amount > 0, fee, 0))"))
    other = await _outcome(engine, _body("lines", "sum(iif(amount > 0, invoices.fee, 0))"))
    assert home == other == {(): 33.0}


#: kind → (formula, dimensions, time dimension) written from ``invoices``, and the cells (or typed error) from both roots.
PRODUCER_KINDS: dict[str, tuple[str, list[str], Optional[str], Outcome]] = {
    "filter": ("sum(CASE WHEN state == 'paid' THEN lines.amount END)", [], None,  # 1.x lowering of FILTER
               {(): 38.0}),
    "ranked": ("rank(sum(iif(state in ('paid', 'sent'), lines.amount, 0)), direction='desc')", ["account_id"], None,
               {("1",): 1.0, ("2",): 2.0}),
    # A ranking key never moves the home: ``fee`` homes on ``invoices`` and ``shipped_at`` fans from it.
    "first": ("first(fee, lines.shipped_at)", ["account_id"], None, UnsafeJoinInputError),
    "last": ("last(lines.amount, issued_at)", [], "issued_at", {("2024-01-01",): 5.0, ("2024-02-01",): 7.0}),
    "windowed": ("sum(iif(state == 'paid', lines.amount, 0), window='1y')", [], "issued_at",
                 {("2024-01-01",): 30.0, ("2024-02-01",): 38.0}),
    "attached-constituent": ("sum(iif(state == 'paid', lines.amount, 0) * max(lines.amount, partition_by=account_id))",
                             ["account_id"], None, {("1",): 600.0, ("2",): 56.0}),
    "explicit-partition": ("sum(iif(state in ('paid', 'sent'), lines.amount, 0), partition_by=account_id)",
                           ["account_id", "state"], None,
                           {("1", "paid"): 35.0, ("1", "sent"): 35.0, ("2", "draft"): 8.0, ("2", "paid"): 8.0}),
}

_ROOT_COLUMNS = ("state", "account_id", "fee", "issued_at")


def _from_lines(text: str) -> str:
    """``text`` respelled from ``lines``: root columns qualified with ``invoices.``, ``lines.`` dropped."""
    for c in _ROOT_COLUMNS:
        text = text.replace(f"{c}", f"invoices.{c}")
    return text.replace("lines.", "")


@pytest.mark.parametrize("kind", sorted(PRODUCER_KINDS))
async def test_every_producer_kind_is_independent_of_the_root(engine: SlayerQueryEngine, kind: str) -> None:
    formula, dims, month, expected = PRODUCER_KINDS[kind]
    home = await _outcome(engine, _body("invoices", formula, dimensions=dims, month=month))
    other = await _outcome(engine, _body("lines", _from_lines(formula), dimensions=[_from_lines(d) for d in dims],
                                         month=None if month is None else _from_lines(month)))
    if isinstance(expected, type):
        assert home == other == expected
        return
    assert other == expected
    _common(home, other)


async def test_undetermined_inputs_refused_from_every_population(engine: SlayerQueryEngine) -> None:
    outcomes = [
        await _outcome(engine, _body("invoices", "sum(lines.amount * payments.paid)")),
        await _outcome(engine, _body("lines", "sum(amount * invoices.payments.paid)")),
        await _outcome(engine, _body("payments", "sum(invoices.lines.amount * paid)")),
    ]
    assert outcomes == [UnsafeJoinInputError] * 3


async def test_unresolved_aggregation_with_a_cross_model_anchor_is_judged_from_it(engine: SlayerQueryEngine) -> None:
    query = SlayerQuery.model_validate(_body("accounts", "sum(invoices.lines.amount * invoices.payments.paid)"))
    with pytest.raises(UnsafeJoinInputError, match="lines|payments"):
        await engine.execute(query)


async def test_reverse_hop_definition_default_homes_on_the_child(engine: SlayerQueryEngine) -> None:
    # ``lines`` is not on the path from ``invoices``: nothing cancels, and ``lines → invoices`` is to-one.
    default = await _outcome(engine, _body("invoices", "lw(fee)"))
    explicit = await _outcome(engine, _body("invoices", "lw(fee, f=lines.amount)"))
    other = await _outcome(engine, _body("lines", "sum(invoices.fee * amount)"))
    assert default == explicit == other == {(): 166.0}


@pytest.mark.parametrize("root", ["invoices", "lines"])
async def test_redundant_partition_with_a_home_changing_default(engine: SlayerQueryEngine, root: str) -> None:
    q = "" if root == "invoices" else "invoices."
    dims = [f"{q}account_id"]
    plain = await _outcome(engine, _body(root, f"lw({q}fee)", dimensions=dims))
    redundant = await _outcome(engine, _body(root, f"lw({q}fee, partition_by={q}account_id)", dimensions=dims))
    assert isinstance(plain, dict), plain
    assert redundant == plain
    assert {k: plain[k] for k in (("1",), ("2",))} == {("1",): 75.0, ("2",): 91.0}


async def test_nested_redundant_partitions_keep_every_rebuilt_home(engine: SlayerQueryEngine) -> None:
    # Stripping the middle aggregate's partition rebuilds the outer one too.
    outcome = await _outcome(engine, _body(
        "invoices", "sum(fee * sum(fee * avg(fee, partition_by=state), partition_by=state))", dimensions=["state"]))
    assert outcome == {("draft",): 125.0, ("paid",): pytest.approx(8000 / 3), ("sent",): 27.0}


def test_rebuilt_aggregates_pair_partition_keys_by_identity_not_position() -> None:
    # Partition keys are a set: only the replaced member maps to its replacement; an unchanged sibling never moves.
    for i in range(12):
        a, b, p = (AggregateKey(source=ColumnKey(leaf=f"{name}{i}"), agg="max") for name in ("a", "b", "p"))
        before = AggregateKey(source=ColumnKey(leaf="rate"), agg="sum", partition_keys=Grain.of([a, b]))
        after = before.model_copy(update={"partition_keys": Grain.of([b, p])})
        out: dict[Any, Any] = {}
        _rebuilt_aggregates(before, after, out)
        assert out == {before: after, a: p}


def _seed_one_to_one(db_path: str) -> None:
    seed_tables(db_path, {
        "orders": ("id INTEGER, price DOUBLE", [(1, 10.0), (2, 20.0), (3, 30.0)]),
        "customers": ("id INTEGER, weight DOUBLE", [(1, 2.0), (2, 4.0)]),
    })


@pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
async def test_a_to_one_input_keeps_the_aggregation_on_its_anchor(dialect: str) -> None:
    # One-to-one by primary keys: order 3 has no customer and still counts in SUM(price).
    # Passes before routing by home too: it guards anchor-first homing against that change.
    models = [SlayerModel.model_validate(m) for m in [
        {"name": "orders", "sql_table": "orders", "data_source": "test",
         "columns": [{"name": "id", "type": "number", "primary_key": True}, {"name": "price", "type": "number"}],
         "joins": [{"target_model": "customers", "join_pairs": [["id", "id"]]}],
         "aggregations": [{"name": "scaled", "formula": "SUM({value}) / MAX({scale})"}]},
        {"name": "customers", "sql_table": "customers", "data_source": "test",
         "columns": [{"name": "id", "type": "number", "primary_key": True}, {"name": "weight", "type": "number"}]},
    ]]
    async with seeded_exec_engine(dialect=dialect, seed=_seed_one_to_one, models=models) as (e, _):
        outcome = await _outcome(e, _body("orders", "scaled(price, scale='customers.weight')"))
    assert outcome == {(): 15.0}
