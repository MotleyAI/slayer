"""Quoted text is never a reference, and reported variable demand is binding's (system §3.8)."""

from __future__ import annotations

import json
from typing import Any, Callable, Optional

import pytest

from slayer.core.errors import SlayerError, TimeSpineError, VariableError
from slayer.core.models import ModelMeasure, SlayerModel
from slayer.core.query import (
    ORDER_PLACEHOLDER_NAMES,
    ComputedDimension,
    OrderItem,
    SlayerQuery,
    extract_placeholder_names,
)
from slayer.core.warnings import NormalizationWarning
from slayer.engine.plan import _StagePlanner
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.inspect.model_render import model_skeleton_fields, render_model_inspection

from tests._dev2187_fixtures import BACKENDS, assert_same, canonical_rows, orders_columns, orders_engine


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with orders_engine(request.param) as eng:
        yield eng


def _orders(**body: Any) -> dict[str, Any]:
    return {"source_model": "orders", **body}


def _month(**extra: Any) -> dict[str, Any]:
    return {"dimension": "created_at", "granularity": "month", **extra}


SPINE_MONTHS = [{"dimension": "time_spine.timestamp", "granularity": "month",
                 "date_range": ["2025-01-01", "2025-03-31"]}]

# A Mode-B surface holding the boolean text ``t``.
SURFACES: dict[str, Callable[[str], dict[str, Any]]] = {
    "filter": lambda t: {"filters": [t], "measures": ["count(*)"]},
    "measure": lambda t: {"measures": [{"formula": f"sum(iif({t}, amount, 0))", "name": "m"}]},
    "computed-dimension": lambda t: {
        "dimensions": [{"expression": f"iif({t}, 'y', 'n')", "name": "flag"}], "measures": ["count(*)"]},
    "time-dimension": lambda t: {
        "time_dimensions": [{"dimension": f"iif({t}, created_at, shipped_at)", "granularity": "month", "name": "d"}],
        "measures": ["count(*)"]},
    "order": lambda t: {"dimensions": ["note"], "measures": ["count(*)"], "order": [{"column": f"iif({t}, 0, 1)"}]},
}


# --------------------------------------------------------------------------- #
# A literal naming the root keeps its text
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("surface", sorted(SURFACES))
async def test_a_literal_naming_the_root_reaches_the_database_verbatim(engine: SlayerQueryEngine, surface: str) -> None:
    resp = await engine.execute(_orders(**SURFACES[surface]("note = 'see orders.x'")), dry_run=True)
    assert "'see orders.x'" in (resp.sql or "")


async def test_a_literal_naming_the_root_filters_on_its_text(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(dimensions=["id"], filters=["note = 'see orders.x'"]))
    assert [r["orders.id"] for r in resp.data] == [1]


async def test_a_literal_naming_the_root_inside_a_measure(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(measures=[{"formula": "sum(iif(note = 'see orders.x', amount, 0))", "name": "m"}]))
    assert float(resp.data[0]["orders.m"]) == 10.0


async def test_a_literal_naming_the_root_inside_an_order_expression(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(
        dimensions=["note"], measures=["count(*)"], order=[{"column": "iif(note = 'see orders.x', 0, 1)"}],
    ))
    assert resp.data[0]["orders.note"] == "see orders.x"


# --------------------------------------------------------------------------- #
# A quoted identifier names exactly what it spells
# --------------------------------------------------------------------------- #
QUOTED_ROOT_SURFACES = {
    "filter": {"filters": ['"orders.x" = 1'], "measures": ["count(*)"]},
    "measure": {"measures": ['sum("orders.x")']},
    "computed-dimension": {"dimensions": [{"expression": '"orders.x" + 1', "name": "q"}], "measures": ["count(*)"]},
    "order": {"dimensions": ["status"], "measures": ["count(*)"], "order": [{"column": '"orders.x" + 1'}]},
}


@pytest.mark.parametrize("surface", sorted(QUOTED_ROOT_SURFACES))
async def test_a_quoted_identifier_naming_the_root_is_one_identifier(engine: SlayerQueryEngine, surface: str) -> None:
    body = _orders(**QUOTED_ROOT_SURFACES[surface])
    with pytest.raises(SlayerError) as info:
        await engine.execute(body, dry_run=True)
    assert "orders.x" in str(info.value)


@pytest.mark.parametrize(("quoted", "bare"), [
    ({"filters": ['"status" = \'paid\''], "measures": ["count(*)"]}, {"filters": ["status = 'paid'"], "measures": ["count(*)"]}),
    ({"measures": ['sum("amount")']}, {"measures": ["sum(amount)"]}),
], ids=["filter", "measure"])
async def test_a_quoted_identifier_names_its_column(engine: SlayerQueryEngine, quoted: dict, bare: dict) -> None:
    await assert_same(engine, _orders(**quoted), _orders(**bare))


async def test_a_quoted_time_dimension_names_its_column(engine: SlayerQueryEngine) -> None:
    quoted = await engine.execute(_orders(time_dimensions=['month("created_at")'], measures=["count(*)"]))
    bare = await engine.execute(_orders(time_dimensions=["month(created_at)"], measures=["count(*)"]))
    assert quoted.columns == bare.columns
    assert canonical_rows(quoted) == canonical_rows(bare)


MAIN_TIME = {"time_dimensions": ["month(created_at)", "month(shipped_at)"],
             "measures": [{"formula": "change(count(*))", "name": "c"}]}


async def test_a_quoted_main_time_dimension_names_its_column(engine: SlayerQueryEngine) -> None:
    await assert_same(engine, _orders(**MAIN_TIME, main_time_dimension='"created_at"'),
                      _orders(**MAIN_TIME, main_time_dimension="created_at"))


async def test_a_quoted_main_time_dimension_naming_the_root_is_one_identifier(engine: SlayerQueryEngine) -> None:
    body = _orders(**MAIN_TIME, main_time_dimension='"orders.created_at"')
    with pytest.raises(SlayerError) as info:
        await engine.execute(body, dry_run=True)
    assert "orders.created_at" in str(info.value)


# --------------------------------------------------------------------------- #
# A literal naming the spine is not a spine query
# --------------------------------------------------------------------------- #
SPINE_LITERAL = {
    "filter": {"filters": ["note = 'time_spine.x'"]},
    "computed-dimension": {"dimensions": [{"expression": "iif(note = 'time_spine.x', 'y', 'n')", "name": "flag"}]},
    "time-dimension": {"time_dimensions": [
        _month(), {"dimension": "iif(note = 'time_spine.x', created_at, shipped_at)", "granularity": "year", "name": "y"},
    ]},
}


async def test_a_literal_naming_the_spine_is_not_a_spine_query(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(time_dimensions=[_month()], measures=["count(*)"],
                                        filters=["note = 'time_spine.x'"]))
    assert [(str(r["orders.created_at"])[:10], str(r["orders._count"])) for r in resp.data] == [("2025-02-01", "1")]


@pytest.mark.parametrize("rooted", [True, False], ids=["root", "inferred"])
@pytest.mark.parametrize(("text", "variables"), [
    ("time_spine.timestamp >= date '2025-01-01' + interval '31 days'", {}),
    ("time_spine.timestamp >= date '2025-01-01' + interval '{n} days'", {"n": 31}),
], ids=["literal", "placeholder"])
async def test_a_spine_filter_reads_as_the_template_binding_parses(
    engine: SlayerQueryEngine, rooted: bool, text: str, variables: dict[str, Any],
) -> None:
    body = {"dimensions": ["orders.status"], "measures": ["count(orders.id)"], "filters": [text]}
    body = _orders(**body) if rooted else body
    with pytest.raises(TimeSpineError, match="not a lower or upper bound"):
        await engine.execute(body, variables=variables)


@pytest.mark.parametrize("surface", sorted(SPINE_LITERAL))
async def test_spine_literals_leave_the_population_alone(engine: SlayerQueryEngine, surface: str) -> None:
    body = {"time_dimensions": [_month()], "measures": ["count(*)"], **SPINE_LITERAL[surface]}
    resp = await engine.execute(_orders(**body))
    assert {str(r["orders.created_at"])[:7] for r in resp.data} <= {"2025-01", "2025-02", "2025-03"}
    assert "time_spine" not in (resp.sql or "").replace("'time_spine.x'", "")


# --------------------------------------------------------------------------- #
# A literal naming a query-backed model does not splice it
# --------------------------------------------------------------------------- #
@pytest.fixture
def spliced(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    names: list[str] = []
    real = _StagePlanner._splice

    def spy(self: _StagePlanner, name: str, **kwargs: Any) -> None:
        names.append(name)
        real(self, name, **kwargs)

    monkeypatch.setattr(_StagePlanner, "_splice", spy)
    return names


SPLICE_LITERAL: dict[str, Callable[[str], dict[str, Any]]] = {
    "filter": lambda lit: {"measures": ["count(orders.id)"], "filters": [f"orders.note <> '{lit}'"]},
    "measure": lambda lit: {"measures": [{"formula": f"sum(iif(orders.note = '{lit}', 1, 0))", "name": "m"}]},
    "computed-dimension": lambda lit: {
        "measures": ["count(orders.id)"],
        "dimensions": [{"expression": f"iif(orders.note = '{lit}', 'y', 'n')", "name": "flag"}]},
}


@pytest.mark.parametrize("surface", sorted(SPLICE_LITERAL))
async def test_a_literal_naming_a_query_backed_model_does_not_splice_it(
    engine: SlayerQueryEngine, spliced: list[str], surface: str,
) -> None:
    await engine.execute({"time_dimensions": SPINE_MONTHS, **SPLICE_LITERAL[surface]("zz")}, dry_run=True)
    neutral = list(spliced)
    spliced.clear()
    await engine.execute({"time_dimensions": SPINE_MONTHS, **SPLICE_LITERAL[surface]("qb.x")}, dry_run=True)
    assert spliced == neutral


async def test_a_filter_literal_naming_a_query_backed_model_splices_nothing(
    engine: SlayerQueryEngine, spliced: list[str],
) -> None:
    await engine.execute({"time_dimensions": SPINE_MONTHS, **SPLICE_LITERAL["filter"]("qb.x")}, dry_run=True)
    assert "qb" not in spliced


async def test_a_query_backed_root_is_spliced(engine: SlayerQueryEngine, spliced: list[str]) -> None:
    await engine.execute({"source_model": "qb", "measures": ["sum(tot)"]}, dry_run=True)
    assert spliced == ["qb"]


# --------------------------------------------------------------------------- #
# Reported variable demand equals binding's lookups
# --------------------------------------------------------------------------- #
DEMANDS: dict[str, set[str]] = {
    "note = '{x}'": {"x"},
    "amount > {x}": {"x"},
    "note = '{x}' AND amount > {y}": {"x", "y"},
    "note = '{{x}}'": set(),
    "regexp_like(note, '[0-9]{3}')": set(),
    "note = '{1, 2}'": set(),
}


@pytest.mark.parametrize("text", sorted(DEMANDS))
@pytest.mark.parametrize("surface", sorted(SURFACES))
async def test_reported_demand_is_what_binding_looks_up(engine: SlayerQueryEngine, surface: str, text: str) -> None:
    body = _orders(**SURFACES[surface](text))
    expected = DEMANDS[text]
    assert extract_placeholder_names(SlayerQuery.model_validate(body)) == expected
    await engine.execute({**body, "variables": dict.fromkeys(expected, 1)}, dry_run=True)
    for name in expected:
        missing = {**body, "variables": dict.fromkeys(expected - {name}, 1)}
        with pytest.raises(VariableError, match=f"'{name}'"):
            await engine.execute(missing, dry_run=True)


@pytest.mark.parametrize("surface", sorted(SURFACES))
async def test_a_quoted_identifier_placeholder_is_refused_and_never_demanded(
    engine: SlayerQueryEngine, surface: str,
) -> None:
    body = _orders(**SURFACES[surface]('"{x}" = 1'))
    try:
        query = SlayerQuery.model_validate(body)
    except ValueError:
        return
    assert extract_placeholder_names(query) == set()
    with pytest.raises(ValueError):
        await engine.execute({**body, "variables": {"x": 1}}, dry_run=True)


def _backing(filter_text: str) -> SlayerModel:
    return SlayerModel(name="qbv", data_source="test", source_queries=[SlayerQuery.model_validate(
        {"source_model": "orders", "measures": ["count(*)"], "filters": [filter_text]})])


@pytest.mark.parametrize(("filter_text", "required"), [
    ("status = '{s}' AND amount > {min}", ["min", "s"]),
    ("status = '{{s}}' OR regexp_like(status, '[0-9]{3}')", []),
    ('"{x}" = 1', []),
], ids=["looked-up", "not-looked-up", "quoted-identifier"])
async def test_inspection_reports_the_looked_up_variables(
    engine: SlayerQueryEngine, filter_text: str, required: list[str],
) -> None:
    rendered = await render_model_inspection(model=_backing(filter_text), storage=engine.storage, engine=None,
                                             format="json")
    assert json.loads(rendered)["backing_query"]["required_variables"] == required


async def test_text_binding_does_not_look_up_runs_without_variables(engine: SlayerQueryEngine) -> None:
    await engine.storage.save_model(_backing("status = '{{s}}' OR regexp_like(status, '[0-9]{3}')"))
    resp = await engine.execute({"source_model": "qbv", "measures": ["count(*)"]})
    assert resp.data


@pytest.mark.parametrize(("filter_text", "needed"), [
    ("note = '{x}'", {"x"}),
    ("note = '{{x}}'", set()),
], ids=["looked-up", "doubled-brace"])
async def test_a_spliced_backing_query_looks_up_what_it_reports(
    engine: SlayerQueryEngine, filter_text: str, needed: set[str],
) -> None:
    await engine.storage.save_model(_backing(filter_text))
    body = {"source_model": "qbv", "measures": ["count(*)"]}
    await engine.execute(body, variables=dict.fromkeys(needed, "a"), dry_run=True)
    for name in needed:
        with pytest.raises(VariableError, match=f"'{name}'"):
            await engine.execute(body, dry_run=True)


@pytest.mark.parametrize(("formula", "required"), [
    ("sum(iif(note = '{x}', amount, 0))", ["x"]),
    ("sum(iif(note = '{{x}}', amount, 0))", []),
    ("sum(iif(regexp_like(note, '[0-9]{3}'), amount, 0))", []),
    ('sum(iif("{x}" = 1, amount, 0))', []),
], ids=["looked-up", "doubled-brace", "digit-braces", "quoted-identifier"])
def test_saved_measure_formula_demand(formula: str, required: list[str]) -> None:
    model = SlayerModel(name="m", sql_table="orders", data_source="test", columns=orders_columns(),
                        measures=[ModelMeasure(name="v", formula=formula)])
    assert model_skeleton_fields(model=model)["variables"]["required"] == required


# --------------------------------------------------------------------------- #
# Parity pins: order coercion
# --------------------------------------------------------------------------- #
ORDER_PINS: dict[str, tuple[Optional[str], str, Optional[str]]] = {
    "status": (None, "status", None),
    "customers.name": ("customers", "name", None),
    "orders.status": ("orders", "status", None),
    "sum(revenue)": (None, "_funcstyle_pending", "sum(revenue)"),
    "my_agg(price)": (None, "_funcstyle_pending", "my_agg(price)"),
    "month(created_at)": (None, "_funcstyle_pending", "month(created_at)"),
    "count(*)": (None, "_funcstyle_pending", "count(*)"),
    "sum(orders.amount)": (None, "_funcstyle_pending", "sum(orders.amount)"),
    'sum("amount")': (None, "_funcstyle_pending", 'sum("amount")'),
    "change(sum(revenue))": (None, "_funcstyle_pending", "change(sum(revenue))"),
    "rev / cnt": (None, "_expr_pending", "rev / cnt"),
    "amount::int": (None, "_expr_pending", "amount::int"),
    "orders.amount + 1": (None, "_expr_pending", "orders.amount + 1"),
}

ORDER_LITERALS = [
    "status || 'a:b'", "iif(note = 'x:y', 1, 0)", "coalesce(note, 'a)b')", "coalesce(note, 'a(b')",
    "'a:b'", "upper(note) || ':'",
]


@pytest.mark.parametrize("entry", sorted(ORDER_PINS))
def test_order_coercion_pin(entry: str) -> None:
    column = OrderItem.model_validate({"column": entry})
    coerced = (column.column.model, column.column.name, column.raw_formula)
    assert coerced == ORDER_PINS[entry]


@pytest.mark.parametrize("entry", ORDER_LITERALS)
def test_an_order_literal_is_one_whole_expression(entry: str) -> None:
    column = OrderItem.model_validate({"column": entry})
    assert column.column.model is None
    assert column.column.name in ORDER_PLACEHOLDER_NAMES
    assert column.raw_formula == entry


@pytest.mark.parametrize(("entry", "expected"), [
    ("status || 'a:b'", ["paid", "open"]),
    ("iif(status || ':' = 'open:', 1, 0)", ["open", "paid"]),
], ids=["concat", "call"])
async def test_an_order_literal_sorts_by_the_whole_expression(
    engine: SlayerQueryEngine, entry: str, expected: list[str],
) -> None:
    resp = await engine.execute(_orders(dimensions=["status"], measures=["count(*)"],
                                        order=[{"column": entry, "direction": "desc"}]))
    assert [r["orders.status"] for r in resp.data] == expected


# --------------------------------------------------------------------------- #
# Parity pins: functional time dimensions
# --------------------------------------------------------------------------- #
TD_PINS: dict[str, tuple[str, str]] = {
    "month(created_at)": ("created_at", "month"),
    "MONTH(created_at)": ("created_at", "month"),
    "month( created_at )": ("created_at", "month"),
    "month(coalesce(shipped_at, created_at))": ("coalesce(shipped_at, created_at)", "month"),
    "month(iif(note = 'a)b', created_at, created_at))": ("iif(note = 'a)b', created_at, created_at)", "month"),
    "month(iif(note = 'a,b', created_at, created_at))": ("iif(note = 'a,b', created_at, created_at)", "month"),
    "month(iif(note = ')', created_at, created_at))": ("iif(note = ')', created_at, created_at)", "month"),
    "month(note || '(')": ("note || '('", "month"),
    "month(date_trunc('day', created_at))": ("date_trunc('day', created_at)", "month"),
    "date_trunc('month', created_at)": ("created_at", "month"),
    "date_trunc('month', coalesce(shipped_at, created_at))": ("coalesce(shipped_at, created_at)", "month"),
    "month(created_at - -c)": ("created_at - -c", "month"),
}

TD_REFUSED = ["month(created_at, x=1)", "month({x})", "{g}(created_at)", "month(created_at)::date"]


@pytest.mark.parametrize("field", ["time_dimensions", "dimensions"])
@pytest.mark.parametrize("entry", sorted(TD_PINS))
def test_functional_time_dimension_pin(entry: str, field: str) -> None:
    query = SlayerQuery.model_validate({"source_model": "orders", field: [entry]})
    assert [(td.dimension, str(td.granularity)) for td in query.time_dimensions or []] == [TD_PINS[entry]]
    assert not query.dimensions


def test_a_datasource_granularity_time_dimension_pin() -> None:
    query = SlayerQuery.model_validate({"source_model": "orders", "time_dimensions": ["fiscal_year(created_at)"]})
    assert [(td.dimension, str(td.granularity)) for td in query.time_dimensions or []] == [("created_at", "fiscal_year")]


@pytest.mark.parametrize("entry", TD_REFUSED)
def test_functional_time_dimension_refusal_pin(entry: str) -> None:
    with pytest.raises(ValueError):
        SlayerQuery.model_validate({"source_model": "orders", "time_dimensions": [entry]})


def test_a_non_granularity_call_stays_a_dimension() -> None:
    query = SlayerQuery.model_validate({"source_model": "orders", "dimensions": ["upper(status)"]})
    assert not query.time_dimensions
    assert [d.expression for d in query.dimensions or [] if isinstance(d, ComputedDimension)] == ["upper(status)"]


async def test_a_quoted_parenthesis_buckets_the_whole_expression(engine: SlayerQueryEngine) -> None:
    quoted = await engine.execute(_orders(time_dimensions=["month(iif(note = 'a)b', created_at, created_at))"],
                                          measures=["count(*)"]))
    bare = await engine.execute(_orders(time_dimensions=["month(created_at)"], measures=["count(*)"]))
    assert sorted(tuple(map(str, r.values())) for r in quoted.data) == sorted(
        tuple(map(str, r.values())) for r in bare.data)


# --------------------------------------------------------------------------- #
# Parity pins: the misplaced-measure check
# --------------------------------------------------------------------------- #
def _misplaced(resp: Any) -> list[str]:
    return [w.original for w in resp.warnings if isinstance(w, NormalizationWarning) and w.rule_id == "MISPLACED_MEASURE"]


async def test_a_bare_column_measure_moves_to_dimensions(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(measures=["status", "count(*)"]))
    assert _misplaced(resp) == ["status"]
    assert "orders.status" in resp.columns


@pytest.mark.parametrize("measures", [["revenue"], ["sum(amount)"]], ids=["saved", "call"])
async def test_a_measure_stays_a_measure(engine: SlayerQueryEngine, measures: list[str]) -> None:
    assert _misplaced(await engine.execute(_orders(measures=measures))) == []
