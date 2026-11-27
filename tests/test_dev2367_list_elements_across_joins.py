"""List elements read across a join, and aggregations homed below the query root (spec: queries/list-children ›
Association across a list hop needs no key; queries/semantics › The home, not the query root, decides where an
aggregation is computed)."""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator
from typing import Any, Optional, Union

import pytest
import sqlglot
from pytest_postgresql import factories
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.errors import TimeAxisError, UnsafeRelationshipInputError
from slayer.core.keys import AggregateKey, FieldKey, Grain, anchor_token
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetMetric,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.database.client import SlayerSQLClient
from slayer.engine.elaborate import elaborate_query
from slayer.engine.population.home import resolve_aggregate_homes
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.ir.terms import AggregateHome, AggregateHomes
from slayer.sql.dialects.base import ServerProfile
from slayer.sql.dialects.postgres import PostgresDialect
from slayer.storage.sqlite_conn import transaction
from tests._dev2120_fixtures import NATIVE_LIST_DIALECTS, contractors_engine, contractors_statements, emit_contractors
from tests._dev2137_fixtures import (
    EXTRA,
    all_datasets,
    assignments_dataset,
    assignments_statements,
    contractors_dataset,
    list_engine,
    num,
)
from tests._engine_helpers import seeded_exec_engine
from tests._pg_helpers import client_datasource, pg_query_engine

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

#: ``count(contractors.skills.*)`` per assignment: Ann's repeated ``go`` counts twice, Bob's NULL element never.
ELEMENTS = {1: 3.0, 2: 3.0, 3: 1.0, 4: 0.0, 5: 1.0, 6: 0.0}
GO = {1: 2.0, 2: 2.0, 3: 0.0, 4: 0.0, 5: 1.0, 6: 0.0}
SQL = {1: 1.0, 2: 1.0, 3: 1.0, 4: 0.0, 5: 0.0, 6: 0.0}
COUNT_ELEMENTS = "count(contractors.skills.*)"

#: ``SUM({value} * length({weight}))`` with ``weight`` defaulting to a list element.
W = Aggregation(name="w", expression="SUM({value} * length({weight}))",
                params=[AggregationParam(name="weight", sql="skills.value")])


def with_contractors(**update: Any) -> list[SlayerDataset]:
    """``all_datasets()`` with ``contractors`` updated (``fields`` / ``metrics`` / ``filters`` / ``aggregations``)."""
    base = contractors_dataset(keyed=update.pop("keyed", True))
    return [base.model_copy(update=update), *(d for d in all_datasets() if d.name != "contractors")]


def m(expression: str, alias: str = "v") -> dict[str, str]:
    return {"metric": expression, "alias": alias}


def body(population: str, *metrics: dict[str, str], dimensions: Optional[list[str]] = None) -> dict[str, Any]:
    return {"population": population, "dimensions": dimensions or [], "metrics": list(metrics)}


def col(resp: SlayerResponse, name: str) -> str:
    """The one result key equal to ``name`` or ending in ``.name``."""
    hits = [k for k in resp.columns if k == name or k.endswith(f".{name}")]
    assert len(hits) == 1, (name, resp.columns)
    return hits[0]


def cells(resp: SlayerResponse, key: str, value: str = "v") -> dict[Any, Any]:
    k, v = col(resp, key), col(resp, value)
    return {r[k]: num(r[v]) for r in resp.data}


def per_assignment(resp: SlayerResponse, value: str = "v") -> dict[int, Any]:
    return {int(k): v for k, v in cells(resp, "id", value).items()}


@pytest.fixture(scope="module")
async def engine() -> AsyncGenerator[SlayerQueryEngine]:
    saved = [DatasetMetric(name="n_skills", expression="count(skills.*)"),
             DatasetMetric(name="n_go", expression="count(skills.*) FILTER (WHERE skills.value = 'go')")]
    async with list_engine(datasets=with_contractors(metrics=saved, aggregations=[W])) as e:
        yield e


# --------------------------------------------------------------------------- #
# queries/list-children: a list-child home told apart by parent row + position
# --------------------------------------------------------------------------- #
class TestElementsAcrossAJoin:
    async def test_elements_per_assignment(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(body("assignments", m(COUNT_ELEMENTS), dimensions=["assignments.id"]))
        assert per_assignment(resp) == ELEMENTS
        assert sorted(resp.columns) == ["assignments.id", "v"]

    async def test_elements_per_assignment_with_a_parent_dimension(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(body("assignments", m(COUNT_ELEMENTS), dimensions=["assignments.id", "contractors.team"]))
        got = {(int(r[col(resp, "id")]), r[col(resp, "team")]): num(r[col(resp, "v")]) for r in resp.data}
        assert got == {(1, "A"): 3.0, (2, "A"): 3.0, (3, "A"): 1.0, (4, "B"): 0.0, (5, "B"): 1.0, (6, None): 0.0}

    async def test_saved_element_metrics(self, engine: SlayerQueryEngine) -> None:
        saved = await engine.execute(body("assignments", m("contractors.n_skills", "n"), m("contractors.n_go", "g"),
                                          dimensions=["assignments.id"]))
        inline = await engine.execute(body(
            "assignments", m(COUNT_ELEMENTS, "n"),
            m("count(contractors.skills.*) FILTER (WHERE contractors.skills.value = 'go')", "g"), dimensions=["assignments.id"]))
        assert per_assignment(saved, "n") == per_assignment(inline, "n") == ELEMENTS
        assert per_assignment(saved, "g") == per_assignment(inline, "g") == GO

    async def test_element_collect_per_assignment(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(body("assignments", m("collect(contractors.skills.value)"), dimensions=["assignments.id"]))
        got = {int(r[col(resp, "id")]): list(r[col(resp, "v")]) for r in resp.data}
        assert got == {1: ["go", "sql"], 2: ["go", "sql"], 3: ["sql"], 4: [], 5: ["go"], 6: []}

    async def test_from_the_parents_population(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(body("contractors", m("count(contractors.skills.*)"), dimensions=["assignments.id"]))
        got = {None if k is None else int(k): v for k, v in cells(resp, "assignments.id").items()}
        assert got == {1: 3.0, 2: 3.0, 3: 1.0, 4: 0.0, 5: 1.0, None: 0.0}

    async def test_keyless_parent(self) -> None:
        async with list_engine(datasets=with_contractors(keyed=False)) as e:
            resp = await e.execute(body("assignments", m(COUNT_ELEMENTS), dimensions=["assignments.id"]))
        assert per_assignment(resp) == ELEMENTS


class TestIdenticalKeylessParents:
    async def test_elements_stay_apart_across_a_join(self) -> None:
        refs = SlayerDataset(
            name="twin_refs", sql_table="twin_refs", data_source="test",
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="name", datatype=DataType.STRING)],
            relationships=[DatasetRelationship(to="twins", from_columns=["name"], to_columns=["name"])])
        extra = (*EXTRA, "CREATE TABLE twin_refs (id INTEGER, name VARCHAR)", "INSERT INTO twin_refs VALUES (1, 'X')")
        async with contractors_engine("duckdb", datasets=[*all_datasets(), refs], extra=extra) as e:
            resp = await e.execute(body("twin_refs", m("count(twins.skills.*)"), dimensions=["twin_refs.id"]))
        assert per_assignment(resp) == {1: 2.0}


class TestPositionsDoNotDependOnFilters:
    async def test_parent_model_filter(self) -> None:
        async with list_engine(datasets=with_contractors(filters=["team = 'A'"])) as e:
            resp = await e.execute(body("assignments", m(COUNT_ELEMENTS), dimensions=["assignments.id"]))
        assert per_assignment(resp) == {1: 3.0, 2: 3.0, 3: 1.0, 4: 0.0, 5: 0.0, 6: 0.0}

    async def test_two_filtered_element_aggregates_in_one_query(self, engine: SlayerQueryEngine) -> None:
        go = m("count(contractors.skills.*) FILTER (WHERE contractors.skills.value = 'go')", "g")
        sql = m("count(contractors.skills.*) FILTER (WHERE contractors.skills.value = 'sql')", "s")
        both = await engine.execute(body("assignments", go, sql, dimensions=["assignments.id"]))
        alone_go = await engine.execute(body("assignments", go, dimensions=["assignments.id"]))
        alone_sql = await engine.execute(body("assignments", sql, dimensions=["assignments.id"]))
        assert per_assignment(both, "g") == per_assignment(alone_go, "g") == GO
        assert per_assignment(both, "s") == per_assignment(alone_sql, "s") == SQL


# --------------------------------------------------------------------------- #
# The position renders on every native-list dialect
# --------------------------------------------------------------------------- #
ELEMENTS_PER_ASSIGNMENT = SlayerQuery.model_validate(body("assignments", m(COUNT_ELEMENTS, "n"),
                                                          dimensions=["assignments.id"]))
#: The position marker each dialect's element expansion carries (ClickHouse: checked structurally).
POSITION = {"postgres": "WITH ORDINALITY", "trino": "WITH ORDINALITY", "bigquery": "WITH OFFSET",
            "duckdb": r"ROW_NUMBER\(\) OVER \(\)", "snowflake": r"\.INDEX\b"}


async def _correlating(_self: Any) -> ServerProfile:
    return ServerProfile(version=(25, 8), readonly=0, correlated_subqueries=True)


def _contractors_scans(tree: exp.Expr) -> int:
    """The most scans of ``contractors`` within any one SELECT (2+ is a self-join)."""
    return max(sum(1 for t in tree.find_all(exp.Table) if t.name == "contractors" and t.find_ancestor(exp.Select) is s)
               for s in tree.find_all(exp.Select))


def _list_columns(tree: exp.Expr) -> set[str]:
    """Column names read off a list-child alias."""
    return {c.name for c in tree.find_all(exp.Column) if c.table.startswith("__slayer_list__")}


@pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
async def test_the_position_renders(dialect: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SlayerSQLClient, "server_profile", _correlating)
    sql = await emit_contractors(ELEMENTS_PER_ASSIGNMENT, dialect=dialect, datasets=all_datasets())
    tree = sqlglot.parse_one(sql, read=dialect)
    assert _contractors_scans(tree) == 1, sql
    assert "__slayer_row_id" not in _list_columns(tree), sql
    upper = sql.upper()
    if dialect == "clickhouse":
        assert upper.count("ARRAY JOIN") == 1, sql
        assert upper.count("ARRAYFILTER(") == 1, sql
        # The one expanded tuple carries the position beside the value.
        assert _list_columns(tree) - {"value", "__slayer_row_id"}, sql
    else:
        assert re.search(POSITION[dialect], upper), sql


async def test_clickhouse_builds_position_tuples_after_null_compaction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SlayerSQLClient, "server_profile", _correlating)
    sql = await emit_contractors(ELEMENTS_PER_ASSIGNMENT, dialect="clickhouse", datasets=all_datasets())
    tree = sqlglot.parse_one(sql, read="clickhouse")
    cast = next(c for c in tree.find_all(exp.Cast) if c.to.is_type(exp.DataType.Type.STRUCT))
    mapped = cast.find_ancestor(exp.Transform)  # ``arrayMap(lambda, array)``
    assert mapped is not None, sql
    assert isinstance(mapped.this, exp.ArrayFilter), sql


@pytest.mark.parametrize("dialect", ["postgres", "trino", "bigquery", "duckdb", "snowflake"])
@pytest.mark.parametrize("q", [
    SlayerQuery.model_validate(body("contractors", m("count(contractors.skills.*)", "n"),
                                    dimensions=["contractors.skills.value"])),
    SlayerQuery.model_validate({"population": "contractors", "metrics": [m("count(contractors.*)", "n")],
                                "filter": ["contractors.skills.value = 'go'"]}),
], ids=["element-dimension", "element-filter"])
async def test_no_position_unless_associating(dialect: str, q: SlayerQuery) -> None:
    sql = await emit_contractors(q, dialect=dialect, datasets=all_datasets())
    assert not re.search(POSITION[dialect], sql.upper()), sql


@pytest.fixture
async def pg(postgresql, tmp_path) -> AsyncGenerator[SlayerQueryEngine]:
    cur = postgresql.cursor()
    for stmt in [*contractors_statements(dialect="postgres"), *assignments_statements(dialect="postgres")]:
        cur.execute(stmt)
    postgresql.commit()
    datasets = [contractors_dataset(data_source="pg"), assignments_dataset(data_source="pg")]
    async with pg_query_engine(client_datasource(postgresql.info), base_dir=str(tmp_path), datasets=datasets) as e:
        yield e


async def test_elements_per_assignment_on_postgres(pg: SlayerQueryEngine) -> None:
    resp = await pg.execute(ELEMENTS_PER_ASSIGNMENT)
    assert per_assignment(resp, "n") == ELEMENTS


def test_postgres_positions_a_json_list(postgresql) -> None:
    # ``json`` has no equality operator; repeated elements keep separate positions, the NULL none.
    cur = postgresql.cursor()
    cur.execute("""CREATE TABLE docs AS SELECT ARRAY['{"a":1}', NULL, '{"a":1}']::json[] AS v""")
    rows = PostgresDialect().positioned_elements(exp.column("v", table="docs")).sql(dialect="postgres")
    cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT p.__slayer_position) FROM docs CROSS JOIN LATERAL ({rows}) AS p")
    assert cur.fetchone() == (2, 2)


# --------------------------------------------------------------------------- #
# queries/semantics: the home, not the query root, decides where it is computed
# --------------------------------------------------------------------------- #
Outcome = Union[dict[Any, Any], type[ValueError]]


async def outcome(engine: SlayerQueryEngine, request: dict[str, Any]) -> Outcome:
    """Non-NULL-keyed cells (keyed on the dimension values) or the refusal's class."""
    try:
        resp = await engine.execute(request)
    except ValueError as exc:
        return type(exc)
    dims = [col(resp, d.rsplit(".", 1)[-1]) for d in request["dimensions"]]
    v = col(resp, "v")
    got = {tuple(r[d] for d in dims): num(r[v]) for r in resp.data}
    return {k: val for k, val in got.items() if None not in k}


class TestHomeBelowTheRoot:
    async def test_root_column_and_list_element(self, engine: SlayerQueryEngine) -> None:
        home = await outcome(engine, body("contractors", m("sum(contractors.rate * length(contractors.skills.value))")))
        other = await outcome(engine, body("assignments", m("sum(contractors.rate * length(contractors.skills.value))")))
        assert home == other == {(): 230.0}

    async def test_root_column_and_list_element_by_team(self, engine: SlayerQueryEngine) -> None:
        home = await outcome(engine, body("contractors", m("sum(contractors.rate * length(contractors.skills.value))"),
                                          dimensions=["contractors.team"]))
        other = await outcome(engine, body("assignments", m("sum(contractors.rate * length(contractors.skills.value))"),
                                           dimensions=["contractors.team"]))
        assert home == other == {("A",): 130.0, ("B",): 100.0}

    async def test_list_element_definition_default(self, engine: SlayerQueryEngine) -> None:
        home = await outcome(engine, body("contractors", m("w(contractors.rate)")))
        other = await outcome(engine, body("assignments", m("w(contractors.rate)")))
        assert home == other == {(): 230.0}

    async def test_root_column_and_reverse_to_many_column(self, engine: SlayerQueryEngine) -> None:
        home = await outcome(engine, body("contractors", m("sum(contractors.rate * assignments.id)")))
        other = await outcome(engine, body("assignments", m("sum(contractors.rate * assignments.id)")))
        assert home == other == {(): 460.0}

    async def test_undetermined_inputs_refused_from_every_population(self, engine: SlayerQueryEngine) -> None:
        home = await outcome(engine, body("contractors", m("sum(assignments.id * length(contractors.skills.value))")))
        other = await outcome(engine, body("assignments", m("sum(assignments.id * length(contractors.skills.value))")))
        assert home == other == UnsafeRelationshipInputError

    @pytest.mark.parametrize("population", ["contractors", "assignments"])
    async def test_redundant_partition_with_a_home_changing_default(
        self, engine: SlayerQueryEngine, population: str,
    ) -> None:
        q = "contractors."
        dims = [f"{q}team"]
        plain = await outcome(engine, body(population, m(f"w({q}rate)"), dimensions=dims))
        redundant = await outcome(engine, body(population, m(f"w({q}rate, partition_by={q}team)"), dimensions=dims))
        assert redundant == plain == {("A",): 130.0, ("B",): 100.0}

    async def test_redundant_partition_inside_an_enclosing_aggregate(self, engine: SlayerQueryEngine) -> None:
        # The stripped root's enclosing aggregate is rebuilt too, and keeps its home.
        inner = "sum(contractors.rate * max(contractors.rate, partition_by=contractors.team){})"
        plain = await outcome(engine, body("contractors", m(f"sum(contractors.rate * {inner.format('')})"),
                                           dimensions=["contractors.team"]))
        redundant = await outcome(engine, body(
            "contractors", m(f"sum(contractors.rate * {inner.format(', partition_by=contractors.team')})"),
            dimensions=["contractors.team"]))
        assert isinstance(plain, dict), plain
        assert redundant == plain


def _seed_one_to_one(db_path: str) -> None:
    with transaction(db_path) as con:
        con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, price REAL)")
        con.executemany("INSERT INTO orders VALUES (?, ?)", [(1, 10.0), (2, 20.0), (3, 30.0)])
        con.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, weight REAL)")
        con.executemany("INSERT INTO customers VALUES (?, ?)", [(1, 2.0), (2, 4.0)])


async def test_a_to_one_input_keeps_the_aggregation_on_its_anchor() -> None:
    # One-to-one by primary keys: customers determines orders too, but order 3 has no customer and still counts.
    scaled = Aggregation(name="scaled", expression="SUM({value}) / MAX({scale})")
    orders = SlayerDataset(
        name="orders", sql_table="orders", data_source="test", aggregations=[scaled],
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="price", datatype=DataType.FLOAT)],
        relationships=[DatasetRelationship(to="customers", from_columns=["id"], to_columns=["id"])],
    )
    customers = SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="weight", datatype=DataType.FLOAT)],
    )
    async with seeded_exec_engine(dialect="sqlite", seed=_seed_one_to_one, datasets=[orders, customers]) as (e, _):
        resp = await e.execute(body("orders", m("scaled(orders.price, scale='customers.weight')")))
    assert [num(r[col(resp, "v")]) for r in resp.data] == [15.0]


def _band(q: str, alias: str = "band", threshold: int = 50) -> dict[str, str]:
    """A computed dimension carrying a re-aggregation; ``q`` prefixes the ``contractors`` fields."""
    agg = f"max(sum({q}rate, partition_by=[{q}team, {q}name]), partition_by={q}team)"
    return {"field": f"CASE WHEN {agg} > {threshold} THEN 1 ELSE 0 END", "alias": alias}


def _band_query(population: str, expression: str, *, two: bool = False) -> SlayerQuery:
    q = "contractors."
    dims = [f"{q}team", _band(q), *([_band(q, "band2", 90)] if two else [])]
    return SlayerQuery.model_validate(body(population, m(expression), dimensions=dims))  # pyright: ignore[reportArgumentType]


async def test_home_survives_a_reaggregation_partition_key() -> None:
    # The target-rooted aggregate rebuilt around the re-aggregation placeholder keeps its home.
    sql = await emit_contractors(_band_query("contractors",
                                             "sum(contractors.rate * length(contractors.skills.value), partition_by=band)"),
                                 dialect="duckdb", datasets=all_datasets())
    assert sql.lstrip().upper().startswith("WITH"), sql


#: (from ``contractors``, from ``assignments``) — both homed on the list child or on ``assignments``.
BAND_SHAPES = {
    "list-element": ("sum(contractors.rate * length(contractors.skills.value))",
                     "sum(contractors.rate * length(contractors.skills.value))"),
    "reverse-to-many": ("sum(contractors.rate * assignments.id)", "sum(contractors.rate * assignments.id)"),
    "list-element-partitioned": ("sum(contractors.rate * length(contractors.skills.value), partition_by=band)",
                                 "sum(contractors.rate * length(contractors.skills.value), partition_by=band)"),
}


def _band_cells(resp: SlayerResponse) -> dict[tuple[Any, Any], Any]:
    team, band, v = col(resp, "team"), col(resp, "band"), col(resp, "v")
    return {(r[team], r[band]): num(r[v]) for r in resp.data if r[team] is not None}


@pytest.mark.parametrize("kind", sorted(BAND_SHAPES))
async def test_a_reaggregation_dimension_is_independent_of_the_root(engine: SlayerQueryEngine, kind: str) -> None:
    home_expr, other_expr = BAND_SHAPES[kind]
    home = _band_cells(await engine.execute(_band_query("contractors", home_expr)))
    other = _band_cells(await engine.execute(_band_query("assignments", other_expr)))
    assert other
    assert {k: home[k] for k in other} == other


@pytest.mark.parametrize(("population", "expression"), [
    ("contractors", "sum(contractors.rate * length(contractors.skills.value), partition_by=[band, band2])"),
    ("assignments", "sum(contractors.rate * length(contractors.skills.value), partition_by=[band, band2])"),
])
async def test_two_reaggregation_dimensions_in_one_partition(
    engine: SlayerQueryEngine, population: str, expression: str,
) -> None:
    # Both rebuilt partition members keep the aggregate's home: it plans, never "no resolved home".
    assert (await engine.execute(_band_query(population, expression, two=True))).data


_RATE_X_ELEMENTS = "contractors.rate * length(contractors.skills.value)"
#: kind → (expression and dimensions, the outcome from both populations: cells rooted on ``assignments``, or the
#: refusal's class). No time axis: first / last / windowed refuse.
PRODUCER_KINDS: dict[str, tuple[str, list[str], Outcome]] = {
    "filter": (f"sum({_RATE_X_ELEMENTS}) FILTER (WHERE contractors.skills.value = 'go')", [], {(): 140.0}),
    "ranked": (f"rank(sum({_RATE_X_ELEMENTS}), direction='desc')", ["contractors.team"], {("A",): 1.0, ("B",): 2.0}),
    "first": (f"first({_RATE_X_ELEMENTS})", [], ValueError),
    "last": (f"last({_RATE_X_ELEMENTS})", [], ValueError),
    "windowed": (f"sum({_RATE_X_ELEMENTS}, window='1y')", [], TimeAxisError),
    "attached-constituent": (f"sum({_RATE_X_ELEMENTS} * max(contractors.rate, partition_by=contractors.team))",
                             ["contractors.team"], {("A",): 2600.0, ("B",): 5000.0}),
    "explicit-partition": (f"sum({_RATE_X_ELEMENTS}, partition_by=contractors.team)",
                           ["contractors.team", "contractors.name"],
                           {("A", "Ann"): 130.0, ("A", "Bob"): 130.0, ("B", "Cid"): 100.0, ("B", "Eve"): 100.0}),
}


@pytest.mark.parametrize("kind", sorted(PRODUCER_KINDS))
async def test_every_producer_kind_is_independent_of_the_root(engine: SlayerQueryEngine, kind: str) -> None:
    expression, dims, expected = PRODUCER_KINDS[kind]
    home = await outcome(engine, body("contractors", m(expression), dimensions=dims))
    other = await outcome(engine, body("assignments", m(expression), dimensions=dims))
    if isinstance(expected, type):
        assert home == other == expected
        return
    assert isinstance(other, dict), other
    assert other == expected, other
    assert isinstance(home, dict), home
    # The contractors root also yields cells assignments never reaches (Dee): compare where both attribute.
    assert {k: home[k] for k in other} == other


# --------------------------------------------------------------------------- #
# The elaborator's home classification (design D6, D7)
# --------------------------------------------------------------------------- #
def _home(population: str, expression: str, *, dimensions: Optional[list[str]] = None) -> AggregateHome:
    """The resolved home of ``expression``'s outer aggregate from ``population``."""
    datasets = with_contractors(aggregations=[W])
    bundle = ResolvedSourceBundle(dialect="duckdb", population=next(d for d in datasets if d.name == population),
                                  referenced_datasets=datasets)
    env = elaborate_query(query=SlayerQuery.model_validate(body(population, m(expression), dimensions=dimensions)),
                          bundle=bundle)
    root = env.prebound.declared_metrics[-1].bound.value_key
    assert isinstance(root, AggregateKey), root
    return env.homes.of(root)


class TestHomeClassification:
    @pytest.mark.parametrize(("population", "expression", "kind", "path"), [
        ("contractors", "sum(contractors.rate)", "host", ()),
        ("contractors", "sum(contractors.rate * length(contractors.skills.value))", "physical_target", ("skills",)),
        ("assignments", "sum(contractors.rate * length(contractors.skills.value))", "physical_target",
         (anchor_token("contractors"), "skills")),
        ("contractors", "w(contractors.rate)", "physical_target", ("skills",)),
        ("assignments", "w(contractors.rate)", "physical_target", (anchor_token("contractors"), "skills")),
        ("contractors", "sum(contractors.rate * assignments.id)", "physical_target", (anchor_token("assignments"),)),
        ("assignments", "sum(contractors.rate * assignments.id)", "host", ()),
        ("contractors", "sum(assignments.id * length(contractors.skills.value))", "unresolved",
         (anchor_token("assignments"),)),
    ])
    def test_kind_and_path(self, population: str, expression: str, kind: str, path: tuple[str, ...]) -> None:
        assert _home(population, expression) == AggregateHome(kind=kind, path=path)  # pyright: ignore[reportArgumentType]

    def test_second_order_homes_on_its_operand(self) -> None:
        assert _home("contractors", "max(sum(contractors.rate, partition_by=contractors.team))").kind == "operand_dataset"

    def test_hostless_aggregate_is_unresolved_at_its_anchor(self) -> None:
        agg = AggregateKey(source=FieldKey(path=("skills",), leaf="value"), agg="count")
        homes = resolve_aggregate_homes(roots=[agg], host_dataset=None, datasets_by_name={},
                                        bundle=ResolvedSourceBundle(dialect="duckdb"), dim_keys=[], td_keys=[],
                                        active_bucket=None)
        assert homes.of(agg) == AggregateHome(kind="unresolved", path=("skills",))
        assert homes.target_rooted(agg)

    def test_a_missing_home_is_an_internal_error(self) -> None:
        homes, agg = AggregateHomes(), AggregateKey(source=FieldKey(leaf="rate"), agg="sum")
        with pytest.raises(RuntimeError, match="no resolved home"):
            homes.of(agg)

    def test_a_rebuilt_aggregate_keeps_its_home(self) -> None:
        old = AggregateKey(source=FieldKey(leaf="rate"), agg="sum", partition_keys=Grain.of([FieldKey(leaf="team")]))
        new = old.model_copy(update={"partition_keys": None})
        homes = AggregateHomes(by_key={old: AggregateHome(kind="physical_target", path=("skills",))})
        assert homes.carried({old: new}).of(new) == homes.of(old)
