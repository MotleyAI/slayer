"""A whole list groups, compares and counts distinctly as its element set; ``length`` counts distinct elements
(spec: models/list-columns, queries/list-functions, queries/expression-grammar; semantics Axiom 3.2)."""

from __future__ import annotations

import contextlib
import datetime as dt
import re
import warnings
from collections import Counter, defaultdict
from collections.abc import AsyncGenerator, Callable, Iterable
from pathlib import Path
from typing import Any, Optional

import duckdb
import pytest
from fastapi.testclient import TestClient
from pytest_postgresql import factories
from mcp.types import TextContent
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.api.server import create_app
from slayer.core import keys
from slayer.core.enums import DEFAULT_AGGREGATIONS_BY_TYPE, DataType
from slayer.core.errors import ListTypeError, SlayerError
from slayer.core.keys import (
    KIND_POLICY,
    VALUE_KEY_TYPES,
    ColumnKey,
    Phase,
    list_element_type,
    prepend_value_key,
    reroot_value_key,
    substitute_value_keys,
    value_type,
)
from slayer.core.models import Aggregation, Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.refs import key_display
from slayer.core.syntax import parse_expr
from slayer.engine.ingestion import ingest_datasource_async
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.inspect.collection_render import render_models_summary
from slayer.inspect.model_render import render_model_inspection
from slayer.mcp.server import create_mcp_server
from slayer.search.render import render_column_text
from slayer.sql import engine_factory
from slayer.sql.dialects import get_dialect
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2101_fixtures import emit
from tests._dev2120_fixtures import NATIVE_LIST_DIALECTS, NO_LIST_DATABASES
from tests._dev2137_fixtures import measure, num, query
from tests._engine_helpers import seeded_exec_engine

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

# --------------------------------------------------------------------------- #
# The seven contractors
# --------------------------------------------------------------------------- #
ROWS: tuple[dict[str, Any], ...] = (
    {"id": 1, "name": "Ann", "team": "A", "rate": 10, "hired": "2024-01-10",
     "skills": ["go", "sql", "go"], "required": ["sql", "go"], "scores": [3, 9, 3]},
    {"id": 2, "name": "Bob", "team": "A", "rate": 20, "hired": "2024-02-10",
     "skills": ["sql", None], "required": ["sql"], "scores": [1]},
    {"id": 3, "name": "Cid", "team": "B", "rate": 30, "hired": "2024-03-10",
     "skills": [], "required": [], "scores": []},
    {"id": 4, "name": "Dee", "team": "B", "rate": 40, "hired": "2024-04-10",
     "skills": None, "required": ["go"], "scores": None},
    {"id": 5, "name": "Eve", "team": "B", "rate": 50, "hired": "2025-01-10",
     "skills": ["go"], "required": None, "scores": [5, None]},
    {"id": 6, "name": "Fay", "team": "C", "rate": 60, "hired": "2025-02-10",
     "skills": ["sql", "go"], "required": ["go", "sql", "sql"], "scores": [9, 3]},
    {"id": 7, "name": "Gil", "team": "C", "rate": 70, "hired": "2025-03-10",
     "skills": [None], "required": [], "scores": [None]},
)
#: ``(id, contractor_id)``; contractor 99 does not exist.
ASSIGNED: tuple[tuple[int, int], ...] = ((1, 1), (2, 1), (3, 2), (4, 3), (5, 6), (6, 7), (7, 99))
BY_ID = {r["id"]: r for r in ROWS}
BY_NAME = {r["name"]: r for r in ROWS}

_TYPES = {
    "duckdb": {"INT": "INTEGER", "TEXT": "VARCHAR", "DATE": "DATE", "skills": "VARCHAR[]", "required": "VARCHAR[]",
               "scores": "INTEGER[]"},
    "postgres": {"INT": "INTEGER", "TEXT": "TEXT", "DATE": "DATE", "skills": "VARCHAR[]", "required": "TEXT[]",
                 "scores": "INTEGER[]"},
}
_SCALARS = (("id", "INT"), ("name", "TEXT"), ("team", "TEXT"), ("rate", "INT"), ("hired", "DATE"))
_LISTS = ("skills", "required", "scores")


def elset(value: Optional[list[Any]]) -> Optional[tuple[Any, ...]]:
    """A list's element set as the canonical sorted tuple; NULL stays ``None``."""
    return None if value is None else tuple(sorted({e for e in value if e is not None}))


def _sql_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def _list_sql(value: Optional[list[Any]], *, backend: str, sql_type: str) -> str:
    if value is None:
        return "NULL"
    items = ", ".join(_sql_value(v) for v in value)
    return f"ARRAY[{items}]::{sql_type}" if backend == "postgres" else f"[{items}]::{sql_type}"


def contractor_statements(backend: str) -> list[str]:
    types = _TYPES[backend]
    cols = [f"{c} {types[k]}" for c, k in _SCALARS] + [f"{c} {types[c]}" for c in _LISTS]
    rows = []
    for r in ROWS:
        scalars = [f"DATE '{r['hired']}'" if c == "hired" else _sql_value(r[c]) for c, _ in _SCALARS]
        lists = [_list_sql(r[c], backend=backend, sql_type=types[c]) for c in _LISTS]
        rows.append("(" + ", ".join(scalars + lists) + ")")
    return [f"CREATE TABLE contractors ({', '.join(cols)})",
            "INSERT INTO contractors VALUES " + ", ".join(rows),
            "CREATE TABLE assignments (id INTEGER, contractor_id INTEGER)",
            "INSERT INTO assignments VALUES " + ", ".join(f"({a}, {c})" for a, c in ASSIGNED)]


def contractors_model() -> SlayerModel:
    return SlayerModel(name="contractors", sql_table="contractors", data_source="test", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="name", type=DataType.TEXT),
        Column(name="team", type=DataType.TEXT),
        Column(name="rate", type=DataType.INT),
        Column(name="hired", type=DataType.DATE),
        Column(name="skills", type=DataType.ARRAY, element_type=DataType.TEXT),
        Column(name="required", type=DataType.ARRAY, element_type=DataType.TEXT),
        Column(name="scores", type=DataType.ARRAY, element_type=DataType.INT),
    ])


def assignments_model() -> SlayerModel:
    return SlayerModel(
        name="assignments", sql_table="assignments", data_source="test",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="contractor_id", type=DataType.INT)],
        joins=[ModelJoin(target_model="contractors", join_pairs=[["contractor_id", "id"]])],
    )


def models() -> list[SlayerModel]:
    return [contractors_model(), assignments_model()]


@contextlib.asynccontextmanager
async def duckdb_engine(*, statements: Iterable[str], engine_models: list[SlayerModel],
                        ) -> AsyncGenerator[SlayerQueryEngine]:
    def seed(path: str) -> None:
        with contextlib.closing(duckdb.connect(path)) as con:
            for stmt in statements:
                con.execute(stmt)
    async with seeded_exec_engine(dialect="duckdb", seed=seed, models=engine_models) as (engine, _db):
        yield engine


@contextlib.asynccontextmanager
async def postgres_engine(conn: Any, base: Path, *, statements: Iterable[str],
                          engine_models: list[SlayerModel]) -> AsyncGenerator[SlayerQueryEngine]:
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
    conn.commit()
    info = conn.info
    ds = DatasourceConfig(name="test", type="postgres", host=info.host, port=info.port, database=info.dbname,
                          username=info.user, password="")
    storage = YAMLStorage(base_dir=str(base / "store"))
    await storage.save_datasource(ds)
    for model in engine_models:
        await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    try:
        yield engine
    finally:
        await engine.aclose()
        engine.close()
        engine_factory.invalidate_engine(ds)


@pytest.fixture(params=["duckdb", "postgres"])
async def engine(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    """The seven contractors on DuckDB and on Postgres."""
    if request.param == "duckdb":
        async with duckdb_engine(statements=contractor_statements("duckdb"), engine_models=models()) as e:
            yield e
        return
    async with postgres_engine(request.getfixturevalue("postgresql"), tmp_path,
                               statements=contractor_statements("postgres"), engine_models=models()) as e:
        yield e


@pytest.fixture(scope="module")
async def duck() -> AsyncGenerator[SlayerQueryEngine]:
    """The seven contractors on DuckDB, for checker-level refusals."""
    async with duckdb_engine(statements=contractor_statements("duckdb"), engine_models=models()) as e:
        yield e


# --------------------------------------------------------------------------- #
# Reading results
# --------------------------------------------------------------------------- #
def cell(row: dict[str, Any], suffix: str) -> Any:
    (value,) = [v for k, v in row.items() if k == suffix or k.endswith(f".{suffix}")]
    return value


def as_set(value: Any) -> Optional[tuple[Any, ...]]:
    """A grouped list cell (already its element set) as a tuple."""
    return None if value is None else tuple(value)


def as_stored(value: Any) -> Optional[list[Any]]:
    return None if value is None else list(value)


def year(value: Any) -> int:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.year
    return int(str(value)[:4])


def truth(value: Any) -> Optional[bool]:
    return None if value is None else bool(value)


def cells(resp: SlayerResponse, *dims: str, value: str) -> dict[Any, Any]:
    out = {}
    for r in resp.data:
        key = tuple(as_set(cell(r, d)) if d in ("skills", "s") else cell(r, d) for d in dims)
        out[key[0] if len(key) == 1 else key] = num(cell(r, value))
    return out


def whole_list_payloads(resp: SlayerResponse) -> list[Any]:
    return [w for w in resp.warnings or [] if type(w).__name__ == "WholeListWarning"]


async def names(engine: SlayerQueryEngine, filter_: str, **body: Any) -> set[str]:
    resp = await engine.execute(query(dimensions=["name"], filters=[filter_], **body))
    return {r["contractors.name"] for r in resp.data}


async def count_rows(engine: SlayerQueryEngine, filter_: str, **body: Any) -> Optional[float]:
    resp = await engine.execute(query(measures=[measure("count(*)", "n")], filters=[filter_], **body))
    return num(resp.data[0]["contractors.n"])


async def values(engine: SlayerQueryEngine, expr: str) -> dict[str, Any]:
    resp = await engine.execute(query(dimensions=["name", {"expression": expr, "name": "v"}]))
    return {r["contractors.name"]: r["contractors.v"] for r in resp.data}


GROUPS: Counter[Optional[tuple[Any, ...]]] = Counter(elset(r["skills"]) for r in ROWS)


def test_the_fixture_has_the_scenario_groups() -> None:
    assert GROUPS == {("go", "sql"): 2, ("sql",): 1, (): 2, ("go",): 1, None: 1}


# --------------------------------------------------------------------------- #
# Grouping
# --------------------------------------------------------------------------- #
#: A list-valued expression → its value per record.
LIST_EXPRESSIONS: dict[str, Callable[[dict[str, Any]], Optional[list[Any]]]] = {
    "skills": lambda r: r["skills"],
    "coalesce(skills, required)": lambda r: r["skills"] if r["skills"] is not None else r["required"],
    "CASE WHEN team = 'A' THEN skills ELSE required END": lambda r: r["skills"] if r["team"] == "A" else r["required"],
}


class TestGrouping:
    async def test_list_as_a_dimension(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["skills"], measures=[measure("count(*)", "n")]))
        assert cells(resp, "skills", value="n") == {("go", "sql"): 2, ("sql",): 1, (): 2, ("go",): 1, None: 1}
        assert len(resp.data) == 5
        assert any("skills.value" in w.human_message() for w in whole_list_payloads(resp))

    async def test_distinct_values_group_without_measures(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["skills"]))
        assert Counter(as_set(cell(r, "skills")) for r in resp.data) == Counter(set(GROUPS))

    async def test_grouped_cell_shows_the_element_set(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["name", "skills"], measures=[measure("count(*)", "n")]))
        assert {cell(r, "name"): as_stored(cell(r, "skills")) for r in resp.data} == {
            n: None if s is None else list(s) for n, s in ((r["name"], elset(r["skills"])) for r in ROWS)}

    async def test_list_projected_per_record(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["id", "skills"], distinct_dimension_values=False))
        assert {int(cell(r, "id")): as_stored(cell(r, "skills")) for r in resp.data} == {
            r["id"]: r["skills"] for r in ROWS}
        assert not whole_list_payloads(resp)

    @pytest.mark.parametrize("expr", sorted(LIST_EXPRESSIONS))
    async def test_list_valued_expression_raw_and_grouped(self, engine: SlayerQueryEngine, expr: str) -> None:
        fn = LIST_EXPRESSIONS[expr]
        raw = await engine.execute(query(dimensions=["id", {"name": "s", "expression": expr}],
                                         distinct_dimension_values=False))
        assert {int(cell(r, "id")): as_stored(cell(r, "s")) for r in raw.data} == {r["id"]: fn(r) for r in ROWS}
        grouped = await engine.execute(query(dimensions=[{"name": "s", "expression": expr}],
                                             measures=[measure("count(*)", "n")]))
        assert cells(grouped, "s", value="n") == Counter(elset(fn(r)) for r in ROWS)

    async def test_beside_a_scalar_dimension(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["team", "skills"], measures=[measure("count(*)", "n")]))
        assert cells(resp, "team", "skills", value="n") == Counter((r["team"], elset(r["skills"])) for r in ROWS)

    async def test_through_a_to_one_join(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute({"source_model": "assignments", "dimensions": ["contractors.skills"],
                                     "measures": [measure("count(*)", "n")]})
        got = {as_set(cell(r, "contractors.skills")): num(cell(r, "n")) for r in resp.data}
        assert got == Counter(elset(BY_ID[c]["skills"]) if c in BY_ID else None for _, c in ASSIGNED)


# --------------------------------------------------------------------------- #
# A list as a grain member
# --------------------------------------------------------------------------- #
def _rank_desc(totals: dict[Any, float]) -> dict[Any, int]:
    return {k: 1 + sum(1 for v in totals.values() if v > totals[k]) for k in totals}


class TestPartitionKeys:
    async def test_a_list_as_a_partition_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["id", "skills"],
                                          measures=[measure("count(*, partition_by=skills)", "n")]))
        assert {int(cell(r, "id")): num(cell(r, "n")) for r in resp.data} == {
            1: 2, 6: 2, 3: 2, 7: 2, 2: 1, 4: 1, 5: 1}

    async def test_a_partition_coarser_than_the_query(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["team", "skills"],
                                          measures=[measure("count(*, partition_by=skills)", "n")]))
        assert cells(resp, "team", "skills", value="n") == {
            (r["team"], elset(r["skills"])): GROUPS[elset(r["skills"])] for r in ROWS}

    async def test_a_transform_partitioned_by_a_list(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(
            dimensions=["skills", "team"],
            measures=[measure("rank(sum(rate), partition_by=skills, direction='desc')", "r")]))
        totals: dict[Any, dict[str, float]] = defaultdict(dict)
        for r in ROWS:
            totals[elset(r["skills"])][r["team"]] = totals[elset(r["skills"])].get(r["team"], 0) + r["rate"]
        expected = {(t, s): rank for s, per_team in totals.items() for t, rank in _rank_desc(per_team).items()}
        assert cells(resp, "team", "skills", value="r") == expected

    async def test_nested_producers(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(measures=[measure("count(count(*, partition_by=skills))", "cells"),
                                                    measure("max(count(*, partition_by=skills))", "biggest")]))
        row = resp.data[0]
        assert (num(row["contractors.cells"]), num(row["contractors.biggest"])) == (5, 2)

    async def test_a_cross_model_aggregate_joined_back_on_a_list(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["skills"], measures=[
            measure("count(*)", "n"), measure("count(assignments.id)", "a")]))
        expected: dict[Any, list[int]] = defaultdict(lambda: [0, 0])
        for r in ROWS:
            expected[elset(r["skills"])][0] += 1
        # An assignment's absent contractor reads a NULL list.
        for _, c in ASSIGNED:
            expected[elset(BY_ID.get(c, {}).get("skills"))][1] += 1
        got = {as_set(cell(r, "skills")): [num(cell(r, "n")), num(cell(r, "a"))] for r in resp.data}
        assert got == dict(expected)

    async def test_association_over_a_list(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["skills", "assignments.id"],
                                          measures=[measure("sum(rate)", "r")], to_many_handling="associate"))
        got = {int(cell(r, "assignments.id")): (as_set(cell(r, "skills")), num(cell(r, "r")))
               for r in resp.data if cell(r, "assignments.id") is not None}
        assert got == {a: (elset(BY_ID[c]["skills"]), BY_ID[c]["rate"]) for a, c in ASSIGNED if c in BY_ID}


# --------------------------------------------------------------------------- #
# Time transforms over a list-grouped series
# --------------------------------------------------------------------------- #
YEARLY = {"dimension": "hired", "granularity": "year"}
SERIES: Counter[tuple[Any, int]] = Counter((elset(r["skills"]), int(r["hired"][:4])) for r in ROWS)


class TestTimeTransforms:
    async def _series(self, engine: SlayerQueryEngine, formula: str, **body: Any) -> dict[tuple[Any, int], Any]:
        resp = await engine.execute(query(dimensions=["skills"], time_dimensions=[YEARLY],
                                          measures=[measure(formula, "v")], **body))
        return {(as_set(cell(r, "skills")), year(cell(r, "hired"))): num(cell(r, "v")) for r in resp.data}

    async def test_running_total_per_list(self, engine: SlayerQueryEngine) -> None:
        expected = {(s, y): sum(n for (s2, y2), n in SERIES.items() if s2 == s and y2 <= y) for s, y in SERIES}
        assert await self._series(engine, "cumsum(count(*))") == expected

    async def test_previous_period_per_list(self, engine: SlayerQueryEngine) -> None:
        expected = {(s, y): SERIES.get((s, y - 1)) for s, y in SERIES}
        assert await self._series(engine, "time_shift(count(*), -1)") == expected

    async def test_windowed_aggregate_per_list(self, engine: SlayerQueryEngine) -> None:
        assert set(await self._series(engine, "sum(rate, window='400d')")) == set(SERIES)


# --------------------------------------------------------------------------- #
# Comparisons
# --------------------------------------------------------------------------- #
COMPARISONS: list[tuple[str, set[str]]] = [
    ("skills = ['sql', 'go']", {"Ann", "Fay"}),
    ("skills = ['go', 'go', 'sql']", {"Ann", "Fay"}),
    ("skills <> ['go']", {"Ann", "Bob", "Cid", "Fay", "Gil"}),
    ("NOT (skills = ['go'])", {"Ann", "Bob", "Cid", "Fay", "Gil"}),
    ("skills = []", {"Cid", "Gil"}),
    ("skills IS NOT DISTINCT FROM []", {"Cid", "Gil"}),
    ("skills IS DISTINCT FROM ['go']", {"Ann", "Bob", "Cid", "Dee", "Fay", "Gil"}),
    ("skills = required", {"Ann", "Bob", "Cid", "Fay", "Gil"}),
    ("required = skills", {"Ann", "Bob", "Cid", "Fay", "Gil"}),
    ("skills <> required", set()),
    ("skills IS DISTINCT FROM required", {"Dee", "Eve"}),
    ("skills IN (['go'], ['sql'])", {"Bob", "Eve"}),
    ("skills NOT IN (['go'], ['sql'])", {"Ann", "Cid", "Fay", "Gil"}),
    ("skills IN (['sql', 'go'])", {"Ann", "Fay"}),
    ("scores = [3, 9]", {"Ann", "Fay"}),
    ("scores = [9.0, 3]", {"Ann", "Fay"}),
    ("coalesce(skills, required) = ['go']", {"Dee", "Eve"}),
]


class TestComparisons:
    @pytest.mark.parametrize(("filter_", "count_"), [
        ("skills = ['sql', 'go']", 2), ("skills <> ['go']", 5), ("skills IS NOT DISTINCT FROM []", 2),
        ("skills IN (['go'], ['sql'])", 2), ("skills NOT IN (['go'], ['sql'])", 4),
    ])
    async def test_scenario_counts(self, engine: SlayerQueryEngine, filter_: str, count_: int) -> None:
        assert await count_rows(engine, filter_) == count_

    @pytest.mark.parametrize(("filter_", "expected"), COMPARISONS)
    async def test_kept_rows(self, engine: SlayerQueryEngine, filter_: str, expected: set[str]) -> None:
        assert await names(engine, filter_) == expected

    async def test_in_is_the_or_of_equalities(self, engine: SlayerQueryEngine) -> None:
        assert await names(engine, "skills IN (['go'], ['sql', 'go'])") == await names(
            engine, "skills = ['go'] OR skills = ['sql', 'go']") == {"Ann", "Eve", "Fay"}

    async def test_comparison_as_a_value(self, engine: SlayerQueryEngine) -> None:
        got = {k: truth(v) for k, v in (await values(engine, "skills = ['go', 'sql']")).items()}
        assert got == {"Ann": True, "Bob": False, "Cid": False, "Dee": None, "Eve": False, "Fay": True, "Gil": False}

    async def test_comparison_inside_an_aggregation(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(measures=[
            measure("sum(CASE WHEN skills = ['go', 'sql'] THEN 1 ELSE 0 END)", "n")]))
        assert num(resp.data[0]["contractors.n"]) == 2

    async def test_comparison_does_not_warn(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["name"], filters=["skills = ['go']"]))
        assert not whole_list_payloads(resp)


class TestListVariables:
    @pytest.mark.parametrize("filter_", ["skills IN ({wanted})", "skills = {wanted}", "skills NOT IN ({wanted})"])
    async def test_a_list_variable_is_one_list(self, engine: SlayerQueryEngine, filter_: str) -> None:
        kept = {"Ann", "Fay"}
        expected = kept if "NOT" not in filter_ else {"Bob", "Cid", "Eve", "Gil"}
        assert await names(engine, filter_, variables={"wanted": ["sql", "go"]}) == expected

    async def test_an_empty_list_variable_is_the_empty_list(self, engine: SlayerQueryEngine) -> None:
        assert await names(engine, "skills IN ({wanted})", variables={"wanted": []}) == {"Cid", "Gil"}

    async def test_a_scalar_left_side_takes_the_whole_list(self, engine: SlayerQueryEngine) -> None:
        assert await names(engine, "team IN ({teams})", variables={"teams": ["A", "C"]}) == {
            "Ann", "Bob", "Fay", "Gil"}

    async def test_list_variables_as_items(self, engine: SlayerQueryEngine) -> None:
        assert await names(engine, "skills IN ({a}, {b})", variables={"a": ["go"], "b": ["sql"]}) == {"Bob", "Eve"}

    async def test_scalar_variables_as_items(self, engine: SlayerQueryEngine) -> None:
        assert await names(engine, "id IN (1, {b}, {c})", variables={"b": 2, "c": 5}) == {"Ann", "Bob", "Eve"}

    @pytest.mark.parametrize("formula", ["count(*) FILTER (WHERE team IN ({v}))",
                                         "count(*) FILTER (WHERE skills IN ({v}))"])
    async def test_an_unnamed_measure_keeps_its_key_across_values(self, duck: SlayerQueryEngine,
                                                                  formula: str) -> None:
        keys = [
            list((await duck.execute(query(measures=[{"formula": formula}], variables={"v": v}))).data[0])
            for v in (["A"], ["B", "C"])
        ]
        assert keys[0] == keys[1]
        assert keys[0][0].endswith("_v"), keys

    async def test_list_and_scalar_variables_as_items_are_refused(self, duck: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError, match="right-hand side"):
            await names(duck, "skills IN ({a}, {b})", variables={"a": ["go"], "b": 2})


class TestInGrammar:
    def test_several_array_literals_parse(self) -> None:
        parse_expr("skills IN (['go'], ['sql', 'go'])")
        parse_expr("skills NOT IN (['go'], {wanted})")

    def test_mixed_items_fail_to_parse(self) -> None:
        with pytest.raises(ValueError, match="right-hand side"):
            parse_expr("skills IN (['go'], 'sql')")

    async def test_a_scalar_against_list_items_is_refused(self, duck: SlayerQueryEngine) -> None:
        with pytest.raises(ListTypeError):
            await count_rows(duck, "team IN (['A'], ['B'])")


# --------------------------------------------------------------------------- #
# Refusals
# --------------------------------------------------------------------------- #
def _names_the_list_type(msg: str) -> bool:
    return "list" in msg.lower() or "ARRAY" in msg


class TestRefusals:
    @pytest.mark.parametrize(("filter_", "remedy"), [
        ("skills = 'go'", "contains"), ("skills <> 'go'", "contains"), ("skills IN ('go', 'sql')", "contains_any"),
    ])
    async def test_list_against_a_scalar(self, duck: SlayerQueryEngine, filter_: str, remedy: str) -> None:
        with pytest.raises(ListTypeError, match=remedy):
            await count_rows(duck, filter_)

    @pytest.mark.parametrize("filter_", [
        "skills < ['go']", "skills >= ['go']", "skills > required", "skills BETWEEN ['a'] AND ['z']",
    ])
    async def test_ordering_comparisons(self, duck: SlayerQueryEngine, filter_: str) -> None:
        with pytest.raises(ListTypeError) as info:
            await count_rows(duck, filter_)
        assert _names_the_list_type(str(info.value)), str(info.value)

    @pytest.mark.parametrize(("filter_", "types"), [
        ("skills = [DATE '2025-01-01']", ("TEXT", "DATE")), ("skills = scores", ("TEXT", "INT")),
        ("skills IN (['go'], [1])", ("TEXT", "INT")),
    ])
    async def test_lists_of_different_element_types(self, duck: SlayerQueryEngine, filter_: str,
                                                    types: tuple[str, str]) -> None:
        with pytest.raises(SlayerError) as info:
            await count_rows(duck, filter_)
        for needle in types:
            assert needle in str(info.value), str(info.value)

    @pytest.mark.parametrize("distinct", [False, True])
    async def test_ordering_by_a_list(self, duck: SlayerQueryEngine, distinct: bool) -> None:
        body = query(dimensions=["id", "skills"], distinct_dimension_values=distinct,
                     order=[{"column": "skills", "direction": "asc"}])
        with pytest.raises(ListTypeError, match=r"(?i)order") as info:
            await duck.execute(body)
        assert _names_the_list_type(str(info.value)), str(info.value)

    async def test_ordering_a_grouped_list(self, duck: SlayerQueryEngine) -> None:
        body = query(dimensions=["skills"], measures=[measure("count(*)", "n")],
                     order=[{"column": "skills", "direction": "asc"}])
        with pytest.raises(ListTypeError, match=r"(?i)order"):
            await duck.execute(body)

    @pytest.mark.parametrize("agg", ["min", "max", "sum", "avg", "median", "count_distinct_approx", "collect"])
    async def test_other_aggregations(self, duck: SlayerQueryEngine, agg: str) -> None:
        body = query(measures=[measure(f"{agg}(skills)", "n")])
        with pytest.raises(SlayerError) as info:
            await duck.execute(body)
        assert _names_the_list_type(str(info.value)), str(info.value)

    async def test_custom_aggregation_over_a_list(self) -> None:
        model = contractors_model().model_copy(update={
            "aggregations": [Aggregation(name="my_agg", formula="COUNT({value})")]})
        async with duckdb_engine(statements=contractor_statements("duckdb"),
                                 engine_models=[model, assignments_model()]) as e:
            body = query(measures=[measure("my_agg(skills)", "n")])
            with pytest.raises(SlayerError) as info:
                await e.execute(body)
        assert _names_the_list_type(str(info.value)), str(info.value)

    async def test_aggregating_a_collected_list(self, duck: SlayerQueryEngine) -> None:
        body = query(measures=[measure("min(collect(team))", "n")])
        with pytest.raises(SlayerError) as info:
            await duck.execute(body)
        assert _names_the_list_type(str(info.value)), str(info.value)

    async def test_null_test(self, duck: SlayerQueryEngine) -> None:
        assert await names(duck, "skills IS NULL") == {"Dee"}
        assert await names(duck, "skills IS NOT NULL") == {"Ann", "Bob", "Cid", "Eve", "Fay", "Gil"}


# --------------------------------------------------------------------------- #
# Counting lists
# --------------------------------------------------------------------------- #
class TestCounts:
    async def test_distinct_count_of_lists(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(measures=[measure("count_distinct(skills)", "n")]))
        assert num(resp.data[0]["contractors.n"]) == 4
        assert any("count_distinct(skills.value)" in w.human_message() for w in whole_list_payloads(resp))

    async def test_distinct_count_per_group(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["team"], measures=[measure("count_distinct(skills)", "n")]))
        expected = {t: len({elset(r["skills"]) for r in ROWS if r["team"] == t} - {None}) for t in ("A", "B", "C")}
        assert {cell(r, "team"): num(cell(r, "n")) for r in resp.data} == expected

    async def test_colon_spelling(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(measures=["skills:count_distinct"]))
        assert num(next(iter(resp.data[0].values()))) == 4

    async def test_distinct_lists_and_distinct_elements(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(measures=[measure("count_distinct(skills)", "lists"),
                                                    measure("count_distinct(skills.value)", "elements")]))
        row = resp.data[0]
        assert (num(row["contractors.lists"]), num(row["contractors.elements"])) == (4, 2)

    async def test_counting_lists(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(measures=[measure("count(skills)", "n")],
                                          filters=["name IN ('Cid', 'Dee', 'Eve')"]))
        assert num(resp.data[0]["contractors.n"]) == 2


# --------------------------------------------------------------------------- #
# length
# --------------------------------------------------------------------------- #
class TestLength:
    async def test_element_counts(self, engine: SlayerQueryEngine) -> None:
        got = {k: num(v) for k, v in (await values(engine, "length(skills)")).items()}
        assert got == {"Ann": 2, "Bob": 1, "Cid": 0, "Dee": None, "Eve": 1, "Fay": 2, "Gil": 0}

    async def test_repeats_are_counted_through_the_elements(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(dimensions=["id", {"expression": "length(skills)", "name": "len"}],
                                          measures=[measure("count(skills.*)", "c")]))
        ann = next(r for r in resp.data if int(cell(r, "id")) == 1)
        assert (num(cell(ann, "len")), num(cell(ann, "c"))) == (2, 3)

    @pytest.mark.parametrize(("expr", "expected"), [
        ("length(scores)", {"Ann": 2, "Bob": 1, "Cid": 0, "Dee": None, "Eve": 1, "Fay": 2, "Gil": 0}),
        ("length(coalesce(skills, required))", {"Ann": 2, "Bob": 1, "Cid": 0, "Dee": 1, "Eve": 1, "Fay": 2,
                                                "Gil": 0}),
        ("length(['go', 'go', 'sql'])", {n: 2 for n in BY_NAME}),
        ("length(name)", {n: 3 for n in BY_NAME}),
    ])
    async def test_other_arguments(self, engine: SlayerQueryEngine, expr: str, expected: dict[str, Any]) -> None:
        assert {k: num(v) for k, v in (await values(engine, expr)).items()} == expected

    async def test_in_an_aggregation(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(measures=[measure("sum(length(skills))", "n")]))
        assert num(resp.data[0]["contractors.n"]) == 6


# --------------------------------------------------------------------------- #
# Multi-stage
# --------------------------------------------------------------------------- #
STAGE = {"name": "s", "source_model": "contractors", "dimensions": ["skills"], "measures": [measure("count(*)", "n")]}


class TestStages:
    async def test_grouped_list_projected_raw(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute([STAGE, {"source_model": "s", "dimensions": ["skills", "n"],
                                             "distinct_dimension_values": False}])
        assert {as_set(cell(r, "skills")): num(cell(r, "n")) for r in resp.data} == dict(GROUPS)
        assert len(resp.data) == 5

    async def test_regrouped(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute([STAGE, {"source_model": "s", "dimensions": ["skills"],
                                             "measures": [measure("sum(n)", "t")]}])
        assert {as_set(cell(r, "skills")): num(cell(r, "t")) for r in resp.data} == dict(GROUPS)
        if "list_sort" in (resp.sql or "").lower():
            assert "list_sort(list_distinct(list_sort(list_distinct(" not in (resp.sql or "").lower(), resp.sql

    async def test_compared(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute([STAGE, {"source_model": "s", "measures": [measure("sum(n)", "t")],
                                             "filters": ["skills = ['sql', 'go']"]}])
        assert num(cell(resp.data[0], "t")) == 2

    async def test_distinct_counted(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute([STAGE, {"source_model": "s",
                                             "measures": [measure("count_distinct(skills)", "d")]}])
        assert num(cell(resp.data[0], "d")) == 4

    async def test_length(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute([STAGE, {"source_model": "s", "dimensions": [
            "skills", {"expression": "length(skills)", "name": "len"}]}])
        assert {as_set(cell(r, "skills")): num(cell(r, "len")) for r in resp.data} == {
            g: None if g is None else len(g) for g in GROUPS}

    async def test_partition_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute([STAGE, {"source_model": "s", "dimensions": ["skills", "n"],
                                             "measures": [measure("count(*, partition_by=skills)", "c")]}])
        assert {as_set(cell(r, "skills")): num(cell(r, "c")) for r in resp.data} == {g: 1 for g in GROUPS}

    async def test_elements_of_a_grouped_list(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute([STAGE, {"source_model": "s", "dimensions": ["skills.value"],
                                             "measures": [measure("count(skills.*)", "c")]}])
        assert {cell(r, "skills.value"): num(cell(r, "c")) for r in resp.data} == {"go": 2, "sql": 2}

    async def test_contains_on_a_grouped_list(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute([STAGE, {"source_model": "s", "measures": [measure("sum(n)", "t")],
                                             "filters": ["contains(skills, 'go')"]}])
        assert num(cell(resp.data[0], "t")) == 3

    async def test_raw_stage_then_grouped(self, engine: SlayerQueryEngine) -> None:
        raw = {"name": "s", "source_model": "contractors", "dimensions": ["id", "skills"],
               "distinct_dimension_values": False}
        resp = await engine.execute([raw, {"source_model": "s", "dimensions": ["skills"],
                                           "measures": [measure("count(*)", "n")]}])
        assert {as_set(cell(r, "skills")): num(cell(r, "n")) for r in resp.data} == dict(GROUPS)

    async def test_grouped_list_output_keeps_its_list_type(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_model_from_query(query=STAGE, name="per_set")
        skills = model.get_column("skills")
        assert skills is not None
        assert (skills.type, skills.element_type) == (DataType.ARRAY, DataType.TEXT)

    async def test_three_stages(self, engine: SlayerQueryEngine) -> None:
        s1 = {"name": "s1", "source_model": "contractors", "dimensions": ["team", "skills"],
              "measures": [measure("count(*)", "n")]}
        s2 = {"name": "s2", "source_model": "s1", "dimensions": ["skills"], "measures": [measure("sum(n)", "t")]}
        s3 = {"source_model": "s2", "measures": [measure("count_distinct(skills)", "d"), measure("sum(t)", "total")]}
        resp = await engine.execute([s1, s2, s3])
        row = resp.data[0]
        assert (num(cell(row, "d")), num(cell(row, "total"))) == (4, 7)


# --------------------------------------------------------------------------- #
# The warning
# --------------------------------------------------------------------------- #
def _python_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    return [str(w.message) for w in caught if type(w.message).__name__ == "SlayerWholeListWarning"]


class TestWarning:
    @pytest.mark.parametrize(("body", "alternative"), [
        ({"dimensions": ["skills"], "measures": [measure("count(*)", "n")]}, "skills.value"),
        ({"measures": [measure("count_distinct(skills)", "n")]}, "count_distinct(skills.value)"),
    ])
    async def test_warns_on_both_channels(self, duck: SlayerQueryEngine, body: dict[str, Any],
                                          alternative: str) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            resp = await duck.execute(query(**body))
        (payload,) = whole_list_payloads(resp)
        assert alternative in payload.human_message()
        assert _python_warnings(caught) == [payload.human_message()]

    async def test_one_warning_per_operation_and_list(self, duck: SlayerQueryEngine) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            resp = await duck.execute(query(dimensions=["skills"], measures=[
                measure("count_distinct(skills)", "a"), measure("count_distinct(skills)", "b")]))
        payloads = whole_list_payloads(resp)
        assert len(payloads) == 2, [p.human_message() for p in payloads]
        assert any("count_distinct(skills.value)" in p.human_message() for p in payloads)
        assert len(_python_warnings(caught)) == 2

    async def test_one_warning_across_stages(self, duck: SlayerQueryEngine) -> None:
        resp = await duck.execute([STAGE, {"source_model": "s", "dimensions": ["skills"],
                                           "measures": [measure("sum(n)", "t")]}])
        payloads = whole_list_payloads(resp)
        assert len(payloads) == 1, [p.human_message() for p in payloads]

    @pytest.mark.parametrize("body", [
        {"dimensions": ["id", "skills"], "distinct_dimension_values": False},
        {"dimensions": ["name", {"expression": "length(skills)", "name": "n"}]},
        {"measures": [measure("count(skills)", "n")]},
    ])
    async def test_no_warning_without_a_whole_list_operation(self, duck: SlayerQueryEngine,
                                                             body: dict[str, Any]) -> None:
        resp = await duck.execute(query(**body))
        assert not whole_list_payloads(resp)

    @pytest.mark.parametrize("body", [
        {"dimensions": ["skills"], "measures": [measure("count(*)", "n")]},
        {"measures": [measure("count_distinct(skills)", "n")]},
    ])
    async def test_rest_carries_it(self, body: dict[str, Any]) -> None:
        async with duckdb_engine(statements=contractor_statements("duckdb"), engine_models=models()) as e:
            (payload,) = whole_list_payloads(await e.execute(query(**body)))
            client = TestClient(create_app(storage=e.storage))
            resp = client.post("/query", json={"source_model": "contractors", **body})
        assert resp.status_code == 200, resp.text
        assert payload.model_dump(mode="json") in resp.json().get("warnings") or [], resp.json()

    async def test_mcp_renders_it(self) -> None:
        async with duckdb_engine(statements=contractor_statements("duckdb"), engine_models=models()) as e:
            server = create_mcp_server(storage=e.storage, _seed_help=False)
            blocks, _ = await server.call_tool(name="query", arguments={"query": {
                "source_model": "contractors", "dimensions": ["skills"],
                "measures": [{"formula": "count_distinct(skills)", "name": "n"}]}})
        text = "\n".join(b.text for b in blocks if isinstance(b, TextContent))
        assert "count_distinct(skills.value)" in text, text


# --------------------------------------------------------------------------- #
# Opaque elements
# --------------------------------------------------------------------------- #
GRIDS = ("CREATE TABLE grids (id INTEGER, grid INTEGER[][])",
         "INSERT INTO grids VALUES (1, [[1, 2], [3]]), (2, NULL), (3, [[1, 2], [3]])")


def grids_model() -> SlayerModel:
    return SlayerModel(name="grids", sql_table="grids", data_source="test", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="grid", type=DataType.ARRAY, element_type=DataType.UNKNOWN)])


@pytest.fixture(scope="module")
async def grids() -> AsyncGenerator[SlayerQueryEngine]:
    async with duckdb_engine(statements=GRIDS, engine_models=[grids_model()]) as e:
        yield e


class TestOpaqueElements:
    async def test_null_tests_and_count_run(self, grids: SlayerQueryEngine) -> None:
        resp = await grids.execute({"source_model": "grids", "measures": [measure("count(grid)", "n")],
                                    "filters": ["grid IS NULL"]})
        assert num(resp.data[0]["grids.n"]) == 0
        resp = await grids.execute({"source_model": "grids", "measures": [measure("count(grid)", "n")],
                                    "filters": ["grid IS NOT NULL"]})
        assert num(resp.data[0]["grids.n"]) == 2

    @pytest.mark.parametrize("body", [
        {"dimensions": ["id", {"expression": "length(grid)", "name": "n"}]},
        {"dimensions": ["grid"], "measures": [measure("count(*)", "n")]},
        {"dimensions": ["grid"]},
        {"dimensions": ["grid.value"]},
        {"measures": [measure("count(*)", "n")], "filters": ["grid IS NOT DISTINCT FROM grid"]},
        {"measures": [measure("count(*)", "n")], "filters": ["grid = grid"]},
        {"measures": [measure("count_distinct(grid)", "n")]},
        {"measures": [measure("count(*)", "n")], "filters": ["grid IN ([1], [2])"]},
    ])
    async def test_set_readings_refused(self, grids: SlayerQueryEngine, body: dict[str, Any]) -> None:
        with pytest.raises(SlayerError, match="UNKNOWN"):
            await grids.execute({"source_model": "grids", **body})


# --------------------------------------------------------------------------- #
# Aggregation metadata: defaults, save time, re-ingest
# --------------------------------------------------------------------------- #
def _list_column(**kw: Any) -> dict[str, Any]:
    return {"name": "skills", "type": "ARRAY", "element_type": "TEXT", **kw}


class TestAllowedAggregations:
    def test_type_defaults(self) -> None:
        assert DEFAULT_AGGREGATIONS_BY_TYPE[DataType.ARRAY] == frozenset({"count", "count_distinct"})

    @pytest.mark.parametrize("allowed", [["count", "count_distinct"], ["count_distinct"], ["count"]])
    def test_count_and_distinct_count_save(self, allowed: list[str]) -> None:
        SlayerModel.model_validate({"name": "c", "sql_table": "c", "data_source": "test",
                                    "columns": [_list_column(allowed_aggregations=allowed)]})

    @pytest.mark.parametrize("allowed", [["sum"], ["count", "count_distinct", "min"], ["count_distinct_approx"]])
    def test_other_aggregations_fail_to_save(self, allowed: list[str]) -> None:
        data = {"name": "c", "sql_table": "c", "data_source": "test",
                "columns": [_list_column(allowed_aggregations=allowed)]}
        with pytest.raises(ValueError, match="skills"):
            SlayerModel.model_validate(data)

    async def test_round_trips_through_storage(self, tmp_path: Path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(name="test", type="duckdb", database=":memory:"))
        model = SlayerModel.model_validate({"name": "c", "sql_table": "c", "data_source": "test", "columns": [
            _list_column(allowed_aggregations=["count", "count_distinct"])]})
        await storage.save_model(model)
        loaded = await storage.get_model("c", data_source="test")
        assert loaded is not None
        skills = loaded.get_column("skills")
        assert skills is not None
        assert skills.allowed_aggregations == ["count", "count_distinct"]


# --------------------------------------------------------------------------- #
# Agent rendering
# --------------------------------------------------------------------------- #
def _says_set_grouping(text: str) -> bool:
    lines = [ln for ln in text.splitlines() if "skills.value" in ln]
    return any(re.search(r"\bsets?\b", ln, re.IGNORECASE) and re.search(r"group", ln, re.IGNORECASE)
               and re.search(r"compar", ln, re.IGNORECASE) for ln in lines)


class TestRendering:
    async def test_inspect_model(self, duck: SlayerQueryEngine) -> None:
        model = await duck.storage.get_model("contractors", data_source="test")
        assert model is not None
        out = await render_model_inspection(model=model, storage=duck.storage, engine=duck, format="markdown",
                                            sections=["columns"], compact=False)
        assert _says_set_grouping(out), out

    def test_search_column_text(self) -> None:
        model = contractors_model()
        skills = model.get_column("skills")
        assert skills is not None
        text = render_column_text(model=model, column=skills)
        assert _says_set_grouping(text), text

    def test_models_summary(self) -> None:
        markdown = render_models_summary(datasource_name="test", models=[contractors_model()], fmt="markdown",
                                         compact=False)
        assert _says_set_grouping(markdown), markdown


# --------------------------------------------------------------------------- #
# The element-set key kind
# --------------------------------------------------------------------------- #
SKILLS = ColumnKey(leaf="skills")
REQUIRED = ColumnKey(leaf="required")


def _list_set(source: Any) -> Any:
    return keys.ListSetKey(source=source)


def _column_type(key: Any) -> Optional[DataType]:
    return DataType.ARRAY if isinstance(key, ColumnKey) and key.leaf in _LISTS else None


def _column_element(key: Any) -> Optional[DataType]:
    return DataType.TEXT if isinstance(key, ColumnKey) and key.leaf in ("skills", "required") else None


class TestKeyKind:
    def test_is_a_classified_value_key_kind(self) -> None:
        assert keys.ListSetKey in VALUE_KEY_TYPES
        assert keys.ListSetKey in KIND_POLICY

    def test_traversal(self) -> None:
        key = _list_set(SKILLS)
        assert key.children() == (SKILLS,)
        assert key.map_children(lambda c: c) is key
        assert key.map_children(lambda c: REQUIRED) == _list_set(REQUIRED)

    def test_identity(self) -> None:
        assert _list_set(SKILLS) == _list_set(ColumnKey(leaf="skills"))
        assert hash(_list_set(SKILLS)) == hash(_list_set(ColumnKey(leaf="skills")))
        assert _list_set(SKILLS) != SKILLS

    def test_reroot_and_prepend(self) -> None:
        joined = ColumnKey(path=("contractors",), leaf="skills")
        assert reroot_value_key(_list_set(joined), target_path=("contractors",)) == _list_set(SKILLS)
        assert prepend_value_key(_list_set(SKILLS), host_path=("contractors",)) == _list_set(joined)

    def test_substitution_reaches_the_source(self) -> None:
        assert substitute_value_keys(_list_set(SKILLS), {SKILLS: REQUIRED}) == _list_set(REQUIRED)

    def test_typed_as_the_sources_list(self) -> None:
        key = _list_set(SKILLS)
        assert value_type(key, column_type=_column_type) is DataType.ARRAY
        assert list_element_type(key, column_type=_column_type, column_element=_column_element) is DataType.TEXT

    def test_phase_is_the_sources(self) -> None:
        assert _list_set(SKILLS).phase is Phase.ROW

    def test_display(self) -> None:
        text = key_display(_list_set(SKILLS))
        assert "skills" in text
        assert "ListSetKey" not in text
        assert "leaf=" not in text


# --------------------------------------------------------------------------- #
# Dialects
# --------------------------------------------------------------------------- #
class TestDialectHooks:
    @pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
    def test_every_native_list_dialect_builds_the_set_and_its_key(self, dialect: str) -> None:
        d = get_dialect(dialect)
        canonical = d.build_list_set(exp.column("x"), DataType.TEXT)
        assert isinstance(canonical, Expression)
        assert isinstance(d.build_list_key(canonical), Expression)

    @pytest.mark.parametrize(("dialect", "database"), sorted(NO_LIST_DATABASES.items()))
    async def test_no_list_databases_refuse_grouping(self, dialect: str, database: str) -> None:
        body, schema = query(dimensions=["skills"], measures=[measure("count(*)", "n")]), models()
        with pytest.raises(SlayerError, match=database):
            await emit(body, dialect=dialect, models=schema)


#: Every position that reads a list's element set.
SITES: dict[str, SlayerQuery] = {
    "group": query(dimensions=["skills"], measures=[measure("count(*)", "n")]),
    "distinct": query(dimensions=["skills"]),
    "group-expression": query(dimensions=[{"name": "s", "expression": "coalesce(skills, required)"}],
                           measures=[measure("count(*)", "n")]),
    "compare": query(measures=[measure("count(*)", "n")], filters=["skills = ['go', 'sql']"]),
    "compare-columns": query(measures=[measure("count(*)", "n")], filters=["skills = required"]),
    "in": query(measures=[measure("count(*)", "n")], filters=["skills IN (['go'], ['sql'])"]),
    "count_distinct": query(measures=[measure("count_distinct(skills)", "n")]),
    "partition": query(dimensions=["id", "skills"], measures=[measure("count(*, partition_by=skills)", "n")]),
    "join-back": query(dimensions=["skills"], measures=[measure("count(assignments.id)", "n")]),
    "association": query(dimensions=["skills", "assignments.id"], measures=[measure("sum(rate)", "n")],
                      to_many_handling="associate"),
    "window-transform": query(dimensions=["skills"], time_dimensions=[YEARLY], measures=[measure("cumsum(count(*))", "n")]),
    "time-shift": query(dimensions=["skills"], time_dimensions=[YEARLY],
                     measures=[measure("time_shift(count(*), -1)", "n")]),
    "windowed-aggregate": query(dimensions=["skills"], time_dimensions=[YEARLY],
                             measures=[measure("sum(rate, window='400d')", "n")]),
    "rank-partition": query(dimensions=["skills", "team"],
                         measures=[measure("rank(sum(rate), partition_by=skills, direction='desc')", "n")]),
    "nested": query(measures=[measure("count(count(*, partition_by=skills))", "n")]),
    "length": query(dimensions=["id", {"expression": "length(skills)", "name": "n"}]),
}
#: Sites projecting a grouped list.
DISPLAYED = ("group", "distinct", "group-expression", "partition", "join-back", "association", "window-transform",
             "time-shift", "windowed-aggregate", "rank-partition")
#: Each dialect's canonicalisation, as it appears in the emitted SQL (upper-cased).
CANONICAL = {"duckdb": "LIST_DISTINCT(", "postgres": "UNNEST(", "snowflake": "ARRAY_DISTINCT(",
             "clickhouse": "ARRAYDISTINCT(", "trino": "ARRAY_DISTINCT(", "bigquery": "UNNEST("}


class TestEmission:
    @pytest.mark.parametrize("site", sorted(SITES))
    @pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
    async def test_every_site_reads_the_canonical_set(self, dialect: str, site: str) -> None:
        sql = await emit(SITES[site], dialect=dialect, models=models())
        assert CANONICAL[dialect] in sql.upper(), sql

    @pytest.mark.parametrize("site", sorted(set(SITES) - {"length"}))
    async def test_bigquery_compares_the_json_key(self, site: str) -> None:
        sql = await emit(SITES[site], dialect="bigquery", models=models())
        assert "TO_JSON_STRING(" in sql.upper(), sql

    @pytest.mark.parametrize("site", DISPLAYED)
    async def test_bigquery_displays_any_value(self, site: str) -> None:
        sql = await emit(SITES[site], dialect="bigquery", models=models())
        assert "ANY_VALUE(" in sql.upper(), sql

    @pytest.mark.parametrize("site", sorted(SITES))
    @pytest.mark.parametrize("dialect", ["duckdb", "bigquery"])
    async def test_set_and_key_applied_once(self, dialect: str, site: str) -> None:
        sql = re.sub(r"\s+", "", (await emit(SITES[site], dialect=dialect, models=models())).upper())
        assert "LIST_SORT(LIST_DISTINCT(LIST_SORT(LIST_DISTINCT(" not in sql, sql
        assert "TO_JSON_STRING(TO_JSON_STRING(" not in sql, sql
        assert "ANY_VALUE(TO_JSON_STRING(" not in sql, sql

    async def test_bigquery_groups_by_the_key(self) -> None:
        sql = await emit(SITES["group"], dialect="bigquery", models=models())
        assert re.search(r"GROUP BY\s[^;]*TO_JSON_STRING\(", sql.upper()), sql

    async def test_bigquery_distinct_counts_the_key(self) -> None:
        sql = await emit(SITES["count_distinct"], dialect="bigquery", models=models())
        assert re.search(r"COUNT\(\s*DISTINCT\s+TO_JSON_STRING\(", sql.upper()), sql

    async def test_bigquery_partitions_by_the_key(self) -> None:
        sql = await emit(SITES["rank-partition"], dialect="bigquery", models=models())
        assert re.search(r"PARTITION BY\s[^)]*TO_JSON_STRING\(", sql.upper()), sql

    async def test_a_raw_projection_is_not_canonicalised(self) -> None:
        sql = await emit(query(dimensions=["id", "skills"], distinct_dimension_values=False), dialect="duckdb",
                         models=models())
        assert "LIST_DISTINCT(" not in sql.upper(), sql


# --------------------------------------------------------------------------- #
# Postgres array types
# --------------------------------------------------------------------------- #
#: ``widths``: one element set per row spelled in several Postgres array types.
WIDTHS = (
    "CREATE TABLE widths (id INTEGER PRIMARY KEY, v VARCHAR[], t TEXT[], s SMALLINT[], i INTEGER[], b BIGINT[], "
    "r REAL[], d DOUBLE PRECISION[])",
    "INSERT INTO widths VALUES "
    "(1, ARRAY['a', 'b', 'a'], ARRAY['b', 'a'], ARRAY[1, 2], ARRAY[1, 2], ARRAY[2, 1, 2], ARRAY[1.5, 2.5], "
    "ARRAY[2.5, 1.5]), "
    "(2, ARRAY['a'], ARRAY['b'], ARRAY[7], ARRAY[1], ARRAY[2], ARRAY[1.5], ARRAY[2.5]), "
    "(3, ARRAY['c'], ARRAY['c'], ARRAY[4], ARRAY[4], ARRAY[4], ARRAY[4.0], ARRAY[4.0])",
)


@pytest.fixture
async def widths(postgresql: Any, tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with postgres_engine(postgresql, tmp_path, statements=WIDTHS, engine_models=[]) as e:
        ds = await e.storage.get_datasource("test")
        assert ds is not None
        (model,) = await ingest_datasource_async(ds, client=e.client_for(ds), include_tables=["widths"])
        await e.storage.save_model(model)
        yield e


def widths_model() -> SlayerModel:
    def array(name: str, element: DataType, db_type: str) -> Column:
        return Column(name=name, type=DataType.ARRAY, element_type=element, db_type=db_type)
    return SlayerModel(name="widths", sql_table="widths", data_source="test", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        array("v", DataType.TEXT, "VARCHAR[]"), array("t", DataType.TEXT, "TEXT[]"),
        array("s", DataType.INT, "SMALLINT[]"), array("i", DataType.INT, "INTEGER[]"),
        array("b", DataType.INT, "BIGINT[]"), array("r", DataType.DOUBLE, "REAL[]"),
        array("d", DataType.DOUBLE, "DOUBLE PRECISION[]")])


@pytest.mark.parametrize("filter_", ["v = t", "t = v", "i = b", "b = i", "r = d", "d = r", "s = i", "i = d",
                                     "d = i", "v = ['a']", "b IN ([1], [2])"])
async def test_postgres_casts_each_operand_before_unnesting(filter_: str) -> None:
    sql = await emit(SlayerQuery.model_validate({"source_model": "widths", "dimensions": ["id"],
                                                 "filters": [filter_]}), dialect="postgres",
                     models=[widths_model()])
    assert len(re.findall(r"UNNEST\(\s*CAST\(", sql.upper())) >= 2, sql


async def _width_ids(engine: SlayerQueryEngine, filter_: str) -> set[int]:
    resp = await engine.execute({"source_model": "widths", "dimensions": ["id"], "filters": [filter_]})
    return {int(r["widths.id"]) for r in resp.data}


class TestPostgresArrayTypes:
    @pytest.mark.parametrize(("left", "right", "expected"), [
        ("v", "t", {1, 3}), ("i", "b", {1, 3}), ("s", "i", {1, 3}), ("r", "d", {1, 3}), ("i", "d", {3}),
    ])
    async def test_columns_of_one_family_compare_in_both_orders(self, widths: SlayerQueryEngine, left: str,
                                                                right: str, expected: set[int]) -> None:
        assert await _width_ids(widths, f"{left} = {right}") == expected
        assert await _width_ids(widths, f"{right} = {left}") == expected

    @pytest.mark.parametrize(("filter_", "expected"), [
        ("v = ['a', 'b']", {1}), ("b = [1, 2]", {1}), ("s = [2, 1]", {1}), ("r = [2.5, 1.5]", {1}),
        ("i IN ([1, 2], [4])", {1, 3}), ("t IN (['a', 'b'], ['c'])", {1, 3}),
    ])
    async def test_literals_against_each_type(self, widths: SlayerQueryEngine, filter_: str,
                                              expected: set[int]) -> None:
        assert await _width_ids(widths, filter_) == expected

    @pytest.mark.parametrize("column", ["v", "t", "s", "i", "b", "r", "d"])
    async def test_grouping_and_distinct_counting_each_type(self, widths: SlayerQueryEngine, column: str) -> None:
        resp = await widths.execute({"source_model": "widths", "dimensions": [column],
                                     "measures": [measure("count(*)", "n"),
                                                  measure(f"count({column}, partition_by={column})", "p")]})
        assert sum(num(r["widths.n"]) or 0 for r in resp.data) == 3
        total = await widths.execute({"source_model": "widths",
                                      "measures": [measure(f"count_distinct({column})", "sets")]})
        assert num(total.data[0]["widths.sets"]) == len(resp.data)
