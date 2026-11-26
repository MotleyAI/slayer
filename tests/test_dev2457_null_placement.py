"""NULL placement in a Mode B window's ordering, and first / last against FIRST_VALUE / LAST_VALUE.

Spec: queries/window-functions › NULL sorts as the largest value in a window's ordering; › first and last differ
from FIRST_VALUE and LAST_VALUE on NULL keys.

Rows: tests/_dev2457_fixtures.py. scores (id, score, at): 1 10 2024-01-01 | 2 NULL 2024-01-02 | 3 5 NULL.
MySQL and SQL Server have no NULLS clause and sort NULL lowest: the placement is a 0/1 key on the item's
NULL-ness, added only where Ossie's placement differs from theirs.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Sequence
from pathlib import Path
from typing import Any

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.expressions.core import Expression
from sqlglot.tokenizer_core import TokenType

from slayer.core.enums import DataType
from slayer.core.errors import IllegalWindowInFilterError, QueryTypeError, UnevaluableEntityError
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetMetric, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1737_fixtures import seed_backend
from tests._dev2101_fixtures import emit
from tests._dev2457_fixtures import (
    BACKENDS,
    TABLES,
    by,
    floats,
    grouped,
    ints,
    raw,
    run,
    scores_dataset,
    ties_dataset,
    window_engine,
)
from tests._engine_helpers import owned_query_engine
from tests._osi_export_fixtures import orders as export_orders
from tests._osi_export_fixtures import slayer_metric
from tests._osi_export_helpers import export_datasets, export_text, make_env
from tests._ossie_gate import TIER1

BY_SCORE = "ROW_NUMBER() OVER (ORDER BY score)"
BY_SCORE_DESC = "ROW_NUMBER() OVER (ORDER BY score DESC)"
#: Ossie's placement under ``ORDER BY score DESC`` (NULL first) and the one SQLite and DuckDB give (NULL last).
OSSIE_DESC = {1: 2, 2: 1, 3: 3}
NATIVE_DESC = {1: 1, 2: 3, 3: 2}
RANGE_SUM = "SUM(scores.score) OVER (ORDER BY scores.score{nulls} RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)"
#: A document whose window field is written only as an ANSI_SQL entry.
ANSI_DOC = """version: "0.3.0.dev0"
name: m_scores
datasets:
  - name: doc_scores
    source: scores
    fields:
      - name: id
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: id
      - name: rn
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: ROW_NUMBER() OVER (ORDER BY score DESC)
"""
#: Dialects with no NULLS clause, sorting NULL lowest.
KEYED = ("mysql", "tsql")
#: (order item suffix, descending, NULLs first).
PLACEMENTS = [
    pytest.param("", False, False, id="default-asc"),
    pytest.param(" DESC", True, True, id="default-desc"),
    pytest.param(" NULLS FIRST", False, True, id="nulls-first"),
    pytest.param(" DESC NULLS LAST", True, False, id="desc-nulls-last"),
]


def _native(name: str, text: str) -> DatasetField:
    """A field whose definition is the SQLite and DuckDB dialect entry ``text``."""
    return DatasetField(name=name, datatype=DataType.INTEGER, expression=ExpressionObject(dialects=[
        DialectEntry(dialect="SQLITE", expression=text), DialectEntry(dialect="DUCKDB", expression=text)]))


def _entry(name: str, *, dialect: str, text: str) -> DatasetField:
    return DatasetField(name=name, datatype=DataType.INTEGER,
                        expression=ExpressionObject(dialects=[DialectEntry(dialect=dialect, expression=text)]))


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
async def engine(backend: str) -> AsyncGenerator[SlayerQueryEngine]:
    async with window_engine(backend) as eng:
        yield eng


@contextlib.asynccontextmanager
async def _scores_engine(backend: str, *fields: DatasetField,
                         metrics: Sequence[DatasetMetric] = ()) -> AsyncGenerator[SlayerQueryEngine]:
    dataset = scores_dataset(fields=fields).model_copy(update={"metrics": list(metrics)})
    async with window_engine(backend, datasets=[dataset]) as eng:
        yield eng


@contextlib.asynccontextmanager
async def _nulls_first_duckdb(base: Path, *datasets: SlayerDataset) -> AsyncGenerator[SlayerQueryEngine]:
    """A DuckDB engine whose sessions run with ``default_null_order = 'nulls_first'``."""
    db = base / "seed.duckdb"
    seed_backend("duckdb", str(db), TABLES)
    storage = YAMLStorage(base_dir=str(base / "store"))
    datasource = DatasourceConfig(name="test", type="duckdb",
                                  connection_string=f"duckdb:///{db}?default_null_order=nulls_first")
    async with owned_query_engine(storage=storage, datasources=[datasource]) as eng:
        for dataset in datasets:
            await storage.save_dataset(dataset, _validate=False)
        yield eng


async def _columns(engine: SlayerQueryEngine, population: str, *expressions: str) -> list[dict[Any, Any]]:
    """Each expression's value per row id of ``population``."""
    resp = await run(engine, raw(population, *expressions))
    return [by(resp, value=f"v{i}") for i in range(len(expressions))]


class TestPlacement:
    async def test_the_default_placement(self, engine):
        ascending, descending = await _columns(
            engine, "scores", "ROW_NUMBER() OVER (ORDER BY scores.score)",
            "ROW_NUMBER() OVER (ORDER BY scores.score DESC)")
        assert ints(ascending) == {1: 2, 2: 3, 3: 1}
        assert ints(descending) == {1: 2, 2: 1, 3: 3}

    async def test_a_stated_placement(self, engine):
        first, last = await _columns(
            engine, "scores", "ROW_NUMBER() OVER (ORDER BY scores.score NULLS FIRST)",
            "ROW_NUMBER() OVER (ORDER BY scores.score DESC NULLS LAST)")
        assert ints(first) == {1: 3, 2: 1, 3: 2}
        assert ints(last) == {1: 1, 2: 3, 3: 2}

    async def test_a_session_default_does_not_move_nulls(self, tmp_path):
        dataset = scores_dataset(fields=[_entry("session_rn", dialect="DUCKDB", text=BY_SCORE)])
        async with _nulls_first_duckdb(tmp_path, dataset) as eng:
            (session,) = await _columns(eng, "scores", "scores.session_rn")
            assert ints(session) == {1: 3, 2: 1, 3: 2}, "the session does not run with nulls_first"
            (portable,) = await _columns(eng, "scores", "ROW_NUMBER() OVER (ORDER BY scores.score)")
        assert ints(portable) == {1: 2, 2: 3, 3: 1}

    async def test_peers_survive_the_placement(self, engine):
        rank, dense, cume, count = await _columns(
            engine, "ties", "RANK() OVER (ORDER BY ties.a, ties.b DESC)",
            "DENSE_RANK() OVER (ORDER BY ties.a, ties.b DESC)", "CUME_DIST() OVER (ORDER BY ties.a)",
            "COUNT(*) OVER (ORDER BY ties.a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
        assert ints(rank) == {1: 3, 2: 3, 3: 1, 4: 2}
        assert ints(dense) == {1: 3, 2: 3, 3: 1, 4: 2}
        assert floats(cume) == {1: 1.0, 2: 1.0, 3: 0.5, 4: 0.5}
        assert ints(count) == {1: 4, 2: 4, 3: 2, 4: 2}

    async def test_a_placement_the_database_cannot_give_is_refused_runs_where_it_can(self, engine):
        (total,) = await _columns(engine, "scores", RANGE_SUM.format(nulls=""))
        assert ints(total) == {1: 10, 2: None, 3: 5}

    async def test_a_dialect_entry_keeps_the_databases_placement(self, backend):
        portable = DatasetField(name="rn_portable", datatype=DataType.INTEGER,
                                expression=ExpressionObject(sql=BY_SCORE_DESC))
        async with _scores_engine(backend, _native("rn_native", BY_SCORE_DESC), portable) as eng:
            native, ossie = await _columns(eng, "scores", "scores.rn_native", "scores.rn_portable")
        assert ints(native) == NATIVE_DESC
        assert ints(ossie) == OSSIE_DESC

    async def test_an_ossie_sql_entry_places_null_largest(self, backend):
        async with _scores_engine(backend, _entry("rn", dialect="OSSIE_SQL_2026", text=BY_SCORE_DESC)) as eng:
            (got,) = await _columns(eng, "scores", "scores.rn")
        assert ints(got) == OSSIE_DESC

    async def test_a_documents_ansi_sql_entry_places_null_largest(self, engine):
        await engine.storage.save_semantic_model_text(data_source="test", name="m_scores", text=ANSI_DOC)
        (got,) = await _columns(engine, "doc_scores", "doc_scores.rn")
        assert ints(got) == OSSIE_DESC

    async def test_a_metrics_expression_places_null_largest(self, backend):
        """``sum(score)`` is NULL in the cell of score 2."""
        async with _scores_engine(backend, metrics=[
            DatasetMetric(name="rn_desc", expression="ROW_NUMBER() OVER (ORDER BY SUM(score) DESC)"),
            DatasetMetric(name="rn_asc", expression="ROW_NUMBER() OVER (ORDER BY SUM(score))"),
        ]) as eng:
            descending = await run(eng, grouped("scores", "scores.id", "scores.rn_desc"))
            ascending = await run(eng, grouped("scores", "scores.id", "scores.rn_asc"))
        assert ints(by(descending, "d", "m")) == OSSIE_DESC
        assert ints(by(ascending, "d", "m")) == {1: 2, 2: 3, 3: 1}

    async def test_a_window_in_a_fields_filter_keeps_the_databases_placement(self, backend):
        fields = [
            DatasetField(name="top", datatype=DataType.INTEGER, expression="id", filter=f"{BY_SCORE_DESC} = 1"),
            DatasetField(name="bottom", datatype=DataType.INTEGER, expression="id", filter=f"{BY_SCORE} = 1"),
        ]
        async with _scores_engine(backend, *fields) as eng:
            top, bottom = await _columns(eng, "scores", "scores.top", "scores.bottom")
        assert ints(top) == {1: 1, 2: None, 3: None}
        # Ascending, SQLite puts NULL first and DuckDB last.
        native_first = 2 if backend == "sqlite" else 3
        assert ints(bottom) == {i: (i if i == native_first else None) for i in (1, 2, 3)}


class TestFirstAndLast:
    async def test_last_and_last_value_differ(self, engine):
        resp = await run(engine, {"population": "scores",
                                  "metrics": [{"metric": "last(scores.id, scores.at)", "alias": "m"}]})
        assert [int(r["m"]) for r in resp.data] == [2]
        (window,) = await _columns(
            engine, "scores", "LAST_VALUE(scores.id) OVER (ORDER BY scores.at "
                              "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)")
        assert ints(window) == {1: 3, 2: 3, 3: 3}

    async def test_first_and_first_value_differ(self, engine):
        resp = await run(engine, {"population": "scores",
                                  "metrics": [{"metric": "first(scores.id, scores.at)", "alias": "m"}]})
        assert [int(r["m"]) for r in resp.data] == [1]
        (window,) = await _columns(engine, "scores", "FIRST_VALUE(scores.id) OVER (ORDER BY scores.at NULLS FIRST)")
        assert ints(window) == {1: 3, 2: 3, 3: 3}

    @pytest.mark.parametrize("extensions", [True, False], ids=["extensions", "no-extensions"])
    async def test_export_never_writes_a_window_for_first_or_last(self, tmp_path, extensions):
        env = await make_env(tmp_path)
        dataset = export_orders(metrics=[slayer_metric("latest", "last(amount, ordered_at)"),
                                         slayer_metric("earliest", "first(amount, ordered_at)")])
        result = await export_datasets(env, dataset, extensions=extensions)
        assert {"latest", "earliest"} <= {w.metric_name for w in result.warnings}
        text = export_text(result)
        if extensions:
            assert "last(amount, ordered_at)" in text
            assert "first(amount, ordered_at)" in text
        assert "FIRST_VALUE" not in text.upper()
        assert "LAST_VALUE" not in text.upper()


async def _emit(dialect: str, expression: str) -> str:
    """The SQL of a raw-row query of ``scores`` listing ``expression``, on a datasource of ``dialect``."""
    return await emit(SlayerQuery.model_validate(raw("scores", expression)), dialect=dialect,
                      datasets=[scores_dataset(), ties_dataset()])


def _orderings(sql: str, *, dialect: str, function: type[Expression]) -> list[list[exp.Ordered]]:
    """The order keys of each ``function`` window in ``sql``."""
    windows = [w for w in sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Window) if isinstance(w.this, function)]
    assert windows, sql
    return [list(w.args["order"].expressions) if w.args.get("order") else [] for w in windows]


def _written_orderings(sql: str, *, dialect: str) -> list[list[str]]:
    """The order items of each ``ROW_NUMBER`` window as ``sql`` writes them, upper-cased token text."""
    tokens = Dialect.get_or_raise(dialect).tokenize(sql)
    out: list[list[str]] = []
    for at, token in enumerate(tokens):
        if token.token_type is not TokenType.OVER or tokens[at - 3].text.upper() != "ROW_NUMBER":
            continue
        depth, ordering, items, current = 0, False, [], []
        for inner in tokens[at + 1:]:
            depth += (inner.token_type is TokenType.L_PAREN) - (inner.token_type is TokenType.R_PAREN)
            if depth == 0:
                break
            if depth == 1 and inner.token_type is TokenType.ORDER_BY:
                ordering = True
            elif ordering and depth == 1 and inner.token_type is TokenType.COMMA:
                items.append(" ".join(current))
                current = []
            elif ordering:
                current.append(inner.text.upper())
        out.append([*items, " ".join(current)])
    assert out, sql
    return out


def _same(a: Expression, b: Expression) -> bool:
    if isinstance(a, exp.Column) and isinstance(b, exp.Column):
        return a.name.lower() == b.name.lower()
    return a == b


def _is_null_key(key: exp.Ordered, *, item: exp.Ordered) -> bool:
    """``key`` is ``CASE WHEN <item> IS NULL THEN 1 ELSE 0 END``."""
    case = key.this
    if not isinstance(case, exp.Case) or len(case.args["ifs"]) != 1:
        return False
    branch = case.args["ifs"][0]
    return (isinstance(branch.this, exp.Is) and isinstance(branch.this.expression, exp.Null)
            and _same(branch.this.this, item.this) and branch.args["true"] == exp.Literal.number(1)
            and case.args.get("default") == exp.Literal.number(0))


@pytest.mark.parametrize("dialect", [d for d in TIER1 if d not in KEYED])
@pytest.mark.parametrize(("suffix", "descending", "nulls_first"), PLACEMENTS)
async def test_the_placement_is_written_as_a_clause(dialect, suffix, descending, nulls_first):
    sql = await _emit(dialect, f"ROW_NUMBER() OVER (ORDER BY scores.score{suffix})")
    for keys in _orderings(sql, dialect=dialect, function=exp.RowNumber):
        assert len(keys) == 1, sql
        assert isinstance(keys[0].this, exp.Column), sql
        assert keys[0].this.name == "score", sql
        assert bool(keys[0].args.get("desc")) is descending, sql
        assert bool(keys[0].args.get("nulls_first")) is nulls_first, sql
    # A parsed item reads the dialect's default where none is written: the clause itself is checked in the text.
    for items in _written_orderings(sql, dialect=dialect):
        assert len(items) == 1, sql
        assert items[0].endswith("NULLS FIRST" if nulls_first else "NULLS LAST"), sql
        assert ("DESC" in items[0].split()) is descending, sql


@pytest.mark.parametrize("dialect", KEYED)
@pytest.mark.parametrize(("suffix", "descending", "nulls_first"), PLACEMENTS)
async def test_the_placement_is_written_as_a_null_key(dialect, suffix, descending, nulls_first):
    sql = await _emit(dialect, f"ROW_NUMBER() OVER (ORDER BY scores.score{suffix})")
    # NULL sorts lowest natively, so the key is needed only where NULL goes first exactly when descending.
    keyed = nulls_first is descending
    for keys in _orderings(sql, dialect=dialect, function=exp.RowNumber):
        assert len(keys) == (2 if keyed else 1), sql
        item = keys[-1]
        assert isinstance(item.this, exp.Column), sql
        assert item.this.name == "score", sql
        assert bool(item.args.get("desc")) is descending, sql
        if keyed:
            assert _is_null_key(keys[0], item=item), sql
            assert bool(keys[0].args.get("desc")) is nulls_first, sql
        else:
            assert not any(k.find(exp.Case) for k in keys), sql
    for items in _written_orderings(sql, dialect=dialect):
        assert not any("NULLS" in item.split() for item in items), sql


async def test_a_placement_the_database_cannot_give_is_refused_on_mysql():
    expression = RANGE_SUM.format(nulls="")
    with pytest.raises(QueryTypeError) as exc:
        await _emit("mysql", expression)
    assert not isinstance(exc.value, (IllegalWindowInFilterError, UnevaluableEntityError))
    assert "scores.score" in str(exc.value)
    assert "mysql" in str(exc.value).lower()


async def test_a_range_offset_frame_a_ranking_function_ignores_is_not_refused_on_mysql():
    sql = await _emit("mysql", "RANK() OVER (ORDER BY scores.score RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)")
    (keys,) = _orderings(sql, dialect="mysql", function=exp.Rank)
    assert _is_null_key(keys[0], item=keys[1]), sql


async def test_a_placement_the_database_cannot_give_is_refused_mysql_compiles_nulls_first():
    sql = await _emit("mysql", RANGE_SUM.format(nulls=" NULLS FIRST"))
    window = next(w for w in sqlglot.parse_one(sql, dialect="mysql").find_all(exp.Window) if isinstance(w.this, exp.Sum))
    (keys,) = _orderings(sql, dialect="mysql", function=exp.Sum)
    assert len(keys) == 1, sql
    assert isinstance(keys[0].this, exp.Column), sql
    assert keys[0].this.name == "score", sql
    assert not keys[0].args.get("desc"), sql
    assert window.args["spec"].args["kind"] == "RANGE", sql
