"""A ``{var}`` in a model's Mode-A text binds as a literal node, never as spliced text.

Spec: queries/variables › Mode-A surfaces bind variables as literals; models/save-validation;
models/column-filters › Variables substitute into both fields.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncIterator
from typing import Any

import duckdb
import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

DS = "shop"
_LITERAL_VALUES = "literal values"
_TABLES = (
    ("orders", "id INTEGER, status TEXT, name TEXT, amount DOUBLE PRECISION, created_at DATE",
     [(1, "ok", "Ann", 10.0, "2024-01-01"), (2, "new", "O'Brien", 20.0, "2024-02-01"),
      (3, "ok", "Bob", 30.0, "2024-03-01"), (4, "gone", "Cid", 40.0, "2024-04-01")]),
    ("users", "id INTEGER, email TEXT", [(1, "a@x")]),
)
_HOSTILE_SCALAR = "(SELECT count(*) FROM users)"
_HOSTILE_PREDICATE = "0 AND EXISTS (SELECT 1 FROM users WHERE id = 1)"


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as cur:
        for table, cols, rows in _TABLES:
            cur.execute(f"CREATE TABLE {table} ({cols.replace('DOUBLE PRECISION', 'REAL')})")
            cur.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows)


def _seed_duckdb(db_path: str) -> None:
    con = duckdb.connect(db_path)
    try:
        for table, cols, rows in _TABLES:
            con.execute(f"CREATE TABLE {table} ({cols})")
            con.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows)
    finally:
        con.close()


_BASE_COLUMNS = (
    ("id", DataType.INT), ("status", DataType.TEXT), ("name", DataType.TEXT),
    ("amount", DataType.DOUBLE), ("created_at", DataType.DATE),
)


def _columns(*extra: Column) -> list[Column]:
    return [Column(name=n, type=t, primary_key=n == "id") for n, t in _BASE_COLUMNS] + list(extra)


def _table_model(*, filters: list[str] | None = None, extra: tuple[Column, ...] = (), name: str = "orders") -> SlayerModel:
    return SlayerModel(name=name, sql_table="orders", data_source=DS, filters=filters or [], columns=_columns(*extra))


def _sql_model(sql: str, *, name: str = "orders_sql") -> SlayerModel:
    return SlayerModel(name=name, sql=sql, data_source=DS, columns=_columns())


def _query(source: str, formula: str, *, variables: dict[str, Any] | None = None, **extra: Any) -> SlayerQuery:
    raw: dict[str, Any] = {"source_model": source, "measures": [{"formula": formula, "name": "v"}], **extra}
    if variables is not None:
        raw["variables"] = variables
    return SlayerQuery.model_validate(raw)


class _Env:
    def __init__(self, *, storage: YAMLStorage, engine: SlayerQueryEngine) -> None:
        self.storage = storage
        self.engine = engine

    async def run(self, model: SlayerModel, query: SlayerQuery, *, validate: bool = True) -> Any:
        await self.storage.save_model(model, _validate=validate)
        return await self.engine.execute(query)

    async def value(self, model: SlayerModel, query: SlayerQuery) -> Any:
        resp = await self.run(model, query)
        return resp.data[0][f"{model.name}.v"]


@pytest.fixture(params=["sqlite", "duckdb"])
async def env(request) -> AsyncIterator[_Env]:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, f"shop.{request.param}")
        (_seed_duckdb if request.param == "duckdb" else _seed_sqlite)(db_path)
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        ds = DatasourceConfig(name=DS, type=request.param, database=db_path)
        async with owned_query_engine(storage=storage, datasources=[ds]) as engine:
            yield _Env(storage=storage, engine=engine)


@pytest.fixture
async def pg_env(postgresql, tmp_path) -> AsyncIterator[_Env]:
    cur = postgresql.cursor()
    for table, cols, rows in _TABLES:
        cur.execute(f"CREATE TABLE {table} ({cols})")
        cur.executemany(f"INSERT INTO {table} VALUES ({', '.join(['%s'] * len(rows[0]))})", rows)
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig.model_validate({"name": DS, "type": "postgres", "host": info.host, "port": info.port,
                                          "database": info.dbname, "username": info.user, "password": ""})
    storage = YAMLStorage(base_dir=str(tmp_path))
    async with owned_query_engine(storage=storage, datasources=[ds]) as engine:
        yield _Env(storage=storage, engine=engine)


@pytest.fixture
def executed(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []
    real = SlayerSQLClient.execute

    async def _spy(self, sql: str, *args: Any, **kwargs: Any):  # noqa: ANN202
        seen.append(sql)
        return await real(self, sql, *args, **kwargs)

    monkeypatch.setattr(SlayerSQLClient, "execute", _spy)
    return seen


_SCALED = Column(name="scaled", sql="amount * {k}", type=DataType.DOUBLE)
_FLOORED = Column(name="big", sql="amount", filter="amount >= {floor}", type=DataType.DOUBLE)
_TAGGED = Column(name="tagged", sql="name || '{suffix}'", type=DataType.TEXT)
_FLAGGED = Column(name="flagged", sql="CASE WHEN {flag} THEN amount ELSE 0 END", type=DataType.DOUBLE)

#: (model, formula, variables) — one hostile value per Mode-A surface.
_HOSTILE = {
    "column-sql": (lambda: _table_model(extra=(_SCALED,)), "sum(scaled)", {"k": _HOSTILE_SCALAR}),
    "column-filter": (lambda: _table_model(extra=(_FLOORED,)), "sum(big)", {"floor": _HOSTILE_PREDICATE}),
    "model-filter": (lambda: _table_model(filters=["amount >= {floor}"]), "sum(amount)", {"floor": _HOSTILE_PREDICATE}),
    "model-sql": (lambda: _sql_model("SELECT * FROM orders WHERE amount >= {floor}"), "sum(amount)",
                  {"floor": _HOSTILE_PREDICATE}),
    "string-in-bare-slot": (lambda: _table_model(extra=(_SCALED,)), "sum(scaled)", {"k": "abc"}),
}


class TestHostileValuesAreRefused:
    @pytest.mark.parametrize("case", list(_HOSTILE.values()), ids=list(_HOSTILE))
    async def test_refused_before_sql_runs(self, env: _Env, executed: list[str], case: tuple) -> None:
        build, formula, variables = case
        model = build()
        await env.storage.save_model(model)
        query = _query(model.name, formula, variables=variables)
        with pytest.raises(ValueError, match=_LITERAL_VALUES):
            await env.engine.execute(query)
        assert not any("users" in sql for sql in executed)

    async def test_a_model_default_is_bound_as_a_literal_too(self, env: _Env, executed: list[str]) -> None:
        model = _table_model(extra=(_SCALED,)).model_copy(update={"query_variables": {"k": _HOSTILE_SCALAR}})
        await env.storage.save_model(model)
        query = _query("orders", "sum(scaled)")
        with pytest.raises(ValueError, match=_LITERAL_VALUES):
            await env.engine.execute(query)
        assert not any("users" in sql for sql in executed)


class TestLiteralValuesBind:
    async def test_a_number_in_column_sql(self, env: _Env) -> None:
        assert await env.value(_table_model(extra=(_SCALED,)), _query("orders", "sum(scaled)", variables={"k": 2})) == 200

    @pytest.mark.parametrize("surface", ["column-filter", "model-filter", "model-sql"])
    async def test_a_numeric_string_reads_as_its_number(self, env: _Env, surface: str) -> None:
        model = {
            "column-filter": _table_model(extra=(_FLOORED,)),
            "model-filter": _table_model(filters=["amount >= {floor}"]),
            "model-sql": _sql_model("SELECT * FROM orders WHERE amount >= {floor}"),
        }[surface]
        formula = "sum(big)" if surface == "column-filter" else "sum(amount)"
        assert await env.value(model, _query(model.name, formula, variables={"floor": "25"})) == 70

    async def test_a_whole_in_list(self, env: _Env) -> None:
        model = _table_model(filters=["status IN ({statuses})"])
        assert await env.value(model, _query("orders", "sum(amount)", variables={"statuses": ["ok", "new"]})) == 60

    async def test_a_quote_inside_a_string_literal(self, env: _Env) -> None:
        resp = await env.run(_table_model(extra=(_TAGGED,)), SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["id", "tagged"], "filters": ["id = 1"],
            "variables": {"suffix": "O'Brien"},
        }))
        assert [r["orders.tagged"] for r in resp.data] == ["AnnO'Brien"]

    @pytest.mark.parametrize(("flag", "expected"), [(True, 100), (False, 0)])
    async def test_a_boolean_in_a_bare_slot(self, env: _Env, flag: bool, expected: float) -> None:
        model = _table_model(extra=(_FLAGGED,))
        assert await env.value(model, _query("orders", "sum(flagged)", variables={"flag": flag})) == expected

    async def test_sql_and_filter_of_one_column_both_bind(self, env: _Env) -> None:
        both = Column(name="both", sql="amount * {k}", filter="amount >= {floor}", type=DataType.DOUBLE)
        model = _table_model(extra=(both,))
        assert await env.value(model, _query("orders", "sum(both)", variables={"k": 2, "floor": 25})) == 140


#: (field, model) pairs whose placeholder stands in a non-literal position.
_NON_LITERAL = {
    "model-sql-table": _sql_model("SELECT * FROM {table}"),
    "column-function-name": _table_model(extra=(Column(name="c", sql="{f}(amount)", type=DataType.DOUBLE),)),
    "column-quoted-identifier": _table_model(extra=(Column(name="c", sql='"{c}"', type=DataType.DOUBLE),)),
    "column-comment": _table_model(extra=(Column(name="c", sql="amount /* {c} */", type=DataType.DOUBLE),)),
    "column-type-name": _table_model(extra=(Column(name="c", sql="CAST(amount AS {t})", type=DataType.DOUBLE),)),
    "column-partial-in-list": _table_model(extra=(Column(name="c", sql="status IN ('x', {s})", type=DataType.BOOLEAN),)),
}
#: A bare slot is a literal position: saved, and refused at query time when its value is no number or boolean.
_BARE_SLOT = _table_model(extra=(Column(name="c", sql="{col} * 2", type=DataType.DOUBLE),))


class TestNonLiteralPositionsAreRefused:
    @pytest.mark.parametrize("model", list(_NON_LITERAL.values()), ids=list(_NON_LITERAL))
    async def test_refused_at_save(self, env: _Env, model: SlayerModel) -> None:
        with pytest.raises(ValueError, match=_LITERAL_VALUES):
            await env.storage.save_model(model)
        assert await env.storage.get_model(model.name, data_source=DS) is None

    @pytest.mark.parametrize(("model", "formula", "variables"), [
        pytest.param(_BARE_SLOT, "sum(c)", {"col": "amount"}, id="identifier"),
        pytest.param(_NON_LITERAL["column-partial-in-list"], "count(*)", {"s": "ok"}, id="partial-in-list"),
        pytest.param(_NON_LITERAL["model-sql-table"], "count(*)", {"table": "users"}, id="table-name"),
    ])
    async def test_a_stored_model_is_refused_at_query_time(
        self, env: _Env, executed: list[str], model: SlayerModel, formula: str, variables: dict[str, Any],
    ) -> None:
        await env.storage.save_model(model, _validate=False)
        query = _query(model.name, formula, variables=variables)
        with pytest.raises(ValueError, match=_LITERAL_VALUES):
            await env.engine.execute(query)
        assert not any("users" in sql for sql in executed)


class TestOptionalBlocks:
    _BLOCK = _table_model(filters=["{? created_at >= '{from}' ?}"])

    async def test_a_supplied_block_filters_on_the_literal(self, env: _Env) -> None:
        assert await env.value(self._BLOCK, _query("orders", "sum(amount)", variables={"from": "2024-03-01"})) == 70

    async def test_an_unsupplied_block_is_dropped(self, env: _Env) -> None:
        assert await env.value(self._BLOCK, _query("orders", "sum(amount)", variables={"other": 1})) == 100

    async def test_a_hostile_value_inside_a_block_stays_a_literal(self, env: _Env) -> None:
        model = _table_model(filters=["{? amount >= {floor} ?}"])
        await env.storage.save_model(model)
        query = _query("orders", "sum(amount)", variables={"floor": _HOSTILE_PREDICATE})
        with pytest.raises(ValueError, match=_LITERAL_VALUES):
            await env.engine.execute(query)

    async def test_escaped_braces_render_one_brace(self, env: _Env) -> None:
        tag = Column(name="tag", sql="'{{x}}' || '{suffix}'", type=DataType.TEXT)
        resp = await env.run(_table_model(extra=(tag,)), SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["tag"], "variables": {"suffix": "!"},
        }))
        assert {r["orders.tag"] for r in resp.data} == {"{x}!"}


class TestPostgres:
    async def test_literal_values_bind(self, pg_env: _Env) -> None:
        model = _table_model(filters=["status IN ({statuses})", "amount >= {floor}"], extra=(_SCALED, _TAGGED))
        resp = await pg_env.run(model, SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["tagged"], "measures": [{"formula": "sum(scaled)", "name": "v"}],
            "variables": {"statuses": ["ok", "new"], "floor": "15", "k": 2, "suffix": "O'Brien"},
        }))
        assert {r["orders.tagged"]: float(r["orders.v"]) for r in resp.data} == {
            "O'BrienO'Brien": 40.0, "BobO'Brien": 60.0,
        }

    async def test_a_hostile_value_is_refused(self, pg_env: _Env, executed: list[str]) -> None:
        await pg_env.storage.save_model(_table_model(extra=(_SCALED,)))
        query = _query("orders", "sum(scaled)", variables={"k": _HOSTILE_SCALAR})
        with pytest.raises(ValueError, match=_LITERAL_VALUES):
            await pg_env.engine.execute(query)
        assert not any("users" in sql for sql in executed)

    async def test_a_non_literal_position_is_refused_at_save(self, pg_env: _Env) -> None:
        with pytest.raises(ValueError, match=_LITERAL_VALUES):
            await pg_env.storage.save_model(_NON_LITERAL["column-function-name"])
