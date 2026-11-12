"""A query-side string aggregation argument naming a placeholder is Mode B; a definition default stays Mode A.

Spec: aggregations/formula-templates › Aggregation parameters bind once; › A quoted placeholder renders its
binding's literal value.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.errors import UnanalyzableAggregationParameterError
from slayer.core.models import Aggregation, AggregationParam, Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import _engine_generate, seeded_exec_engine

_CUSTOMERS = [(1, 10), (2, 20), (3, 10)]
# (id, customer_id, status, amount, quantity, created_at)
_ORDERS = [
    (1, 1, "ok", 10.0, 1, "2024-01-05"), (2, 1, "new", 20.0, 2, "2024-02-05"),
    (3, 2, "ok", 30.0, 3, "2024-03-05"), (4, 3, "ok", 40.0, 4, "2024-04-05"),
]
_DDL = (
    "CREATE TABLE customers (id INTEGER, region_id INTEGER)",
    "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status TEXT, amount DOUBLE, quantity INTEGER, "
    "created_at DATE)",
    "CREATE TABLE users (id INTEGER)",
)


def _seed(db_path: str) -> None:
    inserts = (("customers", _CUSTOMERS), ("orders", _ORDERS), ("users", [(1,)]))
    if db_path.endswith(".duckdb"):
        con = duckdb.connect(db_path)
        try:
            for stmt in _DDL:
                con.execute(stmt)
            for table, rows in inserts:
                con.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows)
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        for stmt in _DDL:
            cur.execute(stmt)
        for table, rows in inserts:
            cur.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows)


_WSUM = Aggregation(name="wsum", formula="SUM({value} * {w})")
# A SQLite-only default (``unicode``), valid Mode A but outside the Mode-B library.
_USUM = Aggregation(name="usum", formula="SUM({value} * {w})",
                    params=[AggregationParam(name="w", sql="unicode(status) - unicode(status) + quantity")])
_LABEL = Aggregation(name="lbl", formula="MAX(CASE WHEN {value} > 0 THEN '{n}' END)",
                     params=[AggregationParam(name="n", sql="'x'")])


def _models(*aggs: Aggregation) -> list[SlayerModel]:
    customers = SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="region_id", type=DataType.INT)],
    )
    orders = SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="quantity", type=DataType.INT),
            Column(name="created_at", type=DataType.DATE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        aggregations=list(aggs),
    )
    return [customers, orders]


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect=request.param, seed=_seed, models=_models(_WSUM, _USUM)) as (e, _db):
        yield e


@pytest.fixture
def executed(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []

    real = SlayerSQLClient.execute

    async def _spy(self, sql: str, *args: Any, **kwargs: Any):  # noqa: ANN202
        seen.append(sql)
        return await real(self, sql, *args, **kwargs)

    monkeypatch.setattr(SlayerSQLClient, "execute", _spy)
    return seen


def _q(formula: str, **extra: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": formula, "name": "m"}], **extra})


async def _value(engine: SlayerQueryEngine, formula: str) -> float:
    resp = await engine.execute(_q(formula))
    return float(resp.data[0]["orders.m"])


class TestStringArgumentIsModeB:
    async def test_a_string_column_argument_equals_its_unquoted_spelling(self, engine: SlayerQueryEngine) -> None:
        quoted = await _value(engine, "wsum(amount, w='customers.region_id')")
        assert quoted == await _value(engine, "wsum(amount, w=customers.region_id)")
        assert quoted == pytest.approx(10 * 10 + 20 * 10 + 30 * 20 + 40 * 10)

    async def test_a_string_expression_argument_equals_its_unquoted_expression(self, engine: SlayerQueryEngine) -> None:
        quoted = await _value(engine, "weighted_avg(amount, weight='id * 2')")
        assert quoted == await _value(engine, "weighted_avg(amount, weight=id * 2)")
        assert quoted == pytest.approx(30.0)

    @pytest.mark.parametrize(("text", "marker"), [
        ("length(sqlite_version())", "sqlite_version"),
        ("(SELECT count(*) FROM users)", "users"),
        ("unicode(status)", "unicode"),
        ("amount::int || 'x' COLLATE nocase", "nocase"),
    ])
    async def test_dialect_text_fails_at_binding(
        self, engine: SlayerQueryEngine, executed: list[str], text: str, marker: str,
    ) -> None:
        literal = "'" + text.replace("'", "''") + "'"
        with pytest.raises(UnanalyzableAggregationParameterError) as exc:
            await engine.execute(_q(f"wsum(amount, w={literal})"))
        message = str(exc.value)
        assert "wsum" in message
        assert "w" in message
        assert text in message
        assert not any(marker in sql.lower() for sql in executed)

    async def test_a_setting_is_unchanged(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_q(
            "sum(amount, window='60d')", time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
        ))
        assert [float(r["orders.m"]) for r in sorted(resp.data, key=lambda r: str(r["orders.created_at"]))] == [
            10.0, 30.0, 50.0, 70.0,
        ]


class TestDefaultStaysModeA:
    async def test_a_dialect_default_binds(self) -> None:
        async with seeded_exec_engine(dialect="sqlite", seed=_seed, models=_models(_USUM)) as (engine, _db):
            assert await _value(engine, "usum(amount)") == pytest.approx(10 * 1 + 20 * 2 + 30 * 3 + 40 * 4)

    async def test_the_same_text_as_an_argument_is_refused(self) -> None:
        async with seeded_exec_engine(dialect="sqlite", seed=_seed, models=_models(_USUM)) as (engine, _db):
            with pytest.raises(UnanalyzableAggregationParameterError):
                await engine.execute(_q("usum(amount, w='unicode(status) - unicode(status) + quantity')"))


_TRICKY = "a'b\\c"
#: The Mode-B literal ``'a''b\c'`` (value ``a'b\c``), passed as a string argument.
_MODE_B_LITERAL = "'" + "'a''b\\c'".replace("'", "''") + "'"


class TestQuotedPlaceholder:
    @pytest.mark.parametrize("dialect", ["duckdb", "postgres", "mysql", "clickhouse", "bigquery"])
    async def test_a_mode_b_string_literal_cannot_escape_the_literal(self, dialect: str) -> None:
        orders = _models(_LABEL)[1]
        sql = await _engine_generate(
            query=SlayerQuery(source_model="orders", measures=[ModelMeasure(formula=f"lbl(amount, n={_MODE_B_LITERAL})",
                                                                            name="m")]),
            model=orders, dialect=dialect, extra_models=[_models()[0]],
        )
        case = sqlglot.parse_one(sql, read=dialect).find(exp.Case)
        assert case is not None, sql
        literal = case.args["ifs"][0].args["true"]
        assert isinstance(literal, exp.Literal)
        assert literal.is_string
        assert literal.this == _TRICKY
