"""Plain column references render as one quoted identifier in every clause (spec: sql/statement-assembly ›
A column reference renders the same in every clause)."""

from __future__ import annotations

from typing import Any, Optional

import pytest
import sqlglot
from pytest_postgresql import factories
from sqlglot import exp
from sqlglot.expressions.core import Expr

from slayer.core.enums import DataType
from slayer.core.errors import UnknownReferenceError
from slayer.core.keys import ColumnKey
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.plan import plan_query
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql import engine_factory
from slayer.sql.dialects import get_dialect
from slayer.sql.generator import SQLGenerator
from slayer.sql.naming import AliasAllocator
from slayer.sql.scope import ScopeFrame
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import _engine_generate

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

DIALECTS = ["postgres", "snowflake"]
DS = "pg"


def _col(name: str, dtype: DataType = DataType.TEXT, *, pk: bool = False) -> Column:
    return Column(name=name, type=dtype, primary_key=pk)


def _models(
    *, lower: bool = False, accounts_name: str = "accounts", regions_name: str = "regions",
    accounts_table: str = "accounts",
) -> list[SlayerModel]:
    """``orders`` → ``accounts`` → ``regions``, camelCase columns unless ``lower``."""

    def n(name: str) -> str:
        return name.lower() if lower else name

    orders = SlayerModel(name="orders", sql_table="orders", data_source=DS, columns=[
        _col("id", DataType.INT, pk=True), _col(n("orderNumber")), _col(n("accountId"), DataType.INT),
        _col("amount", DataType.DOUBLE), _col(n("createdAt"), DataType.TIMESTAMP),
    ], joins=[ModelJoin(target_model=accounts_name, join_pairs=[[n("accountId"), "id"]])])
    accounts = SlayerModel(name=accounts_name, sql_table=accounts_table, data_source=DS, columns=[
        _col("id", DataType.INT, pk=True), _col(n("extRef")), _col(n("signedUpAt"), DataType.TIMESTAMP),
        _col(n("regionId"), DataType.INT),
    ], joins=[ModelJoin(target_model=regions_name, join_pairs=[[n("regionId"), "id"]])])
    regions = SlayerModel(name=regions_name, sql_table="regions", data_source=DS, columns=[
        _col("id", DataType.INT, pk=True), _col(n("regionName")),
    ])
    return [orders, accounts, regions]


FILTERED = {"source_model": "orders", "dimensions": ["orderNumber", "accounts.extRef"],
            "filters": ["accounts.extRef = 'a'"]}
GROUPED = {"source_model": "orders", "dimensions": ["accounts.extRef"], "measures": [{"formula": "count(*)"}]}


async def _sql(query: dict[str, Any], *, dialect: str = "postgres", models: Optional[list[SlayerModel]] = None) -> str:
    host, *extra = models or _models()
    return await _engine_generate(query=SlayerQuery.model_validate(query), model=host, extra_models=extra,
                                  dialect=dialect)


def _reads(node: Expr, leaf: str) -> list[exp.Column]:
    return [c for c in node.find_all(exp.Column) if c.name == leaf]


def _assert_quoted_reads(node: Expr, *, leaf: str, sql: str, table: Optional[str] = None) -> set[str]:
    """Every read of ``leaf`` under ``node`` is quoted with one qualifier; returns the qualifiers."""
    reads = _reads(node, leaf)
    assert reads, f"no read of {leaf!r}:\n{sql}"
    unquoted = [c.sql() for c in reads if not c.this.quoted]
    assert not unquoted, f"unquoted reads of {leaf!r}: {unquoted}\n{sql}"
    tables = {c.table for c in reads}
    assert len(tables) == 1, f"{leaf!r} read under several qualifiers {tables}:\n{sql}"
    if table is not None:
        assert tables == {table}, f"{leaf!r} qualified {tables}, expected {table!r}:\n{sql}"
    return tables


def _parse(sql: str, dialect: str) -> Expr:
    return sqlglot.parse_one(sql, dialect=dialect)


def _subquery(tree: Expr, alias: str, sql: str) -> Expr:
    found = [s for s in tree.find_all(exp.Subquery) if s.alias == alias]
    assert found, f"no subquery aliased {alias!r}:\n{sql}"
    return found[0]


def _join_alias_of_table(tree: Expr, table: str, sql: str) -> str:
    aliases = [j.this.alias_or_name for j in tree.find_all(exp.Join)
               if isinstance(j.this, exp.Table) and j.this.name == table]
    assert len(aliases) == 1, f"expected one JOIN of {table!r}, got {aliases}:\n{sql}"
    return aliases[0]


class TestJoinedDimension:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_filtered_on_itself(self, dialect: str) -> None:
        sql = await _sql(FILTERED, dialect=dialect)
        tree = _parse(sql, dialect)
        _assert_quoted_reads(tree, leaf="extRef", table="accounts", sql=sql)
        for clause in ("expressions", "group", "where"):
            arg = tree.args.get(clause)
            nodes = arg if isinstance(arg, list) else [arg]
            assert any(n is not None and _reads(n, "extRef") for n in nodes), f"no extRef read in {clause}:\n{sql}"

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_grouped_under_count(self, dialect: str) -> None:
        sql = await _sql(GROUPED, dialect=dialect)
        tree = _parse(sql, dialect)
        _assert_quoted_reads(tree, leaf="extRef", table="accounts", sql=sql)
        assert _reads(tree.args["group"], "extRef"), sql
        assert any(_reads(p, "extRef") for p in tree.expressions), sql


class TestTwoHop:
    async def test_qualifier_is_the_join_alias(self) -> None:
        sql = await _sql({"source_model": "orders", "dimensions": ["accounts.regions.regionName"],
                          "measures": [{"formula": "count(*)"}]})
        tree = _parse(sql, "postgres")
        _assert_quoted_reads(tree, leaf="regionName", table=_join_alias_of_table(tree, "regions", sql), sql=sql)

    async def test_fitted_alias(self) -> None:
        accounts_name, regions_name = "accounts_" + "a" * 40, "regions_" + "r" * 40
        models = _models(accounts_name=accounts_name, regions_name=regions_name)
        sql = await _sql({"source_model": "orders", "dimensions": [f"{accounts_name}.{regions_name}.regionName"],
                          "measures": [{"formula": "count(*)"}]}, models=models)
        tree = _parse(sql, "postgres")
        alias = _join_alias_of_table(tree, "regions", sql)
        assert alias != f"{accounts_name}__{regions_name}", f"alias not fitted:\n{sql}"
        _assert_quoted_reads(tree, leaf="regionName", table=alias, sql=sql)

    async def test_colliding_alias(self) -> None:
        orders, accounts, regions = _models()
        orders = orders.model_copy(update={
            "columns": [*orders.columns, _col("dimId", DataType.INT)],
            "joins": [*orders.joins, ModelJoin(target_model="accounts__regions", join_pairs=[["dimId", "id"]])],
        })
        direct = SlayerModel(name="accounts__regions", sql_table="region_dim", data_source=DS, columns=[
            _col("id", DataType.INT, pk=True), _col("regionCode"),
        ])
        sql = await _sql({"source_model": "orders",
                          "dimensions": ["accounts.regions.regionName", "accounts__regions.regionCode"],
                          "measures": [{"formula": "count(*)"}]},
                         models=[orders, accounts, regions, direct])
        tree = _parse(sql, "postgres")
        chain_alias = _join_alias_of_table(tree, "regions", sql)
        direct_alias = _join_alias_of_table(tree, "region_dim", sql)
        assert chain_alias != direct_alias, sql
        _assert_quoted_reads(tree, leaf="regionName", table=chain_alias, sql=sql)
        _assert_quoted_reads(tree, leaf="regionCode", table=direct_alias, sql=sql)


class TestJoinedTimeDimension:
    async def test_base_select_truncation(self) -> None:
        sql = await _sql({"source_model": "orders", "time_dimensions": ["month(accounts.signedUpAt)"],
                          "measures": [{"formula": "count(*)"}]})
        tree = _parse(sql, "postgres")
        _assert_quoted_reads(tree, leaf="signedUpAt", table="accounts", sql=sql)
        assert _reads(tree.args["group"], "signedUpAt"), sql

    @pytest.mark.parametrize("producer", ["_src", "_base"])
    async def test_window_producers(self, producer: str) -> None:
        """``_src``: the duration window's source; ``_base``: the collapsed window grain."""
        sql = await _sql({"source_model": "orders", "dimensions": ["accounts.extRef"],
                          "time_dimensions": ["month(accounts.signedUpAt)"],
                          "measures": [{"formula": "sum(amount, window='90d')"}]})
        scope = _subquery(_parse(sql, "postgres"), producer, sql)
        _assert_quoted_reads(scope, leaf="signedUpAt", table=_join_alias_of_table(scope, "accounts", sql), sql=sql)


class TestRankedAggregate:
    async def test_partitioned_by_joined_column(self) -> None:
        sql = await _sql({"source_model": "orders", "dimensions": ["accounts.extRef"],
                          "time_dimensions": ["month(createdAt)"], "measures": [{"formula": "last(amount)"}]})
        tree = _parse(sql, "postgres")
        ranked = _subquery(tree, "_ranked_src", sql)
        _assert_quoted_reads(ranked, leaf="extRef", table="accounts", sql=sql)
        window = ranked.find(exp.Window)
        assert window is not None and _reads(window, "extRef"), sql

    async def test_ranked_by_joined_time(self) -> None:
        sql = await _sql({"source_model": "orders", "dimensions": ["orderNumber"],
                          "measures": [{"formula": "last(amount, accounts.signedUpAt)"}]})
        tree = _parse(sql, "postgres")
        ranked = _subquery(tree, "_ranked_src", sql)
        _assert_quoted_reads(ranked, leaf="signedUpAt", table="accounts", sql=sql)
        window = ranked.find(exp.Window)
        assert window is not None and _reads(window, "signedUpAt"), sql


class TestOrderingByJoinedColumn:
    async def test_projected(self) -> None:
        sql = await _sql({**GROUPED, "order": [{"column": "accounts.extRef"}]})
        _assert_quoted_reads(_parse(sql, "postgres"), leaf="extRef", table="accounts", sql=sql)

    async def test_grouped_not_projected(self) -> None:
        """Reads through the host-rooted ``MIN`` CTE, already quoted on 1.x; pinned."""
        sql = await _sql({"source_model": "orders", "dimensions": ["orderNumber"],
                          "measures": [{"formula": "count(*)"}], "order": [{"column": "accounts.extRef"}]})
        _assert_quoted_reads(_parse(sql, "postgres"), leaf="extRef", table="accounts", sql=sql)

    async def test_ungrouped_not_projected(self) -> None:
        sql = await _sql({"source_model": "orders", "dimensions": ["orderNumber"],
                          "distinct_dimension_values": False, "order": [{"column": "accounts.extRef"}]})
        tree = _parse(sql, "postgres")
        _assert_quoted_reads(tree, leaf="extRef", table="accounts", sql=sql)
        [ordered] = tree.args["order"].expressions
        assert isinstance(ordered.this, exp.Column) and ordered.this.name == "extRef", sql


#: Today's emission, pinned: lowercase names render unchanged on every dialect.
LOWER_FILTERED = """SELECT
  orders.ordernumber AS "orders.ordernumber",
  accounts.extref AS "orders.accounts.extref"
FROM orders AS orders
LEFT JOIN accounts AS accounts
  ON orders.accountid = accounts.id
WHERE
  accounts.extref = 'a'
GROUP BY
  orders.ordernumber,
  accounts.extref
ORDER BY
  "orders.ordernumber" ASC,
  "orders.accounts.extref" ASC"""
LOWER_GROUPED = """SELECT
  accounts.extref AS "orders.accounts.extref",
  COUNT(*) AS "orders._count"
FROM orders AS orders
LEFT JOIN accounts AS accounts
  ON orders.accountid = accounts.id
GROUP BY
  accounts.extref
ORDER BY
  "orders.accounts.extref" ASC"""


class TestLowercaseUnchanged:
    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize(("query", "expected"), [
        ({**FILTERED, "dimensions": ["ordernumber", "accounts.extref"], "filters": ["accounts.extref = 'a'"]},
         LOWER_FILTERED),
        ({**GROUPED, "dimensions": ["accounts.extref"]}, LOWER_GROUPED),
    ], ids=["filtered", "grouped"])
    async def test_byte_identical(self, query: dict[str, Any], expected: str, dialect: str) -> None:
        sql = await _sql(query, dialect=dialect, models=_models(lower=True))
        assert sql.strip() == expected


PG_DDL = (
    'CREATE TABLE "accountRecords" (id INTEGER PRIMARY KEY, "extRef" TEXT, "signedUpAt" TIMESTAMP, "regionId" INTEGER)',
    'CREATE TABLE orders (id INTEGER PRIMARY KEY, "orderNumber" TEXT, "accountId" INTEGER, amount DOUBLE PRECISION, '
    '"createdAt" TIMESTAMP)',
    'CREATE TABLE regions (id INTEGER PRIMARY KEY, "regionName" TEXT)',
    "INSERT INTO \"accountRecords\" VALUES (1, 'a', '2024-01-05', NULL), (2, 'b', '2024-02-05', NULL)",
    "INSERT INTO orders VALUES (1, 'n1', 1, 10, '2024-03-01'), (2, 'n2', 1, 20, '2024-03-02'), "
    "(3, 'n3', 2, 30, '2024-03-03')",
)


@pytest.fixture
async def pg_engine(postgresql, tmp_path):
    cur = postgresql.cursor()
    for stmt in PG_DDL:
        cur.execute(stmt)
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig(name=DS, type="postgres", host=info.host, port=info.port, database=info.dbname,
                          username=info.user, password="")
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(ds)
    for model in _models(accounts_table="accountRecords"):
        await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    try:
        yield engine
    finally:
        await engine.aclose()
        engine.close()
        engine_factory.invalidate_engine(ds)


class TestPostgresExecution:
    async def test_filtered(self, pg_engine) -> None:
        resp = await pg_engine.execute(SlayerQuery.model_validate(FILTERED))
        got = {(r["orders.orderNumber"], r["orders.accounts.extRef"]) for r in resp.data}
        assert got == {("n1", "a"), ("n2", "a")}, resp.sql

    async def test_grouped(self, pg_engine) -> None:
        resp = await pg_engine.execute(SlayerQuery.model_validate(GROUPED))
        got = {r["orders.accounts.extRef"]: int(r["orders._count"]) for r in resp.data}
        assert got == {"a": 2, "b": 1}, resp.sql


def _bundle(models: list[SlayerModel]) -> ResolvedSourceBundle:
    host, *rest = models
    return ResolvedSourceBundle(dialect="postgres", source_model=host, referenced_models=rest)


class TestUnresolvableColumnFailsClosed:
    @pytest.mark.parametrize("query", [GROUPED, {"source_model": "orders", "dimensions": ["orderNumber"],
                                                 "filters": ["accounts.extRef = 'a'"]}],
                             ids=["projection", "filter"])
    def test_generator(self, query: dict[str, Any]) -> None:
        q = SlayerQuery.model_validate(query)
        planned = plan_query(query=q, bundle=_bundle(_models()))
        orders, accounts, regions = _models()
        stripped = accounts.model_copy(update={"columns": [c for c in accounts.columns if c.name != "extRef"]})
        with pytest.raises(UnknownReferenceError):
            SQLGenerator(dialect="postgres").generate_from_planned(planned, bundle=_bundle([orders, stripped, regions]))

    @pytest.mark.parametrize("key", [
        ColumnKey(path=(), leaf="missingCol"),
        ColumnKey(path=("accounts",), leaf="missingCol"),
        ColumnKey(path=("nowhere",), leaf="extRef"),
    ], ids=["host-leaf", "joined-leaf", "unknown-path"])
    def test_scope_frame(self, key: ColumnKey) -> None:
        models = _models()
        alloc = AliasAllocator()
        frame = ScopeFrame(
            scope_id=alloc.next_scope_id("orders"), root_model=models[0], root_relation="orders",
            bundle=_bundle(models), dialect=get_dialect("postgres"), allocator=alloc,
        )
        with pytest.raises(UnknownReferenceError):
            frame.resolve(key)
