"""Plain field references render as one quoted identifier in every clause (spec: sql/statement-assembly ›
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
from slayer.core.keys import FieldKey
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.generator import SQLGenerator
from slayer.sql.naming import AliasAllocator
from slayer.sql.scope import ScopeFrame
from tests._engine_helpers import _engine_generate
from tests._pg_helpers import pg_database, pg_query_engine

pg_proc = factories.postgresql_proc(port=None, postgres_options="-c unix_socket_directories=")

DIALECTS = ["postgres", "snowflake"]
DS = "pg"


def _field(name: str, datatype: DataType = DataType.TEXT, *, pk: bool = False) -> DatasetField:
    return DatasetField(name=name, datatype=datatype, primary_key=pk)


def _datasets(
    *, trusted: bool, lower: bool = False, accounts_name: str = "accounts", regions_name: str = "regions",
    accounts_table: str = "accounts",
) -> list[SlayerDataset]:
    """``orders`` → ``accounts`` → ``regions``, camelCase fields unless ``lower``."""

    def n(name: str) -> str:
        return name.lower() if lower else name

    orders = SlayerDataset(name="orders", sql_table="orders", data_source=DS, fields=[
        _field("id", DataType.INT, pk=True), _field(n("orderNumber")), _field(n("accountId"), DataType.INT),
        _field("amount", DataType.DOUBLE), _field(n("createdAt"), DataType.TIMESTAMP),
    ], relationships=[DatasetRelationship(to=accounts_name, from_columns=[n("accountId")], to_columns=["id"],
                                          trusted=trusted)])
    accounts = SlayerDataset(name=accounts_name, sql_table=accounts_table, data_source=DS, fields=[
        _field("id", DataType.INT, pk=True), _field(n("extRef")), _field(n("signedUpAt"), DataType.TIMESTAMP),
        _field(n("regionId"), DataType.INT), _field(n("displayName")),
    ], relationships=[DatasetRelationship(to=regions_name, from_columns=[n("regionId")], to_columns=["id"],
                                          trusted=trusted)])
    regions = SlayerDataset(name=regions_name, sql_table="regions", data_source=DS, fields=[
        _field("id", DataType.INT, pk=True), _field(n("regionName")), _field(n("displayName")),
    ])
    return [orders, accounts, regions]


FILTERED = {"population": "orders", "dimensions": ["orderNumber", "accounts.extRef"],
            "filters": ["accounts.extRef = 'a'"]}
GROUPED = {"population": "orders", "dimensions": ["accounts.extRef"], "metrics": [{"formula": "count(*)"}]}
SIGNED_UP_MONTH = {"field": "accounts.signedUpAt", "granularity": "month"}


@pytest.fixture(params=[False, True], ids=["untrusted", "trusted"])
def trusted(request: pytest.FixtureRequest) -> bool:
    return request.param


async def _sql(
    query: dict[str, Any], *, trusted: bool, dialect: str = "postgres",
    datasets: Optional[list[SlayerDataset]] = None,
) -> str:
    host, *extra = datasets or _datasets(trusted=trusted)
    return await _engine_generate(query=SlayerQuery.model_validate(query), dataset=host, extra_datasets=extra,
                                  dialect=dialect)


def _parse(sql: str, dialect: str = "postgres") -> Expr:
    return sqlglot.parse_one(sql, dialect=dialect)


def _reads(node: Expr, leaf: str) -> list[exp.Column]:
    return [c for c in node.find_all(exp.Column) if c.name == leaf]


def _assert_quoted_reads(node: Expr, *, leaf: str, sql: str) -> list[exp.Column]:
    """Every read of ``leaf`` under ``node`` is quoted; returns the reads."""
    reads = _reads(node, leaf)
    assert reads, f"no read of {leaf!r}:\n{sql}"
    unquoted = [c.sql() for c in reads if not c.this.quoted]
    assert not unquoted, f"unquoted reads of {leaf!r}: {unquoted}\n{sql}"
    return reads


def _sources(select: exp.Select) -> list[Expr]:
    from_ = select.args.get("from_") or select.args.get("from")
    return [*([from_.this] if from_ is not None else []), *(j.this for j in select.args.get("joins") or [])]


def _table_alias(tree: Expr, table: str, sql: str) -> str:
    """The alias of the one ``exp.Table`` named ``table``, anywhere in the tree."""
    aliases = {t.alias_or_name for t in tree.find_all(exp.Table) if t.name == table}
    assert len(aliases) == 1, f"expected one alias for table {table!r}, got {aliases}:\n{sql}"
    return aliases.pop()


def _scopes_reading(tree: Expr, table: str, sql: str) -> list[exp.Select]:
    """The selects whose own FROM / JOIN reads the base table ``table``."""
    found = [s for s in tree.find_all(exp.Select)
             if any(isinstance(src, exp.Table) and src.name == table for src in _sources(s))]
    assert found, f"no select reads table {table!r}:\n{sql}"
    return found


def _own_reads(select: exp.Select, leaf: str) -> list[exp.Column]:
    """Reads of ``leaf`` in ``select`` itself, not in nested subqueries."""
    walked = select.walk(prune=lambda n: n is not select and isinstance(n, (exp.Select, exp.Subquery)))
    return [c for c in walked if isinstance(c, exp.Column) and c.name == leaf]


def _assert_table_reads_qualified(tree: Expr, *, table: str, leaf: str, sql: str) -> str:
    """Each select reading ``table`` reads ``leaf`` under the table's alias, else only under a nested
    projection's alias; returns the table's alias."""
    alias = _table_alias(tree, table, sql)
    for scope in _scopes_reading(tree, table, sql):
        quals = {c.table for c in _own_reads(scope, leaf)}
        nested = {s.alias for s in _sources(scope) if isinstance(s, exp.Subquery)}
        assert alias in quals, f"{leaf!r} not read under {alias!r} where {table!r} is read ({quals}):\n{sql}"
        assert quals <= {alias} | nested, f"{leaf!r} read under a wrong qualifier ({quals}):\n{sql}"
    return alias


def _named(tree: Expr, alias: str, sql: str) -> Expr:
    """The subquery or CTE aliased ``alias``."""
    found = [n for n in tree.find_all(exp.Subquery, exp.CTE) if n.alias == alias]
    assert found, f"no subquery or CTE aliased {alias!r}:\n{sql}"
    return found[0]


def _distinct_projection(tree: Expr, table: str, sql: str) -> exp.Select:
    found = [s for s in _scopes_reading(tree, table, sql) if s.args.get("distinct")]
    assert found, f"no SELECT DISTINCT over {table!r}:\n{sql}"
    return found[0]


def _assert_projected_under_own_name(projection: exp.Select, *, leaf: str, sql: str) -> None:
    carried = [p for p in projection.expressions
               if isinstance(p, exp.Alias) and isinstance(p.this, exp.Column) and p.this.name == leaf]
    assert carried, f"distinct projection does not carry {leaf!r}:\n{sql}"
    for p in carried:
        assert p.this.this.quoted, f"inner read unquoted: {p.sql()}\n{sql}"
        assert p.alias == leaf, f"not projected as {leaf!r}: {p.sql()}\n{sql}"
        assert p.args["alias"].quoted, f"projected alias unquoted: {p.sql()}\n{sql}"


class TestJoinedDimension:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_filtered_on_itself(self, dialect: str, trusted: bool) -> None:
        sql = await _sql(FILTERED, trusted=trusted, dialect=dialect)
        tree = _parse(sql, dialect)
        reads = _assert_quoted_reads(tree, leaf="extRef", sql=sql)
        assert {c.table for c in reads} == {"accounts"}, sql
        assert any(_reads(p, "extRef") for p in tree.expressions), sql
        assert _reads(tree.args["group"], "extRef"), sql
        assert any(_reads(w, "extRef") for w in tree.find_all(exp.Where)), sql
        if not trusted:
            _assert_projected_under_own_name(_distinct_projection(tree, "accounts", sql), leaf="extRef", sql=sql)

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_grouped_under_count(self, dialect: str, trusted: bool) -> None:
        sql = await _sql(GROUPED, trusted=trusted, dialect=dialect)
        tree = _parse(sql, dialect)
        _assert_quoted_reads(tree, leaf="extRef", sql=sql)
        assert _reads(tree.args["group"], "extRef"), sql
        assert any(_reads(p, "extRef") for p in tree.expressions), sql
        if not trusted:
            _assert_projected_under_own_name(_distinct_projection(tree, "accounts", sql), leaf="extRef", sql=sql)


class TestAggregateArgument:
    @pytest.mark.parametrize("dialect", ["postgres", "bigquery"], ids=["postgres", "bigquery-zoned"])
    async def test_aggregates_and_post_aggregation_filter(self, dialect: str, trusted: bool) -> None:
        orders, accounts, regions = _datasets(trusted=trusted)
        if dialect == "bigquery":  # a zoned TIMESTAMP reads through the dialect's zoned read
            accounts = accounts.model_copy(update={"fields": [
                f.model_copy(update={"db_type": "TIMESTAMP"}) if f.name == "signedUpAt" else f
                for f in accounts.fields]})
        sql = await _sql({"population": "orders", "dimensions": ["orderNumber"], "metrics": [
            {"formula": "max(accounts.signedUpAt)", "name": "latest"},
            {"formula": "count(distinct accounts.extRef)", "name": "refs"},
        ], "filters": ["refs > 0"]}, trusted=trusted, dialect=dialect, datasets=[orders, accounts, regions])
        tree = _parse(sql, dialect)
        for leaf in ("signedUpAt", "extRef"):
            reads = _assert_quoted_reads(tree, leaf=leaf, sql=sql)
            assert any(r.find_ancestor(exp.AggFunc) for r in reads), f"{leaf!r} not read inside an aggregate:\n{sql}"
        if dialect == "bigquery":
            casts = [r.find_ancestor(exp.Cast) for r in _reads(tree, "signedUpAt")]
            assert any(c is not None and c.to.sql(dialect="bigquery") == "DATETIME" for c in casts), sql
        post = tree.args.get("having") or tree.args.get("where")
        assert post is not None, f"filter on the count dropped:\n{sql}"
        assert post.find(exp.GT) is not None, f"filter on the count dropped:\n{sql}"


class TestTwoHop:
    async def test_qualifier_is_the_table_alias(self, trusted: bool) -> None:
        sql = await _sql({"population": "orders", "dimensions": ["accounts.regions.regionName"],
                          "metrics": [{"formula": "count(*)"}]}, trusted=trusted)
        tree = _parse(sql)
        _assert_quoted_reads(tree, leaf="regionName", sql=sql)
        _assert_table_reads_qualified(tree, table="regions", leaf="regionName", sql=sql)

    async def test_same_leaf_on_two_hops(self) -> None:
        sql = await _sql({"population": "orders", "dimensions": ["accounts.displayName", "accounts.regions.displayName"],
                          "metrics": [{"formula": "count(*)"}]}, trusted=False)
        tree = _parse(sql)
        _assert_quoted_reads(tree, leaf="displayName", sql=sql)
        regions_alias = _assert_table_reads_qualified(tree, table="regions", leaf="displayName", sql=sql)
        accounts_alias = _assert_table_reads_qualified(tree, table="accounts", leaf="displayName", sql=sql)
        assert regions_alias != accounts_alias, sql
        for scope in _scopes_reading(tree, "regions", sql):
            assert {c.table for c in _own_reads(scope, "displayName")} == {regions_alias}, sql

    async def test_fitted_alias(self, trusted: bool) -> None:
        accounts_name, regions_name = "accounts_" + "a" * 40, "regions_" + "r" * 40
        datasets = _datasets(trusted=trusted, accounts_name=accounts_name, regions_name=regions_name)
        sql = await _sql({"population": "orders", "dimensions": [f"{accounts_name}.{regions_name}.regionName"],
                          "metrics": [{"formula": "count(*)"}]}, trusted=trusted, datasets=datasets)
        tree = _parse(sql)
        _assert_quoted_reads(tree, leaf="regionName", sql=sql)
        alias = _assert_table_reads_qualified(tree, table="regions", leaf="regionName", sql=sql)
        assert alias != f"{accounts_name}__{regions_name}", f"alias not fitted:\n{sql}"

    async def test_colliding_alias(self, trusted: bool) -> None:
        orders, accounts, regions = _datasets(trusted=trusted)
        orders = orders.model_copy(update={
            "fields": [*orders.fields, _field("dimId", DataType.INT)],
            "relationships": [*orders.relationships, DatasetRelationship(
                to="accounts__regions", from_columns=["dimId"], to_columns=["id"], trusted=trusted)],
        })
        direct = SlayerDataset(name="accounts__regions", sql_table="region_dim", data_source=DS, fields=[
            _field("id", DataType.INT, pk=True), _field("regionCode"),
        ])
        sql = await _sql({"population": "orders",
                          "dimensions": ["accounts.regions.regionName", "accounts__regions.regionCode"],
                          "metrics": [{"formula": "count(*)"}]},
                         trusted=trusted, datasets=[orders, accounts, regions, direct])
        tree = _parse(sql)
        _assert_quoted_reads(tree, leaf="regionName", sql=sql)
        _assert_quoted_reads(tree, leaf="regionCode", sql=sql)
        chain_alias = _assert_table_reads_qualified(tree, table="regions", leaf="regionName", sql=sql)
        direct_alias = _assert_table_reads_qualified(tree, table="region_dim", leaf="regionCode", sql=sql)
        assert chain_alias != direct_alias, sql


class TestJoinedTimeDimension:
    async def test_truncation(self, trusted: bool) -> None:
        sql = await _sql({"population": "orders", "time_dimensions": [SIGNED_UP_MONTH],
                          "metrics": [{"formula": "count(*)"}]}, trusted=trusted)
        tree = _parse(sql)
        reads = _assert_quoted_reads(tree, leaf="signedUpAt", sql=sql)
        assert any(r.find_ancestor(exp.TimestampTrunc, exp.DateTrunc) for r in reads), sql
        assert _reads(tree.args["group"], "signedUpAt"), sql

    @pytest.mark.parametrize("producer", ["_src", "_base"])
    async def test_window_producers(self, producer: str, trusted: bool) -> None:
        """``_src``: the duration window's source; ``_base``: the collapsed window grain."""
        sql = await _sql({"population": "orders", "dimensions": ["accounts.extRef"],
                          "time_dimensions": [SIGNED_UP_MONTH],
                          "metrics": [{"formula": "sum(amount, window='90d')"}]}, trusted=trusted)
        tree = _parse(sql)
        _assert_quoted_reads(_named(tree, producer, sql), leaf="signedUpAt", sql=sql)
        _assert_quoted_reads(tree, leaf="signedUpAt", sql=sql)


class TestRankedAggregate:
    async def test_partitioned_by_joined_field(self, trusted: bool) -> None:
        sql = await _sql({"population": "orders", "dimensions": ["accounts.extRef"],
                          "time_dimensions": [{"field": "createdAt", "granularity": "month"}],
                          "metrics": [{"formula": "last(amount)"}]}, trusted=trusted)
        ranked = _named(_parse(sql), "_ranked_src", sql)
        _assert_quoted_reads(ranked, leaf="extRef", sql=sql)
        window = ranked.find(exp.Window)
        assert window is not None, sql
        assert _reads(window, "extRef"), sql

    async def test_ranked_by_joined_time(self, trusted: bool) -> None:
        sql = await _sql({"population": "orders", "dimensions": ["orderNumber"],
                          "metrics": [{"formula": "last(amount, accounts.signedUpAt)"}]}, trusted=trusted)
        ranked = _named(_parse(sql), "_ranked_src", sql)
        _assert_quoted_reads(ranked, leaf="signedUpAt", sql=sql)
        window = ranked.find(exp.Window)
        assert window is not None, sql
        assert _reads(window, "signedUpAt"), sql


class TestOrderingByJoinedField:
    async def test_projected(self, trusted: bool) -> None:
        sql = await _sql({**GROUPED, "order": [{"field": "accounts.extRef"}]}, trusted=trusted)
        _assert_quoted_reads(_parse(sql), leaf="extRef", sql=sql)

    async def test_grouped_not_projected(self, trusted: bool) -> None:
        sql = await _sql({"population": "orders", "dimensions": ["orderNumber"],
                          "metrics": [{"formula": "count(*)"}], "order": [{"field": "accounts.extRef"}]},
                         trusted=trusted)
        _assert_quoted_reads(_parse(sql), leaf="extRef", sql=sql)

    async def test_ungrouped_not_materialised(self, trusted: bool) -> None:
        sql = await _sql({"population": "orders", "dimensions": ["orderNumber"],
                          "distinct_dimension_values": False, "order": [{"field": "accounts.extRef"}]},
                         trusted=trusted)
        tree = _parse(sql)
        _assert_quoted_reads(tree, leaf="extRef", sql=sql)
        [ordered] = tree.args["order"].expressions
        assert isinstance(ordered.this, exp.Column), sql
        assert ordered.this.name == "extRef", sql


class TestSemiJoin:
    async def test_exists_reads_quoted(self, trusted: bool) -> None:
        sql = await _sql({"population": "accounts", "dimensions": ["extRef"],
                          "filters": ["orders.orderNumber = 'n1'"]}, trusted=trusted)
        tree = _parse(sql)
        exists = tree.find(exp.Exists)
        assert exists is not None, f"not lowered to EXISTS:\n{sql}"
        _assert_quoted_reads(exists, leaf="orderNumber", sql=sql)


#: ``main``'s emission before the fix, pinned: lowercase names render unchanged.
LOWER_FILTERED_UNTRUSTED = """SELECT
  orders.ordernumber AS "orders.ordernumber",
  accounts.extref AS "accounts.extref"
FROM orders AS orders
JOIN (
  SELECT DISTINCT
    accounts.id AS id,
    accounts.extref AS extref
  FROM accounts AS accounts
  WHERE
    accounts.extref = 'a'
) AS accounts
  ON orders.accountid = accounts.id
GROUP BY
  orders.ordernumber,
  accounts.extref
ORDER BY
  "orders.ordernumber" ASC,
  "accounts.extref" ASC"""
LOWER_FILTERED_TRUSTED = """SELECT
  orders.ordernumber AS "orders.ordernumber",
  accounts.extref AS "accounts.extref"
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
  "accounts.extref" ASC"""
LOWER_GROUPED_UNTRUSTED = """SELECT
  accounts.extref AS "accounts.extref",
  COUNT(*) AS "orders._count"
FROM orders AS orders
LEFT JOIN (
  SELECT DISTINCT
    accounts.id AS id,
    accounts.extref AS extref
  FROM accounts AS accounts
) AS accounts
  ON orders.accountid = accounts.id
GROUP BY
  accounts.extref
ORDER BY
  "accounts.extref" ASC"""
LOWER_GROUPED_TRUSTED = """SELECT
  accounts.extref AS "accounts.extref",
  COUNT(*) AS "orders._count"
FROM orders AS orders
LEFT JOIN accounts AS accounts
  ON orders.accountid = accounts.id
GROUP BY
  accounts.extref
ORDER BY
  "accounts.extref" ASC"""
LOWER_EXPECTED = {
    ("filtered", False): LOWER_FILTERED_UNTRUSTED, ("filtered", True): LOWER_FILTERED_TRUSTED,
    ("grouped", False): LOWER_GROUPED_UNTRUSTED, ("grouped", True): LOWER_GROUPED_TRUSTED,
}
LOWER_QUERIES = {
    "filtered": {**FILTERED, "dimensions": ["ordernumber", "accounts.extref"], "filters": ["accounts.extref = 'a'"]},
    "grouped": {**GROUPED, "dimensions": ["accounts.extref"]},
}


class TestLowercaseUnchanged:
    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("query", ["filtered", "grouped"])
    async def test_byte_identical(self, query: str, dialect: str, trusted: bool) -> None:
        sql = await _sql(LOWER_QUERIES[query], trusted=trusted, dialect=dialect,
                         datasets=_datasets(trusted=trusted, lower=True))
        assert sql.strip() == LOWER_EXPECTED[(query, trusted)]


PG_DDL = (
    'CREATE TABLE "accountRecords" (id INTEGER PRIMARY KEY, "extRef" TEXT, "signedUpAt" TIMESTAMP, '
    '"regionId" INTEGER, "displayName" TEXT)',
    'CREATE TABLE orders (id INTEGER PRIMARY KEY, "orderNumber" TEXT, "accountId" INTEGER, amount DOUBLE PRECISION, '
    '"createdAt" TIMESTAMP)',
    'CREATE TABLE regions (id INTEGER PRIMARY KEY, "regionName" TEXT, "displayName" TEXT)',
    "INSERT INTO \"accountRecords\" VALUES (1, 'a', '2024-01-05', NULL, NULL), (2, 'b', '2024-02-05', NULL, NULL)",
    "INSERT INTO orders VALUES (1, 'n1', 1, 10, '2024-03-01'), (2, 'n2', 1, 20, '2024-03-02'), "
    "(3, 'n3', 2, 30, '2024-03-03')",
)


class TestPostgresExecution:
    @pytest.mark.parametrize(("query", "expected"), [
        (FILTERED, {("n1", "a"), ("n2", "a")}),
        (GROUPED, {("a", 2), ("b", 1)}),
    ], ids=["filtered", "grouped"])
    async def test_rows(self, query: dict[str, Any], expected: set[tuple[Any, ...]], trusted: bool,
                        pg_proc: Any, tmp_path: Any) -> None:
        datasets = _datasets(trusted=trusted, accounts_table="accountRecords")
        with pg_database(pg_proc, statements=PG_DDL, name=DS) as ds:
            async with pg_query_engine(ds, base_dir=str(tmp_path), datasets=datasets) as engine:
                resp = await engine.execute(SlayerQuery.model_validate(query))
        if query is FILTERED:
            got = {(r["orders.orderNumber"], r["accounts.extRef"]) for r in resp.data}
        else:
            got = {(r["accounts.extRef"], int(r["orders._count"])) for r in resp.data}
        assert got == expected, resp.sql


def _bundle(datasets: list[SlayerDataset]) -> ResolvedSourceBundle:
    host, *rest = datasets
    return ResolvedSourceBundle(dialect="postgres", population=host, referenced_datasets=rest)


class TestUnresolvableFieldFailsClosed:
    @pytest.mark.parametrize("query", [
        {"population": "orders", "dimensions": ["accounts.extRef"], "metrics": ["count(orders.*)"]},
        {"population": "orders", "dimensions": ["orders.orderNumber"], "filters": ["accounts.extRef = 'a'"]},
        {"population": "orders", "dimensions": ["orders.orderNumber"], "distinct_dimension_values": False,
         "order": [{"field": "accounts.extRef"}]},
        {"population": "orders", "dimensions": ["accounts.regions.regionName"]},
    ], ids=["projection", "filter", "order-only", "unknown-path"])
    def test_generator(self, query: dict[str, Any], trusted: bool) -> None:
        """Planned against the full datasets, generated against ones missing the leaf or the path."""
        planned = plan_query(query=SlayerQuery.model_validate(query), bundle=_bundle(_datasets(trusted=trusted)))
        orders, accounts, regions = _datasets(trusted=trusted)
        stripped = accounts.model_copy(update={
            "fields": [f for f in accounts.fields if f.name != "extRef"], "relationships": [],
        })
        generator = SQLGenerator(dialect="postgres")
        bundle = _bundle([orders, stripped, regions])
        with pytest.raises(UnknownReferenceError):
            generator.generate_from_planned(planned, bundle=bundle)

    @pytest.mark.parametrize("key", [
        FieldKey(path=(), leaf="missingCol"),
        FieldKey(path=("accounts",), leaf="missingCol"),
        FieldKey(path=("nowhere",), leaf="extRef"),
        FieldKey(path=("node",), leaf="missingCol"),
    ], ids=["host-leaf", "joined-leaf", "unknown-path", "semi-join-node-leaf"])
    def test_scope_frame(self, key: FieldKey) -> None:
        frame = _frame(node_datasets={("node",): "accounts"})
        with pytest.raises(UnknownReferenceError):
            frame.resolve(key)

    def test_semi_join_node_path_resolves(self) -> None:
        """A semi-join tree-node path resolves through the group's node dataset, quoted."""
        frame = _frame(node_datasets={("node",): "accounts"})
        out = frame.resolve(FieldKey(path=("node",), leaf="extRef"))
        assert isinstance(out, exp.Column), out.sql()
        assert out.this.quoted, out.sql()


def _frame(*, node_datasets: dict[tuple[str, ...], str]) -> ScopeFrame:
    datasets = _datasets(trusted=True)
    alloc = AliasAllocator()
    return ScopeFrame(
        scope_id=alloc.next_scope_id("orders"), root_dataset=datasets[0], root_relation="orders",
        bundle=_bundle(datasets), dialect=get_dialect("postgres"), allocator=alloc, node_datasets=node_datasets,
    )
