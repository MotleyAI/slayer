"""DEV-1753: admit the last four parser-only scalars — greatest, least, trunc, mod.

Closes the parser/binder allowlist divergence pinned by
``tests/test_dev1744_value_expr.py::TestParserAndBinderScalarSetsAgree``. After
this change ``SCALAR_PASSTHROUGH - SCALAR_FUNCTIONS == set()``.

Ratified decisions (see DECISIONS.md):
* ``greatest`` / ``least`` — the NULL divergence is RATIFIED, not normalised:
  SQLite emits scalar ``MAX(a,b)`` / ``MIN(a,b)`` (propagates NULL), every
  GREATEST-native backend ignores NULLs (Snowflake via ``..._IGNORE_NULLS``).
  Witnessed live on SQLite (propagate, here) and DuckDB (ignore, in
  ``tests/integration/test_integration_duckdb.py``).
* ``trunc`` — 1-arg only; the 2-arg form silently drops the digits on SQLite.
* ``mod`` — routed through the ``%`` composer so complex operands are
  parenthesised correctly; a raw ``exp.Mod`` mis-groups ``mod(a+b, c)``.
* 2-arg strip form of ``ltrim`` / ``rtrim`` stays OUT (deferred to DEV-1793).
"""

from __future__ import annotations

import os
import sqlite3

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.formula import SCALAR_PASSTHROUGH
from slayer.core.keys import (
    SCALAR_FUNCTION_ARITY,
    SCALAR_FUNCTIONS,
    check_scalar_arity,
)
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.sql.render.value_expr import render_arithmetic, render_scalar_call
from slayer.storage.yaml_storage import YAMLStorage

TIER1 = [
    "sqlite", "postgres", "duckdb", "mysql",
    "clickhouse", "tsql", "snowflake", "bigquery",
]


def _emit(name: str, args, dialect_name: str) -> str:
    d = get_dialect(dialect_name)
    node = render_scalar_call(
        name=name, args=[a.copy() for a in args], dialect=d,
    )
    return node.sql(dialect=d.sqlglot_name)


# ===========================================================================
# A. The allowlists agree, and the arity table pins each new name.
# ===========================================================================


class TestScalarSetsClosed:
    def test_the_four_are_admitted(self) -> None:
        assert {"greatest", "least", "trunc", "mod"} <= SCALAR_FUNCTIONS

    def test_no_parser_only_names_remain(self) -> None:
        """The DoD headline: the parser and binder now admit exactly the same
        pass-through scalars."""
        assert SCALAR_PASSTHROUGH - SCALAR_FUNCTIONS == set()

    def test_like_stays_the_only_binder_only_name(self) -> None:
        assert SCALAR_FUNCTIONS - SCALAR_PASSTHROUGH == {"like"}

    def test_arity_entries_exist_for_the_four(self) -> None:
        assert SCALAR_FUNCTION_ARITY["greatest"] == (2, None)
        assert SCALAR_FUNCTION_ARITY["least"] == (2, None)
        assert SCALAR_FUNCTION_ARITY["trunc"] == (1, 1)
        assert SCALAR_FUNCTION_ARITY["mod"] == (2, 2)


class TestArityBoundaries:
    def test_mod_requires_exactly_two(self) -> None:
        assert check_scalar_arity(name="mod", argc=1) is not None
        assert check_scalar_arity(name="mod", argc=2) is None
        assert check_scalar_arity(name="mod", argc=3) is not None

    def test_trunc_is_one_arg_only(self) -> None:
        assert check_scalar_arity(name="trunc", argc=1) is None
        msg = check_scalar_arity(name="trunc", argc=2)
        assert msg is not None and "trunc" in msg

    def test_greatest_least_need_at_least_two(self) -> None:
        for name in ("greatest", "least"):
            assert check_scalar_arity(name=name, argc=1) is not None, name
            assert check_scalar_arity(name=name, argc=2) is None, name
            assert check_scalar_arity(name=name, argc=5) is None, name

    def test_ltrim_rtrim_stay_one_arg(self) -> None:
        """DEV-1793 deferral guard: a 2-arg strip form must still be rejected so
        a later widen is a deliberate edit, not a silent drift."""
        assert SCALAR_FUNCTION_ARITY["ltrim"] == (1, 1)
        assert SCALAR_FUNCTION_ARITY["rtrim"] == (1, 1)
        assert check_scalar_arity(name="ltrim", argc=2) is not None
        assert check_scalar_arity(name="rtrim", argc=2) is not None


# ===========================================================================
# B. Per-dialect emission (render layer). Pins the ratified shapes exactly.
# ===========================================================================


_GREATEST_EXPECTED = {
    "sqlite": "MAX(a, b)",
    "postgres": "GREATEST(a, b)",
    "duckdb": "GREATEST(a, b)",
    "mysql": "GREATEST(a, b)",
    "clickhouse": "GREATEST(a, b)",
    "tsql": "GREATEST(a, b)",
    "snowflake": "GREATEST_IGNORE_NULLS(a, b)",
    "bigquery": "GREATEST(a, b)",
}
_LEAST_EXPECTED = {
    "sqlite": "MIN(a, b)",
    "postgres": "LEAST(a, b)",
    "duckdb": "LEAST(a, b)",
    "mysql": "LEAST(a, b)",
    "clickhouse": "LEAST(a, b)",
    "tsql": "LEAST(a, b)",
    "snowflake": "LEAST_IGNORE_NULLS(a, b)",
    "bigquery": "LEAST(a, b)",
}
_TRUNC_EXPECTED = {
    "sqlite": "TRUNC(x)",
    "postgres": "TRUNC(x)",
    "duckdb": "TRUNC(x)",
    "mysql": "TRUNCATE(x)",
    "clickhouse": "trunc(x)",
    "tsql": "ROUND(x, 0, 1)",
    "snowflake": "TRUNC(x)",
    "bigquery": "TRUNC(x)",
}


class TestPerDialectEmission:
    @pytest.mark.parametrize("dialect", TIER1)
    def test_greatest(self, dialect) -> None:
        args = [exp.column("a"), exp.column("b")]
        assert _emit("greatest", args, dialect) == _GREATEST_EXPECTED[dialect]

    @pytest.mark.parametrize("dialect", TIER1)
    def test_least(self, dialect) -> None:
        args = [exp.column("a"), exp.column("b")]
        assert _emit("least", args, dialect) == _LEAST_EXPECTED[dialect]

    @pytest.mark.parametrize("dialect", TIER1)
    def test_trunc(self, dialect) -> None:
        assert _emit("trunc", [exp.column("x")], dialect) == _TRUNC_EXPECTED[dialect]

    def test_trunc_is_lowercase_on_clickhouse(self) -> None:
        """ClickHouse function names are case-sensitive in general; sqlglot
        deliberately lowercases ``trunc``. Pin it (a live check runs in the
        ClickHouse integration suite)."""
        out = _emit("trunc", [exp.column("x")], "clickhouse")
        assert out == "trunc(x)"
        assert "TRUNC(" not in out

    @pytest.mark.parametrize("dialect", TIER1)
    def test_mod_emits_percent_or_bigquery_func(self, dialect) -> None:
        out = _emit("mod", [exp.column("a"), exp.column("b")], dialect)
        expected = "MOD(a, b)" if dialect == "bigquery" else "a % b"
        assert out == expected


# ===========================================================================
# C. mod precedence / round-trip (the DEV-1744 `%` re-verification).
# ===========================================================================


def _mod(*operands: exp.Expression) -> exp.Expression:
    """Build a ``mod`` node the way ``render_value_key`` does: args are already
    rendered, then handed to ``render_scalar_call``."""
    return render_scalar_call(
        name="mod", args=list(operands), dialect=get_dialect("postgres"),
    )


class TestModPrecedence:
    """A raw ``exp.Mod(this=a+b, expression=c)`` emits ``a + b % c`` — which
    re-parses as ``a + (b % c)`` because sqlglot's parser puts ``%`` on the
    ``+``/``-`` tier. Routing ``mod`` through the ``%`` composer parenthesises
    complex operands, so the built tree survives the round-trip."""

    @pytest.mark.parametrize(
        "build,expected_pct,expected_bq",
        [
            (lambda: _mod(exp.column("a"), exp.column("b")),
             "a % b", "MOD(a, b)"),
            (lambda: _mod(sqlglot.parse_one("a + b"), exp.column("c")),
             "(a + b) % c", "MOD(a + b, c)"),
            (lambda: _mod(exp.column("a"), sqlglot.parse_one("b + c")),
             "a % (b + c)", "MOD(a, b + c)"),
            (lambda: _mod(_mod(exp.column("a"), exp.column("b")), exp.column("c")),
             "(a % b) % c", "MOD(MOD(a, b), c)"),
            (lambda: _mod(exp.column("a"), _mod(exp.column("b"), exp.column("c"))),
             "a % (b % c)", "MOD(a, MOD(b, c))"),
        ],
    )
    def test_shapes(self, build, expected_pct, expected_bq) -> None:
        node = build()
        for d in ("postgres", "sqlite", "mysql"):
            assert node.sql(dialect=d) == expected_pct, d
        assert node.sql(dialect="bigquery") == expected_bq

    def test_mod_nested_in_addition_is_grouped(self) -> None:
        node = render_arithmetic(
            op="+",
            operands=[exp.column("x"), _mod(exp.column("b"), exp.column("c"))],
        )
        assert node.sql(dialect="postgres") == "x + (b % c)"

    def test_mod_of_sum_survives_reparse(self) -> None:
        """The concrete regression: ``mod(a+b, c)`` must not collapse to
        ``a + (b % c)`` after sqlglot re-parses the emitted SQL."""
        for d in ("postgres", "sqlite", "mysql"):
            sql = _mod(sqlglot.parse_one("a + b"), exp.column("c")).sql(dialect=d)
            tree = sqlglot.parse_one(sql, read=d)
            assert isinstance(tree, exp.Mod), (d, sql, type(tree).__name__)
            assert isinstance(tree.this, exp.Paren), (d, sql)


# ===========================================================================
# D. End-to-end through the binder: bind, render, execute (SQLite).
# ===========================================================================


async def _engine(base_dir: str, *, dialect: str = "sqlite") -> SlayerQueryEngine:
    db_path = os.path.join(base_dir, "s.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, "
        "amount REAL, disc REAL, qty REAL)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)",
        [
            (1, "new", 10.0, None, 2.0),
            (2, "new", 20.0, 5.0, 4.0),
            (3, "old", 30.0, None, 1.0),
            (4, "old", None, None, 3.0),
        ],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(base_dir, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type=dialect, database=db_path)
    )
    await storage.save_model(
        SlayerModel(
            name="orders", sql_table="orders", data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="disc", type=DataType.DOUBLE),
                Column(name="qty", type=DataType.DOUBLE),
            ],
        )
    )
    return SlayerQueryEngine(storage=storage)


@pytest.fixture
async def engine(tmp_path) -> SlayerQueryEngine:
    return await _engine(str(tmp_path))


class TestEndToEndBinding:
    async def test_each_scalar_binds_and_appears_in_sql(self, engine) -> None:
        cases = [
            ("greatest(amount, disc) > 15", "MAX("),
            ("least(amount, disc) < 15", "MIN("),
            ("trunc(amount) >= 20", "TRUNC("),
            ("mod(id, 2) == 0", "%"),
        ]
        for predicate, needle in cases:
            resp = await engine.execute(
                SlayerQuery(
                    source_model="orders",
                    dimensions=[ColumnRef(name="status")],
                    measures=[ModelMeasure(formula="*:count", name="n")],
                    filters=[predicate],
                ),
                dry_run=True,
            )
            assert needle in (resp.sql or ""), (predicate, resp.sql)

    async def test_mod_with_complex_operand_executes(self, engine) -> None:
        """``mod(amount + qty, 3)`` must group as ``(amount + qty) % 3``. id=2:
        (20+4)%3 = 0; id=1: (10+2)%3 = 0; id=3: (30+1)%3 = 1; id=4 amount NULL."""
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="*:count", name="n")],
                filters=["mod(amount + qty, 3) == 0"],
            )
        )
        assert resp.data[0]["orders.n"] == 2

    @pytest.mark.parametrize(
        "predicate,func",
        [
            ("trunc(amount, 2) > 1", "trunc"),
            ("mod(amount) > 1", "mod"),
            ("greatest(amount) > 1", "greatest"),
        ],
    )
    async def test_arity_errors_surface_at_bind_time(
        self, engine, predicate, func,
    ) -> None:
        with pytest.raises(ValueError, match=func):
            await engine.execute(
                SlayerQuery(
                    source_model="orders",
                    measures=[ModelMeasure(formula="*:count", name="n")],
                    filters=[predicate],
                ),
                dry_run=True,
            )


class TestGreatestLeastNullSemanticsSqlite:
    """The ratified divergence, witnessed live on SQLite (the default store):
    scalar ``MAX(a,b)`` / ``MIN(a,b)`` PROPAGATE NULL. Grouped by ``id`` so each
    aggregate is over one row, isolating the scalar's NULL behavior. The
    NULL-ignoring side is witnessed on DuckDB in the integration suite."""

    async def _values(self, engine, formula):
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="id")],
                measures=[ModelMeasure(formula=formula, name="g")],
            )
        )
        return {int(r["orders.id"]): r["orders.g"] for r in resp.data}

    async def test_greatest_propagates_null(self, engine) -> None:
        got = await self._values(engine, "greatest(amount:max, disc:max)")
        assert got == {1: None, 2: 20.0, 3: None, 4: None}

    async def test_least_propagates_null(self, engine) -> None:
        got = await self._values(engine, "least(amount:max, disc:max)")
        assert got == {1: None, 2: 5.0, 3: None, 4: None}
