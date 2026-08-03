"""DEV-1625 — {variable} substitution in Mode-A (raw-SQL) surfaces.

Scope of DEV-1625 is the DIRECT source model only: SlayerModel.sql,
SlayerModel.filters, Column.sql, Column.filter. Nested source_queries stages,
query-backed direct sources, join targets and cross-model targets are deferred
to DEV-1678 (a couple of boundary pins below assert that deferred behavior).

All end-to-end cases run against a real file-backed SQLite datasource and
assert on RESULT DATA, not just generated SQL strings.
"""
import sqlite3
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, ModelExtension, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Shared SQLite fixture
# ---------------------------------------------------------------------------

def _seed_orders_db_at(db_path) -> None:
    """6-row orders table. Region sums: US=160, EU=275, CA=300, total=735.
    Row 6 carries a single-quote in ``status`` ("O'Brien") to exercise the
    escaping path.
    """
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            region TEXT NOT NULL,
            status TEXT NOT NULL,
            amount REAL NOT NULL
        )
        """
    )
    rows = [
        (1, "US", "completed", 100.0),
        (2, "US", "pending", 50.0),
        (3, "EU", "completed", 200.0),
        (4, "EU", "cancelled", 75.0),
        (5, "CA", "completed", 300.0),
        (6, "US", "O'Brien", 10.0),
    ]
    cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


async def _engine_with(*models: SlayerModel) -> tuple:
    """Build a real SQLite datasource + YAML storage with the given models
    saved. Returns (engine, tmpdir_handle); caller keeps the tmpdir alive.
    """
    tmp = tempfile.TemporaryDirectory()
    db_path = f"{tmp.name}/orders.db"
    _seed_orders_db_at(db_path)
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_path)
    )
    for m in models:
        await storage.save_model(m)
    return SlayerQueryEngine(storage=storage), tmp


def _sum(resp, alias: str) -> float:
    assert resp.row_count == 1, f"expected 1 row, got {resp.row_count}: {resp.data}"
    return resp.data[0][alias]


# ---------------------------------------------------------------------------
# 1. SlayerModel.filters with {var}
# ---------------------------------------------------------------------------

class TestModelFiltersSubstitution:
    def _model(self, **kw) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["region = '{region}'"],
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            **kw,
        )

    async def test_model_filter_var_filters_rows(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"region": "EU"},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 275.0
        finally:
            tmp.cleanup()

    async def test_model_filter_var_us(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"region": "US"},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 160.0
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 2. Column.sql with {var}
# ---------------------------------------------------------------------------

class TestColumnSqlSubstitution:
    def _model(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="scaled", sql="amount * {mult}", type=DataType.DOUBLE),
            ],
        )

    async def test_column_sql_var_scales(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "scaled:sum"}],
                variables={"mult": 2},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.scaled_sum") == 1470.0  # 735 * 2
        finally:
            tmp.cleanup()

    async def test_column_sql_var_identity(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "scaled:sum"}],
                variables={"mult": 1},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.scaled_sum") == 735.0
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 3. Column.filter with {var} (CASE-WHEN at aggregation time)
# ---------------------------------------------------------------------------

class TestColumnFilterSubstitution:
    def _model(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(
                    name="region_amount",
                    sql="amount",
                    filter="region = '{region}'",
                    type=DataType.DOUBLE,
                ),
            ],
        )

    async def test_column_filter_var_windowed_sum(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "region_amount:sum"}],
                variables={"region": "US"},
            )
            resp = await engine.execute(q)
            # SUM(CASE WHEN region='US' THEN amount END) = 100+50+10 = 160
            assert _sum(resp, "orders.region_amount_sum") == 160.0
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 4. SlayerModel.sql with {var} (sql-mode: WHERE + projected scalar)
# ---------------------------------------------------------------------------

class TestModelSqlSubstitution:
    def _model(self) -> SlayerModel:
        return SlayerModel(
            name="floored",
            data_source="ds",
            sql=(
                "SELECT id, region, amount, {floor} AS floor_val "
                "FROM orders WHERE amount >= {floor}"
            ),
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="floor_val", sql="floor_val", type=DataType.DOUBLE),
            ],
        )

    async def test_model_sql_var_where(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="floored",
                measures=[{"formula": "amount:sum"}],
                variables={"floor": 100},
            )
            resp = await engine.execute(q)
            # amount >= 100 → rows 100, 200, 300 → 600
            assert _sum(resp, "floored.amount_sum") == 600.0
        finally:
            tmp.cleanup()

    async def test_model_sql_var_projected_scalar(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="floored",
                dimensions=[ColumnRef(name="floor_val")],
                variables={"floor": 100},
            )
            resp = await engine.execute(q)
            vals = {row["floored.floor_val"] for row in resp.data}
            assert vals == {100.0}
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 5. Precedence: runtime kwarg > outer/stage query.variables > model defaults
# ---------------------------------------------------------------------------

class TestPrecedence:
    def _model(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["region = '{region}'"],
            query_variables={"region": "US"},  # lowest-priority default
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )

    async def test_model_default_used_when_nothing_supplied(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 160.0  # US default
        finally:
            tmp.cleanup()

    async def test_query_variables_override_model_default(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"region": "EU"},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 275.0  # EU
        finally:
            tmp.cleanup()

    async def test_runtime_kwarg_overrides_all(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"region": "EU"},
            )
            resp = await engine.execute(q, variables={"region": "CA"})
            assert _sum(resp, "orders.amount_sum") == 300.0  # runtime CA wins
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 6. Missing variable → raise (execute AND dry_run mirror each other)
# ---------------------------------------------------------------------------

class TestMissingVariable:
    def _model(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["region = '{region}'"],
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )

    async def test_undefined_var_raises_on_execute(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            # Non-empty var set (so the guard runs) but lacking 'region'.
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"other": "x"},
            )
            with pytest.raises(ValueError, match="Undefined variable 'region'"):
                await engine.execute(q)
        finally:
            tmp.cleanup()

    async def test_undefined_var_raises_on_dry_run(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"other": "x"},
            )
            with pytest.raises(ValueError, match="Undefined variable 'region'"):
                await engine.execute(q, dry_run=True)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 7. Escaping / injection safety
# ---------------------------------------------------------------------------

# Quote-bearing string values that stress the escaping paths end-to-end. Each
# maps to a distinct amount so a correct match can be asserted on the sum.
#
# NB: backslash-in-value is deliberately NOT exercised end-to-end. It does not
# round-trip through SQLite in SLayer at all — a PRE-EXISTING quirk independent
# of DEV-1625: the Mode-B parser is ast-based (so `\b` in a filter is a
# backspace escape) and sqlglot's SQLite generator doubles backslashes that
# SQLite never unescapes. Even a literal, no-variable filter `status = 'a\b'`
# mismatches. The escaping-string CONTRACT for backslashes (value recovery
# through ast.parse) is covered precisely by the unit tests + AST round-trip
# above; a strict-xfail below pins the SQLite backslash limitation.
_TRICKY_ROWS = [
    ("O'Brien", 10.0),
    ('say "hi"', 20.0),
]


async def _engine_with_tricky_status(model: SlayerModel) -> tuple:
    tmp = tempfile.TemporaryDirectory()
    db_path = f"{tmp.name}/tricky.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT NOT NULL, amount REAL NOT NULL)"
    )
    cur.executemany(
        "INSERT INTO t (status, amount) VALUES (?, ?)",
        _TRICKY_ROWS + [("a\\b", 30.0)],  # a\b row exists only for the xfail pin
    )
    conn.commit()
    conn.close()
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_path)
    )
    await storage.save_model(model)
    return SlayerQueryEngine(storage=storage), tmp


class TestEscapingEndToEnd:
    """Both escaping modes, exercised through the real parse+compile+SQLite
    pipeline: the substituted value must round-trip so the row it names (and
    only that row) matches."""

    @pytest.mark.parametrize("value,expected", _TRICKY_ROWS)
    async def test_mode_a_model_filter_matches_only_its_row(
        self, value: str, expected: float
    ) -> None:
        # Mode-A: SlayerModel.filters → escape="sql".
        model = SlayerModel(
            name="t",
            sql_table="t",
            data_source="ds",
            filters=["status = '{v}'"],
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with_tricky_status(model)
        try:
            q = SlayerQuery(
                source_model="t",
                measures=[{"formula": "amount:sum"}],
                variables={"v": value},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "t.amount_sum") == expected
        finally:
            tmp.cleanup()

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "PRE-EXISTING SQLite/parser limitation (NOT DEV-1625): a backslash "
            "in a string value does not round-trip through SQLite. DEV-1625's "
            "python escaping correctly recovers the value via ast.parse (proven "
            "by the unit AST round-trip tests), but sqlglot's SQLite generator "
            "then doubles the backslash, and SQLite never unescapes it, so the "
            "literal mismatches the stored value. A literal, no-variable filter "
            "`status = 'a\\b'` mismatches identically. If a future sqlglot/parser "
            "change fixes this, the strict-xfail flips to a failure prompting "
            "removal of this pin."
        ),
    )
    async def test_backslash_value_sqlite_roundtrip_is_known_gap(self) -> None:
        model = SlayerModel(
            name="t",
            sql_table="t",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with_tricky_status(model)
        try:
            q = SlayerQuery(
                source_model="t",
                measures=[{"formula": "amount:sum"}],
                filters=["status = '{v}'"],
                variables={"v": "a\\b"},
            )
            resp = await engine.execute(q)
            # The correct behavior would be to match only the a\b row (30.0).
            assert _sum(resp, "t.amount_sum") == 30.0
        finally:
            tmp.cleanup()

    @pytest.mark.parametrize("value,expected", _TRICKY_ROWS)
    async def test_mode_b_query_filter_matches_only_its_row(
        self, value: str, expected: float
    ) -> None:
        # Mode-B: SlayerQuery.filters → escape="python". Quote values round-trip
        # (Python-AST parse, no SQL re-parse of the value).
        model = SlayerModel(
            name="t",
            sql_table="t",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with_tricky_status(model)
        try:
            q = SlayerQuery(
                source_model="t",
                measures=[{"formula": "amount:sum"}],
                filters=["status = '{v}'"],
                variables={"v": value},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "t.amount_sum") == expected
        finally:
            tmp.cleanup()


class TestEscaping:
    async def test_single_quote_value_escaped_and_matches(self) -> None:
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["status = '{status}'"],
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"status": "O'Brien"},
            )
            resp = await engine.execute(q)
            # Row 6 only. Without escaping the SQL would be a syntax error.
            assert _sum(resp, "orders.amount_sum") == 10.0
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 8. Brace-literal safety (non-empty-vars guard) + escaped {{ }}
# ---------------------------------------------------------------------------

class TestBraceLiterals:
    async def test_no_variable_model_brace_literal_untouched(self) -> None:
        """A model that uses NO variables must be left untouched, so a raw
        brace literal (Postgres-array-style) survives to the DB verbatim."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="j", sql="'{1,2,3}'", type=DataType.TEXT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            q = SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="j")])
            resp = await engine.execute(q)
            assert {row["orders.j"] for row in resp.data} == {"{1,2,3}"}
        finally:
            tmp.cleanup()

    async def test_escaped_braces_render_single_alongside_var(self) -> None:
        """{{tag}} escapes to a literal {tag} while {floor} substitutes."""
        model = SlayerModel(
            name="floored",
            data_source="ds",
            sql=(
                "SELECT id, amount, '{{tag}}' AS tag_val "
                "FROM orders WHERE amount >= {floor}"
            ),
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="tag_val", sql="tag_val", type=DataType.TEXT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            q = SlayerQuery(
                source_model="floored",
                dimensions=[ColumnRef(name="tag_val")],
                variables={"floor": 100},
            )
            resp = await engine.execute(q)
            assert {row["floored.tag_val"] for row in resp.data} == {"{tag}"}
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 9. Cross-model consistency: direct-source model.filters {var} substituted in
#    BOTH the main and the re-rooted CTE.
# ---------------------------------------------------------------------------

class TestCrossModelConsistency:
    def _models(self, *, bidirectional: bool) -> list[SlayerModel]:
        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
            ],
            # A reverse join makes the source model (orders) reachable FROM the
            # cross-model target (customers), so the source's {floor} filter is
            # applied in the re-rooted CTE rather than dropped.
            joins=(
                [{"target_model": "orders", "join_pairs": [["id", "customer_id"]]}]
                if bidirectional
                else []
            ),
        )
        orders = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["amount >= {floor}"],
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
        )
        return [customers, orders]

    async def _engine(self, *, bidirectional: bool) -> tuple:
        tmp = tempfile.TemporaryDirectory()
        db_path = f"{tmp.name}/orders.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        cur.executemany(
            "INSERT INTO customers VALUES (?, ?)",
            [(1, "Ann"), (2, "Bob"), (3, "Cid")],
        )
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)"
        )
        cur.executemany(
            "INSERT INTO orders VALUES (?, ?, ?)",
            [(1, 1, 100.0), (2, 2, 50.0), (3, 3, 200.0)],
        )
        conn.commit()
        conn.close()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=db_path)
        )
        for m in self._models(bidirectional=bidirectional):
            await storage.save_model(m)
        return SlayerQueryEngine(storage=storage), tmp

    async def test_source_model_filter_var_applies_to_reroot(self) -> None:
        # Boundary pin (unreachable target): customers has NO reverse join to
        # orders, so the re-rooted cross-model CTE cannot reach the source and
        # SLayer's pre-existing ``drop_unreachable_filters`` semantics DROP the
        # source's ``amount >= {floor}`` filter there (a static filter behaves
        # identically — verified). What DEV-1625 guarantees is that the {floor}
        # in ``orders.filters`` is SUBSTITUTED wherever it IS emitted: the main
        # ``amount:sum`` CTE gets ``>= 100`` (300.0), and no stray literal
        # ``{floor}`` survives anywhere in the SQL. The cross-model count is
        # unfiltered (all 3 customers) because the filter was dropped upstream.
        engine, tmp = await self._engine(bidirectional=False)
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[
                    {"formula": "amount:sum"},
                    {"formula": "customers.id:count"},
                ],
                variables={"floor": 100},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 300.0
            assert _sum(resp, "orders.customers.id_count") == 3
            assert "{floor}" not in resp.sql  # no un-substituted placeholder
        finally:
            tmp.cleanup()

    async def test_source_model_filter_var_applies_to_reroot_reachable(self) -> None:
        # Reachable target: customers HAS a reverse join to orders, so the
        # re-rooted cross-model CTE keeps the source's ``amount >= {floor}``
        # filter. DEV-1625 must substitute {floor} consistently in BOTH the
        # main CTE and the re-rooted CTE, else the re-rooted CTE errors on a
        # stray ``{floor}``. amount >= 100 → orders 1 & 3 → 2 distinct customers.
        engine, tmp = await self._engine(bidirectional=True)
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[
                    {"formula": "amount:sum"},
                    {"formula": "customers.id:count"},
                ],
                variables={"floor": 100},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 300.0
            assert _sum(resp, "orders.customers.id_count") == 2
            # The substituted predicate reaches the re-rooted CTE (F12): the
            # resolved bound is present and no placeholder token survives.
            assert "amount >= 100" in resp.sql
            assert "{floor}" not in resp.sql
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 10. Deferred-scope boundary pins (DEV-1678)
# ---------------------------------------------------------------------------

class TestDeferredScope:
    async def test_join_target_var_not_substituted(self) -> None:
        """A {var} in a JOIN-TARGET model's Column.sql is NOT substituted by
        DEV-1625 (deferred to DEV-1678): the enclosing query's variable does
        not reach it, so the stray {var} surfaces an error rather than being
        silently filled. Stable boundary pin (holds before AND after)."""
        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                # join-target Column.sql var — not substituted → unsupported
                Column(name="scaled", sql="id * {mult}", type=DataType.DOUBLE),
            ],
        )
        orders = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
        )
        tmp = tempfile.TemporaryDirectory()
        db_path = f"{tmp.name}/orders.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        cur.executemany("INSERT INTO customers VALUES (?, ?)", [(1, "Ann"), (2, "Bob")])
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)"
        )
        cur.executemany(
            "INSERT INTO orders VALUES (?, ?, ?)", [(1, 1, 100.0), (2, 2, 50.0)]
        )
        conn.commit()
        conn.close()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=db_path)
        )
        await storage.save_model(customers)
        await storage.save_model(orders)
        engine = SlayerQueryEngine(storage=storage)
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "customers.scaled:sum"}],
                variables={"mult": 2},
            )
            # The failure must be about the stray, unsubstituted {mult}
            # (emitted as a bare `mult` column ref), not an unrelated setup
            # error — pins that the join-target var truly was not substituted.
            with pytest.raises(Exception, match="mult"):
                await engine.execute(q)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 10b. inspect renders the literal {var} template (no query context)
# ---------------------------------------------------------------------------

class TestInspectShowsTemplate:
    async def test_inspect_model_shows_literal_var(self) -> None:
        from slayer.inspect.model_render import render_model_inspection

        model = SlayerModel(
            name="floored",
            data_source="ds",
            sql="SELECT id, amount FROM orders WHERE amount >= {floor}",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            out = await render_model_inspection(
                model=await engine.storage.get_model("floored"),
                storage=engine.storage,
                engine=None,
                show_sql=True,
            )
            assert "{floor}" in out  # literal template, not substituted
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 11. Unit: hardened substitute_variables (escaping, scalar-only)
# ---------------------------------------------------------------------------

def _python_mode_roundtrips(template: str, value: str) -> bool:
    """Substitute ``value`` into ``template`` with ``escape="python"``, parse the
    result as a Python expression, and return whether the recovered string
    constant equals the original ``value``. This verifies the escaping CONTRACT
    (the substituted filter round-trips through SLayer's ast.parse-based Mode-B
    parser) rather than just an opaque expected string."""
    import ast

    from slayer.core.query import substitute_variables

    substituted = substitute_variables(
        filter_str=template, variables={"v": value}, escape="python"
    )
    # template is `col = <literal>`; SLayer's Mode-B parser reads SQL `=` as
    # equality, so mirror that (`=` → `==`) to get a parseable expression, then
    # pull the RHS constant back out.
    expr = ast.parse(substituted.replace(" = ", " == ", 1), mode="eval")
    compare = expr.body
    assert isinstance(compare, ast.Compare)
    rhs = compare.comparators[0]
    assert isinstance(rhs, ast.Constant)
    return rhs.value == value


class TestSubstituteVariablesHardened:
    def test_sql_mode_string_value_doubles_single_quote(self) -> None:
        from slayer.core.query import substitute_variables

        # Mode-A (sqlglot-parsed) surfaces double the single quote.
        result = substitute_variables(
            filter_str="status = '{v}'", variables={"v": "O'Brien"}, escape="sql"
        )
        assert result == "status = 'O''Brien'"

    def test_sql_mode_string_value_without_quote_unchanged(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="status = '{v}'", variables={"v": "active"}, escape="sql"
        )
        assert result == "status = 'active'"

    def test_sql_mode_backslash_untouched(self) -> None:
        from slayer.core.query import substitute_variables

        # sqlglot treats backslash as an ordinary char in a string literal, so
        # SQL-mode must NOT touch it (only ' is special).
        result = substitute_variables(
            filter_str="path = '{v}'", variables={"v": r"a\b"}, escape="sql"
        )
        assert result == r"path = 'a\b'"

    def test_python_mode_string_value_backslash_escapes_quote(self) -> None:
        from slayer.core.query import substitute_variables

        # Mode-B (Python-AST-parsed) filters backslash-escape the quote — SQL
        # quote-doubling would be parsed as adjacent-literal concatenation
        # ('O''Brien' → 'OBrien') by the Python AST, silently corrupting it.
        result = substitute_variables(
            filter_str="status = '{v}'", variables={"v": "O'Brien"}, escape="python"
        )
        assert result == "status = 'O\\'Brien'"

    def test_python_mode_double_quote_escaped(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str='status = "{v}"', variables={"v": 'say "hi"'}, escape="python"
        )
        assert result == 'status = "say \\"hi\\""'

    def test_python_mode_backslash_doubled_first(self) -> None:
        from slayer.core.query import substitute_variables

        # Backslash must be escaped BEFORE quotes, else a\'b would become
        # a\\'b's quote unescaped. Trailing/standalone backslash → doubled.
        result = substitute_variables(
            filter_str="path = '{v}'", variables={"v": "a\\b"}, escape="python"
        )
        assert result == "path = 'a\\\\b'"

    def test_python_mode_backslash_then_quote(self) -> None:
        from slayer.core.query import substitute_variables

        # Value  a\'b  (backslash, quote) → a\\\'b  (doubled backslash, escaped
        # quote) so the Python AST reads it back as the literal 4-char string.
        result = substitute_variables(
            filter_str="path = '{v}'", variables={"v": "a\\'b"}, escape="python"
        )
        assert result == "path = 'a\\\\\\'b'"

    def test_python_mode_trailing_backslash(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="path = '{v}'", variables={"v": "abc\\"}, escape="python"
        )
        assert result == "path = 'abc\\\\'"

    def test_python_mode_backslash_before_double_quote(self) -> None:
        from slayer.core.query import substitute_variables

        # Symmetric to backslash-before-single-quote, in a double-quoted
        # template: value  a\"b  → a\\\"b so the AST recovers the 4-char value.
        result = substitute_variables(
            filter_str='path = "{v}"', variables={"v": 'a\\"b'}, escape="python"
        )
        assert result == 'path = "a\\\\\\"b"'

    @pytest.mark.parametrize(
        "template,value",
        [
            ("col = '{v}'", "O'Brien"),
            ('col = "{v}"', 'say "hi"'),
            ("col = '{v}'", "a\\b"),
            ("col = '{v}'", "a\\'b"),
            ('col = "{v}"', 'a\\"b'),
            ("col = '{v}'", "abc\\"),
            ("col = '{v}'", "plain"),
        ],
    )
    def test_python_mode_ast_roundtrip(self, template: str, value: str) -> None:
        # Semantic contract: whatever escaping produces, ast.parse of the
        # substituted filter must recover the ORIGINAL value (both quote
        # delimiters, backslash, and backslash-before-quote combinations).
        assert _python_mode_roundtrips(template, value)

    def test_number_value_not_escaped_either_mode(self) -> None:
        from slayer.core.query import substitute_variables

        assert (
            substitute_variables(filter_str="amount > {n}", variables={"n": 100}, escape="sql")
            == "amount > 100"
        )
        assert (
            substitute_variables(filter_str="amount > {n}", variables={"n": 100}, escape="python")
            == "amount > 100"
        )

    def test_float_value_not_escaped(self) -> None:
        from slayer.core.query import substitute_variables

        assert (
            substitute_variables(filter_str="rate < {n}", variables={"n": 0.05}, escape="sql")
            == "rate < 0.05"
        )

    def test_bool_value_accepted(self) -> None:
        from slayer.core.query import substitute_variables

        # bool is an int subclass; kept accepted (renders True/False).
        assert (
            substitute_variables(filter_str="flag = {v}", variables={"v": True}, escape="sql")
            == "flag = True"
        )
        assert (
            substitute_variables(filter_str="flag = {v}", variables={"v": False}, escape="python")
            == "flag = False"
        )

    def test_nan_value_raises(self) -> None:
        from slayer.core.query import substitute_variables

        with pytest.raises(ValueError, match="finite"):
            substitute_variables(
                filter_str="x = {v}", variables={"v": float("nan")}, escape="sql"
            )

    def test_inf_value_raises(self) -> None:
        from slayer.core.query import substitute_variables

        with pytest.raises(ValueError, match="finite"):
            substitute_variables(
                filter_str="x = {v}", variables={"v": float("inf")}, escape="sql"
            )
        with pytest.raises(ValueError, match="finite"):
            substitute_variables(
                filter_str="x = {v}", variables={"v": float("-inf")}, escape="python"
            )

    def test_list_value_still_raises(self) -> None:
        from slayer.core.query import substitute_variables

        with pytest.raises(ValueError, match="must be a string or number"):
            substitute_variables(
                filter_str="x = '{v}'", variables={"v": [1, 2]}, escape="sql"
            )

    def test_escape_is_required_keyword_only(self) -> None:
        from slayer.core.query import substitute_variables

        # Omitting escape is a TypeError (genuinely required kw-only), so no
        # caller silently gets an unintended escaping regime.
        with pytest.raises(TypeError):
            substitute_variables(filter_str="x = {v}", variables={"v": 1})

    def test_invalid_escape_value_raises(self) -> None:
        from slayer.core.query import substitute_variables

        # Literal gives no runtime enforcement; the implementation must reject
        # an unknown mode deterministically rather than silently pick a branch.
        with pytest.raises(ValueError, match="escape"):
            substitute_variables(
                filter_str="x = '{v}'", variables={"v": "a"}, escape="other"
            )


# ---------------------------------------------------------------------------
# 12. Unit: _substitute_model_sql_surfaces touches ONLY the four surfaces
# ---------------------------------------------------------------------------

class TestSubstituteHelperScope:
    def test_only_mode_a_surfaces_substituted(self) -> None:
        # Introduced by DEV-1625; import inline so a missing symbol doesn't
        # break collection of the whole module during TDD phase 1.
        from slayer.engine.query_engine import _substitute_model_sql_surfaces

        from slayer.core.models import Aggregation

        model = SlayerModel(
            name="m",
            data_source="ds",
            sql="SELECT * FROM t WHERE r = '{region}'",
            filters=["a >= {floor}"],
            columns=[
                Column(name="scaled", sql="amount * {mult}", type=DataType.DOUBLE),
                Column(
                    name="flt",
                    sql="amount",
                    filter="r = '{region}'",
                    type=DataType.DOUBLE,
                ),
                # Hidden column is still a Mode-A surface — it IS substituted
                # (the helper iterates all columns, visible or not).
                Column(
                    name="hid", sql="amount * {mult}", type=DataType.DOUBLE, hidden=True
                ),
            ],
            # Mode-B surface carrying a variable-looking placeholder — must be
            # left untouched (substitution never runs on formulas).
            measures=[ModelMeasure(name="rev", formula="amount:sum + {mult}")],
            # Aggregation.formula uses its OWN {expr}/{param} fill mechanism —
            # a query-variable pass must never touch it.
            aggregations=[Aggregation(name="agg", formula="SUM({expr}) * {mult}")],
        )
        out = _substitute_model_sql_surfaces(
            model=model, variables={"region": "US", "floor": 5, "mult": 2}
        )
        # The four Mode-A surfaces are substituted (hidden column included):
        assert out.sql == "SELECT * FROM t WHERE r = 'US'"
        assert out.filters == ["a >= 5"]
        assert out.get_column("scaled").sql == "amount * 2"
        assert out.get_column("flt").filter == "r = 'US'"
        assert out.get_column("hid").sql == "amount * 2"
        # Mode-B / non-Mode-A surfaces untouched even though they contain {mult}:
        assert out.measures[0].formula == "amount:sum + {mult}"
        assert out.aggregations[0].formula == "SUM({expr}) * {mult}"
        # Input model is NOT mutated — every Mode-A surface stays templated:
        assert model.sql == "SELECT * FROM t WHERE r = '{region}'"
        assert model.filters == ["a >= {floor}"]
        assert model.get_column("scaled").sql == "amount * {mult}"
        assert model.get_column("flt").filter == "r = '{region}'"
        assert model.get_column("hid").sql == "amount * {mult}"

    def test_empty_variables_is_noop(self) -> None:
        from slayer.engine.query_engine import _substitute_model_sql_surfaces

        model = SlayerModel(
            name="m",
            sql_table="t",
            data_source="ds",
            columns=[Column(name="j", sql="json_extract(x, '$.a')", type=DataType.DOUBLE)],
        )
        out = _substitute_model_sql_surfaces(model=model, variables={})
        assert out.get_column("j").sql == "json_extract(x, '$.a')"


# ---------------------------------------------------------------------------
# 13. Regression: query-level (Mode-B) filter substitution + unified escaping
# ---------------------------------------------------------------------------

class TestQueryFilterRegression:
    def _model(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )

    async def test_query_filter_var_unchanged_for_quote_free(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                filters=["region = '{region}'"],
                variables={"region": "EU"},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 275.0
        finally:
            tmp.cleanup()

    async def test_query_filter_var_now_escaped_for_quote_value(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                filters=["status = '{status}'"],
                variables={"status": "O'Brien"},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 10.0
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 14. ModelExtension direct source is on the source lineage → substituted
# ---------------------------------------------------------------------------

class TestModelExtensionDirectSource:
    async def test_model_extension_column_sql_var_substituted(self) -> None:
        """A ModelExtension used as the direct source resolves to a template
        model (source_model_origin is None), so its extra Column.sql {var} IS
        substituted by DEV-1625."""
        base = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with(base)
        try:
            ext = ModelExtension(
                source_name="orders",
                columns=[
                    Column(name="scaled", sql="amount * {mult}", type=DataType.DOUBLE)
                ],
            )
            q = SlayerQuery(
                source_model=ext,
                measures=[{"formula": "scaled:sum"}],
                variables={"mult": 2},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.scaled_sum") == 1470.0  # 735 * 2
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 15. Cached-model immutability across repeated executes with different values
# ---------------------------------------------------------------------------

class TestModelImmutabilityAcrossExecutes:
    async def test_same_model_two_values_distinct_results(self) -> None:
        """Executing the same stored model twice with different variable values
        yields different results (proving per-call substitution) AND the stored
        model still holds the literal {var} template afterwards (no mutation of
        the shared cached object)."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["region = '{region}'"],
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            r_eu = await engine.execute(
                SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    variables={"region": "EU"},
                )
            )
            r_ca = await engine.execute(
                SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    variables={"region": "CA"},
                )
            )
            assert _sum(r_eu, "orders.amount_sum") == 275.0
            assert _sum(r_ca, "orders.amount_sum") == 300.0
            # A failed substitution must not partially mutate either — pin that
            # a subsequent missing-var call raises cleanly.
            with pytest.raises(ValueError, match="Undefined variable 'region'"):
                await engine.execute(
                    SlayerQuery(
                        source_model="orders",
                        measures=[{"formula": "amount:sum"}],
                        variables={"other": "x"},
                    )
                )
            # The stored model is untouched — reloading shows the template.
            reloaded = await engine.storage.get_model("orders")
            assert reloaded.filters == ["region = '{region}'"]
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 16. Missing-variable raises on each Mode-A surface (not just model.filters)
# ---------------------------------------------------------------------------

class TestMissingVariablePerSurface:
    async def test_missing_var_in_column_sql_raises(self) -> None:
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="scaled", sql="amount * {mult}", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            # Non-empty var set (guard runs) but lacking 'mult'.
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "scaled:sum"}],
                variables={"other": "x"},
            )
            with pytest.raises(ValueError, match="Undefined variable 'mult'"):
                await engine.execute(q)
        finally:
            tmp.cleanup()

    async def test_missing_var_in_model_sql_raises(self) -> None:
        model = SlayerModel(
            name="floored",
            data_source="ds",
            sql="SELECT id, amount FROM orders WHERE amount >= {floor}",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            q = SlayerQuery(
                source_model="floored",
                measures=[{"formula": "amount:sum"}],
                variables={"other": "x"},
            )
            with pytest.raises(ValueError, match="Undefined variable 'floor'"):
                await engine.execute(q)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 17. get_column_types: defaults-only Mode-A substitution (D3)
# ---------------------------------------------------------------------------

class TestGetColumnTypesDefaults:
    async def test_complete_defaults_probe_succeeds(self) -> None:
        """A Mode-A {var} column with a complete model default resolves at the
        type-probe path (get_column_types), returning real types instead of the
        {} degradation."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            query_variables={"mult": 2},
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="scaled", sql="amount * {mult}", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            types = await engine.get_column_types("orders")
            assert types.get("scaled") == "number"
            assert types.get("amount") == "number"
        finally:
            tmp.cleanup()

    async def test_undefaulted_var_degrades_to_empty(self) -> None:
        """No default for the {var} → probe cannot render → graceful {} (the
        pre-existing degradation is preserved, not a raise)."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="scaled", sql="amount * {mult}", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            types = await engine.get_column_types("orders")
            assert types == {}
        finally:
            tmp.cleanup()

    async def test_partial_defaults_degrades_to_empty(self) -> None:
        """Two placeholders, only one defaulted → substitution activates and
        raises on the missing one, but that must stay inside the probe's
        graceful-failure boundary so the public result is still {} (F1)."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            query_variables={"mult": 2},  # 'floor' intentionally missing
            filters=["amount >= {floor}"],
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="scaled", sql="amount * {mult}", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            types = await engine.get_column_types("orders")
            assert types == {}
        finally:
            tmp.cleanup()
