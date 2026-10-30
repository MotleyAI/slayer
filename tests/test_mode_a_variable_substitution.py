"""{variable} substitution in Mode-A (raw-SQL) surfaces.

Scope is the DIRECT source model only: SlayerModel.sql, SlayerModel.filters,
Column.sql, Column.filter. Nested source_queries stages, query-backed direct
sources, join targets and cross-model targets are out of scope (boundary pins below).

All end-to-end cases run against a real file-backed SQLite datasource and
assert on RESULT DATA, not just generated SQL strings.
"""
from slayer.storage.sqlite_conn import transaction
import tempfile

import pytest
import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import (
    ColumnRef,
    ModelExtension,
    SlayerQuery,
    substitute_variables,
)
from slayer.engine.query_engine import (
    SlayerQueryEngine,
    _render_probe_model,
    _substitute_model_sql_surfaces,
)
from slayer.inspect.model_render import render_model_inspection
from slayer.sql.dialects import _ALL_DIALECTS, MysqlDialect, SqliteDialect
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Shared SQLite fixture
# ---------------------------------------------------------------------------

def _seed_orders_db_at(db_path) -> None:
    """6-row orders table. Region sums: US=160, EU=275, CA=300, total=735.
    Row 6 carries a single-quote in ``status`` ("O'Brien") to exercise the
    escaping path.
    """
    with transaction(str(db_path)) as conn:
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
# A backslash value is covered by test_backslash_value_sqlite_roundtrip.
_TRICKY_ROWS = [
    ("O'Brien", 10.0),
    ('say "hi"', 20.0),
]


async def _engine_with_tricky_status(model: SlayerModel) -> tuple:
    tmp = tempfile.TemporaryDirectory()
    db_path = f"{tmp.name}/tricky.db"
    with transaction(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT NOT NULL, amount REAL NOT NULL)"
        )
        cur.executemany(
            "INSERT INTO t (status, amount) VALUES (?, ?)",
            _TRICKY_ROWS + [("a\\b", 30.0)],  # a\b row exists only for the xfail pin
        )
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
        # Mode-A: SlayerModel.filters.
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

    async def test_backslash_value_sqlite_roundtrip(self) -> None:
        """A backslash in a string value round-trips through SQLite."""
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
            # Matches only the a\b row (30.0).
            assert _sum(resp, "t.amount_sum") == 30.0
        finally:
            tmp.cleanup()

    @pytest.mark.parametrize("value,expected", _TRICKY_ROWS)
    async def test_mode_b_query_filter_matches_only_its_row(
        self, value: str, expected: float
    ) -> None:
        # Mode-B: SlayerQuery.filters bind the value as a literal; quote values round-trip.
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

    async def test_placeholder_shaped_literal_in_variable_free_model_not_raised(
        self,
    ) -> None:
        """Empty-map contract: raise-on-missing applies only once a
        variable is in play. A model with an {identifier}-shaped placeholder in
        a string literal but NO variables anywhere (no query_variables, no
        caller vars) treats it as a literal — it is NOT substituted and does NOT
        raise 'Undefined variable'. `status = '{status}'` matches no seeded row,
        so SUM is NULL; the point is that it executes cleanly (literal), and a
        subsequent query that DOES supply a variable would raise on a missing
        one (covered by TestMissingVariable)."""
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
            resp = await engine.execute(
                SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
            )
            # No raise (literal, not a missing-variable error); no row has the
            # literal status '{status}', so the aggregate is NULL.
            assert resp.data[0]["orders.amount_sum"] is None
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
    def _models(self) -> list[SlayerModel]:
        # No declared reverse join: the orders → customers edge traverses both ways.
        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
            ],
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

    async def _engine(self) -> tuple:
        tmp = tempfile.TemporaryDirectory()
        db_path = f"{tmp.name}/orders.db"
        with transaction(db_path) as conn:
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
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=db_path)
        )
        for m in self._models():
            await storage.save_model(m)
        return SlayerQueryEngine(storage=storage), tmp

    async def test_source_model_filter_var_applies_to_reroot(self) -> None:
        # The host's ``amount >= {floor}`` stays host-local, so the cross-model count is over
        # all 3 customers; {floor} is substituted wherever it is emitted, and nowhere left raw.
        engine, tmp = await self._engine()
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
        # A filter constrains the scope whose root it references, so the host's
        # ``amount >= {floor}`` stays out of the customers-rooted CTE.
        engine, tmp = await self._engine()
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
            # The host model filter stays host-local: all 3 customers counted.
            assert _sum(resp, "orders.customers.id_count") == 3
            # The substituted predicate is emitted where the filter DOES apply,
            # and no placeholder token survives anywhere.
            assert "amount >= 100" in resp.sql
            assert "{floor}" not in resp.sql
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 10. Out-of-scope boundary pins
# ---------------------------------------------------------------------------

class TestDeferredScope:
    async def test_join_target_var_not_substituted(self) -> None:
        """A {var} in a JOIN-TARGET model's Column.sql is NOT substituted:
        the enclosing query's variable does
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
        with transaction(db_path) as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
            cur.executemany("INSERT INTO customers VALUES (?, ?)", [(1, "Ann"), (2, "Bob")])
            cur.execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)"
            )
            cur.executemany(
                "INSERT INTO orders VALUES (?, ?, ?)", [(1, 1, 100.0), (2, 2, 50.0)]
            )
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
            # The failure must be about the stray, unsubstituted {mult}: it is
            # emitted as a bare `mult` column ref, so the DB errors with
            # "no such column: customers.mult" (a DB error, NOT a substitution
            # ValueError — precisely because the join-target var was not
            # substituted). The \bmult\b word boundary keeps an unrelated error
            # mentioning "multiple"/"multi-stage" from satisfying the pin.
            with pytest.raises(Exception, match=r"\bmult\b"):
                await engine.execute(q)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 10b. inspect renders the literal {var} template (no query context)
# ---------------------------------------------------------------------------

class TestInspectShowsTemplate:
    async def test_inspect_model_shows_literal_var(self) -> None:

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

class TestSubstituteVariablesHardened:
    def test_sql_mode_string_value_doubles_single_quote(self) -> None:

        # Mode-A (sqlglot-parsed) surfaces double the single quote. Standard
        # (non-backslash) dialect regime.
        result = substitute_variables(
            filter_str="status = '{v}'", variables={"v": "O'Brien"},
            backslash_escapes=False,
        )
        assert result == "status = 'O''Brien'"

    def test_sql_mode_string_value_without_quote_unchanged(self) -> None:

        result = substitute_variables(
            filter_str="status = '{v}'", variables={"v": "active"},
            backslash_escapes=False,
        )
        assert result == "status = 'active'"

    def test_sql_mode_backslash_untouched_standard_dialect(self) -> None:

        # On a standard dialect backslash is ordinary; only ' is special.
        result = substitute_variables(
            filter_str="path = '{v}'", variables={"v": r"a\b"},
            backslash_escapes=False,
        )
        assert result == r"path = 'a\b'"

    def test_sql_mode_newline_left_raw(self) -> None:

        # SQL string literals permit raw newlines; only the single quote is special.
        result = substitute_variables(
            filter_str="note = '{v}'", variables={"v": "a\nb"},
            backslash_escapes=False,
        )
        assert result == "note = 'a\nb'"

    def test_number_value_not_escaped(self) -> None:

        assert (
            substitute_variables(
                filter_str="amount > {n}", variables={"n": 100},
                backslash_escapes=False,
            )
            == "amount > 100"
        )

    def test_float_value_not_escaped(self) -> None:

        assert (
            substitute_variables(
                filter_str="rate < {n}", variables={"n": 0.05},
                backslash_escapes=False,
            )
            == "rate < 0.05"
        )

    def test_bool_value_accepted(self) -> None:

        # bool is an int subclass; kept accepted (renders True/False).
        assert (
            substitute_variables(
                filter_str="flag = {v}", variables={"v": True},
                backslash_escapes=False,
            )
            == "flag = True"
        )

    def test_nan_value_raises(self) -> None:

        nan = float("nan")
        with pytest.raises(ValueError, match="finite"):
            substitute_variables(
                filter_str="x = {v}", variables={"v": nan},
                backslash_escapes=False,
            )

    def test_inf_value_raises(self) -> None:

        inf = float("inf")
        with pytest.raises(ValueError, match="finite"):
            substitute_variables(
                filter_str="x = {v}", variables={"v": inf},
                backslash_escapes=False,
            )
        neg_inf = float("-inf")
        with pytest.raises(ValueError, match="finite"):
            substitute_variables(
                filter_str="x = {v}", variables={"v": neg_inf},
                backslash_escapes=False,
            )

    def test_dict_value_raises(self) -> None:

        # A dict is neither scalar nor list/tuple → terminal ValueError naming list/tuple.
        with pytest.raises(
            ValueError, match="must be a string, number, or list/tuple"
        ):
            substitute_variables(
                filter_str="x = '{v}'", variables={"v": {"a": 1}},
                backslash_escapes=False,
            )

    def test_set_value_raises(self) -> None:

        # A set is unordered → deliberately NOT accepted (only list/tuple), and
        # falls through to the same terminal message.
        with pytest.raises(
            ValueError, match="must be a string, number, or list/tuple"
        ):
            substitute_variables(
                filter_str="x IN ({v})", variables={"v": {1, 2}},
                backslash_escapes=False,
            )

    def test_escape_regime_parameter_is_gone(self) -> None:
        with pytest.raises(TypeError):
            substitute_variables(
                filter_str="x = {v}", variables={"v": 1}, escape="sql", backslash_escapes=False,  # pyright: ignore[reportCallIssue]  # the removed parameter must fail
            )


# ---------------------------------------------------------------------------
# 12. Unit: _substitute_model_sql_surfaces touches ONLY the four surfaces
# ---------------------------------------------------------------------------

class TestSubstituteHelperScope:
    def test_only_mode_a_surfaces_substituted(self) -> None:
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
            # Saved measure formula: substituted under the Mode-B regime.
            measures=[ModelMeasure(name="rev", formula="amount:sum + {mult}")],
            # Aggregation.formula uses its OWN {expr}/{param} fill mechanism —
            # a query-variable pass must never touch it.
            aggregations=[Aggregation(name="agg", formula="SUM({expr}) * {mult}")],
        )
        out = _substitute_model_sql_surfaces(
            model=model, variables={"region": "US", "floor": 5, "mult": 2},
            dialect=SqliteDialect(),
        )
        # The four Mode-A surfaces are substituted (hidden column included):
        assert out.sql == "SELECT * FROM t WHERE r = 'US'"
        assert out.filters == ["a >= 5"]
        assert out.get_column("scaled").sql == "amount * 2"
        assert out.get_column("flt").filter == "r = 'US'"
        assert out.get_column("hid").sql == "amount * 2"
        # Saved measure formulas substitute (Mode-B regime); Aggregation.formula never does:
        assert out.measures[0].formula == "amount:sum + 2"
        assert out.aggregations[0].formula == "SUM({expr}) * {mult}"
        # Input model is NOT mutated — every Mode-A surface stays templated:
        assert model.sql == "SELECT * FROM t WHERE r = '{region}'"
        assert model.filters == ["a >= {floor}"]
        assert model.get_column("scaled").sql == "amount * {mult}"
        assert model.get_column("flt").filter == "r = '{region}'"
        assert model.get_column("hid").sql == "amount * {mult}"

    def test_empty_variables_is_noop(self) -> None:

        model = SlayerModel(
            name="m",
            sql_table="t",
            data_source="ds",
            columns=[Column(name="j", sql="json_extract(x, '$.a')", type=DataType.DOUBLE)],
        )
        out = _substitute_model_sql_surfaces(
            model=model, variables={}, dialect=SqliteDialect()
        )
        assert out.get_column("j").sql == "json_extract(x, '$.a')"

    def test_list_value_substituted_on_all_four_mode_a_surfaces(self) -> None:
        """A single list variable rendered into each of the four Mode-A
        surfaces — SlayerModel.sql, SlayerModel.filters, Column.sql,
        Column.filter — proving every surface routes list values through
        ``_render_variable_value`` (all comma-joined, auto-quoted, sql-escape)."""

        model = SlayerModel(
            name="m",
            data_source="ds",
            sql="SELECT * FROM t WHERE region IN ({regions})",
            filters=["region IN ({regions})"],
            columns=[
                Column(name="in_flag", sql="region IN ({regions})", type=DataType.BOOLEAN),
                Column(
                    name="amt",
                    sql="amount",
                    filter="region IN ({regions})",
                    type=DataType.DOUBLE,
                ),
            ],
        )
        out = _substitute_model_sql_surfaces(
            model=model, variables={"regions": ["US", "CA"]}, dialect=SqliteDialect()
        )
        rendered = "region IN ('US', 'CA')"
        assert out.sql == f"SELECT * FROM t WHERE {rendered}"
        assert out.filters == [rendered]
        assert out.get_column("in_flag").sql == rendered
        assert out.get_column("amt").filter == rendered


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
        substituted."""
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
            # a subsequent missing-var call raises cleanly. Build the query
            # outside the block so only execute() can throw inside it.
            missing_var_q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"other": "x"},
            )
            with pytest.raises(ValueError, match="Undefined variable 'region'"):
                await engine.execute(missing_var_q)
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

    async def test_list_default_probe_succeeds(self) -> None:
        """A Mode-A ``IN ({var})`` filter with a LIST default in
        ``query_variables`` renders cleanly at the type-probe path, so the probe
        returns real column types instead of the ``{}`` degradation."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            query_variables={"regions": ["US", "CA"]},
            filters=["region IN ({regions})"],
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            types = await engine.get_column_types("orders")
            assert types.get("amount") == "number"
            assert types.get("region") == "string"
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 18. Unit: list/tuple variable rendering (IN-list pushdown)
# ---------------------------------------------------------------------------


class TestListValueRenderingSql:
    """List rendering: comma-joined, strings auto-quoted and
    single-quote-doubled, numbers/bools bare. Template shape is ``IN ({var})``
    — the author writes the parens, NOT the per-element quotes."""

    def test_sql_list_strings_auto_quoted(self) -> None:

        result = substitute_variables(
            filter_str="region IN ({v})",
            variables={"v": ["US", "CA"]},
            backslash_escapes=False,
        )
        assert result == "region IN ('US', 'CA')"

    def test_sql_list_embedded_quote_doubled(self) -> None:

        # Per-element the same sql escaping as scalars: ' → ''.
        result = substitute_variables(
            filter_str="name IN ({v})",
            variables={"v": ["A", "O'Brien", 3]},
            backslash_escapes=False,
        )
        assert result == "name IN ('A', 'O''Brien', 3)"

    def test_sql_list_numbers_and_bools_bare(self) -> None:

        result = substitute_variables(
            filter_str="x IN ({v})",
            variables={"v": [1, 2.5, True, False]},
            backslash_escapes=False,
        )
        assert result == "x IN (1, 2.5, True, False)"

    def test_sql_single_element_list(self) -> None:

        result = substitute_variables(
            filter_str="region IN ({v})",
            variables={"v": ["EU"]},
            backslash_escapes=False,
        )
        assert result == "region IN ('EU')"

    def test_sql_tuple_accepted_same_as_list(self) -> None:

        from_list = substitute_variables(
            filter_str="x IN ({v})", variables={"v": ["A", "B"]},
            backslash_escapes=False,
        )
        from_tuple = substitute_variables(
            filter_str="x IN ({v})", variables={"v": ("A", "B")},
            backslash_escapes=False,
        )
        assert from_list == from_tuple == "x IN ('A', 'B')"

    def test_sql_injection_element_stays_inside_literal(self) -> None:

        # A classic breakout attempt: the closing quote is doubled so the whole
        # payload stays a single string literal inside the IN list.
        result = substitute_variables(
            filter_str="region IN ({v})",
            variables={"v": ["x') OR ('1'='1"]},
            backslash_escapes=False,
        )
        assert result == "region IN ('x'') OR (''1''=''1')"


class TestListValueRenderingErrors:
    def test_empty_list_raises_naming_variable(self) -> None:

        # IN () is invalid SQL; the message names the variable, says "empty", and
        # points at the sentinel-default idiom for "no filter".
        with pytest.raises(ValueError, match="regions") as exc:
            substitute_variables(
                filter_str="region IN ({regions})",
                variables={"regions": []},
                backslash_escapes=False,
            )
        msg = str(exc.value).lower()
        assert "empty" in msg
        # The sentinel-default hint is a required contract, pinned by
        # a stable single-word fragment rather than the full sentence.
        assert "sentinel" in msg

    def test_empty_tuple_raises(self) -> None:

        with pytest.raises(ValueError, match="regions") as exc:
            substitute_variables(
                filter_str="region IN ({regions})",
                variables={"regions": ()},
                backslash_escapes=False,
            )
        assert "empty" in str(exc.value).lower()

    def test_nested_list_element_raises(self) -> None:

        # ``element`` in the match pins the PER-ELEMENT validation path (a valid
        # list with one bad element), not the whole-value type rejection; and the
        # variable name must appear so the author can locate it.
        with pytest.raises(ValueError, match="element") as exc:
            substitute_variables(
                filter_str="region IN ({regions})",
                variables={"regions": ["A", ["B", "C"]]},
                backslash_escapes=False,
            )
        assert "regions" in str(exc.value)

    def test_none_element_raises(self) -> None:

        with pytest.raises(ValueError, match="element") as exc:
            substitute_variables(
                filter_str="region IN ({regions})",
                variables={"regions": ["A", None]},
                backslash_escapes=False,
            )
        assert "regions" in str(exc.value)

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_non_finite_float_element_raises(self, bad: float) -> None:

        with pytest.raises(ValueError, match="finite") as exc:
            substitute_variables(
                filter_str="x IN ({v})",
                variables={"v": [1.0, bad]},
                backslash_escapes=False,
            )
        assert "v" in str(exc.value)

    def test_dict_element_raises(self) -> None:

        with pytest.raises(ValueError, match="element") as exc:
            substitute_variables(
                filter_str="region IN ({regions})",
                variables={"regions": ["A", {"b": 1}]},
                backslash_escapes=False,
            )
        assert "regions" in str(exc.value)


# ---------------------------------------------------------------------------
# 19. End-to-end Mode-A: WHERE region IN ({regions}) with a list variable
# ---------------------------------------------------------------------------


class TestListModeAEndToEnd:
    def _model(self, **kw) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["region IN ({regions})"],
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            **kw,
        )

    async def test_runtime_list_filters_rows(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"regions": ["US", "CA"]},
            )
            resp = await engine.execute(q)
            # US=160, CA=300 → 460 (EU excluded).
            assert _sum(resp, "orders.amount_sum") == 460.0
        finally:
            tmp.cleanup()

    async def test_single_element_list(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"regions": ["EU"]},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 275.0
        finally:
            tmp.cleanup()

    async def test_list_default_used_when_kwarg_absent(self) -> None:
        engine, tmp = await _engine_with(
            self._model(query_variables={"regions": ["EU", "CA"]})
        )
        try:
            q = SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
            resp = await engine.execute(q)
            # EU=275, CA=300 → 575.
            assert _sum(resp, "orders.amount_sum") == 575.0
        finally:
            tmp.cleanup()

    async def test_query_variables_list_overrides_model_default_list(self) -> None:
        # Middle precedence layer: model_defaults < query.variables (no runtime).
        engine, tmp = await _engine_with(
            self._model(query_variables={"regions": ["EU", "CA"]})
        )
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"regions": ["US"]},
            )
            resp = await engine.execute(q)
            # Query-level ['US'] wins over model default ['EU','CA'] → 160.
            assert _sum(resp, "orders.amount_sum") == 160.0
        finally:
            tmp.cleanup()

    async def test_runtime_list_overrides_default_list(self) -> None:
        engine, tmp = await _engine_with(
            self._model(query_variables={"regions": ["EU", "CA"]})
        )
        try:
            q = SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
            resp = await engine.execute(q, variables={"regions": ["US"]})
            # Runtime ['US'] wins over default ['EU','CA'] → 160.
            assert _sum(resp, "orders.amount_sum") == 160.0
        finally:
            tmp.cleanup()

    async def test_injection_element_matches_no_rows(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"regions": ["x') OR ('1'='1"]},
            )
            resp = await engine.execute(q)
            # The payload stays inside its literal, matching no region, so the
            # aggregate is NULL — NOT the whole-table total (735) an escape
            # would have produced.
            assert resp.data[0]["orders.amount_sum"] is None
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 20. End-to-end Mode-B: query filter "region in ({regions})" with a list
# ---------------------------------------------------------------------------


class TestListModeBEndToEnd:
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

    async def test_query_filter_list_in(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                filters=["region in ({regions})"],
                variables={"regions": ["US", "CA"]},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 460.0  # US 160 + CA 300
        finally:
            tmp.cleanup()

    async def test_query_filter_single_element_list(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            # The trailing-comma render (('EU',)) is what keeps a 1-element list
            # a tuple through the Python-AST parser rather than a bare string.
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                filters=["region in ({regions})"],
                variables={"regions": ["EU"]},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 275.0
        finally:
            tmp.cleanup()

    async def test_query_filter_not_in_list(self) -> None:
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                filters=["region not in ({regions})"],
                variables={"regions": ["US"]},
            )
            resp = await engine.execute(q)
            # Everything except US(160) → EU 275 + CA 300 = 575.
            assert _sum(resp, "orders.amount_sum") == 575.0
        finally:
            tmp.cleanup()

    async def test_runtime_list_overrides_query_level_list(self) -> None:
        # Mode-B filters read query.variables, which merges runtime kwarg over
        # the query-level dict (runtime wins) — verify that merge with lists.
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                filters=["region in ({regions})"],
                variables={"regions": ["EU", "CA"]},
            )
            resp = await engine.execute(q, variables={"regions": ["US"]})
            assert _sum(resp, "orders.amount_sum") == 160.0  # runtime ['US'] wins
        finally:
            tmp.cleanup()

    async def test_query_filter_list_escaping_matches_only_its_row(self) -> None:
        # Mode-B escaping through the real parse+compile+SQLite pipeline: a
        # quote-bearing element must round-trip so it matches only its own row.
        engine, tmp = await _engine_with(self._model())
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                filters=["status in ({statuses})"],
                variables={"statuses": ["O'Brien"]},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 10.0  # row 6 only
        finally:
            tmp.cleanup()


# ===========================================================================
# Dialect-aware escaping for Mode-A {var} substitution
# ===========================================================================


# ---------------------------------------------------------------------------
# 19. SQL regime is dialect-aware: backslash_escapes flag (Gap 1)
# ---------------------------------------------------------------------------

class TestSqlModeBackslashEscaping:
    """``backslash_escapes=`` selects the escaping regime.

    - False (standard: SQLite/Postgres/DuckDB/…): backslash is an ordinary
      char, ONLY the single quote is doubled (``'`` → ``''``).
    - True (backslash dialects: MySQL/ClickHouse/Snowflake/…): backslash is an
      escape char, so it is doubled FIRST (``\\`` → ``\\\\``) and the single
      quote is backslash-escaped (``'`` → ``\\'``). The double quote is left
      untouched (inside a single-quoted literal it is an ordinary char on every
      dialect, and ``\\"`` is not a recognised escape on 6 of the 7).
    """

    def test_standard_regime_doubles_quote_leaves_backslash(self) -> None:
        result = substitute_variables(
            filter_str="p = '{v}'", variables={"v": "a\\'b"},
            backslash_escapes=False,
        )
        # a \ ' b  →  double the quote only; backslash untouched.
        assert result == "p = 'a\\''b'"

    def test_backslash_regime_doubles_backslash_and_escapes_quote(self) -> None:
        result = substitute_variables(
            filter_str="p = '{v}'", variables={"v": "a\\'b"},
            backslash_escapes=True,
        )
        # a \ ' b  →  \\ (doubled backslash) + \' (escaped quote).
        assert result == "p = 'a\\\\\\'b'"

    def test_backslash_regime_lone_backslash_doubled(self) -> None:
        result = substitute_variables(
            filter_str="p = '{v}'", variables={"v": "a\\b"},
            backslash_escapes=True,
        )
        assert result == "p = 'a\\\\b'"

    def test_backslash_regime_trailing_backslash_doubled(self) -> None:
        # The classic breakout: a trailing backslash must not eat the closing
        # quote — it is doubled so the literal stays closed.
        result = substitute_variables(
            filter_str="p = '{v}'", variables={"v": "abc\\"},
            backslash_escapes=True,
        )
        assert result == "p = 'abc\\\\'"

    def test_backslash_regime_single_quote_only(self) -> None:
        result = substitute_variables(
            filter_str="p = '{v}'", variables={"v": "O'Brien"},
            backslash_escapes=True,
        )
        assert result == "p = 'O\\'Brien'"

    def test_backslash_regime_double_quote_untouched(self) -> None:
        # Inside a single-quoted literal a bare double quote is an ordinary
        # char; escaping it (\") would CORRUPT the value on 6 of 7 backslash
        # dialects (only MySQL treats \" as " there). So it stays as-is.
        result = substitute_variables(
            filter_str="p = '{v}'", variables={"v": 'say "hi"'},
            backslash_escapes=True,
        )
        assert result == 'p = \'say "hi"\''

    def test_backslash_regime_plain_value_unchanged(self) -> None:
        result = substitute_variables(
            filter_str="p = '{v}'", variables={"v": "active"},
            backslash_escapes=True,
        )
        assert result == "p = 'active'"

    def test_number_passthrough_both_regimes(self) -> None:
        for be in (True, False):
            assert substitute_variables(
                filter_str="x > {n}", variables={"n": 100},
                backslash_escapes=be,
            ) == "x > 100"

    def test_backslash_regime_list_elements_escaped(self) -> None:
        # List rendering must compose with the backslash regime:
        # each string element is auto-quoted AND backslash-escaped.
        result = substitute_variables(
            filter_str="p IN ({v})", variables={"v": ["a\\b", "O'Brien"]},
            backslash_escapes=True,
        )
        assert result == "p IN ('a\\\\b', 'O\\'Brien')"

    def test_backslash_regime_list_elements_standard(self) -> None:
        # Same list under the standard regime: quote-doubled, backslash raw.
        result = substitute_variables(
            filter_str="p IN ({v})", variables={"v": ["a\\b", "O'Brien"]},
            backslash_escapes=False,
        )
        assert result == "p IN ('a\\b', 'O''Brien')"


# ---------------------------------------------------------------------------
# 20. Fail-closed: the backslash_escapes signal is required
# ---------------------------------------------------------------------------

class TestSqlModeFailClosed:
    """A security-flavoured property (correct escaping) must not silently
    default. ``backslash_escapes`` must be specified —
    a new/overlooked SQL call site that forgets it FAILS instead of
    under-escaping on MySQL/ClickHouse."""

    def test_sql_without_backslash_escapes_raises(self) -> None:
        with pytest.raises(ValueError, match="backslash_escapes"):
            substitute_variables(
                filter_str="p = '{v}'", variables={"v": "x"}
            )

    def test_sql_without_flag_raises_even_for_number(self) -> None:
        # The guard fires regardless of value type —
        # so no caller can partially bypass it by happening to pass a number.
        with pytest.raises(ValueError, match="backslash_escapes"):
            substitute_variables(
                filter_str="x > {n}", variables={"n": 1}
            )


# ---------------------------------------------------------------------------
# 21. SQL regime leaves control chars raw
# ---------------------------------------------------------------------------

class TestSqlModeControlChars:
    def test_sql_mode_leaves_control_chars_raw(self) -> None:
        # SQL literals accept raw newlines and sqlglot re-emits them, so sql mode must NOT encode control chars.
        result = substitute_variables(
            filter_str="p = '{v}'", variables={"v": "a\nb"},
            backslash_escapes=False,
        )
        assert result == "p = 'a\nb'"


# ---------------------------------------------------------------------------
# 22. Dialect matrix: escaping round-trips through the parser sqlglot uses
# ---------------------------------------------------------------------------

_MATRIX_VALUES = [
    "O'Brien",              # single quote
    'say "hi"',             # double quote
    "a\\b",                 # backslash
    "a\\'b",                # backslash + single quote
    'a\\"b',                # backslash + double quote
    "abc\\",                # trailing backslash (closing-quote eater)
    "it's a \"mix\"",       # both quote styles
    "line1\nline2",         # raw newline (legal in a SQL literal)
    "tab\there",            # raw tab
    "plain",                # nothing special
]

# (dialect, value) pairs — the escaping regime flag is read from the dialect's
# own ``backslash_escapes_strings`` property AT TEST TIME (not here), so this
# module still imports cleanly before the property is implemented.
_MATRIX = [
    (d, v) for d in _ALL_DIALECTS for v in _MATRIX_VALUES
]


class TestEscapingRegimeDialectMatrix:
    """For every dialect, the sql-escaped literal produced by
    ``substitute_variables`` — using the regime the dialect's OWN
    ``backslash_escapes_strings`` selects — must round-trip through sqlglot's
    parser for that dialect (the exact parser SLayer feeds the substituted
    Mode-A SQL to), AND survive a parse → emit → re-parse cycle. This is the
    core correctness guarantee that the derived flag matches the parser."""

    @pytest.mark.parametrize(
        "dialect,value",
        _MATRIX,
        ids=[f"{d.sqlglot_name}-{v!r}" for d, v in _MATRIX],
    )
    def test_sql_literal_roundtrips_through_dialect_parser(
        self, dialect, value: str
    ) -> None:
        literal = substitute_variables(
            filter_str="'{v}'", variables={"v": value},
            backslash_escapes=dialect.backslash_escapes_strings,
        )
        parsed = sqlglot.parse_one(literal, dialect=dialect.sqlglot_name)
        assert parsed.this == value, (dialect.sqlglot_name, repr(literal))
        # parse → emit → re-parse must preserve the value too.
        reparsed = sqlglot.parse_one(
            parsed.sql(dialect=dialect.sqlglot_name), dialect=dialect.sqlglot_name
        )
        assert reparsed.this == value, (dialect.sqlglot_name, "roundtrip")

    def test_matrix_covers_all_14_dialects(self) -> None:
        assert len({d.sqlglot_name for d, _ in _MATRIX}) == 14


# ---------------------------------------------------------------------------
# 23. Engine threading: the resolved dialect reaches the sql escaping
# ---------------------------------------------------------------------------

class TestSubstituteModelSqlSurfacesDialect:
    """``_substitute_model_sql_surfaces`` requires a dialect and derives the
    escaping regime from it (fail-closed — no caller can under-escape by
    forgetting a bool)."""

    def _model(self) -> SlayerModel:
        return SlayerModel(
            name="m",
            data_source="ds",
            sql="SELECT * FROM t WHERE r = '{region}'",
            columns=[Column(name="a", sql="amount", type=DataType.DOUBLE)],
        )

    def test_backslash_dialect_escapes_value(self) -> None:

        out = _substitute_model_sql_surfaces(
            model=self._model(), variables={"region": "a\\'b"},
            dialect=MysqlDialect(),
        )
        assert out.sql == "SELECT * FROM t WHERE r = 'a\\\\\\'b'"

    def test_standard_dialect_doubles_quote_only(self) -> None:

        out = _substitute_model_sql_surfaces(
            model=self._model(), variables={"region": "a\\'b"},
            dialect=SqliteDialect(),
        )
        assert out.sql == "SELECT * FROM t WHERE r = 'a\\''b'"

    def test_dialect_is_required(self) -> None:

        # Build the model outside the raises-block so only the call under test
        # (missing the required `dialect`) can raise (Sonar S5778).
        model = self._model()
        with pytest.raises(TypeError):
            _substitute_model_sql_surfaces(model=model, variables={"region": "x"})


def _status_literal_from_sql(sql: str, dialect_name: str):
    """Parse generated SQL for ``dialect_name`` and return the string value of
    the ``status = '...'`` literal (the round-tripped variable value)."""
    tree = sqlglot.parse_one(sql, dialect=dialect_name)
    for eq in tree.find_all(sqlglot.exp.EQ):
        lit = eq.expression
        if isinstance(lit, sqlglot.exp.Literal) and lit.is_string:
            return lit.this
    return None


class TestEngineDialectThreadingEndToEnd:
    """Through the real engine (dry_run, no live backslash DB): the datasource's
    dialect must reach the Mode-A escaping. The generated SQL is parsed+re-emitted
    by sqlglot, so we assert the VALUE ROUND-TRIPS (the escaped literal recovers
    the original), not a raw substring — the discriminating proof, since a
    mis-threaded (standard) regime on MySQL would make sqlglot's MySQL parser
    raise on ``'a\\''b'`` and the dry_run would fail outright."""

    async def _dry_run_sql_for(self, ds_type: str, value: str) -> str:
        """Build a one-model engine on a datasource of ``ds_type`` and return
        the generated SQL for a Mode-A model filter carrying ``value``. Uses a
        SQLite file as the actual backend but pins the datasource *type* so the
        dialect (hence escaping regime) is exercised; dry_run skips execution."""
        tmp = tempfile.TemporaryDirectory()
        db_path = f"{tmp.name}/x.db"
        _seed_orders_db_at(db_path)
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type=ds_type, database=db_path)
        )
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["status = '{v}'"],
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                variables={"v": value},
            )
            result = await engine.execute(q, dry_run=True)
            return result.sql
        finally:
            tmp.cleanup()

    async def test_backslash_dialect_threads_regime_so_value_roundtrips(self) -> None:
        # A backslash+quote value on a mysql-type datasource: the dry_run only
        # SUCCEEDS (and the literal round-trips) if the MySQL backslash regime
        # was threaded — the naive/standard regime would emit ``'a\\''b'`` which
        # sqlglot's MySQL parser rejects, raising before any SQL is returned.
        sql = await self._dry_run_sql_for("mysql", "a\\'b")
        assert _status_literal_from_sql(sql, "mysql") == "a\\'b"

    async def test_standard_dialect_value_roundtrips(self) -> None:
        # Standard (sqlite) path: a quote-bearing (backslash-free) value
        # round-trips end-to-end. Backslash values are the pre-existing SQLite
        # gap (see the strict-xfail above), so they are deliberately not used.
        sql = await self._dry_run_sql_for("sqlite", "O'Brien")
        assert _status_literal_from_sql(sql, "sqlite") == "O'Brien"


class TestProbeModelDialectThreading:
    """``_render_probe_model`` threads the dialect into the Mode-A surfaces it
    renders from the model's own defaults."""

    def _template_model(self) -> SlayerModel:
        return SlayerModel(
            name="m",
            data_source="ds",
            sql="SELECT * FROM t WHERE r = '{region}'",
            query_variables={"region": "a\\'b"},
            columns=[Column(name="a", sql="amount", type=DataType.DOUBLE)],
        )

    def test_probe_uses_backslash_dialect(self) -> None:

        out = _render_probe_model(self._template_model(), dialect=MysqlDialect())
        assert out.sql == "SELECT * FROM t WHERE r = 'a\\\\\\'b'"

    def test_probe_uses_standard_dialect(self) -> None:

        out = _render_probe_model(self._template_model(), dialect=SqliteDialect())
        assert out.sql == "SELECT * FROM t WHERE r = 'a\\''b'"

    def test_probe_dialect_required(self) -> None:

        # Build the model outside the raises-block so only the call under test
        # (missing the required `dialect`) can raise (Sonar S5778).
        model = self._template_model()
        with pytest.raises(TypeError):
            _render_probe_model(model)


# ---------------------------------------------------------------------------
# 24. Mode-B end-to-end: control-char values round-trip through the parser
# ---------------------------------------------------------------------------

async def _engine_with_ctrl_status(model: SlayerModel) -> tuple:
    """Seed a table whose status column carries control-char values, so a
    Mode-B (python-escaped) query filter can be shown to match end-to-end."""
    tmp = tempfile.TemporaryDirectory()
    db_path = f"{tmp.name}/ctrl.db"
    with transaction(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT NOT NULL, amount REAL NOT NULL)"
        )
        cur.executemany(
            "INSERT INTO t (status, amount) VALUES (?, ?)",
            [("line1\nline2", 11.0), ("tab\there", 22.0), ("cr\rhere", 33.0), ("plain", 44.0)],
        )
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_path)
    )
    await storage.save_model(model)
    return SlayerQueryEngine(storage=storage), tmp


class TestModeBControlCharsEndToEnd:
    """Legitimate control-char values (Gap 2): a newline/tab/CR-bearing value in
    a Mode-B query filter must parse (no ast.parse SyntaxError) and match ONLY
    its row through the real SQLite pipeline."""

    def _model(self) -> SlayerModel:
        return SlayerModel(
            name="t",
            sql_table="t",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )

    @pytest.mark.parametrize(
        "value,expected",
        [("line1\nline2", 11.0), ("tab\there", 22.0), ("cr\rhere", 33.0)],
    )
    async def test_control_char_value_matches_only_its_row(
        self, value: str, expected: float
    ) -> None:
        engine, tmp = await _engine_with_ctrl_status(self._model())
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
