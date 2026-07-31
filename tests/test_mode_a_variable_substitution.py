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
from slayer.core.query import ColumnRef, SlayerQuery
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
    def _models(self) -> list[SlayerModel]:
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
        for m in self._models():
            await storage.save_model(m)
        return SlayerQueryEngine(storage=storage), tmp

    async def test_source_model_filter_var_applies_to_reroot(self) -> None:
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
            # amount >= 100 → orders 1 (100) & 3 (200). The {floor} filter must
            # be substituted in the cross-model re-rooted CTE too, else the
            # query errors on a stray '{floor}'.
            assert _sum(resp, "orders.amount_sum") == 300.0
            assert _sum(resp, "orders.customers.id_count") == 2
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

class TestSubstituteVariablesHardened:
    def test_string_value_escapes_single_quote(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="status = '{v}'", variables={"v": "O'Brien"}
        )
        assert result == "status = 'O''Brien'"

    def test_string_value_without_quote_unchanged(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="status = '{v}'", variables={"v": "active"}
        )
        assert result == "status = 'active'"

    def test_number_value_not_escaped(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="amount > {n}", variables={"n": 100}
        )
        assert result == "amount > 100"

    def test_list_value_still_raises(self) -> None:
        from slayer.core.query import substitute_variables

        with pytest.raises(ValueError, match="must be a string or number"):
            substitute_variables(filter_str="x = '{v}'", variables={"v": [1, 2]})


# ---------------------------------------------------------------------------
# 12. Unit: _substitute_model_sql_surfaces touches ONLY the four surfaces
# ---------------------------------------------------------------------------

class TestSubstituteHelperScope:
    def test_only_mode_a_surfaces_substituted(self) -> None:
        # Introduced by DEV-1625; import inline so a missing symbol doesn't
        # break collection of the whole module during TDD phase 1.
        from slayer.engine.query_engine import _substitute_model_sql_surfaces

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
            ],
            # Mode-B surface carrying a variable-looking placeholder — must be
            # left untouched (substitution never runs on formulas).
            measures=[ModelMeasure(name="rev", formula="amount:sum + {mult}")],
        )
        out = _substitute_model_sql_surfaces(
            model=model, variables={"region": "US", "floor": 5, "mult": 2}
        )
        # The four Mode-A surfaces are substituted:
        assert out.sql == "SELECT * FROM t WHERE r = 'US'"
        assert out.filters == ["a >= 5"]
        assert out.get_column("scaled").sql == "amount * 2"
        assert out.get_column("flt").filter == "r = 'US'"
        # Mode-B surface untouched even though it contains {mult}:
        assert out.measures[0].formula == "amount:sum + {mult}"
        # Input model is NOT mutated — every Mode-A surface stays templated:
        assert model.sql == "SELECT * FROM t WHERE r = '{region}'"
        assert model.filters == ["a >= {floor}"]
        assert model.get_column("scaled").sql == "amount * {mult}"
        assert model.get_column("flt").filter == "r = '{region}'"

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
