"""{variable} substitution in Mode-A (raw-SQL) surfaces.

Scope is the DIRECT source dataset only: SlayerDataset.sql, SlayerDataset.filters,
DatasetField.sql, DatasetField.filter. Nested source_queries stages, query-backed direct
sources, relationship targets and cross-dataset targets are out of scope (boundary pins below).

All end-to-end cases run against a real file-backed SQLite datasource and
assert on RESULT DATA, not just generated SQL strings.
"""
from slayer.storage.sqlite_conn import transaction
import tempfile
from typing import Optional

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import (
    QueryDimension,
    SlayerQuery,
)
from slayer.engine.query_engine import (
    SlayerQueryEngine,
    _render_probe_dataset,
    _substitute_dataset_sql_surfaces,
)
from slayer.inspect.dataset_render import render_dataset_inspection
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


async def _engine_with(*models: SlayerDataset) -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory]:
    """Build a real SQLite datasource + YAML storage with the given datasets
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
        await storage.save_dataset(m)
    return SlayerQueryEngine(storage=storage), tmp


def _sum(resp, alias: str) -> float:
    assert resp.row_count == 1, f"expected 1 row, got {resp.row_count}: {resp.data}"
    return resp.data[0][alias]


# ---------------------------------------------------------------------------
# 1. SlayerModel.filters with {var}
# ---------------------------------------------------------------------------

class TestModelFiltersSubstitution:
    def _dataset(self, **kw) -> SlayerDataset:
        return SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["region = '{region}'"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
            **kw,
        )

    async def test_model_filter_var_filters_rows(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"region": "EU"},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 275.0
        finally:
            tmp.cleanup()

    async def test_model_filter_var_us(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"region": "US"},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 160.0
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 2. DatasetField.sql with {var}
# ---------------------------------------------------------------------------

class TestColumnSqlSubstitution:
    def _dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="scaled", sql="amount * {mult}", datatype=DataType.FLOAT),
            ],
        )

    async def test_column_sql_var_scales(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(scaled)"}],
                "variables": {"mult": 2},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.scaled_sum") == 1470.0  # 735 * 2
        finally:
            tmp.cleanup()

    async def test_column_sql_var_identity(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(scaled)"}],
                "variables": {"mult": 1},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.scaled_sum") == 735.0
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 3. DatasetField.filter with {var} (CASE-WHEN at aggregation time)
# ---------------------------------------------------------------------------

class TestColumnFilterSubstitution:
    def _dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
                DatasetField(
                    name="region_amount",
                    sql="amount",
                    filter="region = '{region}'",
                    datatype=DataType.FLOAT,
                ),
            ],
        )

    async def test_column_filter_var_windowed_sum(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(region_amount)"}],
                "variables": {"region": "US"},
            })
            resp = await engine.execute(q)
            # SUM(CASE WHEN region='US' THEN amount END) = 100+50+10 = 160
            assert _sum(resp, "orders.region_amount_sum") == 160.0
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 4. SlayerModel.sql with {var} (sql-mode: WHERE + projected scalar)
# ---------------------------------------------------------------------------

class TestModelSqlSubstitution:
    def _dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="floored",
            data_source="ds",
            sql=(
                "SELECT id, region, amount, {floor} AS floor_val "
                "FROM orders WHERE amount >= {floor}"
            ),
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="floor_val", sql="floor_val", datatype=DataType.FLOAT),
            ],
        )

    async def test_model_sql_var_where(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "floored",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"floor": 100},
            })
            resp = await engine.execute(q)
            # amount >= 100 → rows 100, 200, 300 → 600
            assert _sum(resp, "floored.amount_sum") == 600.0
        finally:
            tmp.cleanup()

    async def test_model_sql_var_projected_scalar(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery(
                population="floored",
                dimensions=[QueryDimension(field="floor_val")],
                variables={"floor": 100},
            )
            resp = await engine.execute(q)
            vals = {row["floored.floor_val"] for row in resp.data}
            assert vals == {100.0}
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 5. Precedence: runtime kwarg > outer/stage query.variables > dataset defaults
# ---------------------------------------------------------------------------

class TestPrecedence:
    def _dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["region = '{region}'"],
            query_variables={"region": "US"},  # lowest-priority default
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )

    async def test_model_default_used_when_nothing_supplied(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(amount)"}]})
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 160.0  # US default
        finally:
            tmp.cleanup()

    async def test_query_variables_override_model_default(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"region": "EU"},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 275.0  # EU
        finally:
            tmp.cleanup()

    async def test_runtime_kwarg_overrides_all(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"region": "EU"},
            })
            resp = await engine.execute(q, variables={"region": "CA"})
            assert _sum(resp, "orders.amount_sum") == 300.0  # runtime CA wins
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 6. Missing variable → raise (execute AND dry_run mirror each other)
# ---------------------------------------------------------------------------

class TestMissingVariable:
    def _dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["region = '{region}'"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )

    async def test_undefined_var_raises_on_execute(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            # Non-empty var set (so the guard runs) but lacking 'region'.
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"other": "x"},
            })
            with pytest.raises(ValueError, match="Undefined variable 'region'"):
                await engine.execute(q)
        finally:
            tmp.cleanup()

    async def test_undefined_var_raises_on_dry_run(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"other": "x"},
            })
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


async def _engine_with_tricky_status(dataset: SlayerDataset) -> tuple:
    tmp = tempfile.TemporaryDirectory()
    db_path = f"{tmp.name}/tricky.db"
    with transaction(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT NOT NULL, amount REAL NOT NULL)"
        )
        cur.executemany(
            "INSERT INTO t (status, amount) VALUES (?, ?)",
            _TRICKY_ROWS + [("a\\b", 30.0)],  # a\b row is for test_backslash_value_sqlite_roundtrip
        )
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_path)
    )
    await storage.save_dataset(dataset)
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
        model = SlayerDataset(
            name="t",
            sql_table="t",
            data_source="ds",
            filters=["status = '{v}'"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with_tricky_status(model)
        try:
            q = SlayerQuery.model_validate({
                "population": "t",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"v": value},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "t.amount_sum") == expected
        finally:
            tmp.cleanup()

    async def test_backslash_value_sqlite_roundtrip(self) -> None:
        """A backslash in a string value round-trips through SQLite."""
        model = SlayerDataset(
            name="t",
            sql_table="t",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with_tricky_status(model)
        try:
            q = SlayerQuery.model_validate({
                "population": "t",
                "metrics": [{"metric": "sum(amount)"}],
                "filter": ["status = '{v}'"],
                "variables": {"v": "a\\b"},
            })
            resp = await engine.execute(q)
            # Matches only the a\b row (30.0).
            assert _sum(resp, "t.amount_sum") == 30.0
        finally:
            tmp.cleanup()

    @pytest.mark.parametrize("value,expected", _TRICKY_ROWS)
    async def test_mode_b_query_filter_matches_only_its_row(
        self, value: str, expected: float
    ) -> None:
        # Mode-B: SlayerQuery.filter bind the value as a literal; quote values round-trip.
        model = SlayerDataset(
            name="t",
            sql_table="t",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with_tricky_status(model)
        try:
            q = SlayerQuery.model_validate({
                "population": "t",
                "metrics": [{"metric": "sum(amount)"}],
                "filter": ["status = '{v}'"],
                "variables": {"v": value},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "t.amount_sum") == expected
        finally:
            tmp.cleanup()


class TestEscaping:
    async def test_single_quote_value_escaped_and_matches(self) -> None:
        model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["status = '{status}'"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"status": "O'Brien"},
            })
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
        """A dataset that uses NO variables must be left untouched, so a raw
        brace literal (Postgres-array-style) survives to the DB verbatim."""
        model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="j", sql="'{1,2,3}'", datatype=DataType.STRING),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            q = SlayerQuery(population="orders", dimensions=[QueryDimension(field="j")])
            resp = await engine.execute(q)
            assert {row["orders.j"] for row in resp.data} == {"{1,2,3}"}
        finally:
            tmp.cleanup()

    async def test_placeholder_shaped_literal_in_variable_free_model_not_raised(
        self,
    ) -> None:
        """Empty-map contract: raise-on-missing applies only once a
        variable is in play. A dataset with an {identifier}-shaped placeholder in
        a string literal but NO variables anywhere (no query_variables, no
        caller vars) treats it as a literal — it is NOT substituted and does NOT
        raise 'Undefined variable'. `status = '{status}'` matches no seeded row,
        so SUM is NULL; the point is that it executes cleanly (literal), and a
        subsequent query that DOES supply a variable would raise on a missing
        one (covered by TestMissingVariable)."""
        model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["status = '{status}'"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            resp = await engine.execute(
                SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(amount)"}]})
            )
            # No raise (literal, not a missing-variable error); no row has the
            # literal status '{status}', so the aggregate is NULL.
            assert resp.data[0]["orders.amount_sum"] is None
        finally:
            tmp.cleanup()

    async def test_escaped_braces_render_single_alongside_var(self) -> None:
        """{{tag}} escapes to a literal {tag} while {floor} substitutes."""
        model = SlayerDataset(
            name="floored",
            data_source="ds",
            sql=(
                "SELECT id, amount, '{{tag}}' AS tag_val "
                "FROM orders WHERE amount >= {floor}"
            ),
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="tag_val", sql="tag_val", datatype=DataType.STRING),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            q = SlayerQuery(
                population="floored",
                dimensions=[QueryDimension(field="tag_val")],
                variables={"floor": 100},
            )
            resp = await engine.execute(q)
            assert {row["floored.tag_val"] for row in resp.data} == {"{tag}"}
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 9. Cross-dataset consistency: direct-source dataset.filters {var} substituted in
#    BOTH the main and the re-rooted CTE.
# ---------------------------------------------------------------------------

class TestCrossModelConsistency:
    def _datasets(self) -> list[SlayerDataset]:
        # No declared reverse relationship: the orders → customers edge traverses both ways.
        customers = SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="name", sql="name", datatype=DataType.STRING),
            ],
        )
        orders = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["amount >= {floor}"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
            relationships=[{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}],
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
        for m in self._datasets():
            await storage.save_dataset(m)
        return SlayerQueryEngine(storage=storage), tmp

    async def test_source_model_filter_var_applies_to_reroot(self) -> None:
        # The host's ``amount >= {floor}`` restricts the customers-rooted count to customers with a
        # passing order; {floor} is substituted wherever it is emitted, and nowhere left raw.
        engine, tmp = await self._engine()
        try:
            q = SlayerQuery(
                population="orders",
                metrics=[
                    {"metric": "sum(amount)"},
                    {"metric": "count(customers.id)"},
                ],
                variables={"floor": 100},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 300.0
            assert _sum(resp, "orders.customers.id_count") == 2
            assert "{floor}" not in resp.sql  # no un-substituted placeholder
        finally:
            tmp.cleanup()

    async def test_source_model_filter_var_applies_to_reroot_reachable(self) -> None:
        # The host's dataset filter restricts the customers-rooted count exactly as the same query filter does.
        engine, tmp = await self._engine()
        try:
            q = SlayerQuery(
                population="orders",
                metrics=[
                    {"metric": "sum(amount)"},
                    {"metric": "count(customers.id)"},
                ],
                variables={"floor": 100},
            )
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 300.0
            # The host's dataset filter restricts the customers-rooted count.
            assert _sum(resp, "orders.customers.id_count") == 2
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
        """A {var} in a RELATIONSHIP-TARGET dataset's DatasetField.sql is NOT substituted:
        the enclosing query's variable does
        not reach it, so the stray {var} surfaces an error rather than being
        silently filled. Stable boundary pin (holds before AND after)."""
        customers = SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="name", sql="name", datatype=DataType.STRING),
                # relationship-target DatasetField.sql var — not substituted → unsupported
                DatasetField(name="scaled", sql="id * {mult}", datatype=DataType.FLOAT),
            ],
        )
        orders = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
            relationships=[{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}],
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
        await storage.save_dataset(customers)
        await storage.save_dataset(orders)
        engine = SlayerQueryEngine(storage=storage)
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(customers.scaled)"}],
                "variables": {"mult": 2},
            })
            # The failure must be about the stray, unsubstituted {mult}: it is
            # emitted as a bare `mult` column ref, so the DB errors with
            # "no such column: customers.mult" (a DB error, NOT a substitution
            # ValueError — precisely because the relationship-target var was not
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

        model = SlayerDataset(
            name="floored",
            data_source="ds",
            sql="SELECT id, amount FROM orders WHERE amount >= {floor}",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            stored = await engine.storage.get_dataset("floored")
            assert stored is not None
            out = await render_dataset_inspection(
                dataset=stored,
                storage=engine.storage,
                engine=None,
                show_sql=True,
            )
            assert "{floor}" in out  # literal template, not substituted
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 11. Unit: variable binding through the engine entry (scalar values)
# ---------------------------------------------------------------------------

def _bind(*, template: str, variables: dict, dialect=None) -> str:
    """``template`` as a dataset filter, bound by the engine entry; returns the bound filter."""
    model = SlayerDataset(name="m", sql_table="t", data_source="ds", filters=[template])
    out = _substitute_dataset_sql_surfaces(
        dataset=model, variables=variables, dialect=dialect or SqliteDialect()
    )
    return out.filters[0]


def _col(dataset: SlayerDataset, name: str) -> DatasetField:
    col = dataset.get_field(name)
    assert col is not None, name
    return col


def _literal(node: exp.Expr):
    """Python value of a parsed literal node."""
    if isinstance(node, exp.Boolean):
        return node.this
    assert isinstance(node, exp.Literal), repr(node)
    if node.is_string:
        return node.this
    return int(node.this) if node.this.isdigit() else float(node.this)


def _parse(text: Optional[str], *, dialect=None) -> exp.Expr:
    assert text is not None
    return sqlglot.parse_one(text, dialect=(dialect or SqliteDialect()).sqlglot_name)


def _literals(text: Optional[str], *, dialect=None) -> list:
    """Values of every literal in ``text`` parsed in ``dialect``."""
    return [_literal(n) for n in _parse(text, dialect=dialect).find_all(exp.Literal, exp.Boolean)]


def _in_values(text: Optional[str], *, dialect=None) -> list:
    """Values of the ``IN`` list in ``text``; every element must be a literal."""
    in_node = _parse(text, dialect=dialect).find(exp.In)
    assert in_node is not None, text
    return [_literal(n) for n in in_node.expressions]


class TestSubstituteVariablesHardened:
    def test_sql_mode_string_value_with_single_quote(self) -> None:
        bound = _bind(template="status = '{v}'", variables={"v": "O'Brien"})
        assert _literals(bound) == ["O'Brien"]

    def test_sql_mode_string_value_without_quote_unchanged(self) -> None:
        bound = _bind(template="status = '{v}'", variables={"v": "active"})
        assert _literals(bound) == ["active"]

    def test_sql_mode_backslash_standard_dialect(self) -> None:
        # On a standard dialect backslash is ordinary.
        bound = _bind(template="path = '{v}'", variables={"v": r"a\b"})
        assert _literals(bound) == [r"a\b"]

    def test_sql_mode_newline(self) -> None:
        bound = _bind(template="note = '{v}'", variables={"v": "a\nb"})
        assert _literals(bound) == ["a\nb"]

    def test_number_value_bare(self) -> None:
        assert _literals(_bind(template="amount > {n}", variables={"n": 100})) == [100]

    def test_float_value_bare(self) -> None:
        assert _literals(_bind(template="rate < {n}", variables={"n": 0.05})) == [0.05]

    def test_bool_value_accepted(self) -> None:
        bound = _bind(template="flag = {v}", variables={"v": True})
        node = _parse(bound).expression
        assert isinstance(node, exp.Boolean), bound
        assert node.this is True, bound

    def test_nan_value_raises(self) -> None:
        nan = float("nan")
        with pytest.raises(ValueError, match="finite"):
            _bind(template="x = {v}", variables={"v": nan})

    def test_inf_value_raises(self) -> None:
        inf = float("inf")
        with pytest.raises(ValueError, match="finite"):
            _bind(template="x = {v}", variables={"v": inf})
        neg_inf = float("-inf")
        with pytest.raises(ValueError, match="finite"):
            _bind(template="x = {v}", variables={"v": neg_inf})

    def test_dict_value_raises(self) -> None:
        # A dict is neither scalar nor list/tuple.
        with pytest.raises(
            ValueError, match="must be a string, number, or list/tuple"
        ):
            _bind(template="x = '{v}'", variables={"v": {"a": 1}})

    def test_set_value_raises(self) -> None:
        # A set is unordered: only list/tuple are lists.
        with pytest.raises(
            ValueError, match="must be a string, number, or list/tuple"
        ):
            _bind(template="x IN ({v})", variables={"v": {1, 2}})


# ---------------------------------------------------------------------------
# 12. Unit: _substitute_model_sql_surfaces touches ONLY the four surfaces
# ---------------------------------------------------------------------------

class TestSubstituteHelperScope:
    def test_only_mode_a_surfaces_substituted(self) -> None:
        model = SlayerDataset(
            name="m",
            data_source="ds",
            sql="SELECT * FROM t WHERE r = '{region}'",
            filters=["a >= {floor}"],
            fields=[
                DatasetField(name="scaled", sql="amount * {mult}", datatype=DataType.FLOAT),
                DatasetField(
                    name="flt",
                    sql="amount",
                    filter="r = '{region}'",
                    datatype=DataType.FLOAT,
                ),
                # Hidden field is still a Mode-A surface — it IS substituted
                # (the helper iterates all fields, visible or not).
                DatasetField(
                    name="hid", sql="amount * {mult}", datatype=DataType.FLOAT, hidden=True
                ),
            ],
            # Saved metric expression: substituted under the Mode-B regime.
            metrics=[DatasetMetric(name="rev", expression="sum(amount) + {mult}")],
            # Aggregation.expression uses its OWN {expr}/{param} fill mechanism —
            # a query-variable pass must never touch it.
            aggregations=[Aggregation(name="agg", expression="SUM({expr}) * {mult}")],
        )
        out = _substitute_dataset_sql_surfaces(
            dataset=model, variables={"region": "US", "floor": 5, "mult": 2},
            dialect=SqliteDialect(),
        )
        # The four Mode-A surfaces are substituted (hidden field included):
        assert _literals(out.sql) == ["US"]
        assert len(out.filters) == 1
        assert _literals(out.filters[0]) == [5]
        assert _literals(_col(out, "scaled").sql) == [2]
        assert _literals(_col(out, "flt").filter) == ["US"]
        assert _literals(_col(out, "hid").sql) == [2]
        # Saved metric expressions substitute (Mode-B regime); Aggregation.expression never does:
        assert _literals(out.metrics[0].text) == [2]
        assert out.aggregations[0].expression == "SUM({expr}) * {mult}"
        # Input dataset is NOT mutated — every Mode-A surface stays templated:
        assert model.sql == "SELECT * FROM t WHERE r = '{region}'"
        assert model.filters == ["a >= {floor}"]
        assert model.get_field("scaled").sql == "amount * {mult}"
        assert model.get_field("flt").filter == "r = '{region}'"
        assert model.get_field("hid").sql == "amount * {mult}"

    def test_empty_variables_is_noop(self) -> None:

        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="ds",
            fields=[DatasetField(name="j", sql="json_extract(x, '$.a')", datatype=DataType.FLOAT)],
        )
        out = _substitute_dataset_sql_surfaces(
            dataset=model, variables={}, dialect=SqliteDialect()
        )
        assert _parse(_col(out, "j").sql) == _parse("json_extract(x, '$.a')")

    def test_list_value_substituted_on_all_four_mode_a_surfaces(self) -> None:
        """One list variable bound into each of the four Mode-A surfaces."""

        model = SlayerDataset(
            name="m",
            data_source="ds",
            sql="SELECT * FROM t WHERE region IN ({regions})",
            filters=["region IN ({regions})"],
            fields=[
                DatasetField(name="in_flag", sql="region IN ({regions})", datatype=DataType.BOOLEAN),
                DatasetField(
                    name="amt",
                    sql="amount",
                    filter="region IN ({regions})",
                    datatype=DataType.FLOAT,
                ),
            ],
        )
        out = _substitute_dataset_sql_surfaces(
            dataset=model, variables={"regions": ["US", "CA"]}, dialect=SqliteDialect()
        )
        assert _in_values(out.sql) == ["US", "CA"]
        assert len(out.filters) == 1
        assert _in_values(out.filters[0]) == ["US", "CA"]
        assert _in_values(_col(out, "in_flag").sql) == ["US", "CA"]
        assert _in_values(_col(out, "amt").filter) == ["US", "CA"]

# ---------------------------------------------------------------------------
# 13. Regression: query-level (Mode-B) filter substitution + unified escaping
# ---------------------------------------------------------------------------

class TestQueryFilterRegression:
    def _dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )

    async def test_query_filter_var_unchanged_for_quote_free(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "filter": ["region = '{region}'"],
                "variables": {"region": "EU"},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 275.0
        finally:
            tmp.cleanup()

    async def test_query_filter_var_now_escaped_for_quote_value(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "filter": ["status = '{status}'"],
                "variables": {"status": "O'Brien"},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 10.0
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 14. Cached-dataset immutability across repeated executes with different values
# ---------------------------------------------------------------------------

class TestModelImmutabilityAcrossExecutes:
    async def test_same_model_two_values_distinct_results(self) -> None:
        """Executing the same stored dataset twice with different variable values
        yields different results (proving per-call substitution) AND the stored
        dataset still holds the literal {var} template afterwards (no mutation of
        the shared cached object)."""
        model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["region = '{region}'"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            r_eu = await engine.execute(
                SlayerQuery.model_validate({
                    "population": "orders",
                    "metrics": [{"metric": "sum(amount)"}],
                    "variables": {"region": "EU"},
                })
            )
            r_ca = await engine.execute(
                SlayerQuery.model_validate({
                    "population": "orders",
                    "metrics": [{"metric": "sum(amount)"}],
                    "variables": {"region": "CA"},
                })
            )
            assert _sum(r_eu, "orders.amount_sum") == 275.0
            assert _sum(r_ca, "orders.amount_sum") == 300.0
            # A failed substitution must not partially mutate either — pin that
            # a subsequent missing-var call raises cleanly. Build the query
            # outside the block so only execute() can throw inside it.
            missing_var_q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"other": "x"},
            })
            with pytest.raises(ValueError, match="Undefined variable 'region'"):
                await engine.execute(missing_var_q)
            # The stored dataset is untouched — reloading shows the template.
            reloaded = await engine.storage.get_dataset("orders")
            assert reloaded is not None
            assert reloaded.filters == ["region = '{region}'"]
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 15. Missing-variable raises on each Mode-A surface (not just dataset.filters)
# ---------------------------------------------------------------------------

class TestMissingVariablePerSurface:
    async def test_missing_var_in_column_sql_raises(self) -> None:
        model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="scaled", sql="amount * {mult}", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            # Non-empty var set (guard runs) but lacking 'mult'.
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(scaled)"}],
                "variables": {"other": "x"},
            })
            with pytest.raises(ValueError, match="Undefined variable 'mult'"):
                await engine.execute(q)
        finally:
            tmp.cleanup()

    async def test_missing_var_in_model_sql_raises(self) -> None:
        model = SlayerDataset(
            name="floored",
            data_source="ds",
            sql="SELECT id, amount FROM orders WHERE amount >= {floor}",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            q = SlayerQuery.model_validate({
                "population": "floored",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"other": "x"},
            })
            with pytest.raises(ValueError, match="Undefined variable 'floor'"):
                await engine.execute(q)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 16. get_result_column_types: defaults-only Mode-A substitution (D3)
# ---------------------------------------------------------------------------

class TestGetResultColumnTypesDefaults:
    async def test_complete_defaults_probe_succeeds(self) -> None:
        """A Mode-A {var} field with a complete dataset default resolves at the
        type-probe path (get_result_column_types), returning real types instead of the
        {} degradation."""
        model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            query_variables={"mult": 2},
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="scaled", sql="amount * {mult}", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            types = await engine.get_result_column_types("orders")
            assert types["scaled"].type in (DataType.INTEGER, DataType.FLOAT)
            assert types["amount"].type in (DataType.INTEGER, DataType.FLOAT)
        finally:
            tmp.cleanup()

    async def test_undefaulted_var_degrades_to_empty(self) -> None:
        """No default for the {var} → probe cannot render → graceful {} (the
        pre-existing degradation is preserved, not a raise)."""
        model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="scaled", sql="amount * {mult}", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            types = await engine.get_result_column_types("orders")
            assert types == {}
        finally:
            tmp.cleanup()

    async def test_partial_defaults_degrades_to_empty(self) -> None:
        """Two placeholders, only one defaulted → substitution activates and
        raises on the missing one, but that must stay inside the probe's
        graceful-failure boundary so the public result is still {} (F1)."""
        model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            query_variables={"mult": 2},  # 'floor' intentionally missing
            filters=["amount >= {floor}"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="scaled", sql="amount * {mult}", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            types = await engine.get_result_column_types("orders")
            assert types == {}
        finally:
            tmp.cleanup()

    async def test_list_default_probe_succeeds(self) -> None:
        """A Mode-A ``IN ({var})`` filter with a LIST default in
        ``query_variables`` renders cleanly at the type-probe path, so the probe
        returns real column types instead of the ``{}`` degradation."""
        model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            query_variables={"regions": ["US", "CA"]},
            filters=["region IN ({regions})"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        engine, tmp = await _engine_with(model)
        try:
            types = await engine.get_result_column_types("orders")
            assert types["amount"].type in (DataType.INTEGER, DataType.FLOAT)
            assert types["region"].type == DataType.STRING
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 17. Unit: list/tuple variable rendering (IN-list pushdown)
# ---------------------------------------------------------------------------


class TestListValueRenderingSql:
    """A list binds as the whole ``IN`` list; template shape is ``IN ({var})``."""

    def test_sql_list_strings(self) -> None:
        bound = _bind(template="region IN ({v})", variables={"v": ["US", "CA"]})
        assert _in_values(bound) == ["US", "CA"]

    def test_sql_list_embedded_quote(self) -> None:
        bound = _bind(template="name IN ({v})", variables={"v": ["A", "O'Brien", 3]})
        assert _in_values(bound) == ["A", "O'Brien", 3]

    def test_sql_list_numbers_and_bools_bare(self) -> None:
        bound = _bind(template="x IN ({v})", variables={"v": [1, 2.5, True, False]})
        assert _in_values(bound) == [1, 2.5, True, False]
        in_node = _parse(bound).find(exp.In)
        assert in_node is not None
        assert [isinstance(n, exp.Boolean) for n in in_node.expressions] == [
            False, False, True, True,
        ]

    def test_sql_single_element_list(self) -> None:
        bound = _bind(template="region IN ({v})", variables={"v": ["EU"]})
        assert _in_values(bound) == ["EU"]

    def test_sql_tuple_accepted_same_as_list(self) -> None:
        from_list = _bind(template="x IN ({v})", variables={"v": ["A", "B"]})
        from_tuple = _bind(template="x IN ({v})", variables={"v": ("A", "B")})
        assert _in_values(from_list) == _in_values(from_tuple) == ["A", "B"]

    def test_sql_injection_element_stays_inside_literal(self) -> None:
        # A classic breakout attempt stays one string literal inside the IN list.
        payload = "x') OR ('1'='1"
        bound = _bind(template="region IN ({v})", variables={"v": [payload]})
        assert _in_values(bound) == [payload]
        assert _parse(bound).find(exp.Or) is None


class TestListValueRenderingErrors:
    def test_empty_list_raises_naming_variable(self) -> None:

        # IN () is invalid SQL; the message names the variable, says "empty", and
        # points at the sentinel-default idiom for "no filter".
        with pytest.raises(ValueError, match="regions") as exc:
            _bind(template="region IN ({regions})", variables={"regions": []})
        msg = str(exc.value).lower()
        assert "empty" in msg
        # The sentinel-default hint is a required contract, pinned by
        # a stable single-word fragment rather than the full sentence.
        assert "sentinel" in msg

    def test_empty_tuple_raises(self) -> None:

        with pytest.raises(ValueError, match="regions") as exc:
            _bind(template="region IN ({regions})", variables={"regions": ()})
        assert "empty" in str(exc.value).lower()

    def test_nested_list_element_raises(self) -> None:

        # ``element`` pins the per-element validation path; the variable must be named.
        with pytest.raises(ValueError, match="element") as exc:
            _bind(
                template="region IN ({regions})",
                variables={"regions": ["A", ["B", "C"]]},
            )
        assert "regions" in str(exc.value)

    def test_none_element_raises(self) -> None:

        with pytest.raises(ValueError, match="element") as exc:
            _bind(template="region IN ({regions})", variables={"regions": ["A", None]})
        assert "regions" in str(exc.value)

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_non_finite_float_element_raises(self, bad: float) -> None:

        with pytest.raises(ValueError, match="finite") as exc:
            _bind(template="x IN ({v})", variables={"v": [1.0, bad]})
        assert "v" in str(exc.value)

    def test_dict_element_raises(self) -> None:

        with pytest.raises(ValueError, match="element") as exc:
            _bind(
                template="region IN ({regions})",
                variables={"regions": ["A", {"b": 1}]},
            )
        assert "regions" in str(exc.value)


# ---------------------------------------------------------------------------
# 18. End-to-end Mode-A: WHERE region IN ({regions}) with a list variable
# ---------------------------------------------------------------------------


class TestListModeAEndToEnd:
    def _dataset(self, **kw) -> SlayerDataset:
        return SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["region IN ({regions})"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
            **kw,
        )

    async def test_runtime_list_filters_rows(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"regions": ["US", "CA"]},
            })
            resp = await engine.execute(q)
            # US=160, CA=300 → 460 (EU excluded).
            assert _sum(resp, "orders.amount_sum") == 460.0
        finally:
            tmp.cleanup()

    async def test_single_element_list(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"regions": ["EU"]},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 275.0
        finally:
            tmp.cleanup()

    async def test_list_default_used_when_kwarg_absent(self) -> None:
        engine, tmp = await _engine_with(
            self._dataset(query_variables={"regions": ["EU", "CA"]})
        )
        try:
            q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(amount)"}]})
            resp = await engine.execute(q)
            # EU=275, CA=300 → 575.
            assert _sum(resp, "orders.amount_sum") == 575.0
        finally:
            tmp.cleanup()

    async def test_query_variables_list_overrides_model_default_list(self) -> None:
        # Middle precedence layer: model_defaults < query.variables (no runtime).
        engine, tmp = await _engine_with(
            self._dataset(query_variables={"regions": ["EU", "CA"]})
        )
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"regions": ["US"]},
            })
            resp = await engine.execute(q)
            # Query-level ['US'] wins over dataset default ['EU','CA'] → 160.
            assert _sum(resp, "orders.amount_sum") == 160.0
        finally:
            tmp.cleanup()

    async def test_runtime_list_overrides_default_list(self) -> None:
        engine, tmp = await _engine_with(
            self._dataset(query_variables={"regions": ["EU", "CA"]})
        )
        try:
            q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(amount)"}]})
            resp = await engine.execute(q, variables={"regions": ["US"]})
            # Runtime ['US'] wins over default ['EU','CA'] → 160.
            assert _sum(resp, "orders.amount_sum") == 160.0
        finally:
            tmp.cleanup()

    async def test_injection_element_matches_no_rows(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"regions": ["x') OR ('1'='1"]},
            })
            resp = await engine.execute(q)
            # The payload stays inside its literal, matching no region, so the
            # aggregate is NULL — NOT the whole-table total (735) an escape
            # would have produced.
            assert resp.data[0]["orders.amount_sum"] is None
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# 19. End-to-end Mode-B: query filter "region in ({regions})" with a list
# ---------------------------------------------------------------------------


class TestListModeBEndToEnd:
    def _dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )

    async def test_query_filter_list_in(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "filter": ["region in ({regions})"],
                "variables": {"regions": ["US", "CA"]},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 460.0  # US 160 + CA 300
        finally:
            tmp.cleanup()

    async def test_query_filter_single_element_list(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            # The trailing-comma render (('EU',)) is what keeps a 1-element list
            # a tuple through the Python-AST parser rather than a bare string.
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "filter": ["region in ({regions})"],
                "variables": {"regions": ["EU"]},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 275.0
        finally:
            tmp.cleanup()

    async def test_query_filter_not_in_list(self) -> None:
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "filter": ["region not in ({regions})"],
                "variables": {"regions": ["US"]},
            })
            resp = await engine.execute(q)
            # Everything except US(160) → EU 275 + CA 300 = 575.
            assert _sum(resp, "orders.amount_sum") == 575.0
        finally:
            tmp.cleanup()

    async def test_runtime_list_overrides_query_level_list(self) -> None:
        # Mode-B filters read query.variables, which merges runtime kwarg over
        # the query-level dict (runtime wins) — verify that merge with lists.
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "filter": ["region in ({regions})"],
                "variables": {"regions": ["EU", "CA"]},
            })
            resp = await engine.execute(q, variables={"regions": ["US"]})
            assert _sum(resp, "orders.amount_sum") == 160.0  # runtime ['US'] wins
        finally:
            tmp.cleanup()

    async def test_query_filter_list_escaping_matches_only_its_row(self) -> None:
        # Mode-B escaping through the real parse+compile+SQLite pipeline: a
        # quote-bearing element must round-trip so it matches only its own row.
        engine, tmp = await _engine_with(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "filter": ["status in ({statuses})"],
                "variables": {"statuses": ["O'Brien"]},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "orders.amount_sum") == 10.0  # row 6 only
        finally:
            tmp.cleanup()


# ===========================================================================
# Dialect-aware escaping for Mode-A {var} substitution
# ===========================================================================


# ---------------------------------------------------------------------------
# 20. Values round-trip on standard and backslash-escaping dialects
# ---------------------------------------------------------------------------

class TestSqlModeBackslashEscaping:
    """The bound literal's value is the original on SQLite (standard) and MySQL
    (backslash escapes)."""

    def test_standard_regime_quote_and_backslash(self) -> None:
        bound = _bind(template="p = '{v}'", variables={"v": "a\\'b"})
        assert _literals(bound) == ["a\\'b"]

    def test_backslash_regime_quote_and_backslash(self) -> None:
        d = MysqlDialect()
        bound = _bind(template="p = '{v}'", variables={"v": "a\\'b"}, dialect=d)
        assert _literals(bound, dialect=d) == ["a\\'b"]

    def test_backslash_regime_lone_backslash(self) -> None:
        d = MysqlDialect()
        bound = _bind(template="p = '{v}'", variables={"v": "a\\b"}, dialect=d)
        assert _literals(bound, dialect=d) == ["a\\b"]

    def test_backslash_regime_trailing_backslash(self) -> None:
        # A trailing backslash must not eat the closing quote.
        d = MysqlDialect()
        bound = _bind(template="p = '{v}'", variables={"v": "abc\\"}, dialect=d)
        assert _literals(bound, dialect=d) == ["abc\\"]

    def test_backslash_regime_single_quote_only(self) -> None:
        d = MysqlDialect()
        bound = _bind(template="p = '{v}'", variables={"v": "O'Brien"}, dialect=d)
        assert _literals(bound, dialect=d) == ["O'Brien"]

    def test_backslash_regime_double_quote(self) -> None:
        d = MysqlDialect()
        bound = _bind(template="p = '{v}'", variables={"v": 'say "hi"'}, dialect=d)
        assert _literals(bound, dialect=d) == ['say "hi"']

    def test_backslash_regime_plain_value_unchanged(self) -> None:
        d = MysqlDialect()
        bound = _bind(template="p = '{v}'", variables={"v": "active"}, dialect=d)
        assert _literals(bound, dialect=d) == ["active"]

    def test_number_passthrough_both_regimes(self) -> None:
        for d in (MysqlDialect(), SqliteDialect()):
            bound = _bind(template="x > {n}", variables={"n": 100}, dialect=d)
            assert _literals(bound, dialect=d) == [100]

    def test_backslash_regime_list_elements(self) -> None:
        d = MysqlDialect()
        bound = _bind(template="p IN ({v})", variables={"v": ["a\\b", "O'Brien"]}, dialect=d)
        assert _in_values(bound, dialect=d) == ["a\\b", "O'Brien"]

    def test_standard_regime_list_elements(self) -> None:
        bound = _bind(template="p IN ({v})", variables={"v": ["a\\b", "O'Brien"]})
        assert _in_values(bound) == ["a\\b", "O'Brien"]


# ---------------------------------------------------------------------------
# 21. Control chars round-trip
# ---------------------------------------------------------------------------

class TestSqlModeControlChars:
    def test_sql_mode_control_chars_roundtrip(self) -> None:
        bound = _bind(template="p = '{v}'", variables={"v": "a\nb"})
        assert _literals(bound) == ["a\nb"]


# ---------------------------------------------------------------------------
# 22. Dialect matrix: bound values round-trip through the parser sqlglot uses
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

_MATRIX = [
    (d, v) for d in _ALL_DIALECTS for v in _MATRIX_VALUES
]


class TestEscapingRegimeDialectMatrix:
    """For every dialect, the literal bound by the engine entry parses back to the
    original value in that dialect, and survives a parse → emit → re-parse cycle."""

    @pytest.mark.parametrize(
        "dialect,value",
        _MATRIX,
        ids=[f"{d.sqlglot_name}-{v!r}" for d, v in _MATRIX],
    )
    def test_sql_literal_roundtrips_through_dialect_parser(
        self, dialect, value: str
    ) -> None:
        bound = _bind(template="p = '{v}'", variables={"v": value}, dialect=dialect)
        assert _literals(bound, dialect=dialect) == [value], (dialect.sqlglot_name, repr(bound))
        # parse → emit → re-parse must preserve the value too.
        emitted = _parse(bound, dialect=dialect).sql(dialect=dialect.sqlglot_name)
        assert _literals(emitted, dialect=dialect) == [value], (dialect.sqlglot_name, "roundtrip")

    def test_matrix_covers_all_14_dialects(self) -> None:
        assert len({d.sqlglot_name for d, _ in _MATRIX}) == 14


# ---------------------------------------------------------------------------
# 23. Engine threading: the resolved dialect reaches the binding
# ---------------------------------------------------------------------------

class TestSubstituteModelSqlSurfacesDialect:
    """``_substitute_model_sql_surfaces`` requires a dialect and binds for it."""

    def _dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="m",
            data_source="ds",
            sql="SELECT * FROM t WHERE r = '{region}'",
            fields=[DatasetField(name="a", sql="amount", datatype=DataType.FLOAT)],
        )

    def test_backslash_dialect_value_roundtrips(self) -> None:

        d = MysqlDialect()
        out = _substitute_dataset_sql_surfaces(
            dataset=self._dataset(), variables={"region": "a\\'b"}, dialect=d,
        )
        assert _literals(out.sql, dialect=d) == ["a\\'b"]

    def test_standard_dialect_value_roundtrips(self) -> None:

        out = _substitute_dataset_sql_surfaces(
            dataset=self._dataset(), variables={"region": "a\\'b"},
            dialect=SqliteDialect(),
        )
        assert _literals(out.sql) == ["a\\'b"]

    def test_dialect_is_required(self) -> None:

        # Build the dataset outside the raises-block so only the call under test
        # (missing the required `dialect`) can raise (Sonar S5778).
        model = self._dataset()
        with pytest.raises(TypeError):
            _substitute_dataset_sql_surfaces(dataset=model, variables={"region": "x"})


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
        """Build a one-dataset engine on a datasource of ``ds_type`` and return
        the generated SQL for a Mode-A dataset filter carrying ``value``. Uses a
        SQLite file as the actual backend but pins the datasource *type* so the
        dialect (hence escaping regime) is exercised; dry_run skips execution."""
        tmp = tempfile.TemporaryDirectory()
        db_path = f"{tmp.name}/x.db"
        _seed_orders_db_at(db_path)
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type=ds_type, database=db_path)
        )
        model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            filters=["status = '{v}'"],
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        await storage.save_dataset(model)
        engine = SlayerQueryEngine(storage=storage)
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
                "variables": {"v": value},
            })
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
    renders from the dataset's own defaults."""

    def _template_dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="m",
            data_source="ds",
            sql="SELECT * FROM t WHERE r = '{region}'",
            query_variables={"region": "a\\'b"},
            fields=[DatasetField(name="a", sql="amount", datatype=DataType.FLOAT)],
        )

    def test_probe_uses_backslash_dialect(self) -> None:

        d = MysqlDialect()
        out = _render_probe_dataset(self._template_dataset(), dialect=d)
        assert _literals(out.sql, dialect=d) == ["a\\'b"]

    def test_probe_uses_standard_dialect(self) -> None:

        out = _render_probe_dataset(self._template_dataset(), dialect=SqliteDialect())
        assert _literals(out.sql) == ["a\\'b"]

    def test_probe_dialect_required(self) -> None:

        # Build the dataset outside the raises-block so only the call under test
        # (missing the required `dialect`) can raise (Sonar S5778).
        model = self._template_dataset()
        with pytest.raises(TypeError):
            _render_probe_dataset(model)


# ---------------------------------------------------------------------------
# 24. Mode-B end-to-end: control-char values round-trip through the parser
# ---------------------------------------------------------------------------

async def _engine_with_ctrl_status(dataset: SlayerDataset) -> tuple:
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
    await storage.save_dataset(dataset)
    return SlayerQueryEngine(storage=storage), tmp


class TestModeBControlCharsEndToEnd:
    """Legitimate control-char values (Gap 2): a newline/tab/CR-bearing value in
    a Mode-B query filter must parse (no ast.parse SyntaxError) and match ONLY
    its row through the real SQLite pipeline."""

    def _dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="t",
            sql_table="t",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )

    @pytest.mark.parametrize(
        "value,expected",
        [("line1\nline2", 11.0), ("tab\there", 22.0), ("cr\rhere", 33.0)],
    )
    async def test_control_char_value_matches_only_its_row(
        self, value: str, expected: float
    ) -> None:
        engine, tmp = await _engine_with_ctrl_status(self._dataset())
        try:
            q = SlayerQuery.model_validate({
                "population": "t",
                "metrics": [{"metric": "sum(amount)"}],
                "filter": ["status = '{v}'"],
                "variables": {"v": value},
            })
            resp = await engine.execute(q)
            assert _sum(resp, "t.amount_sum") == expected
        finally:
            tmp.cleanup()
