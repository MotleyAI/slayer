"""Opt-in data-profiling cardinality detection (DEV-1688).

``engine.detect_join_cardinality`` full-scans each join's two sides (non-null
key rows vs distinct key-tuples) and classifies the cardinality from observed
uniqueness. It EVIDENCES and RECOMMENDS: ``persist=False`` (default) never
writes — the report is the deliverable so a human/agent can decide. Data can
DISPROVE a stored value with certainty (a duplicate is a counterexample); a
hard disproof surfaces as ``CONTRADICTS_HARD``.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import ForcedFilterError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.engine.cardinality import CardinalityVerdict, JoinCardinalityReport
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _seed_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT);
        CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER);
        CREATE TABLE user_profiles (customer_id INTEGER PRIMARY KEY, bio TEXT);
        CREATE TABLE carts (id INTEGER PRIMARY KEY);
        CREATE TABLE cart_lines (id INTEGER PRIMARY KEY, cart_id INTEGER);
        CREATE TABLE left_tbl (k INTEGER);
        CREATE TABLE right_tbl (k INTEGER, label TEXT);
        CREATE TABLE ck_parent (a INTEGER, b TEXT, PRIMARY KEY (a, b));
        CREATE TABLE ck_child (a INTEGER, b TEXT);
        CREATE TABLE empty_src (k INTEGER);
        CREATE TABLE empty_tgt (k INTEGER PRIMARY KEY);
        CREATE TABLE all_null_src (k INTEGER);
        CREATE TABLE populated_src (k INTEGER);

        INSERT INTO customers VALUES (1,'US'),(2,'EU'),(3,'AP');
        -- customer_id has duplicates (1,1) and a NULL -> NOT unique.
        INSERT INTO orders VALUES (1,1),(2,1),(3,2),(4,NULL);
        -- customer_id unique -> one row per customer.
        INSERT INTO user_profiles VALUES (1,'a'),(2,'b'),(3,'c');
        INSERT INTO carts VALUES (1),(2);
        -- cart_id has duplicates -> NOT unique.
        INSERT INTO cart_lines VALUES (1,1),(2,1),(3,2);
        INSERT INTO left_tbl VALUES (1),(1),(2);
        INSERT INTO right_tbl VALUES (1,'x'),(1,'y'),(3,'z');
        -- composite parent key is unique; child (a,b) has a dup + a NULL-key row.
        INSERT INTO ck_parent VALUES (1,'x'),(1,'y'),(2,'x');
        INSERT INTO ck_child VALUES (1,'x'),(1,'x'),(2,'x'),(1,NULL);
        -- empty_src / empty_tgt stay empty on purpose.
        -- all_null_src has rows, but every key is NULL -> empty population.
        INSERT INTO all_null_src VALUES (NULL),(NULL);
        -- populated source pointing at an EMPTY target.
        INSERT INTO populated_src VALUES (1),(2),(3);
        """
    )
    conn.commit()
    conn.close()


def _col(name: str, *, pk: bool = False, dtype: DataType = DataType.INT) -> Column:
    return Column(name=name, sql=name, type=dtype, primary_key=pk)


def _models() -> list[SlayerModel]:
    def m(name, table, cols, joins=None):
        return SlayerModel(
            name=name, sql_table=table, data_source="ds", columns=cols, joins=joins or []
        )

    return [
        m("customers", "customers", [_col("id", pk=True), _col("region", dtype=DataType.TEXT)]),
        m(
            "orders",
            "orders",
            [_col("id", pk=True), _col("customer_id")],
            [ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ),
        m(
            "user_profiles",
            "user_profiles",
            [_col("customer_id", pk=True), _col("bio", dtype=DataType.TEXT)],
            [ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ),
        m("cart_lines", "cart_lines", [_col("id", pk=True), _col("cart_id")]),
        m(
            "carts",
            "carts",
            [_col("id", pk=True)],
            [ModelJoin(target_model="cart_lines", join_pairs=[["id", "cart_id"]])],
        ),
        m("right_m", "right_tbl", [_col("k"), _col("label", dtype=DataType.TEXT)]),
        m(
            "left_m",
            "left_tbl",
            [_col("k")],
            [ModelJoin(target_model="right_m", join_pairs=[["k", "k"]])],
        ),
        m("empty_tgt", "empty_tgt", [_col("k", pk=True)]),
        m(
            "empty_src",
            "empty_src",
            [_col("k")],
            [ModelJoin(target_model="empty_tgt", join_pairs=[["k", "k"]])],
        ),
        m(
            "populated_src",
            "populated_src",
            [_col("k")],
            [ModelJoin(target_model="empty_tgt", join_pairs=[["k", "k"]])],
        ),
        m(
            "all_null_src",
            "all_null_src",
            [_col("k")],
            [ModelJoin(target_model="customers", join_pairs=[["k", "id"]])],
        ),
        m("ck_parent", "ck_parent", [_col("a"), _col("b", dtype=DataType.TEXT)]),
        m(
            "ck_child",
            "ck_child",
            [_col("a"), _col("b", dtype=DataType.TEXT)],
            [
                ModelJoin(
                    target_model="ck_parent",
                    join_pairs=[["a", "a"], ["b", "b"]],
                )
            ],
        ),
    ]


async def _build_engine(workspace: Path) -> tuple[SlayerQueryEngine, YAMLStorage, DatasourceConfig]:
    db = str(workspace / "d.db")
    _seed_db(db)
    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    ds = DatasourceConfig(name="ds", type="sqlite", database=db)
    await storage.save_datasource(ds)
    for model in _models():
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage), storage, ds


def _find(report: JoinCardinalityReport, model: str, target: str):
    return next(
        f for f in report.findings if f.model == model and f.target_model == target
    )


# ---------------------------------------------------------------------------
# Classification from data
# ---------------------------------------------------------------------------


class TestClassification:
    async def test_many_to_one(self, workspace: Path) -> None:
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "orders", "customers")
        assert f.detected is JoinCardinality.MANY_TO_ONE
        assert f.source_side.observed_unique is False
        assert f.target_side.observed_unique is True

    async def test_one_to_one(self, workspace: Path) -> None:
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "user_profiles", "customers")
        assert f.detected is JoinCardinality.ONE_TO_ONE

    async def test_one_to_many(self, workspace: Path) -> None:
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "carts", "cart_lines")
        assert f.detected is JoinCardinality.ONE_TO_MANY

    async def test_many_to_many(self, workspace: Path) -> None:
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "left_m", "right_m")
        assert f.detected is JoinCardinality.MANY_TO_MANY

    async def test_null_keys_excluded_from_population(self, workspace: Path) -> None:
        # orders.customer_id has a NULL row; non-null population is 3 rows,
        # 2 distinct -> not unique -> many_to_one still holds.
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "orders", "customers")
        assert f.source_side.row_count == 3
        assert f.source_side.distinct_count == 2

    async def test_composite_key_many_to_one(self, workspace: Path) -> None:
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "ck_child", "ck_parent")
        assert f.detected is JoinCardinality.MANY_TO_ONE

    async def test_composite_key_null_row_excluded(self, workspace: Path) -> None:
        # ck_child has 4 rows but one has a NULL in the (a,b) key -> the
        # non-null population is 3 tuples, 2 distinct.
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "ck_child", "ck_parent")
        assert f.source_side.row_count == 3
        assert f.source_side.distinct_count == 2


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------


class TestVerdicts:
    async def test_fills_none_when_stored_absent(self, workspace: Path) -> None:
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "orders", "customers")
        assert f.stored is None
        assert f.verdict is CardinalityVerdict.FILLS_NONE

    async def test_confirms_matching_stored(self, workspace: Path) -> None:
        engine, storage, _ = await _build_engine(workspace)
        orders = await storage.get_model("orders", data_source="ds")
        orders.joins[0] = orders.joins[0].model_copy(
            update={"cardinality": JoinCardinality.MANY_TO_ONE}
        )
        await storage.save_model(orders)

        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "orders", "customers")
        assert f.verdict is CardinalityVerdict.CONFIRMS

    async def test_contradicts_hard_when_data_disproves(self, workspace: Path) -> None:
        engine, storage, _ = await _build_engine(workspace)
        # Store a wrong one_to_one: it claims the source side is unique, but
        # orders.customer_id has duplicates -> data disproves it.
        orders = await storage.get_model("orders", data_source="ds")
        orders.joins[0] = orders.joins[0].model_copy(
            update={"cardinality": JoinCardinality.ONE_TO_ONE}
        )
        await storage.save_model(orders)

        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "orders", "customers")
        assert f.detected is JoinCardinality.MANY_TO_ONE
        assert f.verdict is CardinalityVerdict.CONTRADICTS_HARD

    async def test_refines_when_change_is_not_a_hard_disproof(
        self, workspace: Path
    ) -> None:
        engine, storage, _ = await _build_engine(workspace)
        # Stored many_to_many claims the target side is non-unique. The data
        # shows the target IS unique, but "no duplicates observed" does NOT
        # disprove a non-uniqueness claim -> a soft REFINES, not a hard
        # contradiction.
        orders = await storage.get_model("orders", data_source="ds")
        orders.joins[0] = orders.joins[0].model_copy(
            update={"cardinality": JoinCardinality.MANY_TO_MANY}
        )
        await storage.save_model(orders)

        report = await engine.detect_join_cardinality(data_source="ds", model="orders")
        f = _find(report, "orders", "customers")
        assert f.detected is JoinCardinality.MANY_TO_ONE
        assert f.verdict is CardinalityVerdict.REFINES

    async def test_skipped_unsupported_for_expression_join_key(
        self, workspace: Path
    ) -> None:
        engine, storage, _ = await _build_engine(workspace)
        # A join key backed by a non-bare SQL expression is out of scope for
        # v1 profiling (can't DISTINCT a physical column).
        expr = SlayerModel(
            name="orders_expr",
            sql_table="orders",
            data_source="ds",
            columns=[
                _col("id", pk=True),
                Column(name="ck", sql="customer_id + 0", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["ck", "id"]])],
        )
        await storage.save_model(expr)

        report = await engine.detect_join_cardinality(
            data_source="ds", model="orders_expr"
        )
        f = _find(report, "orders_expr", "customers")
        assert f.verdict is CardinalityVerdict.SKIPPED_UNSUPPORTED
        assert f.detected is None
        assert f.note

    async def test_skipped_unsupported_for_sql_mode_model(
        self, workspace: Path
    ) -> None:
        engine, storage, _ = await _build_engine(workspace)
        # A sql-mode source model with a join is out of scope for v1 profiling.
        raw = SlayerModel(
            name="raw_orders",
            sql="SELECT id, customer_id FROM orders",
            data_source="ds",
            columns=[_col("id", pk=True), _col("customer_id")],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        await storage.save_model(raw)

        report = await engine.detect_join_cardinality(data_source="ds", model="raw_orders")
        f = _find(report, "raw_orders", "customers")
        assert f.verdict is CardinalityVerdict.SKIPPED_UNSUPPORTED
        assert f.detected is None
        assert f.note  # explains why it was skipped


# ---------------------------------------------------------------------------
# Declared-unique contradictions (reported, never mutated)
# ---------------------------------------------------------------------------


class TestUniqueContradictions:
    async def test_reports_declared_unique_with_dups(self, workspace: Path) -> None:
        engine, storage, _ = await _build_engine(workspace)
        # Declare orders.customer_id unique (wrong — it has duplicates).
        orders = await storage.get_model("orders", data_source="ds")
        cc = next(c for c in orders.columns if c.name == "customer_id")
        idx = orders.columns.index(cc)
        orders.columns[idx] = cc.model_copy(update={"unique": True})
        await storage.save_model(orders)

        report = await engine.detect_join_cardinality(data_source="ds", model="orders")
        f = _find(report, "orders", "customers")
        assert any("customer_id" in c for c in f.unique_contradictions)

        # The declared flag is NOT mutated by detection.
        reloaded = await storage.get_model("orders", data_source="ds")
        assert next(c for c in reloaded.columns if c.name == "customer_id").unique is True

    async def test_sole_pk_column_with_dups_is_reported(self, workspace: Path) -> None:
        """A column that IS the whole primary key does claim solo uniqueness."""
        engine, storage, _ = await _build_engine(workspace)
        # cart_lines.cart_id has dups; declare it the sole PK of a probe model.
        probe = SlayerModel(
            name="solo_pk_lines",
            sql_table="cart_lines",
            data_source="ds",
            columns=[_col("cart_id", pk=True), _col("id")],
            joins=[ModelJoin(target_model="carts", join_pairs=[["cart_id", "id"]])],
        )
        await storage.save_model(probe)

        report = await engine.detect_join_cardinality(
            data_source="ds", model="solo_pk_lines"
        )
        f = _find(report, "solo_pk_lines", "carts")
        assert any("cart_id" in c for c in f.unique_contradictions)

    async def test_composite_pk_member_with_dups_is_not_a_contradiction(
        self, workspace: Path
    ) -> None:
        """A member of a COMPOSITE primary key claims nothing on its own.

        ``primary_key`` is stamped on every member of a composite PK, but
        ``(a, b)`` being unique says nothing about ``a`` alone — the same
        subset rule ``is_key_set_unique`` applies. Reporting it would misfire
        on every composite-PK table (regression: jaffle_shop ``supplies``,
        PK ``(id, sku)``, joined on ``sku`` alone).
        """
        engine, storage, _ = await _build_engine(workspace)
        # ck_child.a has duplicates; both a and b are stamped PK (composite).
        probe = SlayerModel(
            name="ck_child_solo",
            sql_table="ck_child",
            data_source="ds",
            columns=[_col("a", pk=True), _col("b", pk=True, dtype=DataType.TEXT)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["a", "id"]])],
        )
        await storage.save_model(probe)

        report = await engine.detect_join_cardinality(
            data_source="ds", model="ck_child_solo"
        )
        f = _find(report, "ck_child_solo", "customers")
        # 'a' has dups, but it is only half of the composite key — no claim broken.
        assert not [c for c in f.unique_contradictions if "ck_child_solo.a" in c]


# ---------------------------------------------------------------------------
# Persistence (report-only default; opt-in write)
# ---------------------------------------------------------------------------


class TestPersistence:
    async def test_persist_false_does_not_write(self, workspace: Path) -> None:
        engine, storage, _ = await _build_engine(workspace)
        await engine.detect_join_cardinality(data_source="ds", persist=False)
        orders = await storage.get_model("orders", data_source="ds")
        assert orders.joins[0].cardinality is None

    async def test_persist_true_writes_detected_per_join(
        self, workspace: Path
    ) -> None:
        engine, storage, _ = await _build_engine(workspace)
        await engine.detect_join_cardinality(data_source="ds", persist=True)

        orders = await storage.get_model("orders", data_source="ds")
        assert orders.joins[0].cardinality is JoinCardinality.MANY_TO_ONE
        # Correct per-(model, join) identity — a different join gets its own value.
        profiles = await storage.get_model("user_profiles", data_source="ds")
        assert profiles.joins[0].cardinality is JoinCardinality.ONE_TO_ONE

    async def test_model_filter_scopes_scan(self, workspace: Path) -> None:
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds", model="orders")
        assert {f.model for f in report.findings} == {"orders"}


# ---------------------------------------------------------------------------
# Row-level security: profiling must observe the tenant-scoped rows only
# ---------------------------------------------------------------------------


class TestSessionPolicyAppliedToProfiling:
    """DEV-1688 x DEV-1578.

    A configured SessionPolicy must scope the profiling scans too. Otherwise
    detection full-scans every tenant's rows -- leaking cross-tenant
    cardinality and reporting an arity that does not describe the caller's own
    view. Same reasoning as the refresh-key scan.
    """

    async def test_profiling_sql_is_tenant_scoped(self, workspace: Path) -> None:
        """Assert at the EXECUTION boundary, not at ``_apply_policy``'s return.

        Spying on ``_apply_policy`` only proves the rewrite was computed. If
        the scan then discarded it and submitted the original SQL, that spy
        would still pass while the report carried cross-tenant statistics. So
        capture what actually reaches the SQL client.
        """
        _, storage, _ = await _build_engine(workspace)
        # `region` exists on customers but not orders; "pass" lets the tables
        # that lack it through so we can observe the rewrite on the one that
        # has it.
        policy = SessionPolicy(
            ruleset=ColumnFilterRuleset(
                column="region", value="US", on_unapplicable="pass"
            )
        )
        scoped = SlayerQueryEngine(storage=storage, policy=policy)

        executed: list[str] = []
        real_client_cls = SlayerSQLClient

        class _RecordingClient(real_client_cls):
            async def execute(self, sql=None, **kwargs):
                executed.append(sql if sql is not None else kwargs.get("sql", ""))
                return await super().execute(sql=sql, **kwargs) if sql is not None \
                    else await super().execute(**kwargs)

        with patch(
            "slayer.engine.query_engine.SlayerSQLClient", _RecordingClient
        ):
            report = await scoped.detect_join_cardinality(
                data_source="ds", model="orders"
            )

        # Two scans per side, two sides.
        assert len(executed) == 4
        customers_scans = [q for q in executed if "customers" in q]
        assert len(customers_scans) == 2
        # BOTH customer-side scans (row count AND distinct) carry the filter.
        for q in customers_scans:
            assert "'US'" in q, f"unscoped SQL reached the datasource: {q}"

        # And the scoped statistics are what the report actually contains:
        # only 1 of the 3 customers is in region 'US'.
        f = _find(report, "orders", "customers")
        assert f.target_side.row_count == 1
        assert f.target_side.distinct_count == 1

    async def test_policy_failure_is_not_bypassed(self, workspace: Path) -> None:
        """Fail-closed must propagate — detection must not sidestep the policy.

        Before the profiling scans were routed through ``_apply_policy`` this
        silently succeeded, scanning every row regardless of the policy.
        """
        _, storage, _ = await _build_engine(workspace)
        policy = SessionPolicy(
            ruleset=ColumnFilterRuleset(column="tenant_id", value="t1")
        )
        scoped = SlayerQueryEngine(storage=storage, policy=policy)
        with pytest.raises(ForcedFilterError):
            await scoped.detect_join_cardinality(data_source="ds", model="orders")

    async def test_no_policy_leaves_sql_untouched(self, workspace: Path) -> None:
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(
            data_source="ds", model="orders"
        )
        # Unscoped engine still profiles normally (zero-overhead no-op path).
        f = _find(report, "orders", "customers")
        assert f.detected is JoinCardinality.MANY_TO_ONE


class TestSideStatsSqlShape:
    """The profiling SQL is built via sqlglot, not string concatenation."""

    def test_identifiers_are_quoted_and_nulls_excluded(self) -> None:
        rows_sql, dist_sql = SlayerQueryEngine._side_stats_sql(
            table="public.orders", key_cols=["customer_id"], sqlglot_name="postgres",
        )
        for sql in (rows_sql, dist_sql):
            assert 'NOT "customer_id" IS NULL' in sql
            assert '"public"."orders"' in sql
        assert "DISTINCT" in dist_sql
        assert "DISTINCT" not in rows_sql

    def test_composite_keys_exclude_nulls_on_every_column(self) -> None:
        rows_sql, dist_sql = SlayerQueryEngine._side_stats_sql(
            table="t", key_cols=["a", "b"], sqlglot_name="postgres",
        )
        for sql in (rows_sql, dist_sql):
            assert 'NOT "a" IS NULL' in sql
            assert 'NOT "b" IS NULL' in sql

    def test_hostile_identifier_stays_a_single_identifier(self) -> None:
        payload = 'a"; DROP TABLE users; --'
        rows_sql, _ = SlayerQueryEngine._side_stats_sql(
            table="t", key_cols=[payload, "b"], sqlglot_name="postgres",
        )
        # sqlglot doubles the embedded quote, so the payload cannot terminate
        # the identifier and start a new statement.
        assert 'a""; DROP TABLE users; --' in rows_sql
        # The only statement is the SELECT: nothing escaped the quoting.
        assert rows_sql.strip().startswith("SELECT")
        assert 'NOT "b" IS NULL' in rows_sql


class TestEmptyPopulationIsNoEvidence:
    """An empty key population proves nothing about arity.

    row_count == distinct_count == 0 makes observed_unique True, so without a
    guard a join between two empty tables would "detect" one_to_one and
    persist=True would write it. Absence of rows is not weak evidence of
    uniqueness -- it is no evidence.
    """

    async def test_empty_tables_detect_nothing(self, workspace: Path) -> None:
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "empty_src", "empty_tgt")
        assert f.detected is None
        assert f.verdict is CardinalityVerdict.NO_EVIDENCE
        assert "no evidence" in (f.note or "")
        # The observed stats are still reported for transparency.
        assert f.source_side.row_count == 0
        assert f.target_side.row_count == 0

    async def test_all_null_keys_are_an_empty_population(
        self, workspace: Path
    ) -> None:
        # The table HAS rows, but every key is NULL, so the profiled
        # population is empty just the same.
        engine, _, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(data_source="ds")
        f = _find(report, "all_null_src", "customers")
        assert f.detected is None
        assert f.verdict is CardinalityVerdict.NO_EVIDENCE
        assert f.source_side.row_count == 0
        # The non-empty target side is still profiled and reported.
        assert f.target_side.row_count == 3

    async def test_populated_source_with_empty_target_detects_nothing(
        self, workspace: Path
    ) -> None:
        """The TARGET side alone being empty is equally no evidence.

        Guards the asymmetric case: an implementation that checked only
        `source_side.row_count` would infer (and persist) an arity here off a
        target scan that read nothing.
        """
        engine, storage, _ = await _build_engine(workspace)
        report = await engine.detect_join_cardinality(
            data_source="ds", persist=True
        )
        f = _find(report, "populated_src", "empty_tgt")
        assert f.verdict is CardinalityVerdict.NO_EVIDENCE
        assert f.detected is None
        # The source really was populated — only the target was empty.
        assert f.source_side.row_count == 3
        assert f.target_side.row_count == 0
        # persist=True must not have written an arity.
        reloaded = await storage.get_model("populated_src", data_source="ds")
        j = next(j for j in reloaded.joins if j.target_model == "empty_tgt")
        assert j.cardinality is None

    async def test_empty_side_is_never_persisted(self, workspace: Path) -> None:
        engine, storage, _ = await _build_engine(workspace)
        await engine.detect_join_cardinality(data_source="ds", persist=True)
        reloaded = await storage.get_model("empty_src", data_source="ds")
        j = next(j for j in reloaded.joins if j.target_model == "empty_tgt")
        assert j.cardinality is None, "an empty scan must not write an arity"


    async def test_no_evidence_is_distinct_from_skipped_unsupported(
        self, workspace: Path
    ) -> None:
        """The two non-detecting verdicts must stay tellable apart.

        `no_evidence` is retryable once data lands; `skipped_unsupported` is a
        shape that can never be profiled.
        """
        engine, storage, _ = await _build_engine(workspace)
        raw = SlayerModel(
            name="raw_empty",
            sql="SELECT k FROM empty_src",
            data_source="ds",
            columns=[_col("k")],
            joins=[ModelJoin(target_model="empty_tgt", join_pairs=[["k", "k"]])],
        )
        await storage.save_model(raw)
        report = await engine.detect_join_cardinality(data_source="ds")

        empty = _find(report, "empty_src", "empty_tgt")
        unsupported = _find(report, "raw_empty", "empty_tgt")
        assert empty.verdict is CardinalityVerdict.NO_EVIDENCE
        assert unsupported.verdict is CardinalityVerdict.SKIPPED_UNSUPPORTED
        assert empty.verdict != unsupported.verdict
        # Neither detects a value.
        assert empty.detected is None
        assert unsupported.detected is None
