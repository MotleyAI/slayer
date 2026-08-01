"""DEV-1705 Stage 1 — unit tests for the ``assert_scope_closed`` scope validator.

These tests pin the validator's *contract* against hand-written SQL strings that
faithfully mirror the shapes the typed-pipeline generator emits (host base,
first/last ranked subquery, forward ``_cm_*`` CTE + combined SELECT, host-rooted
``_fm_`` / windowed ``_wm_`` legacy shapes, ``time_shift`` CTE pair, RLS-wrapped
correlated ``EXISTS``). They are intentionally decoupled from the generator (J3=C
in the DEV-1705 spec) so they never break when a later stage — or Stage 11's
legacy deletion — changes what the generator emits: the validator must keep
closing/flagging *these SQL shapes* regardless of who produces them.

Two closure laws (DEV-1703 Law-2 / the ``assert_scope_closed`` contract):
  C1 — every table qualifier referenced in a scope binds to that scope's own
       FROM/JOIN sources (or a visible CTE).
  C2 — a reference resolving into another SELECT scope must name a column that
       scope projects (star projection without EXCEPT/REPLACE ⇒ unprovable ⇒
       allowed; conservative, no false positives).

Pre-RLS by default; ``allow_rls_correlation=True`` whitelists the intentional
``_rls_src`` correlation the session-policy transform injects post-generation.
"""

from __future__ import annotations

import pytest

from slayer.sql.scope_check import (
    ScopeLeakError,
    assert_scope_closed,
    check_scope_closed,
    maybe_validate_scopes,
)

# --------------------------------------------------------------------------- #
# Faithful SQL shape fixtures (lifted from real typed-pipeline emission).
# --------------------------------------------------------------------------- #

# Plain host base SELECT with a LEFT JOIN — every qualifier bound in-scope.
SQL_HOST_BASE = """
SELECT
  orders.status AS "orders.status",
  SUM(orders.revenue) AS "orders.revenue_sum"
FROM orders AS orders
LEFT JOIN customers AS customers ON orders.customer_id = customers.id
WHERE customers.region_id = 5
GROUP BY orders.status
""".strip()

# first/last ranked subquery: derived table aliased to the source relation,
# projecting ``orders.*`` (a plain star). The outer scope references
# ``orders.balance`` — legal because the inner star projects everything.
SQL_FIRST_LAST_RANKED = """
SELECT
  DATE_TRUNC('MONTH', orders.created_at) AS "orders.created_at_month",
  MAX(CASE WHEN orders._last_rn = 1 THEN orders.balance END) AS "orders.balance_last"
FROM (
  SELECT
    orders.*,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', created_at) ORDER BY ordered_at DESC) AS _last_rn
  FROM orders AS orders
) AS orders
GROUP BY DATE_TRUNC('MONTH', orders.created_at)
""".strip()

# forward cross-model ``_cm_*`` CTE + combined SELECT. The combined SELECT names
# only projected columns of ``_base`` / ``_cm_customers``.
SQL_CROSS_MODEL_CM = """
WITH _base AS (
  SELECT orders.status AS "orders.status", SUM(orders.revenue) AS "orders.revenue_sum"
  FROM orders AS orders
  GROUP BY orders.status
),
_cm_customers AS (
  SELECT orders.status AS "orders.status", SUM(customers.balance) AS "customers.balance_sum"
  FROM orders AS orders
  LEFT JOIN customers AS customers ON orders.customer_id = customers.id
  GROUP BY orders.status
)
SELECT _base."orders.status", _base."orders.revenue_sum", _cm_customers."customers.balance_sum"
FROM _base
LEFT JOIN _cm_customers ON _base."orders.status" = _cm_customers."orders.status"
""".strip()

# time_shift: shifted_x re-aggregates with the shifted time column; sjoin_x
# carries prior aliases forward and LEFT JOINs shifted_x. All CTE-qualified.
SQL_TIME_SHIFT = """
WITH base AS (
  SELECT orders.created_at AS "orders.created_at", SUM(orders.revenue) AS "orders.revenue_sum"
  FROM orders AS orders
  GROUP BY orders.created_at
),
shifted_x AS (
  SELECT orders.created_at AS "orders.created_at", SUM(orders.revenue) AS shifted_val
  FROM orders AS orders
  GROUP BY orders.created_at
),
sjoin_x AS (
  SELECT base.*, shifted_x.shifted_val AS "orders.revenue_sum_prev"
  FROM base
  LEFT JOIN shifted_x ON base."orders.created_at" = shifted_x."orders.created_at"
)
SELECT sjoin_x."orders.created_at", sjoin_x."orders.revenue_sum", sjoin_x."orders.revenue_sum_prev"
FROM sjoin_x
""".strip()

# Legacy host-rooted filtered-measure ``_fm_`` shape (J3=C: validator must close
# it even though the typed pipeline re-expresses it as a ``_cm_`` w/ cte_root_model).
SQL_LEGACY_FM = """
WITH _fm_paid AS (
  SELECT orders.status AS "orders.status", SUM(orders.revenue) AS "orders.paid_revenue_sum"
  FROM orders AS orders
  WHERE orders.paid = TRUE
  GROUP BY orders.status
)
SELECT orders.status AS "orders.status",
       SUM(orders.revenue) AS "orders.revenue_sum",
       _fm_paid."orders.paid_revenue_sum"
FROM orders AS orders
LEFT JOIN _fm_paid ON orders.status = _fm_paid."orders.status"
GROUP BY orders.status, _fm_paid."orders.paid_revenue_sum"
""".strip()

# Legacy windowed-measure ``_wm_`` range-join shape (the intended Stage-10 typed
# shape too — validator must close it now via hand-written SQL).
SQL_LEGACY_WM = """
WITH _wm_orders__revenue_90d AS (
  SELECT _base."orders.status" AS "orders.status",
         SUM(_src.revenue) AS "orders.revenue_90d"
  FROM (SELECT DISTINCT orders.status AS "orders.status" FROM orders AS orders) AS _base
  LEFT JOIN orders AS _src
    ON _src.status = _base."orders.status"
   AND _src.created_at >= _base."orders.status"
  GROUP BY _base."orders.status"
)
SELECT _wm_orders__revenue_90d."orders.status",
       _wm_orders__revenue_90d."orders.revenue_90d"
FROM _wm_orders__revenue_90d
""".strip()

# Mixed-case / reserved-word quoted identifiers (DEV-1686 parity).
SQL_QUOTED_MIXED_CASE = """
SELECT "Orders"."Grant" AS "Orders.Grant", SUM("Orders"."Revenue") AS "Orders.Revenue_sum"
FROM "Orders" AS "Orders"
GROUP BY "Orders"."Grant"
""".strip()

# UNION CTE referenced downstream — public names are positional (first leg).
SQL_UNION_CTE = """
WITH u AS (
  SELECT a.x AS k, a.v AS val FROM a AS a
  UNION ALL
  SELECT b.y AS k2, b.w AS val2 FROM b AS b
)
SELECT u.k, u.val FROM u
""".strip()


# --------------------------------------------------------------------------- #
# Positive: closed SQL must pass across every scope shape and several dialects.
# --------------------------------------------------------------------------- #
class TestClosedShapesPass:
    @pytest.mark.parametrize(
        "sql",
        [
            SQL_HOST_BASE,
            SQL_FIRST_LAST_RANKED,
            SQL_CROSS_MODEL_CM,
            SQL_TIME_SHIFT,
            SQL_LEGACY_FM,
            SQL_LEGACY_WM,
            SQL_UNION_CTE,
        ],
    )
    def test_closed_sql_does_not_raise(self, sql: str) -> None:
        assert_scope_closed(sql)  # pre-RLS default; must not raise
        assert check_scope_closed(sql).closed is True

    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb", "mysql", "snowflake"])
    def test_closed_sql_across_dialects(self, dialect: str) -> None:
        assert_scope_closed(SQL_CROSS_MODEL_CM, dialect=dialect)

    def test_quoted_mixed_case_qualifiers_close(self) -> None:
        # DEV-1686: reserved-word / mixed-case quoted identifiers must not
        # be miscompared (no false positive from naive casefolding).
        assert_scope_closed(SQL_QUOTED_MIXED_CASE, dialect="postgres")

    def test_ranked_star_projection_allows_any_source_column(self) -> None:
        # The outer scope references orders.balance / orders.created_at, which
        # only exist behind the inner ``orders.*`` star — the star exception
        # must permit them (no false positive).
        assert check_scope_closed(SQL_FIRST_LAST_RANKED).closed is True

    def test_plain_unqualified_star_projection_allows_column(self) -> None:
        # A derived SELECT projecting a bare ``*`` (not ``rel.*``) also exports
        # every name — a reference to ``c.a`` through it must not be flagged.
        assert_scope_closed("SELECT c.a FROM (SELECT * FROM t AS t) AS c")

    def test_star_replace_keeps_projected_names(self) -> None:
        # ``t.* REPLACE (x + 1 AS x)`` still projects ``x`` (and every other
        # column) — a reference to ``c.x`` must not leak.
        assert_scope_closed(
            "SELECT c.x FROM (SELECT t.* REPLACE (t.x + 1 AS x) FROM t AS t) AS c",
            dialect="duckdb",
        )

    def test_order_group_having_alias_refs_not_flagged(self) -> None:
        sql = (
            'SELECT orders.status AS "orders.status", '
            'SUM(orders.revenue) AS "orders.revenue_sum" '
            "FROM orders AS orders "
            'GROUP BY orders.status HAVING SUM(orders.revenue) > 100 '
            'ORDER BY "orders.revenue_sum" DESC'
        )
        assert_scope_closed(sql)

    def test_window_function_over_partition_not_flagged(self) -> None:
        sql = (
            "SELECT orders.status AS s, "
            "SUM(orders.revenue) OVER (PARTITION BY orders.status) AS running "
            "FROM orders AS orders"
        )
        assert_scope_closed(sql)


# --------------------------------------------------------------------------- #
# Negative: genuine scope leaks must raise ScopeLeakError with a useful message.
# --------------------------------------------------------------------------- #
class TestLeaksRaise:
    def test_outer_references_table_bound_only_in_inner_cte(self) -> None:
        # ``t1`` is bound inside CTE ``c`` but referenced by the outer SELECT.
        sql = "WITH c AS (SELECT t1.x AS x FROM t1 AS t1) SELECT t1.y FROM c"
        with pytest.raises(ScopeLeakError, match=r"t1"):
            assert_scope_closed(sql)

    def test_dev1531_first_last_out_of_scope_source_ref(self) -> None:
        # The canonical DEV-1531 leak: the outer aggregate body references
        # ``regions.population`` (a further-join column) that lives only inside
        # the ranked subquery and is NOT projected out of it.
        sql = """
SELECT
  DATE_TRUNC('MONTH', orders.created_at) AS "orders.created_at_month",
  MAX(CASE WHEN orders._last_rn = 1 THEN regions.population END) AS "orders.rp_last"
FROM (
  SELECT orders.*, ROW_NUMBER() OVER (ORDER BY orders.ordered_at DESC) AS _last_rn
  FROM orders AS orders
  LEFT JOIN regions AS regions ON orders.region_id = regions.id
) AS orders
GROUP BY DATE_TRUNC('MONTH', orders.created_at)
""".strip()
        with pytest.raises(ScopeLeakError, match=r"regions"):
            assert_scope_closed(sql)

    def test_combined_references_unprojected_cte_column(self) -> None:
        # The combined SELECT names ``_cm_customers.balance_sum`` but the CTE
        # projects only ``balance_total`` — an unprojected-column leak (C2).
        sql = """
WITH _cm_customers AS (
  SELECT orders.status AS "orders.status", SUM(customers.balance) AS balance_total
  FROM orders AS orders
  LEFT JOIN customers AS customers ON orders.customer_id = customers.id
  GROUP BY orders.status
)
SELECT _cm_customers."orders.status", _cm_customers.balance_sum
FROM _cm_customers
""".strip()
        with pytest.raises(ScopeLeakError, match=r"balance_sum"):
            assert_scope_closed(sql)

    def test_star_except_hides_column_is_a_leak(self) -> None:
        # ``c`` projects ``t.* EXCEPT (a)`` — a reference to ``c.a`` must NOT be
        # excused by the star exception.
        sql = "SELECT c.a FROM (SELECT t.* EXCEPT (a) FROM t AS t) AS c"
        with pytest.raises(ScopeLeakError, match=r"\ba\b"):
            assert_scope_closed(sql, dialect="duckdb")

    def test_union_cte_second_leg_name_is_a_leak(self) -> None:
        # A UNION scope's public schema is positional (first leg): ``k``/``val``.
        # Referencing the second leg's name ``k2`` downstream is a leak (C2 #4).
        sql = """
WITH u AS (
  SELECT a.x AS k, a.v AS val FROM a AS a
  UNION ALL
  SELECT b.y AS k2, b.w AS val2 FROM b AS b
)
SELECT u.k2 FROM u
""".strip()
        with pytest.raises(ScopeLeakError, match=r"k2"):
            assert_scope_closed(sql)

    def test_leak_result_lists_offending_reference_c1(self) -> None:
        # C1 (unbound table) leak — structured result, not just the exception.
        sql = "WITH c AS (SELECT t1.x AS x FROM t1 AS t1) SELECT t1.y FROM c"
        result = check_scope_closed(sql)
        assert result.closed is False
        assert result.skipped is False
        assert result.leaks, "expected at least one recorded leak"
        assert any(leak.reference == "t1.y" for leak in result.leaks), result.leaks

    def test_leak_result_lists_offending_reference_c2(self) -> None:
        # C2 (unprojected column) leak — structured result names the ref.
        sql = (
            "WITH _cm_customers AS (SELECT orders.status AS s, "
            "SUM(customers.balance) AS balance_total FROM orders AS orders "
            "LEFT JOIN customers AS customers ON orders.customer_id = customers.id "
            "GROUP BY orders.status) SELECT _cm_customers.balance_sum FROM _cm_customers"
        )
        result = check_scope_closed(sql)
        assert result.closed is False and result.skipped is False
        assert any(leak.reference.endswith("balance_sum") for leak in result.leaks), result.leaks

    def test_multiple_distinct_leaks_all_reported(self) -> None:
        # Two distinct unbound qualifiers in the outer scope — both reported.
        sql = (
            "WITH c AS (SELECT t1.x AS x, t2.y AS y FROM t1 AS t1 "
            "CROSS JOIN t2 AS t2) SELECT t1.a, t2.b FROM c"
        )
        result = check_scope_closed(sql)
        refs = {leak.reference for leak in result.leaks}
        assert {"t1.a", "t2.b"} <= refs, refs


# --------------------------------------------------------------------------- #
# RLS: pre-RLS strictness vs. post-RLS correlated-EXISTS allowlist.
# --------------------------------------------------------------------------- #
class TestRlsCorrelationAllowlist:
    # Faithful RLS wrap (session_policy.py): ``orders`` becomes a filtered
    # subquery whose EXISTS body correlates ``_rls_j0`` back to ``_rls_src``.
    RLS_WRAPPED = """
SELECT orders.status AS "orders.status", SUM(orders.revenue) AS "orders.revenue_sum"
FROM (
  SELECT * FROM orders AS _rls_src
  WHERE EXISTS (
    SELECT 1 FROM customers AS _rls_j0
    WHERE _rls_j0.id = _rls_src.customer_id AND _rls_j0.organization_uuid = '7ef3'
  )
) AS orders
GROUP BY orders.status
""".strip()

    def test_rls_correlation_rejected_pre_rls(self) -> None:
        # Without the allowlist, the intentional ``_rls_src`` correlation reads
        # as an out-of-scope reference — proving the validator is strict by
        # default (pre-RLS mode).
        with pytest.raises(ScopeLeakError, match=r"_rls_src"):
            assert_scope_closed(self.RLS_WRAPPED)

    def test_rls_correlation_allowed_in_post_rls_mode(self) -> None:
        assert_scope_closed(self.RLS_WRAPPED, allow_rls_correlation=True)
        assert check_scope_closed(self.RLS_WRAPPED, allow_rls_correlation=True).closed is True

    def test_allowlist_does_not_excuse_a_real_leak(self) -> None:
        # A non-RLS out-of-scope ref must still raise even in post-RLS mode.
        sql = "WITH c AS (SELECT t1.x AS x FROM t1 AS t1) SELECT t1.y FROM c"
        with pytest.raises(ScopeLeakError):
            assert_scope_closed(sql, allow_rls_correlation=True)

    def test_allowlist_preserves_real_leak_alongside_rls_correlation(self) -> None:
        # One statement carrying BOTH an allowed `_rls_src` correlation AND a
        # genuine out-of-scope ref (`ghost.col`): in post-RLS mode only the
        # real leak survives.
        sql = (
            "SELECT ghost.col AS leaked FROM ("
            "  SELECT * FROM orders AS _rls_src"
            "  WHERE EXISTS (SELECT 1 FROM customers AS _rls_j0"
            "                WHERE _rls_j0.id = _rls_src.customer_id)"
            ") AS orders"
        )
        result = check_scope_closed(sql, allow_rls_correlation=True)
        assert result.closed is False
        refs = {leak.reference for leak in result.leaks}
        assert any(r.startswith("ghost.") for r in refs), refs
        assert not any("_rls_src" in r for r in refs), refs


# --------------------------------------------------------------------------- #
# Best-effort / documented limitations (J1=A: sound-on-corpus, never a false +).
# --------------------------------------------------------------------------- #
class TestBestEffortLimitations:
    def test_unqualified_ambiguous_ref_is_unverifiable_not_a_leak(self) -> None:
        # ``id`` could come from either joined source; with no schema the
        # validator cannot resolve it — it must classify as unverifiable and
        # NOT raise (no false positive), never silently assert validity either.
        sql = (
            "SELECT id FROM a AS a "
            "LEFT JOIN b AS b ON a.k = b.k"
        )
        assert_scope_closed(sql)  # must not raise

    def test_recursive_cte_self_reference_not_flagged(self) -> None:
        # Generator emits no recursion; validator must not choke on / falsely
        # flag a self-referential CTE if one is ever handed to it.
        sql = (
            "WITH RECURSIVE r AS ("
            "  SELECT n.x AS x FROM n AS n "
            "  UNION ALL "
            "  SELECT r.x + 1 AS x FROM r WHERE r.x < 10"
            ") SELECT r.x FROM r"
        )
        assert_scope_closed(sql)


# --------------------------------------------------------------------------- #
# BigQuery parse carve-out: bounded + reported (Codex #7 / J2=A). Residual owned
# by Stage 9 (DEV-1713).
# --------------------------------------------------------------------------- #
class TestBigQueryParseCarveOut:
    def test_bigquery_typeerror_is_skipped_not_raised(self, monkeypatch) -> None:
        import slayer.sql.scope_check as sc

        def _boom(*_args, **_kwargs):
            raise TypeError("sqlglot bigquery quirk")

        monkeypatch.setattr(sc.sqlglot, "parse_one", _boom)
        result = check_scope_closed("SELECT 1", dialect="bigquery")
        assert result.skipped is True
        assert result.closed is True  # skipped ⇒ not a leak
        # assert_scope_closed must also swallow it (no raise) for bigquery.
        assert_scope_closed("SELECT 1", dialect="bigquery")

    def test_typeerror_propagates_for_non_carveout_dialect(self, monkeypatch) -> None:
        import slayer.sql.scope_check as sc

        def _boom(*_args, **_kwargs):
            raise TypeError("unexpected")

        monkeypatch.setattr(sc.sqlglot, "parse_one", _boom)
        with pytest.raises(TypeError):
            check_scope_closed("SELECT 1", dialect="postgres")


# --------------------------------------------------------------------------- #
# maybe_validate_scopes: env-gated, evaluated at call time (Codex #9).
# --------------------------------------------------------------------------- #
class TestEnvGatedRuntimeHook:
    LEAKY = "WITH c AS (SELECT t1.x AS x FROM t1 AS t1) SELECT t1.y FROM c"

    def test_disabled_when_env_unset(self, monkeypatch) -> None:
        monkeypatch.delenv("SLAYER_VALIDATE_SCOPES", raising=False)
        maybe_validate_scopes(self.LEAKY)  # no-op, must not raise

    def test_enabled_when_env_truthy(self, monkeypatch) -> None:
        monkeypatch.setenv("SLAYER_VALIDATE_SCOPES", "1")
        with pytest.raises(ScopeLeakError):
            maybe_validate_scopes(self.LEAKY)

    @pytest.mark.parametrize("falsy", ["0", "", "false", "no"])
    def test_disabled_for_falsy_values(self, monkeypatch, falsy: str) -> None:
        monkeypatch.setenv("SLAYER_VALIDATE_SCOPES", falsy)
        maybe_validate_scopes(self.LEAKY)  # no-op

    def test_read_at_call_time_not_import_time(self, monkeypatch) -> None:
        # Toggling after import must take effect immediately.
        monkeypatch.delenv("SLAYER_VALIDATE_SCOPES", raising=False)
        maybe_validate_scopes(self.LEAKY)  # off
        monkeypatch.setenv("SLAYER_VALIDATE_SCOPES", "1")
        with pytest.raises(ScopeLeakError):
            maybe_validate_scopes(self.LEAKY)  # on

    def test_closed_sql_passes_when_enabled(self, monkeypatch) -> None:
        monkeypatch.setenv("SLAYER_VALIDATE_SCOPES", "1")
        maybe_validate_scopes(SQL_CROSS_MODEL_CM)  # closed ⇒ no raise
