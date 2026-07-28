"""Unit tests for the forced-filter SQL rewrite (DEV-1578 / DEV-1627 / DEV-1718).

``apply_session_policy`` is a pure sqlglot transform: given final SQL, a
dialect, a ``SessionPolicy``, and a ``has_column`` probe callback, it wraps
every *physical* table reference according to the policy's ``ruleset``.

* A ``ColumnFilterRuleset`` wraps every table that has the column in a filtered
  ``SELECT * ... WHERE`` subquery (``has_column`` probed; ``block`` / ``pass`` /
  fail-closed).
* A ``JoinFilterRuleset`` classifies each physical table structurally (no
  probe): the anchor table is wrapped directly, a join target via a correlated
  ``EXISTS`` semi-join, a whitelisted table is passed through, and anything
  else fails closed.
"""

import sqlglot
import pytest
from sqlglot import exp

from slayer.core.errors import ForcedFilterError
from slayer.core.policy import (
    ColumnFilterRuleset,
    JoinFilterRule,
    JoinFilterRuleset,
    SessionPolicy,
)
from slayer.sql.session_policy import (
    ScopedTable,
    _attach_ch_correlated_setting,
    apply_session_policy,
)


def _norm(sql: str, dialect: str = "sqlite") -> str:
    return sqlglot.parse_one(sql, dialect=dialect).sql(dialect=dialect)


def has_column_factory(tables):
    def has_column(scoped: ScopedTable, column: str):
        entry = tables.get(scoped.name, "missing")
        if entry == "missing" or entry is None:
            return None
        return column in entry

    return has_column


ALWAYS = lambda scoped, column: True  # noqa: E731


def _boom_probe(scoped, column):
    raise AssertionError("has_column must not be probed on the join path")


def _col_policy(**kw):
    return SessionPolicy(ruleset=ColumnFilterRuleset(**kw))


# ===========================================================================
# ColumnFilterRuleset
# ===========================================================================


def test_scalar_value_emits_equality():
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=_col_policy(column="org", value="7ef3"),
        has_column=ALWAYS,
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders WHERE org = '7ef3') AS orders"
    )


def test_list_value_emits_in():
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=_col_policy(column="org", value=["a", "b"]),
        has_column=ALWAYS,
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders WHERE org IN ('a', 'b')) AS orders"
    )


def test_bool_value_emits_boolean_literal():
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=_col_policy(column="is_active", value=True),
        has_column=ALWAYS,
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders WHERE is_active = TRUE) AS orders"
    )


def test_joined_table_wrapped_alias_preserved():
    out = apply_session_policy(
        "SELECT * FROM customers c LEFT JOIN orders o ON c.id = o.customer_id",
        dialect="sqlite",
        policy=_col_policy(column="org", value="x"),
        has_column=ALWAYS,
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM customers WHERE org = 'x') AS c "
        "LEFT JOIN (SELECT * FROM orders WHERE org = 'x') AS o "
        "ON c.id = o.customer_id"
    )


def test_self_join_each_occurrence_wrapped_once():
    out = apply_session_policy(
        "SELECT * FROM orders a JOIN orders b ON a.id = b.id",
        dialect="sqlite",
        policy=_col_policy(column="org", value="x"),
        has_column=ALWAYS,
    )
    assert out.count("WHERE org = 'x'") == 2


def test_schema_passed_to_has_column():
    seen = {}

    def has_column(scoped, column):
        seen["scoped"] = scoped
        return True

    apply_session_policy(
        "SELECT * FROM public.orders",
        dialect="postgres",
        policy=_col_policy(column="org", value="x"),
        has_column=has_column,
    )
    assert seen["scoped"].name == "orders"
    assert seen["scoped"].schema_name == "public"


def test_cte_reference_skipped_physical_wrapped():
    out = apply_session_policy(
        "WITH _cm_x AS (SELECT * FROM customers) "
        "SELECT * FROM orders LEFT JOIN _cm_x ON orders.id = _cm_x.id",
        dialect="sqlite",
        policy=_col_policy(column="org", value="x"),
        has_column=ALWAYS,
    )
    assert out.count("WHERE org = 'x'") == 2
    assert "_cm_x ON orders" in out


def test_collision_physical_inside_cte_is_wrapped():
    out = apply_session_policy(
        "WITH orders AS (SELECT * FROM orders) SELECT * FROM orders",
        dialect="sqlite",
        policy=_col_policy(column="org", value="x"),
        has_column=ALWAYS,
    )
    assert out.count("WHERE org = 'x'") == 1


@pytest.mark.parametrize("setop", ["UNION ALL", "UNION", "INTERSECT", "EXCEPT"])
def test_set_operation_both_branches_wrapped(setop):
    out = apply_session_policy(
        f"SELECT id FROM orders {setop} SELECT id FROM archived_orders",
        dialect="sqlite",
        policy=_col_policy(column="org", value="x"),
        has_column=ALWAYS,
    )
    assert out.count("WHERE org = 'x'") == 2


def test_block_raises_naming_table():
    policy = _col_policy(column="org", value="x")
    has_column = has_column_factory({"exchange_rates": {"rate", "day"}})
    with pytest.raises(ForcedFilterError) as exc:
        apply_session_policy(
            "SELECT * FROM exchange_rates",
            dialect="sqlite",
            policy=policy,
            has_column=has_column,
        )
    assert exc.value.table == "exchange_rates"
    assert exc.value.column == "org"
    assert "exchange_rates" in str(exc.value)


def test_pass_leaves_table_unfiltered():
    policy = _col_policy(column="org", value="x", on_unapplicable="pass")
    has_column = has_column_factory({"exchange_rates": {"rate", "day"}})
    out = apply_session_policy(
        "SELECT * FROM exchange_rates",
        dialect="sqlite",
        policy=policy,
        has_column=has_column,
    )
    assert out == _norm("SELECT * FROM exchange_rates")
    assert "WHERE" not in out.upper()


def test_none_presence_fails_closed_even_with_pass():
    policy = _col_policy(column="org", value="x", on_unapplicable="pass")
    has_column = has_column_factory({"orders": None})
    with pytest.raises(ForcedFilterError) as exc:
        apply_session_policy(
            "SELECT * FROM orders",
            dialect="sqlite",
            policy=policy,
            has_column=has_column,
        )
    assert exc.value.table == "orders"


def test_unknown_table_fails_closed():
    policy = _col_policy(column="org", value="x")
    has_column = has_column_factory({})  # nothing known -> None
    with pytest.raises(ForcedFilterError):
        apply_session_policy(
            "SELECT * FROM orders",
            dialect="sqlite",
            policy=policy,
            has_column=has_column,
        )


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO orders (id) VALUES (1)",
        "UPDATE orders SET amount = 0",
        "DELETE FROM orders WHERE id = 1",
    ],
)
def test_non_select_root_fails_closed(sql):
    policy = _col_policy(column="org", value="x")
    with pytest.raises(ForcedFilterError):
        apply_session_policy(sql, dialect="sqlite", policy=policy, has_column=ALWAYS)


def test_value_literal_is_injection_safe():
    policy = _col_policy(column="org", value="x' OR '1'='1")
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    assert "WHERE org = 'x'' OR ''1''=''1'" in out
    reparsed = sqlglot.parse_one(out, dialect="sqlite")
    assert reparsed.find(exp.Or) is None


# ===========================================================================
# JoinFilterRuleset — structural classification + correlated EXISTS
# ===========================================================================


def _join_ruleset(**kw):
    base = dict(
        table="customers",
        column="organization_uuid",
        value="orgA",
        joins=[
            JoinFilterRule(
                target_table="orders",
                join_path=["orders.customer_id = customers.id"],
            )
        ],
    )
    base.update(kw)
    return JoinFilterRuleset(**base)


def _jpolicy(**kw):
    return SessionPolicy(ruleset=_join_ruleset(**kw))


def _exists_nodes(sql, dialect="sqlite"):
    return list(sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Exists))


# -- anchor direct-wrap ------------------------------------------------------


def test_anchor_table_wrapped_directly():
    out = apply_session_policy(
        "SELECT * FROM customers",
        dialect="sqlite",
        policy=_jpolicy(),
        has_column=_boom_probe,  # never probed on the join path
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM customers "
        "WHERE organization_uuid = 'orgA') AS customers"
    )
    assert not _exists_nodes(out)


def test_anchor_list_value_emits_in():
    out = apply_session_policy(
        "SELECT * FROM customers",
        dialect="sqlite",
        policy=_jpolicy(value=["orgA", "orgB"]),
        has_column=_boom_probe,
    )
    assert "organization_uuid IN ('orgA', 'orgB')" in out


# -- target correlated EXISTS ------------------------------------------------


def test_target_emits_correlated_exists():
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=_jpolicy(),
        has_column=_boom_probe,
    )
    parsed = sqlglot.parse_one(out, dialect="sqlite")
    exists = list(parsed.find_all(exp.Exists))
    assert len(exists) == 1
    body = exists[0].this
    assert body.find(exp.Table).name == "customers"
    assert "organization_uuid = 'orgA'" in out
    assert "customer_id" in body.sql()
    assert parsed.find(exp.Join) is None  # semi-join, not an outer join
    assert out.rstrip().endswith("AS orders")


def test_both_path_orientations_emit_identical_sql():
    """A path written master-first produces the SAME SQL as target-first."""
    target_first = _jpolicy(
        joins=[
            JoinFilterRule(
                target_table="orders",
                join_path=["orders.customer_id = customers.id"],
            )
        ]
    )
    master_first = _jpolicy(
        joins=[
            JoinFilterRule(
                target_table="orders",
                join_path=["customers.id = orders.customer_id"],
            )
        ]
    )
    kw = dict(dialect="sqlite", has_column=_boom_probe)
    out_tf = apply_session_policy("SELECT * FROM orders", policy=target_first, **kw)
    out_mf = apply_session_policy("SELECT * FROM orders", policy=master_first, **kw)
    assert out_tf == out_mf


def test_multihop_terminal_on_anchor():
    rule = JoinFilterRule(
        target_table="line_items",
        join_path=[
            "line_items.order_id = orders.id",
            "orders.customer_id = customers.id",
        ],
    )
    out = apply_session_policy(
        "SELECT * FROM line_items",
        dialect="sqlite",
        policy=_jpolicy(joins=[rule]),
        has_column=_boom_probe,
    )
    body = _exists_nodes(out)[0].this
    assert body.find(exp.Join) is not None
    body_tables = {t.name for t in body.find_all(exp.Table)}
    assert body_tables == {"orders", "customers"}
    # terminal predicate lives on the anchor (customers), not the intermediate
    customers_tbl = next(t for t in body.find_all(exp.Table) if t.name == "customers")
    term = next(
        eq for eq in body.find_all(exp.EQ)
        if isinstance(eq.expression, exp.Literal) and eq.expression.this == "orgA"
    )
    assert term.this.table == customers_tbl.alias_or_name


# -- whitelist ---------------------------------------------------------------


def test_whitelisted_table_passed_through():
    out = apply_session_policy(
        "SELECT * FROM exchange_rates",
        dialect="sqlite",
        policy=_jpolicy(whitelist=["exchange_rates"]),
        has_column=_boom_probe,
    )
    assert out == _norm("SELECT * FROM exchange_rates")
    assert "WHERE" not in out.upper()
    assert not _exists_nodes(out)


# -- unlisted table fails closed ---------------------------------------------


def test_unlisted_table_fails_closed():
    policy = _jpolicy()
    with pytest.raises(ForcedFilterError) as exc:
        apply_session_policy(
            "SELECT * FROM secret_table",
            dialect="sqlite",
            policy=policy,
            has_column=_boom_probe,
        )
    assert exc.value.table == "secret_table"


def test_all_three_kinds_in_one_query():
    out = apply_session_policy(
        "SELECT * FROM orders o "
        "LEFT JOIN customers c ON c.id = o.customer_id "
        "LEFT JOIN exchange_rates r ON r.day = o.day",
        dialect="sqlite",
        policy=_jpolicy(whitelist=["exchange_rates"]),
        has_column=_boom_probe,
    )
    # orders -> EXISTS; customers -> direct wrap; exchange_rates -> passthrough
    assert len(_exists_nodes(out)) == 1
    assert "(SELECT * FROM customers WHERE organization_uuid = 'orgA') AS c" in out
    assert "exchange_rates" in out
    # exchange_rates is NOT wrapped (still a bare table ref on the right side)
    assert out.count("organization_uuid = 'orgA'") == 2  # anchor wrap + EXISTS


# -- diamond / self-join -----------------------------------------------------


def test_same_target_twice_each_gets_own_exists():
    out = apply_session_policy(
        "SELECT * FROM orders a JOIN orders b ON a.id = b.id",
        dialect="sqlite",
        policy=_jpolicy(),
        has_column=_boom_probe,
    )
    parsed = sqlglot.parse_one(out, dialect="sqlite")
    assert len(list(parsed.find_all(exp.Exists))) == 2
    assert "AS a" in out and "AS b" in out
    subqueries = [
        s for s in parsed.find_all(exp.Subquery) if s.this.find(exp.Exists) is not None
    ]
    assert len(subqueries) == 2
    for sub in subqueries:
        assert sub.this.find(exp.Table).name == "orders"
    assert parsed is not None  # re-parses cleanly


def test_user_sql_containing_rls_alias_still_isolated():
    """A user query that itself references a table literally named _rls_src is
    wrapped in its own fresh subquery scope — the internal aliases don't leak /
    collide across wraps."""
    # _rls_src is unlisted -> fails closed (proves it is treated as a real,
    # user table, not confused with the internal wrapper alias).
    policy = _jpolicy()
    with pytest.raises(ForcedFilterError) as exc:
        apply_session_policy(
            "SELECT * FROM _rls_src",
            dialect="sqlite",
            policy=policy,
            has_column=_boom_probe,
        )
    assert exc.value.table == "_rls_src"


def test_target_named_like_internal_alias_no_collision():
    """A join target physically named ``_rls_src`` (the internal wrapper base
    alias) rewrites without collision: the correlated EXISTS emits
    ``FROM _rls_src AS _rls_src`` and re-parses cleanly, still correlating the
    base row to the anchor."""
    ruleset = JoinFilterRuleset(
        table="customers",
        column="organization_uuid",
        value="orgA",
        joins=[
            JoinFilterRule(
                target_table="_rls_src",
                join_path=["_rls_src.customer_id = customers.id"],
            )
        ],
    )
    out = apply_session_policy(
        "SELECT * FROM _rls_src",
        dialect="sqlite",
        policy=SessionPolicy(ruleset=ruleset),
        has_column=_boom_probe,
    )
    parsed = sqlglot.parse_one(out, dialect="sqlite")  # re-parses cleanly
    assert len(list(parsed.find_all(exp.Exists))) == 1
    assert "organization_uuid = 'orgA'" in out
    assert out.rstrip().endswith("AS _rls_src")


def test_corrupted_ruleset_terminal_not_anchor_fails_closed():
    """Defensive terminal assertion: a ruleset corrupted via model_copy (which
    bypasses the cross-field validator) so a join's oriented terminal no longer
    reaches the anchor fails closed at SQL generation rather than landing the
    tenant predicate on a non-anchor table."""
    good = SessionPolicy(ruleset=_join_ruleset())  # anchor=customers
    # Swap the anchor out from under the (unchanged) join path. Corrupting at the
    # POLICY level via model_copy skips both the SessionPolicy and the nested
    # JoinFilterRuleset validators that would otherwise reject this.
    corrupted_ruleset = good.ruleset.model_copy(update={"table": "different_anchor"})
    bad_policy = good.model_copy(update={"ruleset": corrupted_ruleset})
    with pytest.raises(ForcedFilterError) as exc:
        apply_session_policy(
            "SELECT * FROM orders",
            dialect="sqlite",
            policy=bad_policy,
            has_column=_boom_probe,
        )
    assert exc.value.table == "orders"
    assert exc.value.column == "organization_uuid"


def test_corrupted_join_path_valueerror_wrapped_as_forced_filter():
    """A corrupt join rule whose target_table is no longer a path endpoint makes
    oriented_hops() raise ValueError; the SQL layer wraps it as a fail-closed
    ForcedFilterError rather than leaking a raw ValueError."""
    good = SessionPolicy(ruleset=_join_ruleset())  # target orders -> customers
    # Break the path so orders is no longer an endpoint (target_table unchanged);
    # model_copy at each level skips validation.
    bad_rule = good.ruleset.joins[0].model_copy(
        update={"join_path": ("foo.a = customers.b",)}
    )
    bad_ruleset = good.ruleset.model_copy(update={"joins": (bad_rule,)})
    bad_policy = good.model_copy(update={"ruleset": bad_ruleset})
    with pytest.raises(ForcedFilterError) as exc:
        apply_session_policy(
            "SELECT * FROM orders",
            dialect="sqlite",
            policy=bad_policy,
            has_column=_boom_probe,
        )
    assert exc.value.table == "orders"


def test_corrupted_qualified_anchor_bare_terminal_fails_closed():
    """Defensive: a model_copy that drops a qualified anchor's terminal to a
    bare, wrong-schema name fails closed at the SQL boundary via _reaches_anchor
    (not merely _table_names_match, which would accept the bare terminal)."""
    rule = JoinFilterRule(
        target_table="public.orders",
        join_path=["public.orders.customer_id = public.customers.id"],
    )
    good = SessionPolicy(
        ruleset=JoinFilterRuleset(
            table="public.customers", column="organization_uuid", value="orgA",
            joins=[rule],
        )
    )
    bad_rule = good.ruleset.joins[0].model_copy(
        update={"join_path": ("public.orders.customer_id = customers.id",)}
    )
    bad_ruleset = good.ruleset.model_copy(update={"joins": (bad_rule,)})
    bad_policy = good.model_copy(update={"ruleset": bad_ruleset})
    with pytest.raises(ForcedFilterError):
        apply_session_policy(
            "SELECT * FROM public.orders",
            dialect="postgres",
            policy=bad_policy,
            has_column=_boom_probe,
        )


def test_corrupted_anchor_as_intermediate_fails_closed():
    """Defensive: a model_copy that makes the anchor appear as an intermediate
    hop (while the terminal still reaches it) bypasses construction but fails
    closed at the SQL boundary — _build_exists re-runs the full per-rule anchor
    validation, not just reachability."""
    good = SessionPolicy(ruleset=_join_ruleset())  # anchor customers, target orders
    # orders -> customers -> x -> customers: anchor 'customers' appears twice.
    bad_rule = good.ruleset.joins[0].model_copy(
        update={
            "join_path": (
                "orders.customer_id = customers.id",
                "customers.x = x.y",
                "x.z = customers.w",
            )
        }
    )
    bad_ruleset = good.ruleset.model_copy(update={"joins": (bad_rule,)})
    bad_policy = good.model_copy(update={"ruleset": bad_ruleset})
    with pytest.raises(ForcedFilterError):
        apply_session_policy(
            "SELECT * FROM orders",
            dialect="sqlite",
            policy=bad_policy,
            has_column=_boom_probe,
        )


def test_forced_filter_error_has_no_rule_name():
    """DEV-1718: ForcedFilterError dropped the rule_name param/attribute."""
    with pytest.raises(TypeError):
        ForcedFilterError("boom", rule_name="tenant")
    err = ForcedFilterError("boom", table="orders", column="org")
    assert not hasattr(err, "rule_name")


# -- joins=() anchor-only ----------------------------------------------------


def test_anchor_only_ruleset_anchor_passes():
    ruleset = JoinFilterRuleset(
        table="customers", column="organization_uuid", value="orgA",
        whitelist=["exchange_rates"],
    )
    policy = SessionPolicy(ruleset=ruleset)
    out = apply_session_policy(
        "SELECT * FROM customers", dialect="sqlite", policy=policy, has_column=_boom_probe
    )
    assert "organization_uuid = 'orgA'" in out
    assert not _exists_nodes(out)


def test_anchor_only_ruleset_whitelist_passes():
    ruleset = JoinFilterRuleset(
        table="customers", column="organization_uuid", value="orgA",
        whitelist=["exchange_rates"],
    )
    out = apply_session_policy(
        "SELECT * FROM exchange_rates",
        dialect="sqlite",
        policy=SessionPolicy(ruleset=ruleset),
        has_column=_boom_probe,
    )
    assert out == _norm("SELECT * FROM exchange_rates")


def test_anchor_only_ruleset_unlisted_fails():
    ruleset = JoinFilterRuleset(
        table="customers", column="organization_uuid", value="orgA",
    )
    policy = SessionPolicy(ruleset=ruleset)
    with pytest.raises(ForcedFilterError):
        apply_session_policy(
            "SELECT * FROM orders",
            dialect="sqlite",
            policy=policy,
            has_column=_boom_probe,
        )


# -- zero physical tables ----------------------------------------------------


def test_zero_physical_tables_passes():
    out = apply_session_policy(
        "SELECT 1", dialect="sqlite", policy=_jpolicy(), has_column=_boom_probe
    )
    assert _norm(out) == _norm("SELECT 1")


def test_cte_only_no_physical_table_passes():
    out = apply_session_policy(
        "WITH x AS (SELECT 1 AS n) SELECT * FROM x",
        dialect="sqlite",
        policy=_jpolicy(),
        has_column=_boom_probe,
    )
    assert not _exists_nodes(out)


def test_cte_reading_physical_target_is_classified():
    """A physical target inside a CTE body is still wrapped with EXISTS."""
    out = apply_session_policy(
        "WITH x AS (SELECT * FROM orders) SELECT * FROM x",
        dialect="sqlite",
        policy=_jpolicy(),
        has_column=_boom_probe,
    )
    assert len(_exists_nodes(out)) == 1


# -- table-identity matching -------------------------------------------------


def test_target_match_is_case_insensitive():
    out = apply_session_policy(
        "SELECT * FROM ORDERS",
        dialect="sqlite",
        policy=_jpolicy(),
        has_column=_boom_probe,
    )
    assert len(_exists_nodes(out)) == 1


def test_bare_target_matches_any_schema():
    out = apply_session_policy(
        "SELECT * FROM public.orders",
        dialect="postgres",
        policy=_jpolicy(),
        has_column=_boom_probe,
    )
    assert len(_exists_nodes(out, dialect="postgres")) == 1


# -- injection safety --------------------------------------------------------


def test_join_terminal_value_is_injection_safe():
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=_jpolicy(value="x' OR '1'='1"),
        has_column=_boom_probe,
    )
    reparsed = sqlglot.parse_one(out, dialect="sqlite")
    body = next(iter(reparsed.find_all(exp.Exists))).this
    assert body.find(exp.Or) is None
    assert "'x'' OR ''1''=''1'" in out


# -- ClickHouse --------------------------------------------------------------


def test_clickhouse_join_appends_settings_and_calls_hook():
    called = {"n": 0}

    def hook():
        called["n"] += 1

    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="clickhouse",
        policy=_jpolicy(),
        has_column=_boom_probe,
        on_correlated_emitted=hook,
    )
    assert "allow_experimental_correlated_subqueries" in out
    assert called["n"] == 1
    settings = sqlglot.parse_one(out, dialect="clickhouse").args.get("settings")
    assert any("allow_experimental_correlated_subqueries" in s.sql() for s in settings)


def test_clickhouse_correlated_setting_forced_on_when_disabled():
    ast = sqlglot.parse_one(
        "SELECT * FROM t SETTINGS allow_experimental_correlated_subqueries = 0",
        dialect="clickhouse",
    )
    _attach_ch_correlated_setting(ast)
    out = ast.sql(dialect="clickhouse")
    assert "allow_experimental_correlated_subqueries = 1" in out
    assert out.count("SETTINGS") == 1


def test_non_clickhouse_join_calls_hook_no_settings():
    called = {"n": 0}

    def hook():
        called["n"] += 1

    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=_jpolicy(),
        has_column=_boom_probe,
        on_correlated_emitted=hook,
    )
    assert "allow_experimental_correlated_subqueries" not in out
    assert called["n"] == 1


def test_clickhouse_column_only_does_not_append_settings_or_call_hook():
    called = {"n": 0}

    def hook():
        called["n"] += 1

    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="clickhouse",
        policy=_col_policy(column="organization_uuid", value="orgA"),
        has_column=ALWAYS,
        on_correlated_emitted=hook,
    )
    assert "allow_experimental_correlated_subqueries" not in out
    assert called["n"] == 0


def test_clickhouse_anchor_only_no_settings():
    """An anchor-only join ruleset (no EXISTS emitted) attaches no CH setting."""
    out = apply_session_policy(
        "SELECT * FROM customers",
        dialect="clickhouse",
        policy=_jpolicy(),
        has_column=_boom_probe,
    )
    assert "allow_experimental_correlated_subqueries" not in out
