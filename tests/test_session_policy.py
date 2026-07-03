"""Unit tests for the forced-filter SQL rewrite (DEV-1578).

``apply_session_policy`` is a pure sqlglot transform: given final SQL, a
dialect, a ``SessionPolicy``, and a ``has_column`` probe callback, it wraps
every *physical* table reference whose configured column(s) apply in a
filtered ``SELECT * ... WHERE`` subquery, preserving the original alias.
These tests use a fake ``has_column`` so no database is touched.

``has_column`` contract: returns ``True`` (column present), ``False``
(table confirmed to lack the column), or ``None`` (presence cannot be
confirmed -> fail closed regardless of ``on_unapplicable``).
"""

import sqlglot
import pytest
from sqlglot import exp

from slayer.core.errors import ForcedFilterError
from slayer.core.policy import (
    ColumnFilterRule,
    JoinFilterRule,
    JoinHop,
    SessionPolicy,
)
from slayer.sql.session_policy import (
    ScopedTable,
    _attach_ch_correlated_setting,
    apply_session_policy,
)


def _norm(sql: str, dialect: str = "sqlite") -> str:
    """Round-trip ``sql`` through sqlglot so identity comparisons ignore
    cosmetic formatting differences."""
    return sqlglot.parse_one(sql, dialect=dialect).sql(dialect=dialect)


def has_column_factory(tables):
    """Build a fake ``has_column``.

    ``tables`` maps table name -> set of column names (presence is True/False
    for known tables), or table name -> None to model "cannot confirm".
    A table not in the map also yields None (unknown -> fail closed).
    """

    def has_column(scoped: ScopedTable, column: str):
        entry = tables.get(scoped.name, "missing")
        if entry == "missing" or entry is None:
            return None
        return column in entry

    return has_column


ALWAYS = lambda scoped, column: True  # noqa: E731


# -- core operator shapes ----------------------------------------------------


def test_scalar_value_emits_equality():
    policy = SessionPolicy(
        data_filters=[ColumnFilterRule(column="org", value="7ef3")]
    )
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders WHERE org = '7ef3') AS orders"
    )


def test_list_value_emits_in():
    policy = SessionPolicy(
        data_filters=[ColumnFilterRule(column="org", value=["a", "b"])]
    )
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders WHERE org IN ('a', 'b')) AS orders"
    )


def test_tuple_value_emits_in():
    """An already-tuple value (post-validation shape) emits IN."""
    policy = SessionPolicy(
        data_filters=[ColumnFilterRule(column="org", value=("a", "b"))]
    )
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders WHERE org IN ('a', 'b')) AS orders"
    )


def test_bool_value_emits_boolean_literal():
    policy = SessionPolicy(
        data_filters=[ColumnFilterRule(column="is_active", value=True)]
    )
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders WHERE is_active = TRUE) AS orders"
    )


def test_float_value_emits_numeric_literal():
    policy = SessionPolicy(
        data_filters=[ColumnFilterRule(column="ratio", value=3.5)]
    )
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders WHERE ratio = 3.5) AS orders"
    )


def test_multi_rule_composes_with_and():
    policy = SessionPolicy(
        data_filters=[
            ColumnFilterRule(column="org", value="x"),
            ColumnFilterRule(column="tenant", value=1),
        ]
    )
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders "
        "WHERE org = 'x' AND tenant = 1) AS orders"
    )


# -- table-reference handling ------------------------------------------------


def test_joined_table_wrapped_alias_preserved():
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    out = apply_session_policy(
        "SELECT * FROM customers c LEFT JOIN orders o ON c.id = o.customer_id",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM customers WHERE org = 'x') AS c "
        "LEFT JOIN (SELECT * FROM orders WHERE org = 'x') AS o "
        "ON c.id = o.customer_id"
    )


def test_self_join_each_occurrence_wrapped_once():
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    out = apply_session_policy(
        "SELECT * FROM orders a JOIN orders b ON a.id = b.id",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders WHERE org = 'x') AS a "
        "JOIN (SELECT * FROM orders WHERE org = 'x') AS b ON a.id = b.id"
    )
    # exactly two wraps, no double-wrapping
    assert out.count("WHERE org = 'x'") == 2


def test_schema_qualified_table_preserved():
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    out = apply_session_policy(
        "SELECT * FROM public.orders",
        dialect="postgres",
        policy=policy,
        has_column=ALWAYS,
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM public.orders WHERE org = 'x') AS orders",
        dialect="postgres",
    )


def test_schema_passed_to_has_column():
    seen = {}

    def has_column(scoped, column):
        seen["scoped"] = scoped
        return True

    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    apply_session_policy(
        "SELECT * FROM public.orders",
        dialect="postgres",
        policy=policy,
        has_column=has_column,
    )
    assert seen["scoped"].name == "orders"
    assert seen["scoped"].schema_name == "public"


def test_sql_mode_inner_table_wrapped_outer_subquery_untouched():
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    out = apply_session_policy(
        "SELECT * FROM (SELECT * FROM raw_tbl) AS m",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM "
        "(SELECT * FROM raw_tbl WHERE org = 'x') AS raw_tbl) AS m"
    )


# -- CTE / scope handling ----------------------------------------------------


def test_cte_reference_skipped_physical_wrapped():
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    out = apply_session_policy(
        "WITH _cm_x AS (SELECT * FROM customers) "
        "SELECT * FROM orders LEFT JOIN _cm_x ON orders.id = _cm_x.id",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    # customers (inside CTE) and orders are physical -> wrapped; _cm_x ref is not
    assert out.count("WHERE org = 'x'") == 2
    assert "FROM (SELECT * FROM customers WHERE org = 'x') AS customers" in out
    assert "FROM (SELECT * FROM orders WHERE org = 'x') AS orders" in out
    # the CTE reference itself stays a bare identifier
    assert "_cm_x ON orders" in out


def test_collision_physical_inside_cte_is_wrapped():
    """A physical table sharing a CTE's name (inside the CTE body) is still
    wrapped; only the genuine CTE reference is skipped (scope-aware)."""
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    out = apply_session_policy(
        "WITH orders AS (SELECT * FROM orders) SELECT * FROM orders",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    assert out == _norm(
        "WITH orders AS (SELECT * FROM "
        "(SELECT * FROM orders WHERE org = 'x') AS orders) "
        "SELECT * FROM orders"
    )
    assert out.count("WHERE org = 'x'") == 1


def test_chained_ctes_not_failed():
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    out = apply_session_policy(
        "WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM a JOIN t2 ON 1 = 1) "
        "SELECT * FROM b",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    # t1 and t2 are physical; a and b are CTE refs
    assert out.count("WHERE org = 'x'") == 2
    assert "(SELECT * FROM t1 WHERE org = 'x') AS t1" in out
    assert "(SELECT * FROM t2 WHERE org = 'x') AS t2" in out


@pytest.mark.parametrize("setop", ["UNION ALL", "UNION", "INTERSECT", "EXCEPT"])
def test_set_operation_both_branches_wrapped(setop):
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    out = apply_session_policy(
        f"SELECT id FROM orders {setop} SELECT id FROM archived_orders",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    assert out.count("WHERE org = 'x'") == 2
    assert "(SELECT * FROM orders WHERE org = 'x') AS orders" in out
    assert "(SELECT * FROM archived_orders WHERE org = 'x') AS archived_orders" in out


def test_catalog_qualified_table_passed_to_has_column():
    seen = {}

    def has_column(scoped, column):
        seen["scoped"] = scoped
        return True

    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    out = apply_session_policy(
        "SELECT * FROM proj.dataset.tbl",
        dialect="bigquery",
        policy=policy,
        has_column=has_column,
    )
    assert seen["scoped"].catalog == "proj"
    assert seen["scoped"].schema_name == "dataset"
    assert seen["scoped"].name == "tbl"
    assert "WHERE org = 'x'" in out


# -- on_unapplicable / fail-closed -------------------------------------------


def test_block_raises_naming_table_and_rule():
    policy = SessionPolicy(
        data_filters=[ColumnFilterRule(name="tenant", column="org", value="x")]
    )
    has_column = has_column_factory({"exchange_rates": {"rate", "day"}})
    with pytest.raises(ForcedFilterError) as exc:
        apply_session_policy(
            "SELECT * FROM exchange_rates",
            dialect="sqlite",
            policy=policy,
            has_column=has_column,
        )
    assert exc.value.table == "exchange_rates"
    assert exc.value.rule_name == "tenant"
    assert exc.value.column == "org"
    assert "exchange_rates" in str(exc.value)
    assert "tenant" in str(exc.value)


def test_pass_leaves_table_unfiltered():
    policy = SessionPolicy(
        data_filters=[
            ColumnFilterRule(column="org", value="x", on_unapplicable="pass")
        ]
    )
    has_column = has_column_factory({"exchange_rates": {"rate", "day"}})
    out = apply_session_policy(
        "SELECT * FROM exchange_rates",
        dialect="sqlite",
        policy=policy,
        has_column=has_column,
    )
    assert out == _norm("SELECT * FROM exchange_rates")
    assert "WHERE" not in out.upper()


def test_pass_rule_skipped_other_rule_applied():
    """Per-rule on_unapplicable: a 'pass' rule that doesn't apply is skipped,
    while a different rule that does apply still wraps the table."""
    policy = SessionPolicy(
        data_filters=[
            ColumnFilterRule(column="org", value="x"),  # applies
            ColumnFilterRule(
                column="missing_col", value="y", on_unapplicable="pass"
            ),  # skipped
        ]
    )
    has_column = has_column_factory({"orders": {"org", "id"}})
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=policy,
        has_column=has_column,
    )
    assert out == _norm(
        "SELECT * FROM (SELECT * FROM orders WHERE org = 'x') AS orders"
    )


def test_block_rule_fails_even_when_other_rule_applies():
    """A 'block' rule whose column is absent fails the whole query, even
    though another rule applied to the same table."""
    policy = SessionPolicy(
        data_filters=[
            ColumnFilterRule(column="org", value="x"),  # applies
            ColumnFilterRule(column="missing_col", value="y"),  # block (default)
        ]
    )
    has_column = has_column_factory({"orders": {"org", "id"}})
    with pytest.raises(ForcedFilterError):
        apply_session_policy(
            "SELECT * FROM orders",
            dialect="sqlite",
            policy=policy,
            has_column=has_column,
        )


def test_none_presence_fails_closed_even_with_pass():
    policy = SessionPolicy(
        data_filters=[
            ColumnFilterRule(column="org", value="x", on_unapplicable="pass")
        ]
    )
    has_column = has_column_factory({"orders": None})  # cannot confirm
    with pytest.raises(ForcedFilterError) as exc:
        apply_session_policy(
            "SELECT * FROM orders",
            dialect="sqlite",
            policy=policy,
            has_column=has_column,
        )
    assert exc.value.table == "orders"


def test_unknown_table_fails_closed():
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    has_column = has_column_factory({})  # nothing known -> None
    with pytest.raises(ForcedFilterError):
        apply_session_policy(
            "SELECT * FROM orders",
            dialect="sqlite",
            policy=policy,
            has_column=has_column,
        )


# -- root guard --------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO orders (id) VALUES (1)",
        "UPDATE orders SET amount = 0",
        "DELETE FROM orders WHERE id = 1",
    ],
)
def test_non_select_root_fails_closed(sql):
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    with pytest.raises(ForcedFilterError):
        apply_session_policy(
            sql, dialect="sqlite", policy=policy, has_column=ALWAYS
        )


# -- empty policy / identity -------------------------------------------------


def test_empty_policy_is_identity_no_parse():
    policy = SessionPolicy()
    sql = "SELECT  *  FROM   orders"  # deliberately odd whitespace
    out = apply_session_policy(
        sql, dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    # returned verbatim (no parse/regenerate), proving zero-overhead skip
    assert out == sql


def test_empty_policy_does_not_probe():
    called = {"n": 0}

    def has_column(scoped, column):
        called["n"] += 1
        return True

    apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=SessionPolicy(),
        has_column=has_column,
    )
    assert called["n"] == 0


# -- injection safety --------------------------------------------------------


def test_value_literal_is_injection_safe():
    from sqlglot import exp

    policy = SessionPolicy(
        data_filters=[ColumnFilterRule(column="org", value="x' OR '1'='1")]
    )
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    # the value is a single quoted literal with the quote escaped, NOT raw SQL
    assert "WHERE org = 'x'' OR ''1''=''1'" in out
    # structurally: the predicate is one EQ to a string literal — no injected
    # OR node leaks into the AST
    reparsed = sqlglot.parse_one(out, dialect="sqlite")
    assert reparsed.find(exp.Or) is None
    eq = reparsed.find(exp.EQ)
    assert isinstance(eq.expression, exp.Literal)
    assert eq.expression.this == "x' OR '1'='1"


# -- bigquery best-effort ----------------------------------------------------


def test_bigquery_dialect_wraps():
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="bigquery", policy=policy, has_column=ALWAYS
    )
    assert "WHERE org = 'x'" in out
    # round-trips as valid bigquery SQL
    assert sqlglot.parse_one(out, dialect="bigquery") is not None


# ===========================================================================
# JoinFilterRule — correlated-EXISTS rewrite (DEV-1627)
# ===========================================================================


def _hop(**kw):
    base = {
        "from_table": "orders",
        "from_column": "customer_id",
        "to_table": "customers",
        "to_column": "id",
    }
    base.update(kw)
    return JoinHop(**base)


def _single_hop_rule(**kw):
    base = {
        "target_table": "orders",
        "join_path": [_hop()],
        "column": "organization_uuid",
        "value": "orgA",
    }
    base.update(kw)
    return JoinFilterRule(**base)


# Mandatory block backstop (DEV-1627, decision 5): a policy with any join rule
# must also carry a block column rule. In single-target tests the target table
# overrides this column rule (never consulted), so behaviour is unchanged.
_BACKSTOP = ColumnFilterRule(name="backstop", column="organization_uuid", value="orgA")


def _jpolicy(*rules):
    """SessionPolicy carrying the mandatory block backstop plus the join
    rule(s) under test."""
    return SessionPolicy(data_filters=[_BACKSTOP, *rules])


def _exists_nodes(sql, dialect="sqlite"):
    return list(sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Exists))


# -- basic shape -------------------------------------------------------------


def test_single_hop_emits_correlated_exists():
    policy = _jpolicy(_single_hop_rule())
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,  # never consulted for a join-targeted table
    )
    parsed = sqlglot.parse_one(out, dialect="sqlite")
    exists = parsed.find_all(exp.Exists)
    exists = list(exists)
    assert len(exists) == 1
    body = exists[0].this
    # EXISTS body selects from the first hop's to_table (customers)
    assert body.find(exp.Table).name == "customers"
    # terminal tenant predicate is present
    assert "organization_uuid = 'orgA'" in out
    # correlation: the wrapped table's from_column appears in the EXISTS body
    assert "customer_id" in body.sql()
    # semi-join, not a cardinality-changing JOIN in the outer query
    assert parsed.find(exp.Join) is None
    # outer wrap alias preserved
    assert out.rstrip().endswith("AS orders")


def test_join_rule_does_not_consult_has_column():
    """Join-targeted tables need no column-presence probe (override)."""
    called = {"n": 0}

    def has_column(scoped, column):
        called["n"] += 1
        return True

    policy = _jpolicy(_single_hop_rule())
    apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=policy,
        has_column=has_column,
    )
    assert called["n"] == 0


def test_single_hop_scalar_terminal_is_equality():
    policy = _jpolicy(_single_hop_rule(value="orgA"))
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    assert "organization_uuid = 'orgA'" in out
    assert " IN " not in out.upper()


def test_single_hop_list_terminal_is_in():
    policy = _jpolicy(_single_hop_rule(value=["orgA", "orgB"]))
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    body = _exists_nodes(out)[0].this
    in_node = body.find(exp.In)
    assert in_node is not None
    assert {e.this for e in in_node.expressions} == {"orgA", "orgB"}


def test_multihop_emits_chained_joins_terminal_on_last():
    rule = JoinFilterRule(
        target_table="line_items",
        join_path=[
            JoinHop(
                from_table="line_items",
                from_column="order_id",
                to_table="orders",
                to_column="id",
            ),
            JoinHop(
                from_table="orders",
                from_column="customer_id",
                to_table="customers",
                to_column="id",
            ),
        ],
        column="organization_uuid",
        value="orgA",
    )
    policy = _jpolicy(rule)
    out = apply_session_policy(
        "SELECT * FROM line_items",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    body = _exists_nodes(out)[0].this
    # body has an inner JOIN for the second hop
    assert body.find(exp.Join) is not None
    # body references all three physical tables
    body_tables = {t.name for t in body.find_all(exp.Table)}
    assert body_tables == {"orders", "customers"}
    # first hop table + line_items correlation are present
    assert "order_id" in body.sql()
    # terminal predicate lives on the LAST hop's table (customers), structurally
    customers_tbl = next(t for t in body.find_all(exp.Table) if t.name == "customers")
    last_alias = customers_tbl.alias_or_name
    term = next(
        eq
        for eq in body.find_all(exp.EQ)
        if isinstance(eq.expression, exp.Literal) and eq.expression.this == "orgA"
    )
    assert isinstance(term.this, exp.Column)
    assert term.this.name == "organization_uuid"
    assert term.this.table == last_alias  # NOT the intermediate "orders" hop
    # still a semi-join on the outer query: the multihop JOIN lives INSIDE the
    # EXISTS body, never at the outer query level (the chained hop join is a
    # correlated-subquery detail, not a cardinality-changing outer join).
    outer = sqlglot.parse_one(out, dialect="sqlite")
    assert not outer.args.get("joins")
    assert out.rstrip().endswith("AS line_items")


def test_multiple_join_rules_same_table_and_combined():
    """Two join rules targeting the same table emit two EXISTS predicates,
    AND-combined in the wrapper WHERE."""
    rule_a = _single_hop_rule(name="by_customer")
    rule_b = JoinFilterRule(
        name="by_region",
        target_table="orders",
        join_path=[
            JoinHop(
                from_table="orders",
                from_column="region_id",
                to_table="regions",
                to_column="id",
            )
        ],
        column="organization_uuid",
        value="orgA",
    )
    policy = _jpolicy(rule_a, rule_b)
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    parsed = sqlglot.parse_one(out, dialect="sqlite")
    exists = list(parsed.find_all(exp.Exists))
    assert len(exists) == 2  # one per rule, AND-combined
    body_tables = {t.name for e in exists for t in e.this.find_all(exp.Table)}
    assert body_tables == {"customers", "regions"}


# -- override precedence -----------------------------------------------------


def test_join_rule_overrides_column_rule_on_target_table():
    """A table targeted by a join rule is scoped ONLY by the join (no column
    wrap), even when a column rule would otherwise apply."""
    policy = SessionPolicy(
        data_filters=[
            ColumnFilterRule(column="organization_uuid", value="orgA"),
            _single_hop_rule(),
        ]
    )
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    # the join EXISTS is emitted
    assert len(_exists_nodes(out)) == 1
    # the column rule did NOT also wrap orders with a bare WHERE org = ...
    # (only the terminal predicate inside EXISTS carries organization_uuid)
    assert out.count("organization_uuid = 'orgA'") == 1


def test_left_join_targeted_table_on_right_side_preserved():
    """A join-targeted table on the nullable/right side of a LEFT JOIN stays a
    LEFT JOIN in the outer query (wrapped, not converted to INNER)."""
    policy = _jpolicy(_single_hop_rule(target_table="orders"))
    out = apply_session_policy(
        "SELECT * FROM customers c LEFT JOIN orders o ON o.customer_id = c.id",
        dialect="sqlite",
        policy=policy,
        has_column=lambda scoped, column: True,
    )
    parsed = sqlglot.parse_one(out, dialect="sqlite")
    join = parsed.find(exp.Join)
    assert (join.args.get("side") or "").upper() == "LEFT"  # not INNER
    # orders wrapped with EXISTS on the right side
    assert len(list(parsed.find_all(exp.Exists))) == 1


def test_sql_mode_inner_target_wrapped_outer_untouched():
    """A join-targeted physical table nested inside a query-backed/sql-mode
    subquery is wrapped with EXISTS; the outer alias is left alone."""
    policy = _jpolicy(_single_hop_rule(target_table="orders"))
    out = apply_session_policy(
        "SELECT * FROM (SELECT * FROM orders) AS m",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    assert len(_exists_nodes(out)) == 1
    assert out.rstrip().endswith("AS m")  # outer query-backed alias preserved


def test_sibling_table_still_column_filtered():
    """A non-targeted sibling in the same query still gets the column wrap."""
    policy = SessionPolicy(
        data_filters=[
            ColumnFilterRule(column="organization_uuid", value="orgA"),
            _single_hop_rule(),  # targets orders only
        ]
    )
    out = apply_session_policy(
        "SELECT * FROM orders o "
        "LEFT JOIN customers c ON c.id = o.customer_id",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    # orders -> EXISTS; customers (the JOINed sibling) -> plain column wrap
    assert len(_exists_nodes(out)) == 1
    assert (
        "(SELECT * FROM customers WHERE organization_uuid = 'orgA') AS c"
        in out
    )


def test_untargeted_columnless_table_still_blocks():
    """The column-rule block backstop still fires for a table that is neither
    targeted by a join rule nor has the column."""
    policy = SessionPolicy(
        data_filters=[
            ColumnFilterRule(name="tenant", column="organization_uuid", value="orgA"),
            _single_hop_rule(),  # targets orders, not exchange_rates
        ]
    )
    has_column = has_column_factory({"exchange_rates": {"rate", "day"}})
    with pytest.raises(ForcedFilterError) as exc:
        apply_session_policy(
            "SELECT * FROM exchange_rates",
            dialect="sqlite",
            policy=policy,
            has_column=has_column,
        )
    assert exc.value.table == "exchange_rates"


# -- table-identity matching -------------------------------------------------


def test_target_match_is_case_insensitive():
    policy = _jpolicy(_single_hop_rule(target_table="ORDERS"))
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    assert len(_exists_nodes(out)) == 1


def test_qualified_target_matches_only_matching_schema():
    """A schema-qualified target matches the same-schema table only."""
    rule = _single_hop_rule(
        target_table="public.orders",
        join_path=[_hop(from_table="public.orders")],
    )
    policy = _jpolicy(rule)
    # matching schema -> EXISTS emitted
    out_match = apply_session_policy(
        "SELECT * FROM public.orders",
        dialect="postgres",
        policy=policy,
        has_column=ALWAYS,
    )
    assert len(_exists_nodes(out_match, dialect="postgres")) == 1


def test_qualified_target_does_not_match_other_schema():
    """A public.orders rule must not fire on archive.orders (Codex #1)."""
    rule = _single_hop_rule(
        target_table="public.orders",
        join_path=[_hop(from_table="public.orders")],
    )
    policy = _jpolicy(rule)
    # different schema: join rule doesn't match; column path applies instead.
    # has_column confirms the column is absent -> block (proves the join rule
    # did NOT silently scope archive.orders).
    has_column = has_column_factory({"orders": {"id", "customer_id"}})
    with pytest.raises(ForcedFilterError):
        apply_session_policy(
            "SELECT * FROM archive.orders",
            dialect="postgres",
            policy=policy,
            has_column=has_column,
        )


def test_bare_target_matches_any_schema():
    """A bare target name matches the table in any schema."""
    policy = _jpolicy(_single_hop_rule(target_table="orders"))
    out = apply_session_policy(
        "SELECT * FROM public.orders",
        dialect="postgres",
        policy=policy,
        has_column=ALWAYS,
    )
    assert len(_exists_nodes(out, dialect="postgres")) == 1


# -- repeated physical tables (diamond joins / self-joins) -------------------


def test_diamond_repeated_table_each_occurrence_gets_own_exists():
    """The same physical table reached via two path-aliases is wrapped twice,
    each with an independently-correlated EXISTS (deterministic aliasing)."""
    policy = _jpolicy(_single_hop_rule(target_table="orders"))
    out = apply_session_policy(
        "SELECT * FROM orders a JOIN orders b ON a.id = b.id",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    parsed = sqlglot.parse_one(out, dialect="sqlite")
    # two EXISTS (one per occurrence), both outer aliases preserved
    assert len(list(parsed.find_all(exp.Exists))) == 2
    assert out.count("AS a") >= 1
    assert "AS b" in out
    # Each wrapped occurrence carries its OWN EXISTS, lexically scoped to its
    # own inner base table (correlation can't cross into the sibling wrap).
    subqueries = [
        s
        for s in parsed.find_all(exp.Subquery)
        if s.this.find(exp.Exists) is not None
    ]
    assert len(subqueries) == 2
    for sub in subqueries:
        # the correlated EXISTS lives inside this subquery's own scope
        exists = sub.this.find(exp.Exists)
        assert exists is not None
        # the subquery wraps a single physical `orders`
        assert sub.this.find(exp.Table).name == "orders"
    # SQL re-parses cleanly (no ambiguous correlation)
    assert parsed is not None


# -- CTE coverage ------------------------------------------------------------


def test_join_rule_fires_inside_cte():
    """A physical target table inside a CTE body is wrapped with EXISTS too."""
    policy = _jpolicy(_single_hop_rule(target_table="orders"))
    out = apply_session_policy(
        "WITH _cm_x AS (SELECT * FROM orders) SELECT * FROM _cm_x",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
    )
    assert len(_exists_nodes(out)) == 1
    # the CTE reference itself is not wrapped
    assert "organization_uuid = 'orgA'" in out


# -- injection safety --------------------------------------------------------


def test_join_terminal_value_is_injection_safe():
    policy = _jpolicy(_single_hop_rule(value="x' OR '1'='1"))
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    reparsed = sqlglot.parse_one(out, dialect="sqlite")
    # no injected OR leaks into the AST; the value is a single quoted literal
    body = next(iter(reparsed.find_all(exp.Exists))).this
    assert body.find(exp.Or) is None
    assert "'x'' OR ''1''=''1'" in out


def test_join_identifiers_are_structural_not_raw():
    """A hop/column identifier containing a dot is quoted as one identifier,
    never spliced into the SQL as a qualified path."""
    rule = _single_hop_rule(
        join_path=[_hop(to_column="weird.id")],
    )
    policy = _jpolicy(rule)
    out = apply_session_policy(
        "SELECT * FROM orders", dialect="sqlite", policy=policy, has_column=ALWAYS
    )
    # emitted as a single quoted identifier, not as table.column
    assert '"weird.id"' in out


# -- ClickHouse correlated-subquery handling ---------------------------------


def test_clickhouse_join_appends_settings_and_calls_hook():
    called = {"n": 0}

    def hook():
        called["n"] += 1

    policy = _jpolicy(_single_hop_rule())
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="clickhouse",
        policy=policy,
        has_column=ALWAYS,
        on_correlated_emitted=hook,
    )
    assert "allow_experimental_correlated_subqueries" in out
    assert called["n"] == 1
    # structurally valid ClickHouse SQL with the setting attached at the
    # statement level (not spliced into a comment/wrong clause)
    parsed = sqlglot.parse_one(out, dialect="clickhouse")
    settings = parsed.args.get("settings")
    assert settings
    assert any(
        "allow_experimental_correlated_subqueries" in s.sql() for s in settings
    )


def test_clickhouse_join_preserves_existing_settings():
    """A pre-existing SETTINGS clause (e.g. from a raw sql-mode model) is kept;
    the correlated-subquery setting is appended, not substituted."""
    policy = _jpolicy(_single_hop_rule())
    out = apply_session_policy(
        "SELECT * FROM orders SETTINGS max_threads = 2",
        dialect="clickhouse",
        policy=policy,
        has_column=ALWAYS,
    )
    settings = sqlglot.parse_one(out, dialect="clickhouse").args.get("settings")
    joined = " ".join(s.sql() for s in settings)
    assert "max_threads" in joined  # existing setting preserved
    assert "allow_experimental_correlated_subqueries" in joined  # ours appended


def test_clickhouse_correlated_setting_forced_on_when_disabled():
    """An input that explicitly disables the setting (= 0) is overridden to
    = 1 — a correlated subquery is never emitted with it left off."""
    ast = sqlglot.parse_one(
        "SELECT * FROM t SETTINGS allow_experimental_correlated_subqueries = 0",
        dialect="clickhouse",
    )
    _attach_ch_correlated_setting(ast)
    out = ast.sql(dialect="clickhouse")
    assert "allow_experimental_correlated_subqueries = 1" in out
    assert "= 0" not in out
    assert out.count("SETTINGS") == 1


def test_clickhouse_correlated_setting_single_clause_on_union():
    """A trailing SETTINGS on a UNION's last SELECT stays a single SETTINGS
    clause (no duplicate) when our setting is appended."""
    ast = sqlglot.parse_one(
        "SELECT 1 FROM t UNION ALL SELECT 2 FROM u SETTINGS max_threads = 2",
        dialect="clickhouse",
    )
    _attach_ch_correlated_setting(ast)
    out = ast.sql(dialect="clickhouse")
    assert out.count("SETTINGS") == 1
    assert "max_threads = 2" in out
    assert "allow_experimental_correlated_subqueries = 1" in out
    assert sqlglot.parse_one(out, dialect="clickhouse") is not None  # re-parses


def test_clickhouse_correlated_setting_targets_outer_not_nested_subquery():
    """A SETTINGS clause on a subquery nested in a UNION branch's FROM is NOT
    where the statement-level flag belongs — it must land on the outer union so
    the correlated EXISTS actually runs with the setting enabled."""
    ast = sqlglot.parse_one(
        "SELECT * FROM orders UNION ALL "
        "SELECT * FROM (SELECT * FROM x SETTINGS max_threads = 2) s",
        dialect="clickhouse",
    )
    _attach_ch_correlated_setting(ast)
    # the OUTER union statement (not the nested subquery) carries our setting
    outer_settings = ast.args.get("settings") or []
    assert any(
        "allow_experimental_correlated_subqueries" in s.sql() for s in outer_settings
    )
    # the nested subquery's own SETTINGS is untouched (still just max_threads)
    nested = ast.find(exp.Subquery).this
    nested_settings = " ".join(s.sql() for s in (nested.args.get("settings") or []))
    assert "max_threads" in nested_settings
    assert "allow_experimental_correlated_subqueries" not in nested_settings
    assert sqlglot.parse_one(ast.sql(dialect="clickhouse"), dialect="clickhouse")


def test_non_clickhouse_join_calls_hook_no_settings():
    """The hook fires on any dialect when an EXISTS is emitted, but the
    SETTINGS clause is ClickHouse-only."""
    called = {"n": 0}

    def hook():
        called["n"] += 1

    policy = _jpolicy(_single_hop_rule())
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="sqlite",
        policy=policy,
        has_column=ALWAYS,
        on_correlated_emitted=hook,
    )
    assert "allow_experimental_correlated_subqueries" not in out
    assert called["n"] == 1


def test_clickhouse_column_only_does_not_append_settings_or_call_hook():
    called = {"n": 0}

    def hook():
        called["n"] += 1

    policy = SessionPolicy(
        data_filters=[ColumnFilterRule(column="organization_uuid", value="orgA")]
    )
    out = apply_session_policy(
        "SELECT * FROM orders",
        dialect="clickhouse",
        policy=policy,
        has_column=ALWAYS,
        on_correlated_emitted=hook,
    )
    assert "allow_experimental_correlated_subqueries" not in out
    assert called["n"] == 0
