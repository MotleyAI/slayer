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

from slayer.core.errors import ForcedFilterError
from slayer.core.policy import ColumnFilterRule, SessionPolicy
from slayer.sql.session_policy import ScopedTable, apply_session_policy


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
