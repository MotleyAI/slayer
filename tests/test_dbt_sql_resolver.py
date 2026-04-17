"""Tests for slayer.dbt.sql_resolver.resolve_refs."""

import textwrap

from slayer.dbt.sql_resolver import resolve_refs


class TestRefResolution:
    def test_ref_to_source_returns_bare_name(self) -> None:
        sql = "select * from {{ ref('claim') }}"
        resolved, warnings = resolve_refs(sql, regular_models_sql={})
        assert warnings == []
        assert resolved == "select * from claim"

    def test_ref_to_regular_model_inlines_subquery(self) -> None:
        inner = "select id, amount from raw_orders"
        sql = "select count(*) from {{ ref('orders') }} o"
        resolved, warnings = resolve_refs(
            sql,
            regular_models_sql={"orders": inner},
        )
        assert warnings == []
        # The caller's alias ``o`` sits directly after the inlined subquery.
        assert resolved == "select count(*) from (select id, amount from raw_orders) o"

    def test_transitive_refs(self) -> None:
        # C is a source table; B refs C; A refs B.
        b = "select * from {{ ref('C') }}"
        a = "select * from {{ ref('B') }}"
        outer = "select * from {{ ref('A') }} a"
        resolved, warnings = resolve_refs(
            outer,
            regular_models_sql={"A": a, "B": b},
        )
        assert warnings == []
        # Transitive inlining: the outer wraps A, which wraps B, which
        # references bare C. No AS-aliases are injected by the resolver —
        # the outer ``a`` is the only alias, supplied by the caller.
        assert resolved == "select * from (select * from (select * from C)) a"

    def test_cycle_detection(self) -> None:
        # A refs B; B refs A. The resolver must stop at the cycle and warn.
        a = "select * from {{ ref('B') }}"
        b = "select * from {{ ref('A') }}"
        outer = "select * from {{ ref('A') }}"
        resolved, warnings = resolve_refs(
            outer,
            regular_models_sql={"A": a, "B": b},
        )
        assert any("cycle detected" in w for w in warnings)
        # Final output should still be a string, not crash
        assert isinstance(resolved, str)

    def test_package_qualified_ref(self) -> None:
        sql = "select * from {{ ref('my_pkg', 'orders') }}"
        resolved, warnings = resolve_refs(sql, regular_models_sql={})
        assert warnings == []
        assert resolved == "select * from orders"

    def test_versioned_ref(self) -> None:
        sql = "select * from {{ ref('orders', v=2) }}"
        resolved, warnings = resolve_refs(sql, regular_models_sql={})
        assert warnings == []
        assert resolved == "select * from orders"


class TestConfigStripping:
    def test_strips_config_block(self) -> None:
        sql = textwrap.dedent("""\
            {{ config(materialized='table') }}
            select * from claim
        """)
        resolved, warnings = resolve_refs(sql, regular_models_sql={})
        assert warnings == []
        assert "config" not in resolved
        assert "select * from claim" in resolved

    def test_strips_multiline_config(self) -> None:
        # Single-arg "fits on a line" — config() with comma-separated kwargs
        sql = "{{ config(materialized='table', schema='staging') }}\nselect 1"
        resolved, warnings = resolve_refs(sql, regular_models_sql={})
        assert warnings == []
        assert "config" not in resolved


class TestSourceResolution:
    def test_source_produces_schema_dot_table(self) -> None:
        sql = "select * from {{ source('raw', 'events') }}"
        resolved, warnings = resolve_refs(sql, regular_models_sql={})
        assert warnings == []
        assert resolved == "select * from raw.events"


class TestSecurityAndDiagnostics:
    def test_injection_attempt_does_not_substitute(self) -> None:
        # Semicolons/spaces don't match \\w+ so the regex fails. The Jinja
        # block is left in place and a warning is emitted — never is the
        # hostile payload spliced into SQL.
        sql = "select * from {{ ref('a; DROP TABLE users; --') }}"
        resolved, warnings = resolve_refs(sql, regular_models_sql={})
        assert "DROP TABLE" not in resolved or "{{" in resolved  # still wrapped in {{ }}
        assert "{{ ref(" in resolved
        assert any("unresolved Jinja" in w for w in warnings)

    def test_unknown_macro_emits_warning(self) -> None:
        sql = "select {{ my_custom_macro() }} from t"
        resolved, warnings = resolve_refs(sql, regular_models_sql={})
        # Macro is preserved verbatim
        assert "{{ my_custom_macro() }}" in resolved
        assert any("my_custom_macro" in w and "unresolved" in w for w in warnings)

    def test_max_depth_cap_prevents_runaway(self) -> None:
        # Construct a pathological chain longer than max_depth.
        regular_models = {f"m{i}": f"select * from {{{{ ref('m{i+1}') }}}}" for i in range(20)}
        regular_models["m20"] = "select 1"
        outer = "select * from {{ ref('m0') }}"
        resolved, warnings = resolve_refs(
            outer,
            regular_models_sql=regular_models,
            max_depth=3,
        )
        assert any("max recursion depth" in w for w in warnings)
        assert isinstance(resolved, str)

    def test_no_jinja_pass_through(self) -> None:
        # Input has no Jinja at all — output is identical, no warnings.
        sql = "select a, b from t where a > 0"
        resolved, warnings = resolve_refs(sql, regular_models_sql={})
        assert resolved == sql
        assert warnings == []
