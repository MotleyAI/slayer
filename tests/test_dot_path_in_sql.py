"""Stage 6 carry-over (DEV-1450) — DOT_PATH_IN_SQL slack rule.

AST-based, scope-aware rewrite of root-scope dotted refs in Mode-A surfaces.

This is the sole multi-dot normalization mechanism: the legacy
``slayer.core.models._fix_multidot_sql`` pydantic construction-time regex
validator was removed in the 7b.15 cutover. Mode-A multi-dot refs
(``customers.regions.name``) are now rewritten to the ``__`` alias form
(``customers__regions.name``) only when a model/query flows through
``normalize_model`` / ``normalize_query`` at the engine boundary — never at
plain pydantic construction. The helper-level tests call
``_apply_dot_path_in_sql`` directly; the wiring tests go through
``normalize_model``.
"""

from __future__ import annotations

import warnings as wmod

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.warnings import SlayerNormalizationWarning
from slayer.engine.normalization import (
    _apply_dot_path_in_sql,
    normalize_model,
    normalize_query,
)


def _orders_with_customers_join() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


# ---------------------------------------------------------------------------
# Helper-level tests — exercise the rule directly
# ---------------------------------------------------------------------------


class TestDotPathInSqlHelper:
    def test_three_segment_rewrite(self):
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            "customers.regions.name", location="cols[0].sql", model=m,
        )
        assert "customers__regions" in rewritten
        assert "customers.regions" not in rewritten
        assert len(warnings) == 1
        w = warnings[0]
        assert w.rule_id == "DOT_PATH_IN_SQL"
        assert w.original == "customers.regions.name"
        assert w.normalized == "customers__regions.name"
        assert w.location == "cols[0].sql"

    def test_four_segment_rewrite(self):
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            "customers.regions.cities.name", location="(test)", model=m,
        )
        # The intermediate hops collapse with __; leaf segment stays after
        # a single dot.
        assert "customers__regions__cities.name" in rewritten
        assert "customers.regions.cities" not in rewritten
        assert len(warnings) == 1
        assert warnings[0].original == "customers.regions.cities.name"
        assert warnings[0].normalized == "customers__regions__cities.name"

    def test_two_segment_left_alone(self):
        # Single-hop refs (table.col) are already canonical.
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            "customers.name", location="(test)", model=m,
        )
        assert rewritten == "customers.name"
        assert warnings == []

    def test_unknown_first_segment_left_alone(self):
        # First segment is not a known join target on the host model — could
        # be a real catalog/schema reference. Leave alone.
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            "myschema.orders.col", location="(test)", model=m,
        )
        assert rewritten == "myschema.orders.col"
        assert warnings == []

    def test_already_underscore_form_unchanged(self):
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            "customers__regions.name", location="(test)", model=m,
        )
        assert rewritten == "customers__regions.name"
        assert warnings == []

    def test_bare_name_unchanged(self):
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            "amount", location="(test)", model=m,
        )
        assert rewritten == "amount"
        assert warnings == []

    def test_function_wrapping_rewrite_inner(self):
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            "lower(customers.regions.name)", location="(test)", model=m,
        )
        assert "customers__regions.name" in rewritten
        assert "lower" in rewritten.lower()
        assert len(warnings) == 1

    def test_string_literal_path_not_rewritten(self):
        # The dotted form lives inside a string literal — sqlglot does not
        # parse it as a Column, so the rule must not touch it.
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            "concat('customers.regions.name', amount)",
            location="(test)", model=m,
        )
        assert "'customers.regions.name'" in rewritten
        assert warnings == []

    def test_subquery_ref_left_alone(self):
        # Refs inside a subquery scope are not root-scope and must stay.
        m = _orders_with_customers_join()
        sql_text = "(SELECT customers.regions.name FROM customers)"
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(test)", model=m,
        )
        # Inner sub-query content stays unrewritten; the surrounding text may
        # be reformatted by sqlglot, but the join-path text inside the
        # sub-query must NOT be collapsed into __.
        assert "customers.regions" in rewritten
        assert "customers__regions" not in rewritten
        assert warnings == []

    def test_case_when_inner_rewrites(self):
        # Top-level CASE WHEN is root-scope; refs inside its arms are
        # root-scope too. The rewrite fires.
        m = _orders_with_customers_join()
        sql_text = (
            "CASE WHEN customers.regions.name = 'EU' THEN amount ELSE 0 END"
        )
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(test)", model=m,
        )
        assert "customers__regions.name" in rewritten
        assert len(warnings) == 1

    def test_no_joins_no_op(self):
        m = SlayerModel(
            name="orders", data_source="prod", sql_table="orders",
            columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        )
        rewritten, warnings = _apply_dot_path_in_sql(
            "customers.regions.name", location="(test)", model=m,
        )
        # No joins on the host model — the rule has no anchor and must
        # leave the text alone.
        assert rewritten == "customers.regions.name"
        assert warnings == []

    def test_none_input(self):
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            None, location="(test)", model=m,
        )
        assert rewritten is None
        assert warnings == []

    def test_empty_input(self):
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            "", location="(test)", model=m,
        )
        assert rewritten == ""
        assert warnings == []

    def test_unparseable_sql_left_alone(self):
        m = _orders_with_customers_join()
        sql_text = ")))not(valid"
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(test)", model=m,
        )
        assert rewritten == sql_text
        assert warnings == []

    def test_warnings_carrier_fires(self):
        m = _orders_with_customers_join()
        with wmod.catch_warnings(record=True) as caught:
            wmod.simplefilter("always")
            _apply_dot_path_in_sql(
                "customers.regions.name", location="(test)", model=m,
            )
        slack = [
            c for c in caught
            if isinstance(c.message, SlayerNormalizationWarning)
        ]
        assert len(slack) == 1
        assert slack[0].message.payload.rule_id == "DOT_PATH_IN_SQL"

    def test_multiple_refs_each_warn(self):
        # Two distinct dotted refs in the same expression — each emits.
        m = _orders_with_customers_join()
        sql_text = (
            "concat(customers.regions.name, customers.regions.code)"
        )
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(test)", model=m,
        )
        assert "customers__regions.name" in rewritten
        assert "customers__regions.code" in rewritten
        assert "customers.regions" not in rewritten
        assert len(warnings) == 2

    def test_cte_local_alias_shadow_not_rewritten_warn(self):
        # CTE defines a name "customers" — refs to that name in the outer
        # query are ambiguous with the join target. The spec requires:
        # do not rewrite AND emit an ambiguous warning.
        m = _orders_with_customers_join()
        sql_text = (
            "WITH customers AS (SELECT * FROM other) "
            "SELECT customers.regions.name FROM customers"
        )
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(amb_cte)", model=m,
        )
        # The CTE alias shadows the join target — must not collapse.
        assert "customers__regions" not in rewritten
        # Ambiguous → exactly one warning, no rewrite recorded as normalized.
        amb = [w for w in warnings if w.rule_id == "DOT_PATH_IN_SQL"]
        assert len(amb) == 1
        assert amb[0].original == "customers.regions.name"
        assert amb[0].location == "(amb_cte)"
        # The normalized field signals the rule recognised but skipped.
        assert "ambiguous" in amb[0].normalized.lower()

    def test_as_alias_shadow_not_rewritten_warn(self):
        # AS alias on the FROM clause matches a known join target name.
        # The dotted ref then resolves to the alias, not the join graph.
        m = _orders_with_customers_join()
        sql_text = (
            "SELECT customers.regions.name FROM something AS customers"
        )
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(amb_alias)", model=m,
        )
        assert "customers__regions" not in rewritten
        amb = [w for w in warnings if w.rule_id == "DOT_PATH_IN_SQL"]
        assert len(amb) == 1
        assert "ambiguous" in amb[0].normalized.lower()

    def test_set_op_branch_ref_left_alone(self):
        # Refs inside a UNION/EXCEPT/INTERSECT branch are not root-scope.
        m = _orders_with_customers_join()
        sql_text = (
            "(SELECT customers.regions.name FROM x) "
            "UNION ALL "
            "(SELECT customers.regions.name FROM y)"
        )
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(setop)", model=m,
        )
        assert "customers__regions" not in rewritten
        assert "customers.regions" in rewritten
        assert warnings == []

    def test_correlated_subquery_inner_ref_not_rewritten(self):
        # The outer is a SELECT statement; the inner SELECT's ref to
        # customers.regions.name belongs lexically to the inner subquery,
        # not to the outer root. Must NOT be rewritten.
        m = _orders_with_customers_join()
        sql_text = (
            "SELECT (SELECT customers.regions.name FROM z) AS v, amount "
            "FROM orders"
        )
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(corr)", model=m,
        )
        assert "customers.regions" in rewritten
        assert "customers__regions" not in rewritten
        assert warnings == []

    def test_schema_qualified_from_table_shadow_warn(self):
        # FROM customers.regions uses `customers` as a schema/db name.
        # The first segment of the dotted ref then matches both the
        # known join target AND the FROM schema — emit ambiguous warning
        # and leave alone.
        m = _orders_with_customers_join()
        sql_text = "SELECT customers.regions.name FROM customers.regions"
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(amb_schema)", model=m,
        )
        assert "customers__regions" not in rewritten
        amb = [w for w in warnings if w.rule_id == "DOT_PATH_IN_SQL"]
        assert len(amb) == 1
        assert "ambiguous" in amb[0].normalized.lower()

    def test_catalog_qualified_from_table_shadow_warn(self):
        # mydb.customers.regions — `customers` is the db part. Same shadow.
        m = _orders_with_customers_join()
        sql_text = (
            "SELECT customers.regions.name FROM mydb.customers.regions"
        )
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(amb_catalog)", model=m,
        )
        assert "customers__regions" not in rewritten
        amb = [w for w in warnings if w.rule_id == "DOT_PATH_IN_SQL"]
        assert len(amb) == 1

    def test_unaliased_from_table_does_not_shadow(self):
        # Bare `FROM customers` (no AS alias) — per spec ("AS alias, CTE
        # name, or schema name") this does NOT shadow the join target.
        m = _orders_with_customers_join()
        sql_text = "SELECT customers.regions.name FROM customers"
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(bare)", model=m,
        )
        assert "customers__regions.name" in rewritten
        rewrites = [
            w for w in warnings
            if w.rule_id == "DOT_PATH_IN_SQL"
            and "ambiguous" not in w.normalized.lower()
        ]
        assert len(rewrites) == 1

    def test_multi_statement_input_no_op(self):
        # Multi-statement slack input is unsafe — leave alone uniformly.
        m = _orders_with_customers_join()
        sql_text = "customers.regions.name; customers.regions.code"
        rewritten, warnings = _apply_dot_path_in_sql(
            sql_text, location="(multi)", model=m,
        )
        assert rewritten == sql_text
        assert warnings == []

    def test_first_segment_is_join_target_is_the_contract(self):
        # The rule fires when the first segment of a 3+ segment ref is a
        # known join target on the host model. Intermediate hops are not
        # resolved at normalize-time (no storage access), so a path whose
        # leading segment is a join target but whose intermediate hop does
        # not resolve still rewrites — the resulting __ alias will fail
        # downstream the same way the dotted form would. This pins the
        # contract.
        m = _orders_with_customers_join()
        rewritten, warnings = _apply_dot_path_in_sql(
            "customers.foobar.name", location="(test)", model=m,
        )
        assert "customers__foobar.name" in rewritten
        assert len(warnings) == 1
        assert warnings[0].rule_id == "DOT_PATH_IN_SQL"


# ---------------------------------------------------------------------------
# Wiring tests — normalize_model walks all three Mode-A surfaces
# ---------------------------------------------------------------------------


def _set_raw_column_sql(column: Column, *, raw: str) -> Column:
    """Set ``Column.sql`` to a raw slack-form string for the normalize pass.

    Construction no longer rewrites multi-dot refs (the legacy validator is
    gone), so the raw form survives until ``normalize_model`` runs. Assigning
    after construction keeps these helpers symmetric with the ``.filter`` /
    ``.filters`` setters below, which set fields directly to feed the same
    normalize pass.
    """
    column.sql = raw
    return column


def _set_raw_column_filter(column: Column, *, raw: str) -> Column:
    column.filter = raw
    return column


def _set_raw_model_filters(model: SlayerModel, *, raw_filters: list[str]) -> SlayerModel:
    model.filters = raw_filters
    return model


class TestNormalizeModelWiresDotPath:
    def test_column_sql_surface_rewritten(self):
        m = _orders_with_customers_join()
        col = Column(name="region_name", type=DataType.TEXT)
        col = _set_raw_column_sql(col, raw="customers.regions.name")
        m.columns = list(m.columns) + [col]

        result = normalize_model(m)
        out_col = next(c for c in result.model.columns if c.name == "region_name")
        assert out_col.sql == "customers__regions.name"

        dot_ws = [w for w in result.warnings if w.rule_id == "DOT_PATH_IN_SQL"]
        assert len(dot_ws) == 1
        w = dot_ws[0]
        assert w.original == "customers.regions.name"
        assert w.normalized == "customers__regions.name"
        # Location is per-column: columns[<idx>].sql
        assert w.location.startswith("columns[") and w.location.endswith(".sql")

    def test_column_filter_surface_rewritten(self):
        m = _orders_with_customers_join()
        col = Column(name="region_amount", type=DataType.DOUBLE)
        col = _set_raw_column_filter(col, raw="customers.regions.name = 'EU'")
        m.columns = list(m.columns) + [col]

        result = normalize_model(m)
        out_col = next(c for c in result.model.columns if c.name == "region_amount")
        assert out_col.filter is not None
        assert "customers__regions.name" in out_col.filter
        assert "customers.regions" not in out_col.filter

        dot_ws = [w for w in result.warnings if w.rule_id == "DOT_PATH_IN_SQL"]
        assert len(dot_ws) == 1
        w = dot_ws[0]
        assert w.original == "customers.regions.name"
        assert w.normalized == "customers__regions.name"
        assert w.location.startswith("columns[") and w.location.endswith(".filter")

    def test_model_filters_surface_rewritten(self):
        m = _orders_with_customers_join()
        m = _set_raw_model_filters(
            m, raw_filters=["customers.regions.name IS NOT NULL"],
        )
        result = normalize_model(m)
        assert len(result.model.filters) == 1
        assert "customers__regions.name" in result.model.filters[0]
        assert "customers.regions" not in result.model.filters[0]

        dot_ws = [w for w in result.warnings if w.rule_id == "DOT_PATH_IN_SQL"]
        assert len(dot_ws) == 1
        w = dot_ws[0]
        assert w.original == "customers.regions.name"
        assert w.normalized == "customers__regions.name"
        assert w.location == "filters[0]"

    def test_canonical_input_no_warnings(self):
        m = _orders_with_customers_join()
        col = Column(
            name="region_name",
            type=DataType.TEXT,
            sql="customers__regions.name",  # already canonical, passes validator
        )
        m.columns = list(m.columns) + [col]
        result = normalize_model(m)
        assert not any(w.rule_id == "DOT_PATH_IN_SQL" for w in result.warnings)

    def test_no_joins_means_no_dot_path_warnings(self):
        m = SlayerModel(
            name="standalone", data_source="prod", sql_table="standalone",
            columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        )
        result = normalize_model(m)
        assert not any(w.rule_id == "DOT_PATH_IN_SQL" for w in result.warnings)


# ---------------------------------------------------------------------------
# Boundary: Mode-B fields must NOT be touched by DOT_PATH_IN_SQL
# ---------------------------------------------------------------------------


class TestDotPathInSqlIsModeAOnly:
    def test_model_measure_formula_not_rewritten(self):
        # ModelMeasure.formula is Mode-B (DSL). The dotted form there is a
        # join-path reference (the dotted-join Mode-B convention) and must
        # NOT be rewritten by DOT_PATH_IN_SQL.
        m = _orders_with_customers_join()
        mm = ModelMeasure(name="region_count", formula="customers.regions.name:count")
        m.measures = list(m.measures) + [mm]
        result = normalize_model(m)
        # Mode-B form unchanged.
        out_mm = next(x for x in result.model.measures if x.name == "region_count")
        assert out_mm.formula == "customers.regions.name:count"
        # No DOT_PATH_IN_SQL warning fired against a Mode-B surface.
        for w in result.warnings:
            assert w.rule_id != "DOT_PATH_IN_SQL" or "formula" not in w.location

    def test_query_filters_mode_b_not_rewritten(self):
        # SlayerQuery.filters is Mode-B. normalize_query must not run
        # DOT_PATH_IN_SQL over its filters.
        m = _orders_with_customers_join()
        q = SlayerQuery(
            source_model="orders",
            filters=["customers.regions.name = 'EU'"],
        )
        result = normalize_query(q, model=m)
        # Mode-B dotted form preserved verbatim.
        assert result.query.filters[0] == "customers.regions.name = 'EU'"
        # And no DOT_PATH_IN_SQL warning.
        assert not any(w.rule_id == "DOT_PATH_IN_SQL" for w in result.warnings)
