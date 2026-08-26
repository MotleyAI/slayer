"""DEV-1743 — the DOT_PATH_IN_SQL dotted→``__`` rewrite is RETIRED.

Mode-A free SQL (``Column.sql`` / ``Column.filter`` / ``SlayerModel.filters``)
is now dotted-canonical: a dotted join path (``customers.regions.name``) is
PRESERVED verbatim by ``normalize_model`` / ``normalize_query`` and resolved
structurally at bind/generation time (and the save-time door). No
DOT_PATH_IN_SQL warning is ever emitted. These tests pin that preservation
and confirm Mode-B fields are untouched.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.normalization import (
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
    # DEV-1743: the DOT_PATH_IN_SQL dotted->dunder rewrite is RETIRED. Mode-A
    # free SQL is now dotted-canonical, so ``normalize_model`` PRESERVES the
    # dotted join path verbatim (structural resolution moved to bind/generation
    # and the save-time door). No DOT_PATH_IN_SQL warning is emitted.
    def test_column_sql_surface_preserved(self):
        m = _orders_with_customers_join()
        col = Column(name="region_name", type=DataType.TEXT)
        col = _set_raw_column_sql(col, raw="customers.regions.name")
        m.columns = list(m.columns) + [col]

        result = normalize_model(m)
        out_col = next(c for c in result.model.columns if c.name == "region_name")
        assert out_col.sql == "customers.regions.name"

        dot_ws = [w for w in result.warnings if w.rule_id == "DOT_PATH_IN_SQL"]
        assert dot_ws == []

    def test_column_filter_surface_preserved(self):
        m = _orders_with_customers_join()
        col = Column(name="region_amount", type=DataType.DOUBLE)
        col = _set_raw_column_filter(col, raw="customers.regions.name = 'EU'")
        m.columns = list(m.columns) + [col]

        result = normalize_model(m)
        out_col = next(c for c in result.model.columns if c.name == "region_amount")
        assert out_col.filter is not None
        assert "customers.regions.name" in out_col.filter
        assert "customers__regions" not in out_col.filter

        dot_ws = [w for w in result.warnings if w.rule_id == "DOT_PATH_IN_SQL"]
        assert dot_ws == []

    def test_model_filters_surface_preserved(self):
        m = _orders_with_customers_join()
        m = _set_raw_model_filters(
            m, raw_filters=["customers.regions.name IS NOT NULL"],
        )
        result = normalize_model(m)
        assert len(result.model.filters) == 1
        assert "customers.regions.name" in result.model.filters[0]
        assert "customers__regions" not in result.model.filters[0]

        dot_ws = [w for w in result.warnings if w.rule_id == "DOT_PATH_IN_SQL"]
        assert dot_ws == []

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
