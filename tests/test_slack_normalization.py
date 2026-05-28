"""Stage 6 (DEV-1450) — slack normalization layer (FUNC_STYLE_AGG +
MISPLACED_MEASURE).

The DOT_PATH_IN_SQL rule slot is wired but inactive in stage 6 (full AST
rewrite lands in a follow-up). Tests cover the two active rules and the
engine wiring: warnings emit via ``warnings.warn(...)`` AND appear in
``SlayerResponse.warnings``.
"""

from __future__ import annotations

import warnings

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.warnings import SlayerNormalizationWarning
from slayer.engine.normalization import (
    NormalizationResult,
    normalize_model,
    normalize_query,
)


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
    )


# ---------------------------------------------------------------------------
# FUNC_STYLE_AGG
# ---------------------------------------------------------------------------


class TestFuncStyleAgg:
    def test_sum_rewrite(self):
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "sum(revenue)"}],
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = normalize_query(q)

        assert result.query.measures[0].formula == "revenue:sum"
        assert len(result.warnings) == 1
        w = result.warnings[0]
        assert w.rule_id == "FUNC_STYLE_AGG"
        assert w.original == "sum(revenue)"
        assert w.normalized == "revenue:sum"
        assert w.location == "measures[0].formula"

        # The Python warnings channel also fires the carrier.
        slack = [c for c in caught if isinstance(c.message, SlayerNormalizationWarning)]
        assert len(slack) == 1
        assert slack[0].message.payload.rule_id == "FUNC_STYLE_AGG"

    def test_count_star_rewrite(self):
        q = SlayerQuery(source_model="orders", measures=[{"formula": "count(*)"}])
        result = normalize_query(q)
        assert result.query.measures[0].formula == "*:count"
        assert any(w.rule_id == "FUNC_STYLE_AGG" for w in result.warnings)

    def test_canonical_form_no_warning(self):
        q = SlayerQuery(source_model="orders", measures=[{"formula": "revenue:sum"}])
        result = normalize_query(q)
        assert result.warnings == []
        assert result.query.measures[0].formula == "revenue:sum"

    def test_filter_rewrite(self):
        q = SlayerQuery(
            source_model="orders",
            filters=["sum(revenue) > 100"],
        )
        result = normalize_query(q)
        assert result.query.filters[0] == "revenue:sum > 100"
        assert len(result.warnings) == 1
        assert result.warnings[0].location == "filters[0]"

    def test_multiple_rewrites_in_one_formula(self):
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "sum(revenue) / count(*)"}],
        )
        result = normalize_query(q)
        assert "revenue:sum" in result.query.measures[0].formula
        assert "*:count" in result.query.measures[0].formula
        assert len(result.warnings) == 2

    def test_ambiguous_first_with_colon_arg_left_alone(self):
        # last(revenue:sum) is a valid transform call, not a slack agg.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "last(revenue:sum)"}],
        )
        result = normalize_query(q)
        assert result.query.measures[0].formula == "last(revenue:sum)"
        assert result.warnings == []

    def test_custom_agg_recognised_via_model(self):
        # A model-level custom aggregation name is recognised by
        # normalize_model — that's where the model's aggregations list
        # is in scope.
        from slayer.core.models import Aggregation
        m = _orders().model_copy(update={
            "aggregations": [Aggregation(name="custom_sum", formula="SUM({value})")],
            "measures": [ModelMeasure(name="rev_c", formula="custom_sum(revenue)")],
        })
        result = normalize_model(m)
        assert result.model.measures[0].formula == "revenue:custom_sum"
        assert any(w.rule_id == "FUNC_STYLE_AGG" for w in result.warnings)


# ---------------------------------------------------------------------------
# MISPLACED_MEASURE
# ---------------------------------------------------------------------------


class TestMisplacedMeasure:
    def test_bare_column_in_measures_moves_to_dimensions(self):
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "status"}],
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = normalize_query(q, model=_orders())

        assert result.query.measures == []
        dim_names = [getattr(d, "name", d) for d in result.query.dimensions]
        assert "status" in dim_names
        assert any(w.rule_id == "MISPLACED_MEASURE" for w in result.warnings)
        assert any(
            isinstance(c.message, SlayerNormalizationWarning)
            and c.message.payload.rule_id == "MISPLACED_MEASURE"
            for c in caught
        )

    def test_named_modelmeasure_formula_not_moved(self):
        # If the bare name matches a ModelMeasure, it's a valid measure ref
        # (not a column) and stays in measures.
        m = _orders().model_copy(update={
            "measures": [ModelMeasure(name="aov", formula="revenue:avg")],
        })
        q = SlayerQuery(source_model="orders", measures=[{"formula": "aov"}])
        result = normalize_query(q, model=m)
        assert result.query.measures and result.query.measures[0].formula == "aov"
        assert result.warnings == []

    def test_unknown_bare_token_left_alone(self):
        # Not a column and not a measure — the resolver will error later,
        # but normalization does not preemptively rewrite.
        q = SlayerQuery(source_model="orders", measures=[{"formula": "noseucha"}])
        result = normalize_query(q, model=_orders())
        assert result.query.measures and result.query.measures[0].formula == "noseucha"

    def test_no_model_means_no_move(self):
        # MISPLACED_MEASURE needs model context to classify.
        q = SlayerQuery(source_model="orders", measures=[{"formula": "status"}])
        result = normalize_query(q, model=None)
        # Without a model the rule no-ops.
        assert result.query.measures and result.query.measures[0].formula == "status"
        assert not any(w.rule_id == "MISPLACED_MEASURE" for w in result.warnings)

    def test_formula_with_call_not_moved(self):
        # Anything containing parens is treated as a formula, even if the
        # function name happens to also be a column.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "sum(revenue)"}],
        )
        result = normalize_query(q, model=_orders())
        # FUNC_STYLE_AGG fires; MISPLACED_MEASURE does not.
        assert any(w.rule_id == "FUNC_STYLE_AGG" for w in result.warnings)
        assert not any(w.rule_id == "MISPLACED_MEASURE" for w in result.warnings)

    # DEV-1484 backfill from TestAutoMoveDimensions.test_colon_fields_kept
    def test_colon_form_measures_kept(self):
        # Colon-form aggregations (`revenue:sum`, `*:count`) are real
        # measures — never reclassified as dimensions.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "revenue:sum"}, {"formula": "*:count"}],
        )
        result = normalize_query(q, model=_orders())
        assert len(result.query.measures) == 2
        assert not result.query.dimensions
        assert not any(w.rule_id == "MISPLACED_MEASURE" for w in result.warnings)

    # DEV-1484 backfill from TestAutoMoveDimensions.test_arithmetic_kept
    def test_arithmetic_formula_kept(self):
        # An arithmetic-over-aggregates formula stays a measure.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "revenue:sum / *:count"}],
        )
        result = normalize_query(q, model=_orders())
        assert len(result.query.measures) == 1
        assert not any(w.rule_id == "MISPLACED_MEASURE" for w in result.warnings)

    # DEV-1484 backfill from TestAutoMoveDimensions.test_invalid_cross_model_path_kept
    def test_dotted_cross_model_ref_kept(self):
        # A dotted cross-model ref (`customers.nonexistent`) is NOT a bare
        # local column, so MISPLACED_MEASURE leaves it in measures (the
        # binder errors later if the path is invalid). This also covers the
        # legacy dotted-named-measure case — every dotted ref is kept.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customers.nonexistent"}, {"formula": "revenue:sum"}],
        )
        result = normalize_query(q, model=_orders())
        assert len(result.query.measures) == 2
        assert not any(w.rule_id == "MISPLACED_MEASURE" for w in result.warnings)

    # DEV-1484 backfill from
    # TestAutoMoveDimensions.test_dotted_named_measure_not_moved_via_named_queries
    def test_dotted_named_measure_ref_kept(self):
        # A dotted ref to a named ModelMeasure on a joined model
        # (`customers.name_count`) is dotted, so MISPLACED_MEASURE keeps it
        # in measures rather than moving it to dimensions.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customers.name_count"}, {"formula": "revenue:sum"}],
        )
        result = normalize_query(q, model=_orders())
        assert len(result.query.measures) == 2
        assert not any(w.rule_id == "MISPLACED_MEASURE" for w in result.warnings)

    # DEV-1484 backfill from TestAutoMoveDimensions.test_no_fields_noop
    def test_no_measures_is_noop(self):
        # No measures to classify — the rule short-circuits and leaves the
        # query untouched.
        q = SlayerQuery(source_model="orders", dimensions=["status"])
        result = normalize_query(q, model=_orders())
        assert not result.query.measures
        assert [getattr(d, "name", d) for d in result.query.dimensions] == ["status"]
        assert not any(w.rule_id == "MISPLACED_MEASURE" for w in result.warnings)

    # DEV-1484 backfill from TestAutoMoveDimensions.test_appends_to_existing_dimensions
    def test_moved_column_appends_to_existing_dimensions(self):
        # A misplaced bare column is appended to existing dimensions, not
        # replacing them.
        m = _orders().model_copy(update={
            "columns": _orders().columns + [Column(name="customer_id", type=DataType.INT)],
        })
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customer_id"}, {"formula": "revenue:sum"}],
            dimensions=["status"],
        )
        result = normalize_query(q, model=m)
        assert len(result.query.measures) == 1
        assert result.query.measures[0].formula == "revenue:sum"
        dim_names = [getattr(d, "name", d) for d in result.query.dimensions]
        assert "status" in dim_names
        assert "customer_id" in dim_names
        assert any(w.rule_id == "MISPLACED_MEASURE" for w in result.warnings)


# ---------------------------------------------------------------------------
# NormalizationResult shape
# ---------------------------------------------------------------------------


class TestResult:
    def test_canonical_input_returns_unchanged_query(self):
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "revenue:sum"}],
            dimensions=["status"],
        )
        result = normalize_query(q, model=_orders())
        assert isinstance(result, NormalizationResult)
        assert result.warnings == []
        # No semantic change — measures and dimensions match (after the
        # SlayerQuery ColumnRef coercion).
        assert len(result.query.measures) == 1
        assert [getattr(d, "name", d) for d in result.query.dimensions] == ["status"]

    def test_normalize_model_returns_model_with_warnings(self):
        m = _orders().model_copy(update={
            "measures": [ModelMeasure(name="rev_s", formula="sum(revenue)")],
        })
        result = normalize_model(m)
        assert result.model.measures[0].formula == "revenue:sum"
        assert len(result.warnings) == 1


# ---------------------------------------------------------------------------
# Engine wiring — SlayerResponse.warnings is populated
# ---------------------------------------------------------------------------


@pytest.fixture
def engine_with_orders(tmp_path):
    from slayer.core.models import DatasourceConfig
    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.storage.yaml_storage import YAMLStorage
    import sqlite3

    db = tmp_path / "test.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(
        "CREATE TABLE orders ("
        "  id INTEGER PRIMARY KEY,"
        "  revenue REAL,"
        "  status TEXT,"
        "  created_at TIMESTAMP);"
        "INSERT INTO orders VALUES "
        "  (1, 10.0, 'paid', '2026-01-01 00:00:00'),"
        "  (2, 20.0, 'paid', '2026-01-02 00:00:00'),"
        "  (3, 30.0, 'open', '2026-01-03 00:00:00');"
    )
    conn.commit()
    conn.close()

    storage = YAMLStorage(base_dir=tmp_path / "models")
    engine = SlayerQueryEngine(storage=storage)

    import asyncio
    asyncio.get_event_loop().run_until_complete(
        storage.save_datasource(DatasourceConfig(
            name="prod", type="sqlite", url=f"sqlite:///{db}"
        ))
    )
    asyncio.get_event_loop().run_until_complete(
        storage.save_model(_orders())
    )
    yield engine


class TestEngineWiring:
    async def test_execute_dry_run_surfaces_warnings(self):
        from slayer.engine.query_engine import SlayerQueryEngine
        from slayer.storage.yaml_storage import YAMLStorage
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=Path(td) / "models")
            from slayer.core.models import DatasourceConfig
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite", url="sqlite:///:memory:")
            )
            await storage.save_model(_orders())
            engine = SlayerQueryEngine(storage=storage)

            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "sum(revenue)"}],
                dimensions=["status"],
            )
            resp = await engine.execute(q, dry_run=True)
            assert resp.warnings
            assert any(w.rule_id == "FUNC_STYLE_AGG" for w in resp.warnings)

    async def test_clean_query_has_empty_warnings(self):
        from slayer.engine.query_engine import SlayerQueryEngine
        from slayer.storage.yaml_storage import YAMLStorage
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=Path(td) / "models")
            from slayer.core.models import DatasourceConfig
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite", url="sqlite:///:memory:")
            )
            await storage.save_model(_orders())
            engine = SlayerQueryEngine(storage=storage)

            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "revenue:sum"}],
                dimensions=["status"],
            )
            resp = await engine.execute(q, dry_run=True)
            assert resp.warnings == []

    async def test_custom_agg_in_query_filter_recognised(self):
        # Codex review fix: the engine threads custom aggregation names
        # from the source model into normalize_query, so a slack-form
        # `custom_sum(revenue)` in a query measure or filter is rewritten
        # and surfaces in SlayerResponse.warnings just like built-ins.
        from slayer.engine.query_engine import SlayerQueryEngine
        from slayer.storage.yaml_storage import YAMLStorage
        from slayer.core.models import Aggregation, DatasourceConfig
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=Path(td) / "models")
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite", url="sqlite:///:memory:")
            )
            m = _orders().model_copy(update={
                "aggregations": [Aggregation(name="custom_sum", formula="SUM({value})")],
            })
            await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)

            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "custom_sum(revenue)"}],
            )
            resp = await engine.execute(q, dry_run=True)
            assert any(
                w.rule_id == "FUNC_STYLE_AGG"
                and "custom_sum" in w.original
                for w in resp.warnings
            )

    # DEV-1484 backfill from TestAutoMoveDimensions.test_cross_model_dimension_moved
    async def test_cross_model_dimension_in_measures_groups_correctly(self):
        # Legacy `_auto_move_fields_to_dimensions` moved a bare cross-model
        # dimension ref out of measures. On the typed pipeline the slack rule
        # leaves dotted refs alone, but the binder classifies a cross-model
        # dotted ref in `measures` as a dimension end-to-end: it must surface
        # in GROUP BY and the projection, with the join emitted.
        from slayer.engine.query_engine import SlayerQueryEngine
        from slayer.storage.yaml_storage import YAMLStorage
        from slayer.core.models import DatasourceConfig, ModelJoin
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=Path(td) / "models")
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite", url="sqlite:///:memory:")
            )
            await storage.save_model(SlayerModel(
                name="customers", data_source="prod", sql_table="customers",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="name", type=DataType.TEXT),
                ],
            ))
            await storage.save_model(SlayerModel(
                name="orders", data_source="prod", sql_table="orders",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="customer_id", type=DataType.INT),
                    Column(name="revenue", type=DataType.DOUBLE),
                ],
                joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
            ))
            engine = SlayerQueryEngine(storage=storage)
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "customers.name"}, {"formula": "revenue:sum"}],
            )
            resp = await engine.execute(q, dry_run=True)
            sql = resp.sql
            assert "GROUP BY" in sql and "customers.name" in sql, sql
            assert "JOIN customers" in sql, sql

    async def test_save_model_normalizes_formulas(self):
        from slayer.engine.query_engine import SlayerQueryEngine
        from slayer.storage.yaml_storage import YAMLStorage
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=Path(td) / "models")
            from slayer.core.models import DatasourceConfig
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite", url="sqlite:///:memory:")
            )
            engine = SlayerQueryEngine(storage=storage)
            m = _orders().model_copy(update={
                "measures": [ModelMeasure(name="rev_s", formula="sum(revenue)")],
            })
            saved = await engine.save_model(m)
            assert saved.measures[0].formula == "revenue:sum"
