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
from slayer.core.models import (
    Aggregation,
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
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
        m = _orders().model_copy(update={
            "aggregations": [Aggregation(name="custom_sum", formula="SUM({value})")],
            "measures": [ModelMeasure(name="rev_c", formula="custom_sum(revenue)")],
        })
        result = normalize_model(m)
        assert result.model.measures[0].formula == "revenue:custom_sum"
        assert any(w.rule_id == "FUNC_STYLE_AGG" for w in result.warnings)


# ---------------------------------------------------------------------------
# DEV-1500 — normalize_model custom_agg_names param (joined-model custom aggs)
# ---------------------------------------------------------------------------


class TestNormalizeModelCustomAggParam:
    """``normalize_model(model, custom_agg_names=...)`` lets the caller supply
    the full reachable aggregation set (source model + joined models) so a
    funcstyle ``ModelMeasure.formula`` over a joined-model custom aggregation
    is rewritten to colon form.
    """

    def test_param_recognises_joined_custom_agg(self):
        # `rolling_avg` is defined on a JOINED model, not on `orders` itself,
        # so it only appears in the caller-supplied `custom_agg_names`.
        m = _orders().model_copy(update={
            "joins": [ModelJoin(target_model="customers", join_pairs=[["id", "id"]])],
            "measures": [
                ModelMeasure(name="ravg", formula="rolling_avg(customers.score)"),
            ],
        })
        result = normalize_model(m, custom_agg_names=frozenset({"rolling_avg"}))
        assert result.model.measures[0].formula == "customers.score:rolling_avg"
        assert any(w.rule_id == "FUNC_STYLE_AGG" for w in result.warnings)

    def test_none_param_falls_back_to_models_own_aggs(self):
        # Explicit None means "caller did not compute the reachable set" —
        # fall back to the model's own aggregations (backward-compatible).
        m = _orders().model_copy(update={
            "aggregations": [Aggregation(name="custom_sum", formula="SUM({value})")],
            "measures": [ModelMeasure(name="rev_c", formula="custom_sum(revenue)")],
        })
        result = normalize_model(m, custom_agg_names=None)
        assert result.model.measures[0].formula == "revenue:custom_sum"

    def test_empty_frozenset_suppresses_own_agg_fallback(self):
        # Sharp edge (intentional): an explicit empty frozenset suppresses the
        # model's-own fallback, so even a local custom agg is NOT recognised.
        m = _orders().model_copy(update={
            "aggregations": [Aggregation(name="custom_sum", formula="SUM({value})")],
            "measures": [ModelMeasure(name="rev_c", formula="custom_sum(revenue)")],
        })
        result = normalize_model(m, custom_agg_names=frozenset())
        assert result.model.measures[0].formula == "custom_sum(revenue)"
        assert result.warnings == []


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


# ---------------------------------------------------------------------------
# DEV-1500 — joined-model custom aggregations recognised by FUNC_STYLE_AGG
# ---------------------------------------------------------------------------


def _customers_with_rolling_avg() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="score", type=DataType.DOUBLE),
        ],
        aggregations=[Aggregation(name="rolling_avg", formula="AVG({value})")],
    )


def _orders_joining_customers() -> SlayerModel:
    return _orders().model_copy(update={
        "joins": [
            ModelJoin(target_model="customers", join_pairs=[["id", "id"]]),
        ],
    })


async def _engine_with_prod():
    """A fresh engine over an in-memory-SQLite YAML store. Returns
    ``(engine, storage)``; caller owns the TemporaryDirectory lifetime.
    """
    import tempfile
    from pathlib import Path

    from slayer.core.models import DatasourceConfig
    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.storage.yaml_storage import YAMLStorage

    td = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=Path(td.name) / "models")
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", url="sqlite:///:memory:")
    )
    return SlayerQueryEngine(storage=storage), storage, td


class TestJoinedCustomAggFuncStyle:
    """End-to-end: the FUNC_STYLE_AGG slack rule recognises a custom
    aggregation defined on a *joined* model on both the query path
    (``_normalize_stage``) and the model-save path (``save_model``).
    """

    async def test_query_path_surfaces_normalized_warning(self):
        engine, storage, td = await _engine_with_prod()
        try:
            await storage.save_model(_customers_with_rolling_avg())
            await storage.save_model(_orders_joining_customers())
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "rolling_avg(customers.score)"}],
                dimensions=["status"],
            )
            resp = await engine.execute(q, dry_run=True)
            assert any(
                w.rule_id == "FUNC_STYLE_AGG"
                and w.normalized == "customers.score:rolling_avg"
                for w in resp.warnings
            ), resp.warnings
            assert "AVG(" in resp.sql, resp.sql
        finally:
            td.cleanup()

    async def test_save_model_rewrites_joined_custom_agg_formula(self):
        engine, storage, td = await _engine_with_prod()
        try:
            # customers MUST be saved first so the save-time storage walk can
            # discover `rolling_avg` on the joined model.
            await storage.save_model(_customers_with_rolling_avg())
            orders = _orders_joining_customers().model_copy(update={
                "measures": [
                    ModelMeasure(
                        name="ravg", formula="rolling_avg(customers.score)"
                    ),
                ],
            })
            saved = await engine.save_model(orders)
            assert saved.measures[0].formula == "customers.score:rolling_avg"
            # And the rewrite is persisted, not just on the returned object.
            reloaded = await storage.get_model("orders", data_source="prod")
            assert reloaded.measures[0].formula == "customers.score:rolling_avg"
        finally:
            td.cleanup()

    async def test_multistage_named_stage_scoping(self):
        # A NAMED non-root stage sourced from `orders` (which joins
        # `customers`) must have its funcstyle joined-agg rewritten — this
        # exercises the `stage_source_models` branch of `_normalize_stage`
        # plus the bundle's transitive `referenced_models`.
        engine, storage, td = await _engine_with_prod()
        try:
            await storage.save_model(_customers_with_rolling_avg())
            await storage.save_model(_orders_joining_customers())
            stage1 = SlayerQuery(
                name="stage1",
                source_model="orders",
                dimensions=["status"],
                measures=[
                    {"formula": "rolling_avg(customers.score)", "name": "ravg"},
                ],
            )
            root = SlayerQuery(
                source_model="stage1",
                dimensions=["status"],
                measures=[{"formula": "ravg:max"}],
            )
            resp = await engine.execute([stage1, root], dry_run=True)
            assert any(
                w.rule_id == "FUNC_STYLE_AGG"
                and w.normalized == "customers.score:rolling_avg"
                for w in resp.warnings
            ), resp.warnings
            assert "AVG(" in resp.sql, resp.sql
        finally:
            td.cleanup()

    async def test_save_model_skips_missing_join_target_but_finds_others(self):
        # The model joins TWO targets: one persisted (carrying the agg) and
        # one absent. The best-effort walk skips the absent target without
        # error AND still discovers the agg on the persisted joined model
        # (proving the BFS actually runs end-to-end at save time).
        engine, storage, td = await _engine_with_prod()
        try:
            await storage.save_model(_customers_with_rolling_avg())
            orders = _orders_joining_customers().model_copy(update={
                "joins": [
                    ModelJoin(target_model="customers", join_pairs=[["id", "id"]]),
                    ModelJoin(target_model="ghost", join_pairs=[["id", "id"]]),
                ],
                "measures": [
                    ModelMeasure(
                        name="ravg", formula="rolling_avg(customers.score)"
                    ),
                ],
            })
            saved = await engine.save_model(orders)
            assert saved.measures[0].formula == "customers.score:rolling_avg"
        finally:
            td.cleanup()

    async def test_save_model_swallows_ambiguous_join_lookup(self, monkeypatch):
        # If the join-target lookup raises (e.g. AmbiguousModelError), the
        # best-effort walk swallows it and the formula falls through
        # unrewritten — no crash. A spy on storage.get_model proves the BFS
        # actually attempted the lookup (so this test fails today, where no
        # BFS runs, and passes after the fix that swallows the exception).
        from slayer.core.errors import AmbiguousModelError

        engine, storage, td = await _engine_with_prod()
        try:
            await storage.save_model(_customers_with_rolling_avg())
            orders = _orders_joining_customers().model_copy(update={
                "measures": [
                    ModelMeasure(
                        name="ravg", formula="rolling_avg(customers.score)"
                    ),
                ],
            })

            real_get_model = storage.get_model
            calls: list[str] = []

            async def _boom(name, data_source=None):
                calls.append(name)
                if name == "customers":
                    raise AmbiguousModelError("customers", ["prod", "other"])
                return await real_get_model(name, data_source=data_source)

            monkeypatch.setattr(storage, "get_model", _boom)
            saved = await engine.save_model(orders)
            # BFS attempted the join-target lookup (proves the walk runs).
            assert "customers" in calls
            # Exception was swallowed (no crash) and the funcstyle stayed as-is
            # because the joined agg could not be discovered.
            assert saved.measures[0].formula == "rolling_avg(customers.score)"
        finally:
            td.cleanup()

    async def test_save_model_rewrites_joined_custom_agg_at_four_hops(self):
        # Save-time reachable discovery must be unbounded, matching the
        # query-path 4-hop tracking test. Chain: a -> b -> c -> d -> e,
        # rolling_avg lives on `e`. A ModelMeasure on `a` referencing
        # `rolling_avg(b.c.d.e.score)` is rewritten at save time.
        engine, storage, td = await _engine_with_prod()
        try:
            def _chain_model(name, *, joins=None, aggs=None, extra_cols=None):
                cols = [
                    Column(name="id", type=DataType.INT, primary_key=True),
                ] + list(extra_cols or [])
                return SlayerModel(
                    name=name,
                    data_source="prod",
                    sql_table=name,
                    columns=cols,
                    aggregations=[
                        Aggregation(name=a, formula="AVG({value})")
                        for a in (aggs or [])
                    ],
                    joins=[
                        ModelJoin(target_model=t, join_pairs=[["id", "id"]])
                        for t in (joins or [])
                    ],
                )

            await storage.save_model(_chain_model(
                "e", aggs=["rolling_avg"],
                extra_cols=[Column(name="score", type=DataType.DOUBLE)],
            ))
            await storage.save_model(_chain_model("d", joins=["e"]))
            await storage.save_model(_chain_model("c", joins=["d"]))
            await storage.save_model(_chain_model("b", joins=["c"]))
            a = _chain_model("a", joins=["b"]).model_copy(update={
                "measures": [
                    ModelMeasure(
                        name="deep_ravg",
                        formula="rolling_avg(b.c.d.e.score)",
                    ),
                ],
            })
            saved = await engine.save_model(a)
            assert saved.measures[0].formula == "b.c.d.e.score:rolling_avg"
        finally:
            td.cleanup()

    async def test_query_path_rewrites_joined_custom_agg_in_filter(self):
        # Filters in normalize_query are also passed custom_agg_names — a
        # joined custom agg in a filter must rewrite to colon form and
        # surface a FUNC_STYLE_AGG warning anchored at `filters[0]`.
        engine, storage, td = await _engine_with_prod()
        try:
            await storage.save_model(_customers_with_rolling_avg())
            await storage.save_model(_orders_joining_customers())
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "revenue:sum"}],
                dimensions=["status"],
                filters=["rolling_avg(customers.score) > 100"],
            )
            resp = await engine.execute(q, dry_run=True)
            assert any(
                w.rule_id == "FUNC_STYLE_AGG"
                and w.location == "filters[0]"
                and "customers.score:rolling_avg" in w.normalized
                for w in resp.warnings
            ), resp.warnings
        finally:
            td.cleanup()

    async def test_query_path_4hop_joined_custom_agg_warning_form(self):
        # Companion to the SQL-generator 4-hop tracking test that asserts
        # `AVG(` only. This pins the slack-layer warning: a 4-hop joined
        # custom agg in a funcstyle measure surfaces a FUNC_STYLE_AGG
        # warning whose normalized form is the colon-syntax canonical.
        engine, storage, td = await _engine_with_prod()
        try:
            def _chain_model(name, *, joins=None, aggs=None, extra_cols=None):
                cols = [
                    Column(name="id", type=DataType.INT, primary_key=True),
                ] + list(extra_cols or [])
                return SlayerModel(
                    name=name,
                    data_source="prod",
                    sql_table=name,
                    columns=cols,
                    aggregations=[
                        Aggregation(name=a, formula="AVG({value})")
                        for a in (aggs or [])
                    ],
                    joins=[
                        ModelJoin(target_model=t, join_pairs=[["id", "id"]])
                        for t in (joins or [])
                    ],
                )

            await storage.save_model(_chain_model(
                "e", aggs=["rolling_avg"],
                extra_cols=[Column(name="score", type=DataType.DOUBLE)],
            ))
            await storage.save_model(_chain_model("d", joins=["e"]))
            await storage.save_model(_chain_model("c", joins=["d"]))
            await storage.save_model(_chain_model("b", joins=["c"]))
            await storage.save_model(_chain_model("a", joins=["b"]))
            q = SlayerQuery(
                source_model="a",
                measures=[{"formula": "rolling_avg(b.c.d.e.score)"}],
            )
            resp = await engine.execute(q, dry_run=True)
            assert any(
                w.rule_id == "FUNC_STYLE_AGG"
                and w.normalized == "b.c.d.e.score:rolling_avg"
                for w in resp.warnings
            ), resp.warnings
        finally:
            td.cleanup()

    async def test_multistage_named_stage_scoping_does_not_cross_stages(self):
        # Negative scoping guard for the engine: a stage whose source model
        # cannot reach `rolling_avg` must NOT have its funcstyle rewritten,
        # even when ANOTHER stage's source DOES have `rolling_avg`. Under a
        # buggy union-of-all-referenced impl, both stages would rewrite. The
        # scoped impl emits exactly one FUNC_STYLE_AGG warning (for the
        # reachable stage) and the unreachable stage raises at enrichment.
        import warnings as _w

        from slayer.core.errors import UnknownFunctionError
        from slayer.core.warnings import SlayerNormalizationWarning

        engine, storage, td = await _engine_with_prod()
        try:
            # widgets has the custom aggregation; no join to gadgets.
            await storage.save_model(SlayerModel(
                name="widgets",
                data_source="prod",
                sql_table="widgets",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="value", type=DataType.DOUBLE),
                ],
                aggregations=[
                    Aggregation(name="rolling_avg", formula="AVG({value})"),
                ],
            ))
            # gadgets has NO aggregation, no join — cannot reach rolling_avg.
            await storage.save_model(SlayerModel(
                name="gadgets",
                data_source="prod",
                sql_table="gadgets",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="qty", type=DataType.DOUBLE),
                ],
            ))
            reachable = SlayerQuery(
                name="reachable",
                source_model="widgets",
                measures=[{"formula": "rolling_avg(value)"}],
                dimensions=["id"],
            )
            unreachable = SlayerQuery(
                source_model="gadgets",
                measures=[{"formula": "rolling_avg(qty)"}],
                dimensions=["id"],
            )

            with _w.catch_warnings(record=True) as caught:
                _w.simplefilter("always", SlayerNormalizationWarning)
                with pytest.raises(UnknownFunctionError):
                    await engine.execute([reachable, unreachable], dry_run=True)

            payloads = [
                w.message.payload for w in caught
                if isinstance(w.message, SlayerNormalizationWarning)
            ]
            fs = [p for p in payloads if p.rule_id == "FUNC_STYLE_AGG"]
            # Exactly one FUNC_STYLE_AGG warning fired — for the reachable
            # stage only. The unreachable stage's funcstyle was NOT rewritten
            # (which is why the run later raises UnknownFunctionError).
            assert len(fs) == 1, fs
            assert fs[0].normalized == "value:rolling_avg"
        finally:
            td.cleanup()
