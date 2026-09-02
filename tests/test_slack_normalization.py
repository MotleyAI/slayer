"""Stage 6 (DEV-1450) — slack normalization layer (MISPLACED_MEASURE).

FUNC_STYLE_AGG is RETIRED (DEV-1826): the parser accepts functional
aggregations natively as first-class equivalents of colon syntax, so
``normalize_query`` neither rewrites formula text nor warns about it — pinned
here alongside the engine wiring (custom aggregations, joined-model custom
aggregations, multi-hop paths, and stage scoping all resolve at binding).
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
# FUNC_STYLE_AGG retired — functional input is first-class, never rewritten
# ---------------------------------------------------------------------------


class TestFunctionalSpellingFirstClass:
    def test_sum_not_rewritten_no_warning(self):
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "sum(revenue)"}],
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = normalize_query(q)

        assert result.query.measures[0].formula == "sum(revenue)"
        assert result.warnings == []
        slack = [c for c in caught if isinstance(c.message, SlayerNormalizationWarning)]
        assert slack == []

    def test_count_star_not_rewritten(self):
        q = SlayerQuery(source_model="orders", measures=[{"formula": "count(*)"}])
        result = normalize_query(q)
        assert result.query.measures[0].formula == "count(*)"
        assert result.warnings == []

    def test_colon_form_no_warning(self):
        q = SlayerQuery(source_model="orders", measures=[{"formula": "revenue:sum"}])
        result = normalize_query(q)
        assert result.warnings == []
        assert result.query.measures[0].formula == "revenue:sum"

    def test_filter_not_rewritten(self):
        q = SlayerQuery(
            source_model="orders",
            filters=["sum(revenue) > 100"],
        )
        result = normalize_query(q)
        assert result.query.filters[0] == "sum(revenue) > 100"
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
        # function name happens to also be a column — and functional
        # aggregations are first-class, so no warning of any kind fires.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "sum(revenue)"}],
        )
        result = normalize_query(q, model=_orders())
        assert result.query.measures[0].formula == "sum(revenue)"
        assert result.warnings == []

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


# ---------------------------------------------------------------------------
# Engine wiring — functional input executes warning-free; save preserves it
# ---------------------------------------------------------------------------


class TestEngineWiring:
    async def test_functional_query_executes_without_warnings(self):
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
            assert resp.warnings == []
            assert "SUM(" in resp.sql.upper()

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

    async def test_custom_agg_functional_measure_binds(self):
        # A custom aggregation written functionally resolves at binding —
        # no rewrite, no warning, correct SQL.
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
            assert resp.warnings == []
            assert "SUM(" in resp.sql.upper()

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

    async def test_save_model_preserves_functional_spelling(self):
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
            assert saved.measures[0].formula == "sum(revenue)"


# ---------------------------------------------------------------------------
# DEV-1500 → DEV-1826 — joined-model custom aggregations resolve at BINDING
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


def _chain_model(
    name: str,
    *,
    joins: list[str] | None = None,
    aggs: list[str] | None = None,
    extra_cols: list[Column] | None = None,
) -> SlayerModel:
    """Build one node of a linear chain a->b->c->... — used by the 4-hop
    query test to exercise unbounded join-path resolution.
    """
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


async def _save_4hop_chain(storage) -> None:
    """Save the a->b->c->d->e chain shared by the 4-hop tests; ``e`` carries
    ``rolling_avg`` and a ``score`` column.
    """
    await storage.save_model(_chain_model(
        "e", aggs=["rolling_avg"],
        extra_cols=[Column(name="score", type=DataType.DOUBLE)],
    ))
    await storage.save_model(_chain_model("d", joins=["e"]))
    await storage.save_model(_chain_model("c", joins=["d"]))
    await storage.save_model(_chain_model("b", joins=["c"]))


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


def _slack_rewrite_warnings(ws) -> list:
    """Normalization-rewrite warnings only (a BroadcastGrainWarning is a
    legitimate cross-model grain note, orthogonal to spelling)."""
    return [w for w in ws if getattr(w, "rule_id", None) is not None]


class TestJoinedCustomAggFunctional:
    """End-to-end: a custom aggregation defined on a *joined* model, written
    functionally, resolves at binding — no rewrite, no warning — on both the
    query path and after a spelling-preserving save.
    """

    async def test_query_path_binds_functional_joined_agg(self):
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
            assert not _slack_rewrite_warnings(resp.warnings), resp.warnings
            assert "AVG(" in resp.sql, resp.sql
        finally:
            td.cleanup()

    async def test_save_model_preserves_joined_custom_agg_formula(self):
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
            saved = await engine.save_model(orders)
            assert saved.measures[0].formula == "rolling_avg(customers.score)"
            # Spelling persisted verbatim, and the saved measure still binds.
            reloaded = await storage.get_model("orders", data_source="prod")
            assert reloaded.measures[0].formula == "rolling_avg(customers.score)"
            resp = await engine.execute(
                SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "ravg"}],
                    dimensions=["status"],
                ),
                dry_run=True,
            )
            assert "AVG(" in resp.sql, resp.sql
        finally:
            td.cleanup()

    async def test_multistage_named_stage_scoping(self):
        # A NAMED non-root stage sourced from `orders` (which joins
        # `customers`) binds its functional joined-agg against its OWN
        # source model's join graph.
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
            assert not _slack_rewrite_warnings(resp.warnings), resp.warnings
            assert "AVG(" in resp.sql, resp.sql
        finally:
            td.cleanup()

    async def test_query_path_binds_functional_joined_agg_in_filter(self):
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
            assert not _slack_rewrite_warnings(resp.warnings), resp.warnings
            assert "AVG(" in resp.sql, resp.sql
        finally:
            td.cleanup()

    async def test_query_path_4hop_joined_custom_agg(self):
        # Join-path resolution is unbounded: a 4-hop functional custom agg
        # (`rolling_avg` lives on `e`) binds and renders its AVG formula.
        engine, storage, td = await _engine_with_prod()
        try:
            await _save_4hop_chain(storage)
            await storage.save_model(_chain_model("a", joins=["b"]))
            q = SlayerQuery(
                source_model="a",
                measures=[{"formula": "rolling_avg(b.c.d.e.score)"}],
            )
            resp = await engine.execute(q, dry_run=True)
            assert resp.warnings == [], resp.warnings
            assert "AVG(" in resp.sql, resp.sql
        finally:
            td.cleanup()

    async def test_stage_scoping_does_not_leak_across_stages(self):
        # Negative scoping guard: a stage whose source model cannot reach
        # `rolling_avg` fails with the standard unknown-aggregation error,
        # even when ANOTHER stage's source DOES define `rolling_avg`.
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
            with pytest.raises(ValueError, match="Unknown aggregation 'rolling_avg'"):
                await engine.execute([reachable, unreachable], dry_run=True)
        finally:
            td.cleanup()
