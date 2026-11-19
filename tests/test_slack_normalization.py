"""Stage 6 (DEV-1450) — slack normalization layer (MISPLACED_METRIC).

FUNC_STYLE_AGG is RETIRED (DEV-1826): the parser accepts functional
aggregations natively, so
``normalize_query`` neither rewrites formula text nor warns about it — pinned
here alongside the engine wiring (custom aggregations, joined-model custom
aggregations, multi-hop paths, and stage scoping all resolve at binding).
"""

from __future__ import annotations

import tempfile
import warnings
from pathlib import Path

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import PositionTypingError
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery
from slayer.core.warnings import SlayerNormalizationWarning
from slayer.engine.normalization import (
    NormalizationResult,
    normalize_query,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from slayer.storage.base import StorageBackend


def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        data_source="prod",
        sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="revenue", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
        ],
    )


# ---------------------------------------------------------------------------
# FUNC_STYLE_AGG retired — functional input is first-class, never rewritten
# ---------------------------------------------------------------------------


class TestFunctionalSpellingFirstClass:
    def test_sum_not_rewritten_no_warning(self):
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.revenue)"}],
        })
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = normalize_query(q)

        assert result.query is not None
        assert result.query.metrics
        assert result.query.metrics[0].metric == "sum(orders.revenue)"
        assert result.warnings == []
        slack = [c for c in caught if isinstance(c.message, SlayerNormalizationWarning)]
        assert slack == []

    def test_count_star_not_rewritten(self):
        q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "count(orders.*)"}]})
        result = normalize_query(q)
        assert result.query is not None
        assert result.query.metrics
        assert result.query.metrics[0].metric == "count(orders.*)"
        assert result.warnings == []

    def test_filter_not_rewritten(self):
        q = SlayerQuery(
            population="orders",
            filter=["sum(orders.revenue) > 100"],
        )
        result = normalize_query(q)
        assert result.query.filter[0] == "sum(orders.revenue) > 100"
        assert result.warnings == []


# ---------------------------------------------------------------------------
# MISPLACED_METRIC
# ---------------------------------------------------------------------------


class TestMisplacedMeasure:
    def test_bare_column_in_measures_moves_to_dimensions(self):
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "orders.status"}],
        })
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = normalize_query(q, dataset=_orders())

        assert result.query.metrics == []
        dim_names = [getattr(d, "name", d) for d in result.query.dimensions]
        assert "status" in dim_names
        assert any(w.rule_id == "MISPLACED_METRIC" for w in result.warnings)
        assert any(
            isinstance(c.message, SlayerNormalizationWarning)
            and c.message.payload.rule_id == "MISPLACED_METRIC"
            for c in caught
        )

    def test_named_modelmeasure_formula_not_moved(self):
        # If the bare name matches a ModelMeasure, it's a valid measure ref
        # (not a column) and stays in measures.
        m = _orders().model_copy(update={
            "metrics": [DatasetMetric(name="aov", expression="avg(revenue)")],
        })
        q = SlayerQuery(population="orders", metrics=[{"metric": "orders.aov"}])
        result = normalize_query(q, dataset=m)
        assert result.query is not None
        assert result.query.metrics
        assert result.query.metrics[0].metric == "orders.aov"
        assert result.warnings == []

    def test_unknown_bare_token_left_alone(self):
        # Not a column and not a measure — the resolver will error later,
        # but normalization does not preemptively rewrite.
        q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "orders.noseucha"}]})
        result = normalize_query(q, dataset=_orders())
        assert result.query is not None
        assert result.query.metrics
        assert result.query.metrics[0].metric == "orders.noseucha"

    def test_no_model_means_no_move(self):
        # MISPLACED_METRIC needs model context to classify.
        q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "orders.status"}]})
        result = normalize_query(q, dataset=None)
        # Without a model the rule no-ops.
        assert result.query is not None
        assert result.query.metrics
        assert result.query.metrics[0].metric == "orders.status"
        assert not any(w.rule_id == "MISPLACED_METRIC" for w in result.warnings)

    def test_formula_with_call_not_moved(self):
        # Anything containing parens is treated as a formula, even if the
        # function name happens to also be a column — and functional
        # aggregations are first-class, so no warning of any kind fires.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.revenue)"}],
        })
        result = normalize_query(q, dataset=_orders())
        assert result.query is not None
        assert result.query.metrics
        assert result.query.metrics[0].metric == "sum(orders.revenue)"
        assert result.warnings == []

    def test_aggregation_measures_kept(self):
        # Aggregations (`sum(revenue)`, `count(*)`) are real
        # measures — never reclassified as dimensions.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.revenue)"}, {"metric": "count(orders.*)"}],
        })
        result = normalize_query(q, dataset=_orders())
        assert len(result.query.metrics) == 2
        assert not result.query.dimensions
        assert not any(w.rule_id == "MISPLACED_METRIC" for w in result.warnings)

    # DEV-1484 backfill from TestAutoMoveDimensions.test_arithmetic_kept
    def test_arithmetic_formula_kept(self):
        # An arithmetic-over-aggregates formula stays a measure.
        q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.revenue) / count(orders.*)"}]})
        result = normalize_query(q, dataset=_orders())
        assert len(result.query.metrics) == 1
        assert not any(w.rule_id == "MISPLACED_METRIC" for w in result.warnings)

    # DEV-1484 backfill from TestAutoMoveDimensions.test_invalid_cross_model_path_kept
    def test_dotted_cross_model_ref_kept(self):
        # A dotted cross-model ref (`customers.nonexistent`) is NOT a bare
        # local column, so MISPLACED_METRIC leaves it in measures (the
        # binder errors later if the path is invalid). This also covers the
        # legacy dotted-named-measure case — every dotted ref is kept.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "customers.nonexistent"}, {"metric": "sum(orders.revenue)"}],
        })
        result = normalize_query(q, dataset=_orders())
        assert len(result.query.metrics) == 2
        assert not any(w.rule_id == "MISPLACED_METRIC" for w in result.warnings)

    # DEV-1484 backfill from
    # TestAutoMoveDimensions.test_dotted_named_measure_not_moved_via_named_queries
    def test_dotted_named_measure_ref_kept(self):
        # A dotted ref to a named ModelMeasure on a joined model
        # (`customers.name_count`) is dotted, so MISPLACED_METRIC keeps it
        # in measures rather than moving it to dimensions.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "customers.name_count"}, {"metric": "sum(orders.revenue)"}],
        })
        result = normalize_query(q, dataset=_orders())
        assert len(result.query.metrics) == 2
        assert not any(w.rule_id == "MISPLACED_METRIC" for w in result.warnings)

    # DEV-1484 backfill from TestAutoMoveDimensions.test_no_fields_noop
    def test_no_measures_is_noop(self):
        # No measures to classify — the rule short-circuits and leaves the
        # query untouched.
        q = SlayerQuery(population="orders", dimensions=["orders.status"])
        result = normalize_query(q, dataset=_orders())
        assert not result.query.metrics
        assert [getattr(d, "name", d) for d in result.query.dimensions] == ["status"]
        assert not any(w.rule_id == "MISPLACED_METRIC" for w in result.warnings)

    # DEV-1484 backfill from TestAutoMoveDimensions.test_appends_to_existing_dimensions
    def test_moved_column_appends_to_existing_dimensions(self):
        # A misplaced bare column is appended to existing dimensions, not
        # replacing them.
        m = _orders().model_copy(update={
            "fields": _orders().fields + [DatasetField(name="customer_id", datatype=DataType.INTEGER)],
        })
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "orders.customer_id"}, {"metric": "sum(orders.revenue)"}],
            "dimensions": ["orders.status"],
        })
        result = normalize_query(q, dataset=m)
        assert len(result.query.metrics) == 1
        assert result.query is not None
        assert result.query.metrics
        assert result.query.metrics[0].metric == "sum(orders.revenue)"
        dim_names = [getattr(d, "name", d) for d in result.query.dimensions]
        assert "status" in dim_names
        assert "customer_id" in dim_names
        assert any(w.rule_id == "MISPLACED_METRIC" for w in result.warnings)

    def test_moved_column_is_a_column_ref(self):
        # A bare string here fails in the planner on ``.full_name``.
        q = SlayerQuery(population="orders", metrics=[QueryMetric(metric="orders.status")])
        result = normalize_query(query=q, dataset=_orders())
        assert result.query is not None
        dims = result.query.dimensions or []
        assert [type(d) for d in dims] == [QueryDimension]
        assert dims[0].ref is not None
        assert dims[0].ref.full_name == "orders.status"


# ---------------------------------------------------------------------------
# NormalizationResult shape
# ---------------------------------------------------------------------------


class TestResult:
    def test_canonical_input_returns_unchanged_query(self):
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.revenue)"}],
            "dimensions": ["orders.status"],
        })
        result = normalize_query(q, dataset=_orders())
        assert isinstance(result, NormalizationResult)
        assert result.warnings == []
        # No semantic change — measures and dimensions match (after the
        # SlayerQuery ColumnRef coercion).
        assert len(result.query.metrics) == 1
        assert [getattr(d, "name", d) for d in result.query.dimensions] == ["status"]


# ---------------------------------------------------------------------------
# Engine wiring — functional input executes warning-free; save preserves it
# ---------------------------------------------------------------------------


class TestEngineWiring:
    async def test_functional_query_executes_without_warnings(self):
        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=str(Path(td) / "models"))
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite")
            )
            await storage.save_dataset(_orders())
            engine = SlayerQueryEngine(storage=storage)

            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.revenue)"}],
                "dimensions": ["orders.status"],
            })
            resp = await engine.execute(q, dry_run=True)
            assert resp.warnings == []
            assert "SUM(" in resp.sql.upper()

    async def test_clean_query_has_empty_warnings(self):
        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=str(Path(td) / "models"))
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite")
            )
            await storage.save_dataset(_orders())
            engine = SlayerQueryEngine(storage=storage)

            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.revenue)"}],
                "dimensions": ["orders.status"],
            })
            resp = await engine.execute(q, dry_run=True)
            assert resp.warnings == []

    async def test_misplaced_measure_query_compiles(self):
        # A bare column in ``measures`` must survive the move and plan.
        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=str(Path(td) / "models"))
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite")
            )
            await storage.save_dataset(_orders())
            engine = SlayerQueryEngine(storage=storage)

            q = SlayerQuery(
                population="orders",
                metrics=[
                    QueryMetric(metric="orders.status"),
                    QueryMetric(metric="sum(orders.revenue)"),
                ],
            )
            resp = await engine.execute(query=q, dry_run=True)
            assert resp.sql
            assert "status" in resp.sql

    async def test_custom_agg_functional_measure_binds(self):
        # A custom aggregation written functionally resolves at binding —
        # no rewrite, no warning, correct SQL.
        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=str(Path(td) / "models"))
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite")
            )
            m = _orders().model_copy(update={
                "aggregations": [Aggregation(name="custom_sum", expression="SUM({value})")],
            })
            await storage.save_dataset(m)
            engine = SlayerQueryEngine(storage=storage)

            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "custom_sum(orders.revenue)"}],
            })
            resp = await engine.execute(q, dry_run=True)
            assert resp.warnings == []
            assert "SUM(" in resp.sql.upper()

    # DEV-1484 backfill from TestAutoMoveDimensions.test_cross_model_dimension_moved
    async def test_cross_model_column_in_measures_is_a_typing_error(self):
        # MISPLACED_METRIC moves only bare local columns; a dotted ref left in
        # measures never changes the grain, so it is a typing error.
        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=str(Path(td) / "models"))
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite")
            )
            await storage.save_dataset(SlayerDataset(
                name="customers", data_source="prod", sql_table="customers",
                fields=[
                    DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="name", datatype=DataType.STRING),
                ],
            ))
            await storage.save_dataset(SlayerDataset(
                name="orders", data_source="prod", sql_table="orders",
                fields=[
                    DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="customer_id", datatype=DataType.INTEGER),
                    DatasetField(name="revenue", datatype=DataType.FLOAT),
                ],
                relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
            ))
            engine = SlayerQueryEngine(storage=storage)
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "customers.name"}, {"metric": "sum(orders.revenue)"}],
            })
            with pytest.raises(PositionTypingError) as ei:
                await engine.execute(q, dry_run=True)
            assert ei.value.location == "metric 'customers.name'"
            assert "customers.name" in ei.value.summary
            assert "add it to the query dimensions" in str(ei.value)

    async def test_save_model_preserves_functional_spelling(self):
        with tempfile.TemporaryDirectory() as td:
            storage = YAMLStorage(base_dir=str(Path(td) / "models"))
            await storage.save_datasource(
                DatasourceConfig(name="prod", type="sqlite")
            )
            engine = SlayerQueryEngine(storage=storage)
            m = _orders().model_copy(update={
                "metrics": [DatasetMetric(name="rev_s", expression="sum(revenue)")],
            })
            saved = await engine.save_dataset(m)
            assert saved.metrics[0].expression == "sum(revenue)"


# ---------------------------------------------------------------------------
# DEV-1500 → DEV-1826 — joined-model custom aggregations resolve at BINDING
# ---------------------------------------------------------------------------


def _customers_with_rolling_avg() -> SlayerDataset:
    return SlayerDataset(
        name="customers",
        data_source="prod",
        sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="score", datatype=DataType.FLOAT),
        ],
        aggregations=[Aggregation(name="rolling_avg", expression="AVG({value})")],
    )


def _orders_joining_customers() -> SlayerDataset:
    return _orders().model_copy(update={
        "relationships": [
            DatasetRelationship(to="customers", from_columns=["id"], to_columns=["id"]),
        ],
    })


def _chain_dataset(
    name: str,
    *,
    joins: list[str] | None = None,
    aggs: list[str] | None = None,
    extra_cols: list[DatasetField] | None = None,
) -> SlayerDataset:
    """Build one node of a linear chain a->b->c->... — used by the 4-hop
    query test to exercise unbounded join-path resolution.
    """
    cols = [
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
    ] + list(extra_cols or [])
    return SlayerDataset(
        name=name,
        data_source="prod",
        sql_table=name,
        fields=cols,
        aggregations=[
            Aggregation(name=a, expression="AVG({value})")
            for a in (aggs or [])
        ],
        relationships=[
            DatasetRelationship(to=t, from_columns=["id"], to_columns=["id"])
            for t in (joins or [])
        ],
    )


async def _save_4hop_chain(storage: StorageBackend) -> None:
    """Save the a->b->c->d->e chain shared by the 4-hop tests; ``e`` carries
    ``rolling_avg`` and a ``score`` column.
    """
    await storage.save_dataset(_chain_dataset(
        "e", aggs=["rolling_avg"],
        extra_cols=[DatasetField(name="score", datatype=DataType.FLOAT)],
    ))
    await storage.save_dataset(_chain_dataset("d", joins=["e"]))
    await storage.save_dataset(_chain_dataset("c", joins=["d"]))
    await storage.save_dataset(_chain_dataset("b", joins=["c"]))


async def _engine_with_prod():
    """A fresh engine over an in-memory-SQLite YAML store. Returns
    ``(engine, storage)``; caller owns the TemporaryDirectory lifetime.
    """
    td = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=Path(td.name) / "models")
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite")
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
            await storage.save_dataset(_customers_with_rolling_avg())
            await storage.save_dataset(_orders_joining_customers())
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "rolling_avg(customers.score)"}],
                "dimensions": ["orders.status"],
            })
            resp = await engine.execute(q, dry_run=True)
            assert not _slack_rewrite_warnings(resp.warnings), resp.warnings
            assert "AVG(" in resp.sql, resp.sql
        finally:
            td.cleanup()

    async def test_save_model_preserves_joined_custom_agg_formula(self):
        engine, storage, td = await _engine_with_prod()
        try:
            await storage.save_dataset(_customers_with_rolling_avg())
            orders = _orders_joining_customers().model_copy(update={
                "metrics": [
                    DatasetMetric(
                        name="ravg", expression="rolling_avg(customers.score)"
                    ),
                ],
            })
            saved = await engine.save_dataset(orders)
            assert saved.metrics[0].expression == "rolling_avg(customers.score)"
            # Spelling persisted verbatim, and the saved measure still binds.
            reloaded = await storage.get_dataset("orders", data_source="prod")
            assert reloaded.metrics[0].expression == "rolling_avg(customers.score)"
            resp = await engine.execute(
                SlayerQuery.model_validate({
                    "population": "orders",
                    "metrics": [{"metric": "orders.ravg"}],
                    "dimensions": ["orders.status"],
                }),
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
            await storage.save_dataset(_customers_with_rolling_avg())
            await storage.save_dataset(_orders_joining_customers())
            stage1 = SlayerQuery(
                name="stage1",
                population="orders",
                dimensions=["orders.status"],
                metrics=[
                    {"metric": "rolling_avg(customers.score)", "alias": "ravg"},
                ],
            )
            root = SlayerQuery.model_validate({
                "population": "stage1",
                "dimensions": ["stage1.status"],
                "metrics": [{"metric": "max(stage1.ravg)"}],
            })
            resp = await engine.execute([stage1, root], dry_run=True)
            assert not _slack_rewrite_warnings(resp.warnings), resp.warnings
            assert "AVG(" in resp.sql, resp.sql
        finally:
            td.cleanup()

    async def test_query_path_binds_functional_joined_agg_in_filter(self):
        engine, storage, td = await _engine_with_prod()
        try:
            await storage.save_dataset(_customers_with_rolling_avg())
            await storage.save_dataset(_orders_joining_customers())
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.revenue)"}],
                "dimensions": ["orders.status"],
                "filter": ["rolling_avg(customers.score) > 100"],
            })
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
            await storage.save_dataset(_chain_dataset("a", joins=["b"]))
            q = SlayerQuery.model_validate({
                "population": "a",
                "metrics": [{"metric": "rolling_avg(b.c.d.e.score)"}],
            })
            resp = await engine.execute(q, dry_run=True)
            assert not _slack_rewrite_warnings(resp.warnings), resp.warnings
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
            await storage.save_dataset(SlayerDataset(
                name="widgets",
                data_source="prod",
                sql_table="widgets",
                fields=[
                    DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="value", datatype=DataType.FLOAT),
                ],
                aggregations=[
                    Aggregation(name="rolling_avg", expression="AVG({value})"),
                ],
            ))
            # gadgets has NO aggregation, no join — cannot reach rolling_avg.
            await storage.save_dataset(SlayerDataset(
                name="gadgets",
                data_source="prod",
                sql_table="gadgets",
                fields=[
                    DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="qty", datatype=DataType.FLOAT),
                ],
            ))
            reachable = SlayerQuery.model_validate({
                "name": "reachable",
                "population": "widgets",
                "metrics": [{"metric": "rolling_avg(widgets.value)"}],
                "dimensions": ["widgets.id"],
            })
            unreachable = SlayerQuery.model_validate({
                "population": "gadgets",
                "metrics": [{"metric": "rolling_avg(gadgets.qty)"}],
                "dimensions": ["gadgets.id"],
            })
            with pytest.raises(ValueError, match="Unknown function 'rolling_avg'"):
                await engine.execute([reachable, unreachable], dry_run=True)
        finally:
            td.cleanup()
