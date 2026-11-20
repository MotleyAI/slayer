"""Query-backed expansion on the typed pipeline: ``_expand_query_backed_dataset``,
``_validate_and_populate_cache`` and ``get_result_column_types``.

* Stored ``source_queries`` are topo-sorted (Kahn), not required top-to-bottom.
* Virtual-dataset fields are the PUBLIC slots only, typed from typed-plan slot types.
* Per-stage normalize + variable substitution mirror ``_execute_pipeline``.
* ``expand_query_backed_models_in_bundle`` handles nested query-backed relationship
  targets, query-backed stage sources, and the root ``DatasetExtension`` overlay.
* ``get_result_column_types`` renders its probe through ``generate_planned_stages``.
"""
from __future__ import annotations

import os
from slayer.storage.sqlite_conn import transaction
import tempfile
from typing import List, Tuple
from unittest.mock import patch

import pytest
import sqlglot
from sqlglot import exp

import slayer.engine.query_engine as query_engine_module
from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import DatasetExtension, QueryMetric, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="orders_t",
        data_source="ds",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
            DatasetField(name="region", sql="region", datatype=DataType.STRING),
            DatasetField(
                name="amount",
                sql="amount",
                datatype=DataType.FLOAT,
                description="Total order amount in USD",
                format=NumberFormat(type=NumberFormatType.CURRENCY),
            ),
            DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.INTEGER),
        ],
        default_time_dimension="created_at",
    )


def _customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers",
        sql_table="customers_t",
        data_source="ds",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", sql="name", datatype=DataType.STRING),
            DatasetField(name="region", sql="region", datatype=DataType.STRING),
            DatasetField(name="signup_at", sql="signup_at", datatype=DataType.DATETIME),
        ],
    )


def _ds() -> DatasourceConfig:
    return DatasourceConfig(name="ds", type="sqlite", database=":memory:")


def _ds2() -> DatasourceConfig:
    return DatasourceConfig(name="ds2", type="sqlite", database=":memory:")


async def _engine(*extra_datasets: SlayerDataset) -> Tuple[SlayerQueryEngine, tempfile.TemporaryDirectory]:
    """Build YAMLStorage with ``orders`` saved + extras, return engine + tmp."""
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(_ds())
    await storage.save_dataset(_orders_dataset())
    for m in extra_datasets:
        await storage.save_dataset(m)
    engine = SlayerQueryEngine(storage=storage)
    return engine, tmp


async def _dry_run_sql(engine: SlayerQueryEngine, name: str) -> str:
    """The SQL a dry run of the saved dataset ``name`` renders."""
    sql = (await engine.execute(name, dry_run=True)).sql
    assert sql is not None
    return sql


# ---------------------------------------------------------------------------
# Topo-sort acceptance on STORED source_queries (decision #1)
# ---------------------------------------------------------------------------


class TestStoredSourceQueriesTopoSort:
    async def test_run_by_name_accepts_nonordered_join_target_stages(self) -> None:
        """Codex round-5 fix — ``engine.execute(name)`` must topologically
        sort stored ``source_queries`` so out-of-order ``relationships[].to``
        deps work end-to-end. Pre-fix the save path went through
        ``_expand_query_backed_dataset`` (which sorts) but run-by-name went
        through ``_execute_pipeline`` directly, and ``plan_stages._topo_
        sort`` only handles ``population`` deps. So a dataset that saved
        successfully would fail when run by name.
        """
        non_ordered = SlayerDataset(
            name="qb_runbyname_topo",
            data_source="ds",
            source_queries=[
                # Out-of-order: ``main`` references ``kpi`` via
                # relationships.to but appears FIRST.
                SlayerQuery.model_validate({
                    "name": "main",
                    "population": "orders",
                    "extensions": [DatasetExtension.model_validate({
                        "dataset": "orders",
                        # The reverse hop is traversable, so the pair names kpi's
                        # real exported field ("id"; "kpi_id" is only a label).
                        "relationships": [{"to": "kpi", "from_columns": ["id"], "to_columns": ["id"]}],
                    })],
                    "dimensions": ["orders.status"],
                    "metrics": [{"metric": "sum(kpi._count)"}],
                }),
                SlayerQuery.model_validate({
                    "name": "kpi",
                    "population": "orders",
                    "metrics": [{"metric": "count(orders.*)"}],
                    "dimensions": [{"field": "orders.id", "label": "kpi_id"}],
                }),
                SlayerQuery(population="main"),
            ],
        )
        engine, tmp = await _engine()
        try:
            await engine.save_dataset(non_ordered)
            # Save worked (round-1 test pinned this). Now run by name:
            resp = await engine.execute("qb_runbyname_topo", dry_run=True)
            assert resp.sql is not None, "run-by-name must produce SQL"
        finally:
            tmp.cleanup()

    async def test_topo_sort_accepts_nonordered_save_path(self) -> None:
        """A query-backed dataset whose stored ``source_queries`` are NOT in
        topological order saves cleanly under Stage B (legacy strict order
        would reject this as a forward reference)."""
        non_ordered = SlayerDataset(
            name="topo_qb",
            data_source="ds",
            source_queries=[
                # main references kpi (forward ref under legacy strict order)
                SlayerQuery.model_validate({
                    "name": "main",
                    "population": "orders",
                    "extensions": [DatasetExtension.model_validate({
                        "dataset": "orders",
                        # The reverse hop is traversable, so the pair names kpi's
                        # real exported field ("id"; "kpi_id" is only a label).
                        "relationships": [{"to": "kpi", "from_columns": ["id"], "to_columns": ["id"]}],
                    })],
                    "dimensions": ["orders.status"],
                    "metrics": [{"metric": "sum(kpi._count)"}],
                }),
                SlayerQuery.model_validate({
                    "name": "kpi",
                    "population": "orders",
                    "metrics": [{"metric": "count(orders.*)"}],
                    "dimensions": [ {"field": "orders.id", "label": "kpi_id"} ],
                }),
                SlayerQuery(population="main"),
            ],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(non_ordered)
            assert "orders_t" in (await _dry_run_sql(engine, saved.name)).lower()
        finally:
            tmp.cleanup()

    async def test_cycle_raises_clearly(self) -> None:
        engine, tmp = await _engine()
        try:
            mutual = SlayerDataset(
                name="qb_cycle",
                data_source="ds",
                source_queries=[
                    SlayerQuery(
                        name="a",
                        population="orders",
                        extensions=[DatasetExtension.model_validate({
                            "dataset": "orders",
                            "relationships": [{"to": "b", "from_columns": ["id"], "to_columns": ["id"]}],
                        })],
                    ),
                    SlayerQuery(
                        name="b",
                        population="orders",
                        extensions=[DatasetExtension.model_validate({
                            "dataset": "orders",
                            "relationships": [{"to": "a", "from_columns": ["id"], "to_columns": ["id"]}],
                        })],
                    ),
                    SlayerQuery(population="a"),
                ],
            )
            with pytest.raises(ValueError, match=r"[Cc]ycle"):
                await engine.save_dataset(mutual)
        finally:
            tmp.cleanup()

    async def test_root_referenced_raises(self) -> None:
        engine, tmp = await _engine()
        try:
            m = SlayerDataset(
                name="qb_root_ref",
                data_source="ds",
                source_queries=[
                    SlayerQuery(name="a", population="root_stage"),
                    SlayerQuery(name="root_stage", population="orders"),
                ],
            )
            with pytest.raises(ValueError, match="root"):
                await engine.save_dataset(m)
        finally:
            tmp.cleanup()

    async def test_self_reference_raises(self) -> None:
        engine, tmp = await _engine()
        try:
            m = SlayerDataset(
                name="qb_self_ref",
                data_source="ds",
                source_queries=[
                    SlayerQuery(name="a", population="a"),
                    SlayerQuery(population="a"),
                ],
            )
            with pytest.raises(ValueError, match="self"):
                await engine.save_dataset(m)
        finally:
            tmp.cleanup()

    async def test_duplicate_name_raises(self) -> None:
        engine, tmp = await _engine()
        try:
            # SlayerDataset's Pydantic validator catches duplicate stage names
            # at construction time. The same guard exists in
            # ``topologically_order_stages`` as defence in depth, but the
            # user-facing error surfaces here first.
            stage_a1 = SlayerQuery(name="a", population="orders")
            stage_a2 = SlayerQuery(name="a", population="orders")
            stage_c = SlayerQuery(population="a")
            with pytest.raises(
                (ValueError, Exception), match="[Dd]uplicate",
            ):
                SlayerDataset(
                    name="qb_dup",
                    data_source="ds",
                    source_queries=[stage_a1, stage_a2, stage_c],
                )
            del engine  # explicit unused (silence linter)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Virtual-dataset field TYPES, FORMAT, DESCRIPTION (decisions #2, #8)
# ---------------------------------------------------------------------------


class TestVirtualModelColumns:
    async def test_star_count_yields_int_column(self) -> None:
        m = SlayerDataset(
            name="qb_count",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "count(orders.*)"}],
                "dimensions": ["orders.status"],
            })],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(m)
            count_col = next(c for c in saved.fields if c.name == "_count")
            assert count_col.datatype == DataType.INTEGER, (
                f"count(*) must yield DataType.INTEGER, got {count_col.datatype!r}"
            )
        finally:
            tmp.cleanup()

    async def test_sum_inherits_source_column_type(self) -> None:
        """``sum(amount)`` over a DOUBLE source field → DOUBLE virtual field."""
        m = SlayerDataset(
            name="qb_sum",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
                "dimensions": ["orders.status"],
            })],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(m)
            sum_col = next(c for c in saved.fields if c.name == "amount_sum")
            assert sum_col.datatype == DataType.FLOAT
        finally:
            tmp.cleanup()

    async def test_query_measure_type_override_wins_over_inference(self) -> None:
        """Codex review fix — a user-supplied ``type=`` on a query metric
        spec is the override; ``_type_for_metric_expression`` is the fallback.

        ``count(*)`` ordinarily infers ``INT``; explicit ``type=DOUBLE``
        on the same expression must win and surface as a DOUBLE virtual
        field.
        """
        m = SlayerDataset(
            name="qb_type_override",
            data_source="ds",
            source_queries=[SlayerQuery(
                population="orders",
                metrics=[QueryMetric(metric="count(orders.*)", datatype=DataType.FLOAT)],
                dimensions=["orders.status"],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(m)
            count_col = next(c for c in saved.fields if c.name == "_count")
            assert count_col.datatype == DataType.FLOAT, (
                f"User-supplied type=DOUBLE must override INT inference, "
                f"got {count_col.datatype!r}"
            )
        finally:
            tmp.cleanup()

    async def test_promoted_hidden_slot_keeps_metadata(self) -> None:
        """Codex review fix — when a hidden slot is later promoted to
        public, its type / format / description must be filled in.

        Repro: declare ``rank(count(*), direction='desc')`` first (hoists ``count(*)`` as a
        hidden dep with no display metadata), THEN declare ``count(*)``
        as a public metric. The promoted public slot must end up with
        ``type=INT`` (not the default None → DOUBLE fallback).
        """
        m = SlayerDataset(
            name="qb_promoted_hidden",
            data_source="ds",
            source_queries=[SlayerQuery(
                population="orders",
                dimensions=["orders.status"],
                metrics=[
                    # rank uses *:count as a hidden inner; intern order
                    # hoists *:count hidden first, then the public
                    # *:count entry promotes the same slot.
                    {"metric": "rank(count(orders.*), direction='desc')", "alias": "ranked"},
                    {"metric": "count(orders.*)"},
                ],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(m)
            count_col = next(c for c in saved.fields if c.name == "_count")
            assert count_col.datatype == DataType.INTEGER, (
                f"Promoted hidden slot must inherit INT from the public "
                f"intern, got {count_col.datatype!r}"
            )
        finally:
            tmp.cleanup()

    async def test_declared_model_measure_type_honored(self) -> None:
        """A ``DatasetMetric`` with explicit ``type=DOUBLE`` carries through
        to the virtual field.
        """
        orders_with_measure = _orders_dataset().model_copy(update={
            "metrics": [
                DatasetMetric(
                    expression="sum(amount) * 1.0",
                    name="adjusted_total",
                    datatype=DataType.FLOAT,
                ),
            ],
        })
        engine, tmp = await _engine()
        try:
            await engine.storage.save_dataset(orders_with_measure)
            m = SlayerDataset(
                name="qb_with_declared_type",
                data_source="ds",
                source_queries=[SlayerQuery.model_validate({
                    "population": "orders",
                    "metrics": [{"metric": "orders.adjusted_total"}],
                    "dimensions": ["orders.status"],
                })],
            )
            saved = await engine.save_dataset(m)
            col = next(c for c in saved.fields if c.name == "adjusted_total")
            assert col.datatype == DataType.FLOAT
        finally:
            tmp.cleanup()

    async def test_saved_model_measure_type_wins_over_inference(self) -> None:
        """Codex round-5 fix — when the query expression is a bare reference
        to a saved ``DatasetMetric`` whose explicit ``type=`` differs from
        the type ``_type_for_metric_expression`` would infer, the saved
        metric's type wins. ``expand_model_measures`` rewrites the AST
        but drops the metric's type metadata; the type-priority chain
        re-looks-up the saved metric here.

        Repro: ``DatasetMetric(expression="sum(amount)", datatype=DataType.INTEGER)``
        on the source dataset — inference for ``sum(amount)`` over a DOUBLE
        ``amount`` would normally pick DOUBLE, but the explicit
        ``type=INT`` must win.
        """
        orders_with_int_measure = _orders_dataset().model_copy(update={
            "metrics": [
                DatasetMetric(
                    expression="sum(amount)",
                    name="rounded_total",
                    datatype=DataType.INTEGER,  # ← explicit override
                ),
            ],
        })
        engine, tmp = await _engine()
        try:
            await engine.storage.save_dataset(orders_with_int_measure)
            m = SlayerDataset(
                name="qb_saved_metric_type",
                data_source="ds",
                source_queries=[SlayerQuery.model_validate({
                    "population": "orders",
                    # Bare-ref + explicit ``name="rounded_total"`` keeps
                    # the column alias as the user-chosen name so the
                    # test can look it up unambiguously.
                    "metrics": [{"metric": "orders.rounded_total", "alias": "rounded_total"}],
                    "dimensions": ["orders.status"],
                })],
            )
            saved = await engine.save_dataset(m)
            col = next(c for c in saved.fields if c.name == "rounded_total")
            assert col.datatype == DataType.INTEGER, (
                f"Saved DatasetMetric.datatype=Integer must override inference "
                f"(which would pick DOUBLE for sum(amount)); got {col.datatype!r}"
            )
        finally:
            tmp.cleanup()

    async def test_inherits_source_format(self) -> None:
        """Source field with ``NumberFormat(CURRENCY)`` → virtual sum field
        carries the same format (via ``_infer_aggregated_format``).
        """
        m = SlayerDataset(
            name="qb_fmt",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
                "dimensions": ["orders.status"],
            })],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(m)
            col = next(c for c in saved.fields if c.name == "amount_sum")
            assert col.format is not None, "amount:sum must carry format"
            assert col.format.type == NumberFormatType.CURRENCY
        finally:
            tmp.cleanup()

    async def test_inherits_source_description(self) -> None:
        m = SlayerDataset(
            name="qb_desc",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
                "dimensions": ["orders.status"],
            })],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(m)
            col = next(c for c in saved.fields if c.name == "amount_sum")
            assert col.description == "Total order amount in USD"
        finally:
            tmp.cleanup()

    async def test_carries_default_time_dimension(self) -> None:
        """Inner source dataset has ``default_time_dimension="created_at"``;
        the virtual dataset returned by ``_expand_query_backed_dataset`` carries
        it forward (mirrors the legacy ``the legacy query-backed wrap`` contract).

        Save-time persisted ``data_source.default_time_dimension`` follows
        whatever the user passes on construction — the cache populator
        only updates ``fields`` / ``data_source``,
        consistent with legacy behaviour.
        """
        m = SlayerDataset(
            name="qb_td",
            data_source="ds",
            source_queries=[SlayerQuery(
                population="orders",
                dimensions=["orders.status"],
            )],
        )
        engine, tmp = await _engine()
        try:
            virtual = await engine._expand_query_backed_dataset(
                dataset=m,
                runtime_kwarg=None,
            )
            assert virtual.default_time_dimension == "created_at"
        finally:
            tmp.cleanup()

    async def test_excludes_hidden_hoisted_slots(self) -> None:
        """A query with ``rank(sum(amount), direction='desc')`` hoists the inner ``amount_sum``
        as a hidden slot. The migrated path exposes ONLY user-declared
        public fields; ``amount_sum`` is NOT a field on the virtual dataset
        (decision #3 — P4 closure).
        """
        m = SlayerDataset(
            name="qb_hidden_hoist",
            data_source="ds",
            source_queries=[SlayerQuery(
                population="orders",
                dimensions=["orders.status"],
                metrics=[QueryMetric(metric="rank(sum(orders.amount), direction='desc')", alias="rank_by_amt")],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(m)
            col_names = {c.name for c in saved.fields}
            assert "rank_by_amt" in col_names
            assert "amount_sum" not in col_names, (
                f"Hidden hoisted slot leaked into virtual dataset fields: "
                f"{col_names!r}"
            )
            assert "status" in col_names
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Decision D — SourceDatasetOrigin / agg_column_names dropped
# ---------------------------------------------------------------------------


class TestSourceModelOriginDropped:
    async def test_virtual_model_has_no_source_model_origin(self) -> None:
        """Stage B's migrated path no longer SETS ``source_dataset_origin``.
        Field still exists on the dataset (legacy ``enrichment.py`` reads it
        until Stage D), but the migrated output's value is ``None``.
        """
        m = SlayerDataset(
            name="qb_no_origin",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
                "dimensions": ["orders.status"],
            })],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(m)
            assert saved.source_dataset_origin is None, (
                f"Stage B drops SourceDatasetOrigin; got {saved.source_dataset_origin!r}"
            )
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# get_result_column_types: renders through the typed generate_planned_stages
# ---------------------------------------------------------------------------


class TestGetResultColumnTypesTypedPipeline:
    async def test_get_result_column_types_on_query_backed_model_returns_typed_results(
        self,
    ) -> None:
        """Functional probe against a real SQLite-backed query-backed dataset.
        The prelude expansion + typed-path probe returns a non-empty type
        map keyed by virtual-field bare names.
        """
        d = tempfile.mkdtemp()
        db_path = os.path.join(d, "t.db")
        with transaction(db_path) as con:
            cur = con.cursor()
            cur.execute(
                "CREATE TABLE orders ("
                "id INTEGER PRIMARY KEY, status TEXT, amount REAL)"
            )
            cur.executemany(
                "INSERT INTO orders VALUES (?,?,?)",
                [(1, "paid", 10.0), (2, "open", 7.0)],
            )

        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="prod", type="sqlite", database=db_path)
        )
        await storage.save_dataset(SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="prod",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="status", datatype=DataType.STRING),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ],
        ))
        await storage.save_dataset(SlayerDataset(
            name="qb_for_types_live",
            data_source="prod",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
                "dimensions": ["orders.status"],
            })],
        ))

        engine = SlayerQueryEngine(storage=storage)
        types = await engine.get_result_column_types("qb_for_types_live")
        # Returned bare-name keys: ``amount_sum`` (probeable; status is
        # a TEXT field whose default agg may or may not be probed
        # depending on type allowlist).
        assert "amount_sum" in types, types
        assert types["amount_sum"].type in (DataType.INTEGER, DataType.FLOAT), types["amount_sum"]

    async def test_get_result_column_types_renders_via_build_planned_statement(self) -> None:
        """The query-backed probe path builds through ``build_planned_statement``."""
        # Save a real query-backed dataset so the prelude branch runs too.
        m = SlayerDataset(
            name="qb_for_types",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
                "dimensions": ["orders.status"],
            })],
        )
        engine, tmp = await _engine()
        try:
            await engine.save_dataset(m)
            planned_calls: List[Tuple[bool, str]] = []
            original_generate = query_engine_module.build_planned_statement

            def _wrapper(*args, **kwargs):
                built = original_generate(*args, **kwargs)
                planned_calls.append((bool(kwargs.get("type_probe")), built.statement.sql()))
                return built

            with patch.object(
                query_engine_module, "build_planned_statement", _wrapper,
            ):
                # The call may fail at the SQL-execute step (we don't have
                # a real backing table); the assertion is about the
                # render path, which runs before the probe executes.
                try:
                    await engine.get_result_column_types("qb_for_types")
                except Exception:  # noqa: BLE001 — probe-execute failure is expected
                    pass
            probes = [sql for is_probe, sql in planned_calls if is_probe]
            assert len(probes) == 1, (
                "get_result_column_types must build the query-backed probe through "
                f"build_planned_statement exactly once; got {len(probes)}"
            )
            assert "SELECT" in probes[0].upper(), probes[0]
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# ContextVar safety
# ---------------------------------------------------------------------------





# ---------------------------------------------------------------------------
# Save path — dry-run placeholders, data_source refresh, save=False
# ---------------------------------------------------------------------------


class TestInlineNestedSourceQueriesIntegrated:
    """Inline nested ``SlayerDataset.source_queries`` as a stage source are rejected."""

    def test_inline_nested_source_rejected_at_construction(self) -> None:
        """An inline query-backed stage source is rejected before any save-time cycle walk."""
        inline = SlayerDataset(name="_inline_a", source_queries=[SlayerQuery(population="b")])
        with pytest.raises(ValueError, match="(?i)inline"):
            SlayerQuery(name="a", population=inline)  # pyright: ignore[reportArgumentType] — the refused form, on purpose


class TestNestedQueryBackedSavePath:
    """Decision F end-to-end — save a query-backed dataset whose own
    backing query relates to ANOTHER query-backed dataset (qb-A → qb-B)."""

    async def test_qb_a_joins_qb_b_save_path(self) -> None:
        # qb_B aggregates customers.
        qb_b = SlayerDataset(
            name="qb_b_customers_kpi",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "customers",
                "metrics": [{"metric": "count(customers.*)"}],
                "dimensions": ["customers.region"],
            })],
        )
        # qb_A relates to qb_B inline AND references a field on it (so the
        # planner doesn't drop the relationship as unused).
        qb_a = SlayerDataset(
            name="qb_a_orders_with_kpi",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "extensions": [DatasetExtension.model_validate({
                    "dataset": "orders",
                    "relationships": [{
                        "to": "qb_b_customers_kpi",
                        "from_columns": ["region"], "to_columns": ["region"],
                    }],
                })],
                "dimensions": ["orders.status"],
                "metrics": [{"metric": "sum(qb_b_customers_kpi._count)"}],
            })],
        )
        engine, tmp = await _engine(_customers_dataset(), qb_b)
        try:
            saved = await engine.save_dataset(qb_a)
            # Both inner tables must appear in the rendered SQL.
            sql_lower = (await _dry_run_sql(engine, saved.name)).lower()
            assert "orders_t" in sql_lower
            assert "customers_t" in sql_lower
        finally:
            tmp.cleanup()


class TestSavePath:
    async def test_save_with_undefined_var_refused(self) -> None:
        """``filters=["amount > {threshold}"]`` with no ``query_variables`` refuses the save."""
        m = SlayerDataset(
            name="qb_with_var",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
                "dimensions": ["orders.status"],
                "filter": ["orders.amount > {threshold}"],
            })],
        )
        engine, tmp = await _engine()
        try:
            with pytest.raises(ValueError, match="Undefined variable 'threshold'"):
                await engine.save_dataset(m)
        finally:
            tmp.cleanup()

    async def test_sibling_stage_inherits_outer_model_query_variables(self) -> None:
        """Codex round-4 fix — a named non-final sibling stage's filter
        ``amount > {threshold}`` must see the OUTER dataset's
        ``query_variables`` (precedence runtime > stage > outer > dataset
        defaults). Pre-fix, ``apply_variables_to_query`` substituted the
        FINAL stage's filters with ``bundle.query_variables`` but
        siblings only saw the stage-level ``final_stage.variables`` dict
        (which doesn't include ``dataset.query_variables``), so the
        sibling's ``{threshold}`` was filled with the dry-run ``"0"``
        sentinel.
        """
        m = SlayerDataset(
            name="qb_sibling_vars",
            data_source="ds",
            # Outer dataset's defaults — sibling must see ``threshold=100``.
            query_variables={"threshold": "100"},
            source_queries=[
                SlayerQuery.model_validate({
                    "name": "filtered",
                    "population": "orders",
                    "metrics": [{"metric": "sum(orders.amount)"}],
                    "dimensions": ["orders.status"],
                    "filter": ["orders.amount > {threshold}"],
                }),
                SlayerQuery(
                    population="filtered",
                    dimensions=["filtered.status"],
                ),
            ],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(m)
            sql = await _dry_run_sql(engine, saved.name)
            # The sibling's filter ``amount > {threshold}`` must
            # substitute to ``amount > 100`` (the outer dataset default),
            # not to ``amount > 0`` (the dry-run placeholder fallback).
            assert "> 100" in sql, (
                f"Sibling filter must inherit outer dataset.query_variables; "
                f"placeholder fill leaked into:\n{sql}"
            )
        finally:
            tmp.cleanup()

    async def test_data_source_refresh_when_backing_query_changes(self) -> None:
        """Save a query-backed dataset whose backing query routes through
        ds=A; replace its ``source_queries`` to root in ds=B (with a
        DIFFERENT base table name so the inner resolution is unambiguous);
        the second save updates ``dataset.data_source`` to ``"ds2"``
        (mirrors the legacy ``_validate_and_populate_cache`` refresh).
        """
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_datasource(_ds2())
        # Same orders schema, different names in each datasource so the
        # bare-name lookup in the inner stage is unambiguous.
        await storage.save_dataset(
            _orders_dataset().model_copy(update={"name": "orders_a"})
        )
        await storage.save_dataset(
            _orders_dataset().model_copy(update={"name": "orders_b", "data_source": "ds2"})
        )
        engine = SlayerQueryEngine(storage=storage)
        try:
            m1 = SlayerDataset(
                name="qb_ds_refresh",
                data_source="ds",
                source_queries=[SlayerQuery.model_validate({
                    "population": "orders_a",
                    "metrics": [{"metric": "sum(orders_a.amount)"}],
                    "dimensions": ["orders_a.status"],
                })],
            )
            saved1 = await engine.save_dataset(m1)
            assert saved1.data_source == "ds"
            m2 = SlayerDataset(
                name="qb_ds_refresh",
                data_source="ds",  # deliberately stale
                source_queries=[SlayerQuery.model_validate({
                    "population": "orders_b",
                    "metrics": [{"metric": "sum(orders_b.amount)"}],
                    "dimensions": ["orders_b.status"],
                })],
            )
            saved2 = await engine.save_dataset(m2)
            assert saved2.data_source == "ds2", saved2.data_source
        finally:
            tmp.cleanup()

    async def test_create_model_from_query_save_false_populates_cache(self) -> None:
        """``create_model_from_query(..., save=False)`` returns a dataset
        whose ``fields`` / ``data_source`` are
        populated by the migrated ``_validate_and_populate_cache``.
        """
        engine, tmp = await _engine()
        try:
            built = await engine.create_dataset_from_query(
                query=SlayerQuery.model_validate({
                    "population": "orders",
                    "metrics": [{"metric": "sum(orders.amount)"}],
                    "dimensions": ["orders.status"],
                }),
                name="built_no_save",
                save=False,
            )
            assert built.fields, "save=False must still populate fields"
            assert built.data_source == "ds"
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Decision F — expand_query_backed_models_in_bundle helper covers nested
# query-backed targets, query-backed stage sources, and DatasetExtension
# overlay re-apply. (CRITICAL Codex findings #1 + #2.)
# ---------------------------------------------------------------------------


class TestNestedQueryBackedExpansion:
    async def test_query_backed_join_target_renders_through_typed_path(self) -> None:
        """A query-backed dataset related to ANOTHER query-backed dataset
        executes end-to-end through the migrated path.
        """
        kpi_model = SlayerDataset(
            name="customers_kpi",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "customers",
                "metrics": [{"metric": "count(customers.*)"}],
                "dimensions": ["customers.region"],
            })],
        )
        engine, tmp = await _engine(_customers_dataset(), kpi_model)
        try:
            # Save another query-backed dataset that references the first
            # as a relationship target AND references a field on it so the
            # planner doesn't drop the relationship as unused.
            outer = SlayerQuery.model_validate({
                "population": "orders",
                "extensions": [DatasetExtension.model_validate({
                    "dataset": "orders",
                    "relationships": [{
                        "to": "customers_kpi",
                        "from_columns": ["region"], "to_columns": ["region"],
                    }],
                })],
                "metrics": [{"metric": "sum(customers_kpi._count)"}],
                "dimensions": ["orders.status"],
            })
            resp = await engine.execute(outer, dry_run=True)
            assert resp.sql is not None
            # Both backing tables must appear in the rendered SQL.
            assert "orders_t" in resp.sql.lower()
            assert "customers_t" in resp.sql.lower()
        finally:
            tmp.cleanup()

    async def test_modelextension_overlay_over_query_backed_base(self) -> None:
        """A query-backed dataset used as a ``DatasetExtension`` base preserves
        the overlay's extra metric after expansion.
        """
        qb = SlayerDataset(
            name="qb_base",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "dimensions": ["orders.status"],
                "metrics": [{"metric": "sum(orders.amount)"}],
            })],
        )
        engine, tmp = await _engine(qb)
        try:
            q = SlayerQuery.model_validate({
                "population": "qb_base",
                "extensions": [DatasetExtension(
                    dataset="qb_base",
                    metrics=[DatasetMetric(name="extra", expression="sum(amount_sum)")],
                )],
                "dimensions": ["qb_base.status"],
                "metrics": ["qb_base.extra"],
            })
            resp = await engine.execute(q, dry_run=True)
            assert resp.sql is not None
            # The overlay metric appears in the outer projection.
            assert "extra" in resp.sql.lower()
        finally:
            tmp.cleanup()

    async def test_query_backed_stage_source_in_runtime_list(self) -> None:
        """A multi-stage runtime DAG whose non-root stage's source is a
        query-backed dataset renders through the migrated path.
        """
        qb = SlayerDataset(
            name="qb_inner",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "dimensions": ["orders.status"],
                "metrics": [{"metric": "sum(orders.amount)"}],
            })],
        )
        engine, tmp = await _engine(qb)
        try:
            # Stage ``s`` projects ``status`` so the root can reference it.
            queries: list = [
                SlayerQuery.model_validate({
                    "name": "s",
                    "population": "qb_inner",
                    "dimensions": ["qb_inner.status"],
                    "metrics": [{"metric": "sum(qb_inner.amount_sum)"}],
                }),
                SlayerQuery(population="s", dimensions=["s.status"]),
            ]
            resp = await engine.execute(queries, dry_run=True)
            assert resp.sql is not None
            assert "orders_t" in resp.sql.lower()
        finally:
            tmp.cleanup()


class TestCrossDatasourceJoin:
    async def test_cross_datasource_join_on_query_backed_model_rejected(
        self,
    ) -> None:
        """A query-backed dataset in datasource A related to a dataset in
        datasource B fails clearly — relationships cannot cross datasource
        boundaries.
        """
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_datasource(_ds2())
        await storage.save_dataset(_orders_dataset())
        await storage.save_dataset(
            _customers_dataset().model_copy(update={"data_source": "ds2"})
        )

        # Build a query-backed dataset in ds=A that relates to ds=B AND
        # references a field on the target (so the missing cross-DS
        # target surfaces as a binder error rather than a silently
        # dropped relationship).
        bad = SlayerDataset(
            name="xds_qb",
            data_source="ds",
            source_queries=[SlayerQuery(
                population="orders",
                extensions=[DatasetExtension.model_validate({
                    "dataset": "orders",
                    "relationships": [{
                        "to": "customers",
                        "from_columns": ["region"], "to_columns": ["region"],
                    }],
                })],
                dimensions=["orders.status", "customers.name"],
            )],
        )
        engine = SlayerQueryEngine(storage=storage)
        try:
            # The relationship must not silently cross datasources. ds=A's
            # ``customers`` lookup returns None (the only ``customers``
            # is in ds=B); the binder then raises on the
            # ``customers.name`` ref.
            with pytest.raises(Exception):
                await engine.save_dataset(bad)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Wrapper alias shapes (decision B) — pin the rendered SQL's projection
# matches the flat-renamed field set the virtual dataset declares.
# ---------------------------------------------------------------------------


class TestWrapperAliasShapes:
    async def test_local_agg_alias_flattens_to_underscore(self) -> None:
        m = SlayerDataset(
            name="qb_local",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "dimensions": ["orders.status"],
                "metrics": [{"metric": "sum(orders.amount)"}],
            })],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_dataset(m)
            col_names = {c.name for c in saved.fields}
            assert "amount_sum" in col_names
            assert "status" in col_names
            # The wrapper exposes flat names — no dots.
            assert all("." not in n for n in col_names), col_names
        finally:
            tmp.cleanup()

    async def test_cross_model_agg_alias_flattens_with_double_underscore(
        self,
    ) -> None:
        """``sum(customers.revenue)`` (cross-dataset) flattens to canonical
        ``customers__revenue_sum`` (the relationship path AND the agg suffix both
        appear in the flat name).
        """
        # Add a relationship + a numeric customers.revenue field so the cross-
        # dataset agg has something to aggregate.
        orders_with_join = _orders_dataset().model_copy(update={
            "relationships": [DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["id"],
            )],
        })
        customers_with_rev = _customers_dataset().model_copy(update={
            "fields": list(_customers_dataset().fields) + [
                DatasetField(name="revenue", sql="revenue", datatype=DataType.FLOAT),
            ],
        })
        m = SlayerDataset(
            name="qb_cross",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "dimensions": ["orders.status"],
                "metrics": [{"metric": "sum(customers.revenue)"}],
            })],
        )
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_dataset(customers_with_rev)
        await storage.save_dataset(orders_with_join)
        engine = SlayerQueryEngine(storage=storage)
        try:
            saved = await engine.save_dataset(m)
            col_names = {c.name for c in saved.fields}
            assert "customers__revenue_sum" in col_names, col_names
        finally:
            tmp.cleanup()

    async def test_cross_model_parametric_agg_alias_includes_kwargs(
        self,
    ) -> None:
        """``percentile(customers.revenue, p=0.5)`` (cross-dataset parametric)
        flattens to ``customers__revenue_percentile_p_0_5`` (the wrapper keeps the kwarg suffix).
        """
        orders_with_join = _orders_dataset().model_copy(update={
            "relationships": [DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["id"],
            )],
        })
        customers_with_rev = _customers_dataset().model_copy(update={
            "fields": list(_customers_dataset().fields) + [
                DatasetField(name="revenue", sql="revenue", datatype=DataType.FLOAT),
            ],
        })
        m = SlayerDataset(
            name="qb_cross_param",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "dimensions": ["orders.status"],
                "metrics": [{"metric": "percentile(customers.revenue, p=0.5)"}],
            })],
        )
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_dataset(customers_with_rev)
        await storage.save_dataset(orders_with_join)
        engine = SlayerQueryEngine(storage=storage)
        try:
            saved = await engine.save_dataset(m)
            col_names = {c.name for c in saved.fields}
            assert "customers__revenue_percentile_p_0_5" in col_names, col_names
        finally:
            tmp.cleanup()

    async def test_joined_dim_alias_flattens(self) -> None:
        orders_with_join = _orders_dataset().model_copy(update={
            "relationships": [DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["id"],
            )],
        })
        m = SlayerDataset(
            name="qb_joined_dim",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "dimensions": ["customers.name", "customers.signup_at"],
                "metrics": [{"metric": "sum(orders.amount)"}],
            })],
        )
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_dataset(_customers_dataset())
        await storage.save_dataset(orders_with_join)
        engine = SlayerQueryEngine(storage=storage)
        try:
            saved = await engine.save_dataset(m)
            by_name = {c.name: c for c in saved.fields}
            assert "customers__name" in by_name, set(by_name)
            # A joined (dotted) dimension must persist the
            # TARGET field's type. Before the fix ``_type_for_dimension``
            # returned None for dotted refs and ``_query_as_model`` coerced
            # the slot to DOUBLE, mistyping joined string / temporal dims.
            assert by_name["customers__name"].datatype == DataType.STRING, (
                f"joined TEXT dim must persist as TEXT, not DOUBLE; "
                f"got {by_name['customers__name'].datatype!r}"
            )
            assert by_name["customers__signup_at"].datatype == DataType.DATETIME, (
                f"joined TIMESTAMP dim must persist as TIMESTAMP; "
                f"got {by_name['customers__signup_at'].datatype!r}"
            )
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Decision D breaking change pinned — outer dotted ref on query-backed dataset
# ---------------------------------------------------------------------------


class TestP4ClosureBehavior:
    async def test_outer_dotted_ref_canonical_alias_no_longer_resolves(
        self,
    ) -> None:
        """A query-backed dataset that already aggregated ``sum(customers.revenue)``
        exposes the canonical flat name ``customers__revenue_sum``. An outer
        dotted ref (``sum(customers.revenue)``) against the virtual dataset never
        re-aggregates that field through the legacy ``SourceDatasetOrigin``
        lineage walk (the P4 closure): ``customers``, which no relationship path
        reaches from the virtual dataset, is aggregated over its own rows.
        """
        orders_with_join = _orders_dataset().model_copy(update={
            "relationships": [DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["id"],
            )],
        })
        customers_with_rev = _customers_dataset().model_copy(update={
            "fields": list(_customers_dataset().fields) + [
                DatasetField(name="revenue", sql="revenue", datatype=DataType.FLOAT),
            ],
        })
        qb = SlayerDataset(
            name="qb_already_aggregated",
            data_source="ds",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "dimensions": ["orders.status"],
                "metrics": [{"metric": "sum(customers.revenue)"}],
            })],
        )
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_dataset(customers_with_rev)
        await storage.save_dataset(orders_with_join)
        engine = SlayerQueryEngine(storage=storage)
        try:
            await engine.save_dataset(qb)
            # The flat-name form MUST work.
            flat_q = SlayerQuery.model_validate({
                "population": "qb_already_aggregated",
                "metrics": [{"metric": "sum(qb_already_aggregated.customers__revenue_sum)"}],
                "dimensions": ["qb_already_aggregated.status"],
            })
            resp_flat = await engine.execute(flat_q, dry_run=True)
            assert resp_flat.sql is not None
            dotted_q = SlayerQuery.model_validate({
                "population": "qb_already_aggregated",
                "metrics": [{"metric": "sum(customers.revenue)"}],
                "dimensions": ["qb_already_aggregated.status"],
            })
            resp = await engine.execute(dotted_q, dry_run=True)
            assert any(
                w.kind == "unrelated_dataset" and w.dataset == "customers" for w in resp.warnings or []
            ), resp.warnings
            assert resp.sql is not None
            tree = sqlglot.parse_one(resp.sql, dialect="sqlite")
            assert not [c for c in tree.find_all(exp.Column) if c.name.startswith("customers__revenue")], resp.sql
            assert any(
                not sel.args.get("group") and any(t.alias_or_name == "customers" for t in sel.find_all(exp.Table))
                and any(c.name == "revenue" for agg in sel.find_all(exp.AggFunc) for c in agg.find_all(exp.Column))
                for sel in tree.find_all(exp.Select)
            ), resp.sql
        finally:
            tmp.cleanup()
