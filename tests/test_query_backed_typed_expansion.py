"""DEV-1452 Stage B — migrated ``_expand_query_backed_model`` +
``_validate_and_populate_cache`` + ``get_column_types`` on the typed pipeline.

These tests pin the invariants Stage B's migration MUST preserve and the
behavioural breaks it INTENDS:

* Stored ``source_queries`` topo-sort acceptance (Kahn) — replaces the
  legacy strict-top-to-bottom convention with a fault-tolerance contract.
* Virtual-model column TYPES from typed-plan slot types (decision #2).
* Virtual-model columns are PUBLIC slots only (decision #3).
* ``SourceModelOrigin`` / ``agg_column_names`` dropped (decision D).
* Per-stage normalize + variable substitution mirror ``_execute_pipeline``.
* Shared ``expand_query_backed_models_in_bundle`` helper handles nested
  query-backed join targets, query-backed stage sources, and the root
  ``ModelExtension`` overlay re-apply (decision F).
* ``get_column_types`` does not call ``SQLGenerator.generate(enriched=...)``.
* The two ContextVars (``_join_target_resolving_var`` /
  ``_forbidden_sibling_refs_var``) are NEVER touched by the migrated path.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import Tuple
from unittest.mock import patch

import pytest

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders_t",
        data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(
                name="amount",
                sql="amount",
                type=DataType.DOUBLE,
                description="Total order amount in USD",
                format=NumberFormat(type=NumberFormatType.CURRENCY),
            ),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
        ],
        default_time_dimension="created_at",
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        sql_table="customers_t",
        data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="signup_at", sql="signup_at", type=DataType.TIMESTAMP),
        ],
    )


def _ds() -> DatasourceConfig:
    return DatasourceConfig(name="ds", type="sqlite", database=":memory:")


def _ds2() -> DatasourceConfig:
    return DatasourceConfig(name="ds2", type="sqlite", database=":memory:")


async def _engine(*extra_models: SlayerModel) -> Tuple[SlayerQueryEngine, tempfile.TemporaryDirectory]:
    """Build YAMLStorage with ``orders`` saved + extras, return engine + tmp."""
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(_ds())
    await storage.save_model(_orders_model())
    for m in extra_models:
        await storage.save_model(m)
    engine = SlayerQueryEngine(storage=storage)
    return engine, tmp


# ---------------------------------------------------------------------------
# Topo-sort acceptance on STORED source_queries (decision #1)
# ---------------------------------------------------------------------------


class TestStoredSourceQueriesTopoSort:
    async def test_run_by_name_accepts_nonordered_join_target_stages(self) -> None:
        """Codex round-5 fix — ``engine.execute(name)`` must topologically
        sort stored ``source_queries`` so out-of-order ``joins[].target_
        model`` deps work end-to-end. Pre-fix the save path went through
        ``_expand_query_backed_model`` (which sorts) but run-by-name went
        through ``_execute_pipeline`` directly, and ``plan_stages._topo_
        sort`` only handles ``source_model`` deps. So a model that saved
        successfully would fail when run by name.
        """
        non_ordered = SlayerModel(
            name="qb_runbyname_topo",
            data_source="ds",
            source_queries=[
                # Out-of-order: ``main`` references ``kpi`` via
                # joins.target_model but appears FIRST.
                SlayerQuery(
                    name="main",
                    source_model={
                        "source_name": "orders",
                        "joins": [{"target_model": "kpi", "join_pairs": [["id", "kpi_id"]]}],
                    },
                    dimensions=["status"],
                    measures=[{"formula": "kpi._count:sum"}],
                ),
                SlayerQuery(
                    name="kpi",
                    source_model="orders",
                    measures=[{"formula": "*:count"}],
                    dimensions=[{"name": "id", "label": "kpi_id"}],  # type: ignore[list-item]
                ),
                SlayerQuery(source_model="main"),
            ],
        )
        engine, tmp = await _engine()
        try:
            await engine.save_model(non_ordered)
            # Save worked (round-1 test pinned this). Now run by name:
            resp = await engine.execute("qb_runbyname_topo", dry_run=True)
            assert resp.sql is not None, "run-by-name must produce SQL"
        finally:
            tmp.cleanup()

    async def test_topo_sort_accepts_nonordered_save_path(self) -> None:
        """A query-backed model whose stored ``source_queries`` are NOT in
        topological order saves cleanly under Stage B (legacy strict order
        would reject this as a forward reference)."""
        non_ordered = SlayerModel(
            name="topo_qb",
            data_source="ds",
            source_queries=[
                # main references kpi (forward ref under legacy strict order)
                SlayerQuery(
                    name="main",
                    source_model={
                        "source_name": "orders",
                        "joins": [{"target_model": "kpi", "join_pairs": [["id", "kpi_id"]]}],
                    },
                    dimensions=["status"],
                    measures=[{"formula": "kpi._count:sum"}],
                ),
                SlayerQuery(
                    name="kpi",
                    source_model="orders",
                    measures=[{"formula": "*:count"}],
                    dimensions=[ {"name": "id", "label": "kpi_id"} ],  # type: ignore[list-item]
                ),
                SlayerQuery(source_model="main"),
            ],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(non_ordered)
            assert saved.backing_query_sql is not None
            assert "orders_t" in saved.backing_query_sql.lower()
        finally:
            tmp.cleanup()

    async def test_cycle_raises_clearly(self) -> None:
        engine, tmp = await _engine()
        try:
            mutual = SlayerModel(
                name="qb_cycle",
                data_source="ds",
                source_queries=[
                    SlayerQuery(
                        name="a",
                        source_model={
                            "source_name": "orders",
                            "joins": [{"target_model": "b", "join_pairs": [["id", "id"]]}],
                        },
                    ),
                    SlayerQuery(
                        name="b",
                        source_model={
                            "source_name": "orders",
                            "joins": [{"target_model": "a", "join_pairs": [["id", "id"]]}],
                        },
                    ),
                    SlayerQuery(source_model="a"),
                ],
            )
            with pytest.raises(ValueError, match=r"[Cc]ycle"):
                await engine.save_model(mutual)
        finally:
            tmp.cleanup()

    async def test_root_referenced_raises(self) -> None:
        engine, tmp = await _engine()
        try:
            m = SlayerModel(
                name="qb_root_ref",
                data_source="ds",
                source_queries=[
                    SlayerQuery(name="a", source_model="root_stage"),
                    SlayerQuery(name="root_stage", source_model="orders"),
                ],
            )
            with pytest.raises(ValueError, match="root"):
                await engine.save_model(m)
        finally:
            tmp.cleanup()

    async def test_self_reference_raises(self) -> None:
        engine, tmp = await _engine()
        try:
            m = SlayerModel(
                name="qb_self_ref",
                data_source="ds",
                source_queries=[
                    SlayerQuery(name="a", source_model="a"),
                    SlayerQuery(source_model="a"),
                ],
            )
            with pytest.raises(ValueError, match="self"):
                await engine.save_model(m)
        finally:
            tmp.cleanup()

    async def test_duplicate_name_raises(self) -> None:
        engine, tmp = await _engine()
        try:
            # SlayerModel's Pydantic validator catches duplicate stage names
            # at construction time. The same guard exists in
            # ``topologically_order_stages`` as defence in depth, but the
            # user-facing error surfaces here first.
            with pytest.raises(
                (ValueError, Exception), match="[Dd]uplicate",
            ):
                SlayerModel(
                    name="qb_dup",
                    data_source="ds",
                    source_queries=[
                        SlayerQuery(name="a", source_model="orders"),
                        SlayerQuery(name="a", source_model="orders"),
                        SlayerQuery(source_model="a"),
                    ],
                )
            del engine  # explicit unused (silence linter)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Virtual-model column TYPES, FORMAT, DESCRIPTION (decisions #2, #8)
# ---------------------------------------------------------------------------


class TestVirtualModelColumns:
    async def test_star_count_yields_int_column(self) -> None:
        m = SlayerModel(
            name="qb_count",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                dimensions=["status"],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            count_col = next(c for c in saved.columns if c.name == "_count")
            assert count_col.type == DataType.INT, (
                f"*:count must yield DataType.INT, got {count_col.type!r}"
            )
        finally:
            tmp.cleanup()

    async def test_sum_inherits_source_column_type(self) -> None:
        """``amount:sum`` over a DOUBLE source column → DOUBLE virtual col."""
        m = SlayerModel(
            name="qb_sum",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["status"],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            sum_col = next(c for c in saved.columns if c.name == "amount_sum")
            assert sum_col.type == DataType.DOUBLE
        finally:
            tmp.cleanup()

    async def test_query_measure_type_override_wins_over_inference(self) -> None:
        """Codex review fix — a user-supplied ``type=`` on a query measure
        spec is the override; ``_type_for_measure_formula`` is the fallback.

        ``*:count`` ordinarily infers ``INT``; explicit ``type=DOUBLE``
        on the same formula must win and surface as a DOUBLE virtual
        column.
        """
        m = SlayerModel(
            name="qb_type_override",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="*:count", type=DataType.DOUBLE)],
                dimensions=["status"],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            count_col = next(c for c in saved.columns if c.name == "_count")
            assert count_col.type == DataType.DOUBLE, (
                f"User-supplied type=DOUBLE must override INT inference, "
                f"got {count_col.type!r}"
            )
        finally:
            tmp.cleanup()

    async def test_promoted_hidden_slot_keeps_metadata(self) -> None:
        """Codex review fix — when a hidden slot is later promoted to
        public, its type / format / description must be filled in.

        Repro: declare ``rank(*:count)`` first (hoists ``*:count`` as a
        hidden dep with no display metadata), THEN declare ``*:count``
        as a public measure. The promoted public slot must end up with
        ``type=INT`` (not the default None → DOUBLE fallback).
        """
        m = SlayerModel(
            name="qb_promoted_hidden",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                dimensions=["status"],
                measures=[
                    # rank uses *:count as a hidden inner; intern order
                    # hoists *:count hidden first, then the public
                    # *:count entry promotes the same slot.
                    {"formula": "rank(*:count)", "name": "ranked"},
                    {"formula": "*:count"},
                ],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            count_col = next(c for c in saved.columns if c.name == "_count")
            assert count_col.type == DataType.INT, (
                f"Promoted hidden slot must inherit INT from the public "
                f"intern, got {count_col.type!r}"
            )
        finally:
            tmp.cleanup()

    async def test_declared_model_measure_type_honored(self) -> None:
        """A ``ModelMeasure`` with explicit ``type=DOUBLE`` carries through
        to the virtual column.
        """
        orders_with_measure = _orders_model().model_copy(update={
            "measures": [
                ModelMeasure(
                    formula="amount:sum * 1.0",
                    name="adjusted_total",
                    type=DataType.DOUBLE,
                ),
            ],
        })
        engine, tmp = await _engine()
        try:
            await engine.storage.save_model(orders_with_measure)
            m = SlayerModel(
                name="qb_with_declared_type",
                data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "adjusted_total"}],
                    dimensions=["status"],
                )],
            )
            saved = await engine.save_model(m)
            col = next(c for c in saved.columns if c.name == "adjusted_total")
            assert col.type == DataType.DOUBLE
        finally:
            tmp.cleanup()

    async def test_saved_model_measure_type_wins_over_inference(self) -> None:
        """Codex round-5 fix — when the query formula is a bare reference
        to a saved ``ModelMeasure`` whose explicit ``type=`` differs from
        the type ``_type_for_measure_formula`` would infer, the saved
        measure's type wins. ``expand_model_measures`` rewrites the AST
        but drops the measure's type metadata; the type-priority chain
        re-looks-up the saved measure here.

        Repro: ``ModelMeasure(formula="amount:sum", type=DataType.INT)``
        on the source model — inference for ``amount:sum`` over a DOUBLE
        ``amount`` would normally pick DOUBLE, but the explicit
        ``type=INT`` must win.
        """
        orders_with_int_measure = _orders_model().model_copy(update={
            "measures": [
                ModelMeasure(
                    formula="amount:sum",
                    name="rounded_total",
                    type=DataType.INT,  # ← explicit override
                ),
            ],
        })
        engine, tmp = await _engine()
        try:
            await engine.storage.save_model(orders_with_int_measure)
            m = SlayerModel(
                name="qb_saved_measure_type",
                data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders",
                    # Bare-ref + explicit ``name="rounded_total"`` keeps
                    # the column alias as the user-chosen name so the
                    # test can look it up unambiguously.
                    measures=[{"formula": "rounded_total", "name": "rounded_total"}],
                    dimensions=["status"],
                )],
            )
            saved = await engine.save_model(m)
            col = next(c for c in saved.columns if c.name == "rounded_total")
            assert col.type == DataType.INT, (
                f"Saved ModelMeasure.type=INT must override inference "
                f"(which would pick DOUBLE for amount:sum); got {col.type!r}"
            )
        finally:
            tmp.cleanup()

    async def test_inherits_source_format(self) -> None:
        """Source column with ``NumberFormat(CURRENCY)`` → virtual sum col
        carries the same format (via ``_infer_aggregated_format``).
        """
        m = SlayerModel(
            name="qb_fmt",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["status"],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            col = next(c for c in saved.columns if c.name == "amount_sum")
            assert col.format is not None, "amount:sum must carry format"
            assert col.format.type == NumberFormatType.CURRENCY
        finally:
            tmp.cleanup()

    async def test_inherits_source_description(self) -> None:
        m = SlayerModel(
            name="qb_desc",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["status"],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            col = next(c for c in saved.columns if c.name == "amount_sum")
            assert col.description == "Total order amount in USD"
        finally:
            tmp.cleanup()

    async def test_carries_default_time_dimension(self) -> None:
        """Inner source model has ``default_time_dimension="created_at"``;
        the virtual model returned by ``_expand_query_backed_model`` carries
        it forward (mirrors the legacy ``_query_as_model`` contract).

        Save-time persisted ``data_source.default_time_dimension`` follows
        whatever the user passes on construction — the cache populator
        only updates ``columns`` / ``backing_query_sql`` / ``data_source``,
        consistent with legacy behaviour.
        """
        m = SlayerModel(
            name="qb_td",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                dimensions=["status"],
            )],
        )
        engine, tmp = await _engine()
        try:
            virtual = await engine._expand_query_backed_model(
                model=m,
                outer_vars=None,
                runtime_kwarg=None,
                dry_run_placeholders=True,
                _resolving=set(),
            )
            assert virtual.default_time_dimension == "created_at"
        finally:
            tmp.cleanup()

    async def test_excludes_hidden_hoisted_slots(self) -> None:
        """A query with ``rank(amount:sum)`` hoists the inner ``amount_sum``
        as a hidden slot. The migrated path exposes ONLY user-declared
        public columns; ``amount_sum`` is NOT a column on the virtual model
        (decision #3 — P4 closure).
        """
        m = SlayerModel(
            name="qb_hidden_hoist",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                dimensions=["status"],
                measures=[{"formula": "rank(amount:sum)", "name": "rank_by_amt"}],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            col_names = {c.name for c in saved.columns}
            assert "rank_by_amt" in col_names
            assert "amount_sum" not in col_names, (
                f"Hidden hoisted slot leaked into virtual model columns: "
                f"{col_names!r}"
            )
            assert "status" in col_names
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Decision D — SourceModelOrigin / agg_column_names dropped
# ---------------------------------------------------------------------------


class TestSourceModelOriginDropped:
    async def test_virtual_model_has_no_source_model_origin(self) -> None:
        """Stage B's migrated path no longer SETS ``source_model_origin``.
        Field still exists on the model (legacy ``enrichment.py`` reads it
        until Stage D), but the migrated output's value is ``None``.
        """
        m = SlayerModel(
            name="qb_no_origin",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["status"],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            assert saved.source_model_origin is None, (
                f"Stage B drops SourceModelOrigin; got {saved.source_model_origin!r}"
            )
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# get_column_types: NO legacy SQLGenerator.generate(enriched=) calls
# ---------------------------------------------------------------------------


class TestGetColumnTypesTypedPipeline:
    async def test_get_column_types_on_query_backed_model_returns_typed_results(
        self,
    ) -> None:
        """Functional probe against a real SQLite-backed query-backed model.
        The prelude expansion + typed-path probe returns a non-empty type
        map keyed by virtual-column bare names.
        """
        d = tempfile.mkdtemp()
        db_path = os.path.join(d, "t.db")
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, status TEXT, amount REAL)"
        )
        cur.executemany(
            "INSERT INTO orders VALUES (?,?,?)",
            [(1, "paid", 10.0), (2, "open", 7.0)],
        )
        con.commit()
        con.close()

        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="prod", type="sqlite", database=db_path)
        )
        await storage.save_model(SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
            ],
        ))
        await storage.save_model(SlayerModel(
            name="qb_for_types_live",
            data_source="prod",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["status"],
            )],
        ))

        engine = SlayerQueryEngine(storage=storage)
        types = await engine.get_column_types("qb_for_types_live")
        # Returned bare-name keys: ``amount_sum`` (probeable; status is
        # a TEXT column whose default agg may or may not be probed
        # depending on type allowlist).
        assert "amount_sum" in types, types
        # The returned type token is whatever ``client.get_column_types``
        # produces — typically "number" for SQLite.
        assert types["amount_sum"], types["amount_sum"]

    async def test_get_column_types_makes_zero_legacy_generate_calls(self) -> None:
        """Patch ``SQLGenerator.generate`` and assert zero calls during
        ``get_column_types``. Stage B's migration replaces the legacy probe
        path with ``plan_stages + generate_planned_stages``.
        """
        # Save a real query-backed model so the prelude branch runs too.
        m = SlayerModel(
            name="qb_for_types",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["status"],
            )],
        )
        engine, tmp = await _engine()
        try:
            await engine.save_model(m)
            from slayer.sql.generator import SQLGenerator

            with patch.object(
                SQLGenerator, "generate", autospec=True,
                side_effect=AssertionError(
                    "Stage B: get_column_types must not call legacy "
                    "SQLGenerator.generate(enriched=...)."
                ),
            ):
                # The call may fail at the SQL-execute step (we don't have
                # a real backing table), but the assertion is about the
                # generate-path: if we got past the planned-render and the
                # error comes from the SQL probe, that's fine.
                try:
                    await engine.get_column_types("qb_for_types")
                except AssertionError:
                    raise
                except Exception:
                    # Probe-execute failure is expected (no real DB row);
                    # the SQLGenerator.generate patch did not fire, so we
                    # passed.
                    pass
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# ContextVar safety
# ---------------------------------------------------------------------------


class _TrackingContextVar:
    """Drop-in replacement for ``contextvars.ContextVar`` that records every
    ``.set`` / ``.get`` / ``.reset`` call to a list. Used to assert the
    migrated Stage B paths never touch the legacy ContextVars.

    Inherits the read-only behaviour of real ``ContextVar.set`` by simply
    tracking the call and storing the value; the migrated path does not
    rely on actual ContextVar semantics so a no-op tracker suffices.
    """

    def __init__(self, name: str, *, default=None) -> None:
        self.name = name
        self._value = default
        self._default = default
        self.set_calls: list = []
        self.get_calls: int = 0

    def set(self, value):
        self.set_calls.append(value)
        prev, self._value = self._value, value
        # Return a sentinel object that ``reset(token)`` can accept.
        return ("token", prev)

    def get(self, *args):
        self.get_calls += 1
        if self._value is None and args:
            return args[0]
        return self._value

    def reset(self, token) -> None:
        if token[0] == "token":
            self._value = token[1]


class TestContextVarSafety:
    async def test_expand_query_backed_model_does_not_touch_contextvars(
        self,
    ) -> None:
        """Migrated ``_expand_query_backed_model`` MUST NOT set / get the
        two legacy ContextVars. They still exist for ``_query_as_model``
        until Stage D; this test pins that the migrated path is independent.
        """
        from slayer.engine import query_engine as qe

        tracker_a = _TrackingContextVar("_join_target_resolving", default=None)
        tracker_b = _TrackingContextVar("_forbidden_sibling_refs", default=None)

        # Multi-stage with named non-final stages exercises the legacy
        # ``_scope_named_queries_to_prior`` + ``_forbidden_sibling_refs_var``
        # set path (and the engine's ``_join_target_resolving_var`` when a
        # query-backed model is used as a join target).
        kpi_qb = SlayerModel(
            name="kpi_for_join",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                dimensions=["customer_id"],
            )],
        )
        m = SlayerModel(
            name="qb_multistage",
            data_source="ds",
            source_queries=[
                SlayerQuery(
                    name="kpi",
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["status"],
                ),
                SlayerQuery(
                    source_model={
                        "source_name": "orders",
                        "joins": [{
                            "target_model": "kpi_for_join",
                            "join_pairs": [["customer_id", "customer_id"]],
                        }],
                    },
                    dimensions=["status"],
                ),
            ],
        )
        engine, tmp = await _engine(kpi_qb)
        try:
            with patch.object(qe, "_join_target_resolving_var", tracker_a), \
                 patch.object(qe, "_forbidden_sibling_refs_var", tracker_b):
                await engine.save_model(m)
            assert not tracker_a.set_calls, (
                f"_join_target_resolving_var.set called by migrated path: "
                f"{tracker_a.set_calls!r}"
            )
            assert not tracker_b.set_calls, (
                f"_forbidden_sibling_refs_var.set called by migrated path: "
                f"{tracker_b.set_calls!r}"
            )
            assert tracker_a.get_calls == 0, (
                f"_join_target_resolving_var.get called by migrated path: "
                f"{tracker_a.get_calls}"
            )
            assert tracker_b.get_calls == 0, (
                f"_forbidden_sibling_refs_var.get called by migrated path: "
                f"{tracker_b.get_calls}"
            )
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Save path — dry-run placeholders, data_source refresh, save=False
# ---------------------------------------------------------------------------


class TestInlineNestedSourceQueriesIntegrated:
    """Decision E end-to-end — inline nested ``SlayerModel.source_queries``
    contributes to ordering when the outer model is saved / executed.

    The recursive ``_extract_sibling_refs`` walk catches edges hidden
    inside inline nested ``source_queries`` so cycles + forward refs are
    flagged at save time. End-to-end execution of inline-nested stages
    that reference outer siblings is a separate concern (the inline
    expansion runs against storage, not the enclosing named_queries dict).
    """

    async def test_inline_nested_cycle_via_save_path_raises(self) -> None:
        """A cycle that runs through an inline-nested stage's own
        ``source_queries`` must be caught at save time by the recursive
        edge walk.
        """
        engine, tmp = await _engine()
        try:
            m = SlayerModel(
                name="qb_inline_cycle",
                data_source="ds",
                source_queries=[
                    SlayerQuery(
                        name="a",
                        source_model=SlayerModel(
                            name="_inline_a",
                            source_queries=[SlayerQuery(source_model="b")],
                        ),
                    ),
                    SlayerQuery(name="b", source_model="a"),
                    SlayerQuery(source_model="a"),
                ],
            )
            with pytest.raises(ValueError, match=r"[Cc]ycle"):
                await engine.save_model(m)
        finally:
            tmp.cleanup()


class TestNestedQueryBackedSavePath:
    """Decision F end-to-end — save a query-backed model whose own
    backing query joins ANOTHER query-backed model (qb-A joins qb-B)."""

    async def test_qb_a_joins_qb_b_save_path(self) -> None:
        # qb_B aggregates customers.
        qb_b = SlayerModel(
            name="qb_b_customers_kpi",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="customers",
                measures=[{"formula": "*:count"}],
                dimensions=["region"],
            )],
        )
        # qb_A joins qb_B inline AND references a column on it (so the
        # planner doesn't drop the join as unused).
        qb_a = SlayerModel(
            name="qb_a_orders_with_kpi",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model={
                    "source_name": "orders",
                    "joins": [{
                        "target_model": "qb_b_customers_kpi",
                        "join_pairs": [["region", "region"]],
                    }],
                },
                dimensions=["status"],
                measures=[{"formula": "qb_b_customers_kpi._count:sum"}],
            )],
        )
        engine, tmp = await _engine(_customers_model(), qb_b)
        try:
            saved = await engine.save_model(qb_a)
            assert saved.backing_query_sql is not None
            # Both inner tables must appear in the rendered backing SQL.
            sql_lower = saved.backing_query_sql.lower()
            assert "orders_t" in sql_lower
            assert "customers_t" in sql_lower
        finally:
            tmp.cleanup()


class TestSavePath:
    async def test_save_with_undefined_var_uses_placeholder_fill(self) -> None:
        """``filters=["amount > {threshold}"]`` with no ``query_variables``
        substitutes ``"0"`` at save time so dry-run validation succeeds.
        """
        m = SlayerModel(
            name="qb_with_var",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["status"],
                filters=["amount > {threshold}"],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            assert saved.backing_query_sql is not None
            # The placeholder is filled with literal 0 at save time.
            assert "0" in saved.backing_query_sql
        finally:
            tmp.cleanup()

    async def test_sibling_stage_inherits_outer_model_query_variables(self) -> None:
        """Codex round-4 fix — a named non-final sibling stage's filter
        ``amount > {threshold}`` must see the OUTER model's
        ``query_variables`` (precedence runtime > stage > outer > model
        defaults). Pre-fix, ``apply_variables_to_query`` substituted the
        FINAL stage's filters with ``bundle.query_variables`` but
        siblings only saw the stage-level ``final_stage.variables`` dict
        (which doesn't include ``model.query_variables``), so the
        sibling's ``{threshold}`` was filled with the dry-run ``"0"``
        sentinel.
        """
        m = SlayerModel(
            name="qb_sibling_vars",
            data_source="ds",
            # Outer model's defaults — sibling must see ``threshold=100``.
            query_variables={"threshold": "100"},
            source_queries=[
                SlayerQuery(
                    name="filtered",
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["status"],
                    filters=["amount > {threshold}"],  # ← references outer var
                ),
                SlayerQuery(
                    source_model="filtered",
                    dimensions=["status"],
                ),
            ],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            assert saved.backing_query_sql is not None
            # The sibling's filter ``amount > {threshold}`` must
            # substitute to ``amount > 100`` (the outer model default),
            # not to ``amount > 0`` (the dry-run placeholder fallback).
            assert "> 100" in saved.backing_query_sql, (
                f"Sibling filter must inherit outer model.query_variables; "
                f"placeholder fill leaked into:\n{saved.backing_query_sql}"
            )
        finally:
            tmp.cleanup()

    async def test_data_source_refresh_when_backing_query_changes(self) -> None:
        """Save a query-backed model whose backing query routes through
        ds=A; replace its ``source_queries`` to root in ds=B (with a
        DIFFERENT base table name so the inner resolution is unambiguous);
        the second save updates ``model.data_source`` to ``"ds2"``
        (mirrors the legacy ``_validate_and_populate_cache`` refresh).
        """
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_datasource(_ds2())
        # Same orders schema, different names in each datasource so the
        # bare-name lookup in the inner stage is unambiguous.
        await storage.save_model(
            _orders_model().model_copy(update={"name": "orders_a"})
        )
        await storage.save_model(
            _orders_model().model_copy(update={"name": "orders_b", "data_source": "ds2"})
        )
        engine = SlayerQueryEngine(storage=storage)
        try:
            m1 = SlayerModel(
                name="qb_ds_refresh",
                data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders_a",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["status"],
                )],
            )
            saved1 = await engine.save_model(m1)
            assert saved1.data_source == "ds"
            m2 = SlayerModel(
                name="qb_ds_refresh",
                data_source="ds",  # deliberately stale
                source_queries=[SlayerQuery(
                    source_model="orders_b",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["status"],
                )],
            )
            saved2 = await engine.save_model(m2)
            assert saved2.data_source == "ds2", saved2.data_source
        finally:
            tmp.cleanup()

    async def test_create_model_from_query_save_false_populates_cache(self) -> None:
        """``create_model_from_query(..., save=False)`` returns a model
        whose ``columns`` / ``backing_query_sql`` / ``data_source`` are
        populated by the migrated ``_validate_and_populate_cache``.
        """
        engine, tmp = await _engine()
        try:
            built = await engine.create_model_from_query(
                query=SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["status"],
                ),
                name="built_no_save",
                save=False,
            )
            assert built.columns, "save=False must still populate columns"
            assert built.backing_query_sql is not None
            assert built.data_source == "ds"
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Decision F — expand_query_backed_models_in_bundle helper covers nested
# query-backed targets, query-backed stage sources, and ModelExtension
# overlay re-apply. (CRITICAL Codex findings #1 + #2.)
# ---------------------------------------------------------------------------


class TestNestedQueryBackedExpansion:
    async def test_query_backed_join_target_renders_through_typed_path(self) -> None:
        """A query-backed model joined to ANOTHER query-backed model
        executes end-to-end through the migrated path.
        """
        kpi_model = SlayerModel(
            name="customers_kpi",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="customers",
                measures=[{"formula": "*:count"}],
                dimensions=["region"],
            )],
        )
        engine, tmp = await _engine(_customers_model(), kpi_model)
        try:
            # Save another query-backed model that references the first
            # as a join target AND references a column on it so the
            # planner doesn't drop the join as unused.
            outer = SlayerQuery(
                source_model={
                    "source_name": "orders",
                    "joins": [{
                        "target_model": "customers_kpi",
                        "join_pairs": [["region", "region"]],
                    }],
                },
                measures=[{"formula": "customers_kpi._count:sum"}],
                dimensions=["status"],
            )
            resp = await engine.execute(outer, dry_run=True)
            assert resp.sql is not None
            # Both backing tables must appear in the rendered SQL.
            assert "orders_t" in resp.sql.lower()
            assert "customers_t" in resp.sql.lower()
        finally:
            tmp.cleanup()

    async def test_modelextension_overlay_over_query_backed_base(self) -> None:
        """A query-backed model used as a ``ModelExtension`` base preserves
        the overlay's extra columns after expansion (bundle.inline_extensions
        re-application).
        """
        qb = SlayerModel(
            name="qb_base",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                dimensions=["status"],
                measures=[{"formula": "amount:sum"}],
            )],
        )
        engine, tmp = await _engine(qb)
        try:
            # Apply an overlay that adds an extra dimension column.
            q = SlayerQuery(
                source_model=ModelExtension(
                    source_name="qb_base",
                    columns=[Column(name="extra", sql="'lit'", type=DataType.TEXT)],
                ),
                dimensions=["extra", "status"],
                measures=[{"formula": "amount_sum:sum"}],
            )
            resp = await engine.execute(q, dry_run=True)
            assert resp.sql is not None
            # The overlay column appears in the outer projection.
            assert "extra" in resp.sql.lower()
        finally:
            tmp.cleanup()

    async def test_query_backed_stage_source_in_runtime_list(self) -> None:
        """A multi-stage runtime DAG whose non-root stage's source is a
        query-backed model renders through the migrated path.
        """
        qb = SlayerModel(
            name="qb_inner",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                dimensions=["status"],
                measures=[{"formula": "amount:sum"}],
            )],
        )
        engine, tmp = await _engine(qb)
        try:
            # Stage ``s`` projects ``status`` so the root can reference it.
            queries: list = [
                SlayerQuery(
                    name="s",
                    source_model="qb_inner",
                    dimensions=["status"],
                    measures=[{"formula": "amount_sum:sum"}],
                ),
                SlayerQuery(source_model="s", dimensions=["status"]),
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
        """A query-backed model in datasource A joining a model in
        datasource B fails clearly — joins cannot cross datasource
        boundaries.
        """
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_datasource(_ds2())
        await storage.save_model(_orders_model())
        await storage.save_model(
            _customers_model().model_copy(update={"data_source": "ds2"})
        )

        # Build a query-backed model in ds=A that joins to ds=B AND
        # references a column on the target (so the missing cross-DS
        # target surfaces as a binder error rather than a silently
        # dropped join).
        bad = SlayerModel(
            name="xds_qb",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model={
                    "source_name": "orders",
                    "joins": [{
                        "target_model": "customers",
                        "join_pairs": [["region", "region"]],
                    }],
                },
                dimensions=["status", "customers.name"],
            )],
        )
        engine = SlayerQueryEngine(storage=storage)
        try:
            # The join must not silently cross datasources. ds=A's
            # ``customers`` lookup returns None (the only ``customers``
            # is in ds=B); the binder then raises on the
            # ``customers.name`` ref.
            with pytest.raises(Exception):
                await engine.save_model(bad)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Wrapper alias shapes (decision B) — pin the rendered SQL's projection
# matches the flat-renamed column set the virtual model declares.
# ---------------------------------------------------------------------------


class TestWrapperAliasShapes:
    async def test_local_agg_alias_flattens_to_underscore(self) -> None:
        m = SlayerModel(
            name="qb_local",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                dimensions=["status"],
                measures=[{"formula": "amount:sum"}],
            )],
        )
        engine, tmp = await _engine()
        try:
            saved = await engine.save_model(m)
            col_names = {c.name for c in saved.columns}
            assert "amount_sum" in col_names
            assert "status" in col_names
            # The wrapper exposes flat names — no dots.
            assert all("." not in n for n in col_names), col_names
        finally:
            tmp.cleanup()

    async def test_cross_model_agg_alias_flattens_with_double_underscore(
        self,
    ) -> None:
        """``customers.revenue:sum`` (cross-model) flattens to canonical
        ``customers__revenue_sum`` (the join path AND the agg suffix both
        appear in the flat name).
        """
        # Add a join + a numeric customers.revenue column so the cross-
        # model agg has something to aggregate.
        orders_with_join = _orders_model().model_copy(update={
            "joins": [ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            )],
        })
        customers_with_rev = _customers_model().model_copy(update={
            "columns": list(_customers_model().columns) + [
                Column(name="revenue", sql="revenue", type=DataType.DOUBLE),
            ],
        })
        m = SlayerModel(
            name="qb_cross",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                dimensions=["status"],
                measures=[{"formula": "customers.revenue:sum"}],
            )],
        )
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_model(customers_with_rev)
        await storage.save_model(orders_with_join)
        engine = SlayerQueryEngine(storage=storage)
        try:
            saved = await engine.save_model(m)
            col_names = {c.name for c in saved.columns}
            assert "customers__revenue_sum" in col_names, col_names
        finally:
            tmp.cleanup()

    async def test_cross_model_parametric_agg_alias_includes_kwargs(
        self,
    ) -> None:
        """``customers.revenue:percentile(p=0.5)`` (cross-model parametric)
        flattens to ``customers__revenue_percentile_p_0_5`` (decision B —
        the wrapper helper preserves the kwarg suffix per DEV-1450).
        """
        orders_with_join = _orders_model().model_copy(update={
            "joins": [ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            )],
        })
        customers_with_rev = _customers_model().model_copy(update={
            "columns": list(_customers_model().columns) + [
                Column(name="revenue", sql="revenue", type=DataType.DOUBLE),
            ],
        })
        m = SlayerModel(
            name="qb_cross_param",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                dimensions=["status"],
                measures=[{"formula": "customers.revenue:percentile(p=0.5)"}],
            )],
        )
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_model(customers_with_rev)
        await storage.save_model(orders_with_join)
        engine = SlayerQueryEngine(storage=storage)
        try:
            saved = await engine.save_model(m)
            col_names = {c.name for c in saved.columns}
            assert "customers__revenue_percentile_p_0_5" in col_names, col_names
        finally:
            tmp.cleanup()

    async def test_joined_dim_alias_flattens(self) -> None:
        orders_with_join = _orders_model().model_copy(update={
            "joins": [ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            )],
        })
        m = SlayerModel(
            name="qb_joined_dim",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                dimensions=["customers.name"],
                measures=[{"formula": "amount:sum"}],
            )],
        )
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_model(_customers_model())
        await storage.save_model(orders_with_join)
        engine = SlayerQueryEngine(storage=storage)
        try:
            saved = await engine.save_model(m)
            col_names = {c.name for c in saved.columns}
            assert "customers__name" in col_names, col_names
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Decision D breaking change pinned — outer dotted ref on query-backed model
# ---------------------------------------------------------------------------


class TestP4ClosureBehavior:
    async def test_outer_dotted_ref_canonical_alias_no_longer_resolves(
        self,
    ) -> None:
        """A query-backed model that already aggregated ``customers.revenue:sum``
        exposes the canonical flat name ``customers__revenue_sum``. Outer
        dotted refs (``customers.revenue:sum``) against the virtual model
        DO NOT silently re-aggregate via the legacy ``SourceModelOrigin``
        lineage walk — this is the P4 closure (decision D).

        The exact error surface (``ValueError`` from binder, or empty SQL
        with no matching ref) depends on the typed pipeline's resolution
        order; the assertion is that the dotted form does not silently
        succeed with a duplicate aggregation.
        """
        orders_with_join = _orders_model().model_copy(update={
            "joins": [ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            )],
        })
        customers_with_rev = _customers_model().model_copy(update={
            "columns": list(_customers_model().columns) + [
                Column(name="revenue", sql="revenue", type=DataType.DOUBLE),
            ],
        })
        qb = SlayerModel(
            name="qb_already_aggregated",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                dimensions=["status"],
                measures=[{"formula": "customers.revenue:sum"}],
            )],
        )
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_model(customers_with_rev)
        await storage.save_model(orders_with_join)
        engine = SlayerQueryEngine(storage=storage)
        try:
            await engine.save_model(qb)
            # The flat-name form MUST work.
            flat_q = SlayerQuery(
                source_model="qb_already_aggregated",
                measures=[{"formula": "customers__revenue_sum:sum"}],
                dimensions=["status"],
            )
            resp_flat = await engine.execute(flat_q, dry_run=True)
            assert resp_flat.sql is not None
            # The dotted form must surface a clear binder error — not a
            # silent re-aggregation through the legacy SourceModelOrigin
            # lineage walk. The error message must reference the failed
            # ref so callers can correct their query.
            dotted_q = SlayerQuery(
                source_model="qb_already_aggregated",
                measures=[{"formula": "customers.revenue:sum"}],
                dimensions=["status"],
            )
            with pytest.raises(
                (ValueError, KeyError),
                match=r"(customers|revenue|not found|cannot resolve|unknown|join)",
            ):
                await engine.execute(dotted_q, dry_run=True)
        finally:
            tmp.cleanup()
