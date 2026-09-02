"""DEV-1842 task 1.8 — saved measures on query-backed models fail closed.

A query-backed model (non-empty ``source_queries``) that also declares
``measures`` directly is rejected at validation with a message naming the model
and the remedy (BREAKING, user-approved). Measures supplied by a
``ModelExtension`` over a query-backed base keep working, and a same-named
extension measure is rejected (loud duplicate error, never a silent shadow).
"""

from __future__ import annotations

import tempfile

import pytest
from pydantic import ValidationError

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import (
    _apply_extension_overlay,
    build_resolved_source_bundle,
)
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1842_fixtures import dev1842_models
from tests._engine_helpers import _engine_generate


class TestQueryBackedFailClosed:
    def test_direct_measures_on_query_backed_model_rejected(self) -> None:
        backing = SlayerQuery(source_model="orders", dimensions=["status"],
                              measures=[{"formula": "amount:sum"}])
        dead = ModelMeasure(name="dead", formula="amount:sum")
        with pytest.raises(ValidationError):
            SlayerModel(name="qb_bad", data_source="ds",
                        source_queries=[backing], measures=[dead])

    def test_rejection_names_model_and_remedy(self) -> None:
        backing = SlayerQuery(source_model="orders", dimensions=["status"],
                              measures=[{"formula": "amount:sum"}])
        dead = ModelMeasure(name="dead", formula="amount:sum")
        with pytest.raises(ValidationError) as ei:
            SlayerModel(name="qb_named", data_source="ds",
                        source_queries=[backing], measures=[dead])
        message = str(ei.value)
        assert "qb_named" in message
        # The remedy names BOTH escape hatches: the backing query's final stage
        # and a ModelExtension.
        low = message.lower()
        assert "stage" in low
        assert "extension" in low

    def test_table_backed_model_with_measures_still_allowed(self) -> None:
        """The rejection is scoped to query-backed models — an ordinary
        table-backed model keeps declaring measures."""
        m = SlayerModel(
            name="ok", data_source="ds", sql_table="ok",
            columns=[Column(name="amount", type=DataType.DOUBLE)],
            measures=[ModelMeasure(name="live", formula="amount:sum")],
        )
        assert m.measures[0].name == "live"


class TestExtensionMeasuresOverQueryBacked:
    async def test_extension_measure_over_query_backed_base_resolves(self) -> None:
        """A ``ModelExtension`` adds a measure onto a query-backed base; a query
        referencing it resolves and renders (overlay re-applied post-expansion)."""
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            await storage.save_model(SlayerModel(
                name="orders", sql_table="orders", data_source="ds",
                columns=[
                    Column(name="status", type=DataType.TEXT),
                    Column(name="amount", type=DataType.DOUBLE),
                ],
            ))
            await storage.save_model(SlayerModel(
                name="qb_base", data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders", dimensions=["status"],
                    measures=[{"formula": "amount:sum"}],
                )],
            ))
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(
                source_model=ModelExtension(
                    source_name="qb_base",
                    measures=[ModelMeasure(name="doubled",
                                           formula="amount_sum:sum * 2")],
                ),
                dimensions=["status"],
                measures=[{"formula": "doubled", "name": "r"}],
            )
            resp = await engine.execute(query, dry_run=True)
            assert resp.sql is not None

    async def test_extension_new_measure_uses_its_formula(self) -> None:
        """An extension-supplied measure (a NEW name) resolves to its own
        formula end-to-end — the overlay measure participates in binder
        resolution exactly like a model measure."""
        base = SlayerModel(
            name="t", data_source="ds", sql_table="t",
            columns=[Column(name="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model=ModelExtension(
                source_name="t",
                measures=[ModelMeasure(name="scaled", formula="amount:sum * 100")],
            ),
            measures=[{"formula": "scaled", "name": "r"}],
        )
        sql = await _engine_generate(
            query=query, model=base, dialect="duckdb", validate=False,
        )
        assert "100" in sql

    async def test_extension_measure_may_be_dotted(self) -> None:
        """An extension-supplied measure whose formula is a DOTTED cross-model
        reference resolves — extension measures participate in dotted resolution
        exactly like model measures (task 1.8, 'bare and dotted alike')."""
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="sqlite", database=":memory:")
            )
            for m in dev1842_models():
                await storage.save_model(m, _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(
                source_model=ModelExtension(
                    source_name="orders",
                    measures=[ModelMeasure(name="ext_aov", formula="customers.aov")],
                ),
                dimensions=["customers.tier"],
                measures=[{"formula": "ext_aov", "name": "r"}],
            )
            resp = await engine.execute(query, dry_run=True)
            assert resp.sql is not None


class TestSameNameExtensionRejected:
    """User decision (corrects D1): a ``ModelExtension`` measure reusing a model
    measure's name is rejected by the DEV-1443 duplicate-name guard — a loud
    error, never a silent last-wins shadow."""

    async def test_same_name_overlay_is_rejected(self) -> None:
        base = SlayerModel(
            name="t", data_source="ds", sql_table="t",
            columns=[Column(name="amount", type=DataType.DOUBLE)],
            measures=[ModelMeasure(name="sh", formula="amount:sum")],
        )
        query = SlayerQuery(
            source_model=ModelExtension(
                source_name="t",
                measures=[ModelMeasure(name="sh", formula="amount:sum * 100")],
            ),
            measures=[{"formula": "sh", "name": "r"}],
        )
        with pytest.raises(ValueError, match=r"duplicate measure names"):
            await _engine_generate(
                query=query, model=base, dialect="duckdb", validate=False,
            )


class TestOverlayNamespaceDisjoint:
    """The overlay re-runs the FULL namespace check (``model_copy`` bypasses the
    validator): duplicate columns and column/measure collisions are loud errors,
    not just duplicate measures."""

    def test_overlay_column_reusing_base_column_rejected(self) -> None:
        base = SlayerModel(
            name="t", data_source="ds", sql_table="t",
            columns=[Column(name="amount", type=DataType.DOUBLE)],
        )
        ext = ModelExtension(
            source_name="t",
            columns=[Column(name="amount", type=DataType.DOUBLE)],
        )
        with pytest.raises(ValueError, match=r"duplicate column names"):
            _apply_extension_overlay(base, ext)

    def test_overlay_measure_colliding_with_base_column_rejected(self) -> None:
        base = SlayerModel(
            name="t", data_source="ds", sql_table="t",
            columns=[Column(name="amount", type=DataType.DOUBLE)],
        )
        ext = ModelExtension(
            source_name="t",
            measures=[ModelMeasure(name="amount", formula="amount:sum")],
        )
        with pytest.raises(
            ValueError, match=r"name collision between columns and measures"
        ):
            _apply_extension_overlay(base, ext)


class TestStageExtensionMeasuresOverQueryBackedRejected:
    """DEV-1842 decision D — a stage sourced from a ModelExtension-with-measures
    over a query-backed base is a loud error (deferred-overlay re-application is
    wired only for the root source, so the measures would be silently dropped)."""

    async def test_stage_extension_measures_over_query_backed_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            await storage.save_model(SlayerModel(
                name="orders", sql_table="orders", data_source="ds",
                columns=[
                    Column(name="status", type=DataType.TEXT),
                    Column(name="amount", type=DataType.DOUBLE),
                ],
            ))
            await storage.save_model(SlayerModel(
                name="qb_base", data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders", dimensions=["status"],
                    measures=[{"formula": "amount:sum"}],
                )],
            ))
            stage = SlayerQuery(
                source_model=ModelExtension(
                    source_name="qb_base",
                    measures=[ModelMeasure(name="doubled",
                                           formula="amount_sum:sum * 2")],
                ),
                dimensions=["status"],
                measures=[{"formula": "doubled", "name": "r"}],
            )
            root = SlayerQuery(source_model="orders", dimensions=["status"])
            with pytest.raises(ValueError, match=r"may not add measures"):
                await build_resolved_source_bundle(
                    query=root, storage=storage, data_source="ds",
                    named_queries={"s1": stage},
                )
