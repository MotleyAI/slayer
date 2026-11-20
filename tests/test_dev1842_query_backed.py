"""Saved metrics on query-backed datasets fail closed.

A query-backed dataset (non-empty ``source_queries``) that also declares
``metrics`` directly is rejected at validation with a message naming the dataset
and the remedy (BREAKING, user-approved). Metrics supplied by a
``DatasetExtension`` over a query-backed base keep working, and a same-named
extension metric is rejected (loud duplicate error, never a silent shadow).
"""

from __future__ import annotations

import tempfile

import pytest
from pydantic import ValidationError

from slayer.core.enums import DataType
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import DatasetExtension, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import apply_extension_overlay
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1842_fixtures import dev1842_datasets
from tests._engine_helpers import _engine_generate
import slayer.engine.bundle_builder


class TestQueryBackedFailClosed:
    def test_direct_measures_on_query_backed_model_rejected(self) -> None:
        backing = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": [{"metric": "sum(orders.amount)"}],
        })
        dead = DatasetMetric(name="dead", expression="sum(amount)")
        with pytest.raises(ValidationError):
            SlayerDataset(name="qb_bad", data_source="ds",
                        source_queries=[backing], metrics=[dead])

    def test_rejection_names_model_and_remedy(self) -> None:
        backing = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": [{"metric": "sum(orders.amount)"}],
        })
        dead = DatasetMetric(name="dead", expression="sum(amount)")
        with pytest.raises(ValidationError) as ei:
            SlayerDataset(name="qb_named", data_source="ds",
                        source_queries=[backing], metrics=[dead])
        message = str(ei.value)
        assert "qb_named" in message
        # The remedy names BOTH escape hatches: the backing query's final stage
        # and a DatasetExtension.
        low = message.lower()
        assert "stage" in low
        assert "extension" in low

    def test_table_backed_model_with_measures_still_allowed(self) -> None:
        """The rejection is scoped to query-backed datasets — an ordinary
        table-backed dataset keeps declaring metrics."""
        m = SlayerDataset(
            name="ok", data_source="ds", sql_table="ok",
            fields=[DatasetField(name="amount", datatype=DataType.FLOAT)],
            metrics=[DatasetMetric(name="live", expression="sum(amount)")],
        )
        assert m.metrics[0].name == "live"


class TestExtensionMeasuresOverQueryBacked:
    async def test_extension_measure_over_query_backed_base_resolves(self) -> None:
        """A ``DatasetExtension`` adds a metric onto a query-backed base; a query
        referencing it resolves and renders (overlay re-applied post-expansion)."""
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            await storage.save_dataset(SlayerDataset(
                name="orders", sql_table="orders", data_source="ds",
                fields=[
                    DatasetField(name="status", datatype=DataType.STRING),
                    DatasetField(name="amount", datatype=DataType.FLOAT),
                ],
            ))
            await storage.save_dataset(SlayerDataset(
                name="qb_base", data_source="ds",
                source_queries=[SlayerQuery.model_validate({
                    "population": "orders",
                    "dimensions": ["orders.status"],
                    "metrics": [{"metric": "sum(orders.amount)"}],
                })],
            ))
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery.model_validate({
                "population": "qb_base", "extensions": [DatasetExtension(
                    dataset="qb_base",
                    metrics=[DatasetMetric(name="doubled",
                                           expression="sum(amount_sum) * 2")],
                )],
                "dimensions": ["qb_base.status"],
                "metrics": [{"metric": "qb_base.doubled", "alias": "r"}],
            })
            resp = await engine.execute(query, dry_run=True)
            assert resp.sql is not None

    async def test_extension_new_measure_uses_its_formula(self) -> None:
        """An extension-supplied metric (a NEW name) resolves to its own
        expression end-to-end — the overlay metric participates in binder
        resolution exactly like a dataset metric."""
        base = SlayerDataset(
            name="t", data_source="ds", sql_table="t",
            fields=[DatasetField(name="amount", datatype=DataType.FLOAT)],
        )
        query = SlayerQuery.model_validate({
            "population": "t", "extensions": [DatasetExtension(
                dataset="t",
                metrics=[DatasetMetric(name="scaled", expression="sum(amount) * 100")],
            )],
            "metrics": [{"metric": "t.scaled", "alias": "r"}],
        })
        sql = await _engine_generate(
            query=query, dataset=base, dialect="duckdb", validate=False,
        )
        assert "100" in sql

    async def test_extension_measure_may_be_dotted(self) -> None:
        """An extension-supplied metric whose expression is a DOTTED cross-dataset
        reference resolves — extension metrics participate in dotted resolution
        exactly like dataset metrics (task 1.8, 'bare and dotted alike')."""
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="sqlite", database=":memory:")
            )
            for m in dev1842_datasets():
                await storage.save_dataset(m, _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery.model_validate({
                "population": "orders", "extensions": [DatasetExtension(
                    dataset="orders",
                    metrics=[DatasetMetric(name="ext_aov", expression="customers.aov")],
                )],
                "dimensions": ["customers.tier"],
                "metrics": [{"metric": "orders.ext_aov", "alias": "r"}],
            })
            resp = await engine.execute(query, dry_run=True)
            assert resp.sql is not None


class TestSameNameExtensionRejected:
    """User decision (corrects D1): a ``DatasetExtension`` metric reusing a dataset
    metric's name is rejected by the duplicate-name guard — a loud
    error, never a silent last-wins shadow."""

    async def test_same_name_overlay_is_rejected(self) -> None:
        base = SlayerDataset(
            name="t", data_source="ds", sql_table="t",
            fields=[DatasetField(name="amount", datatype=DataType.FLOAT)],
            metrics=[DatasetMetric(name="sh", expression="sum(amount)")],
        )
        query = SlayerQuery.model_validate({
            "population": "t", "extensions": [DatasetExtension(
                dataset="t",
                metrics=[DatasetMetric(name="sh", expression="sum(amount) * 100")],
            )],
            "metrics": [{"metric": "t.sh", "alias": "r"}],
        })
        with pytest.raises(ValueError, match=r"duplicate metric names"):
            await _engine_generate(
                query=query, dataset=base, dialect="duckdb", validate=False,
            )


class TestOverlayNamespaceDisjoint:
    """The overlay re-runs the FULL namespace check (``model_copy`` bypasses the
    validator): duplicate fields and field/metric collisions are loud errors,
    not just duplicate metrics."""

    def test_overlay_measure_colliding_with_base_column_rejected(self) -> None:
        base = SlayerDataset(
            name="t", data_source="ds", sql_table="t",
            fields=[DatasetField(name="amount", datatype=DataType.FLOAT)],
        )
        ext = DatasetExtension(
            dataset="t",
            metrics=[DatasetMetric(name="amount", expression="sum(amount)")],
        )
        with pytest.raises(
            ValueError, match=r"name collision between fields and metrics"
        ):
            apply_extension_overlay(base, ext)


class TestStageExtensionMeasuresOverQueryBacked:
    """A non-root stage may extend a query-backed dataset with metrics."""

    async def test_stage_extension_measures_over_query_backed_build(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            await storage.save_dataset(SlayerDataset(
                name="orders", sql_table="orders", data_source="ds",
                fields=[
                    DatasetField(name="status", datatype=DataType.STRING),
                    DatasetField(name="amount", datatype=DataType.FLOAT),
                ],
            ))
            await storage.save_dataset(SlayerDataset(
                name="qb_base", data_source="ds",
                source_queries=[SlayerQuery.model_validate({
                    "population": "orders",
                    "dimensions": ["orders.status"],
                    "metrics": [{"metric": "sum(orders.amount)"}],
                })],
            ))
            stage = SlayerQuery.model_validate({
                "population": "qb_base", "extensions": [DatasetExtension(
                    dataset="qb_base",
                    metrics=[DatasetMetric(name="doubled",
                                           expression="sum(amount_sum) * 2")],
                )],
                "dimensions": ["qb_base.status"],
                "metrics": [{"metric": "qb_base.doubled", "alias": "r"}],
            })
            root = SlayerQuery(population="orders", dimensions=["orders.status"])
            bundle = await slayer.engine.bundle_builder.build_resolved_source_bundle(
                query=root, storage=storage, data_source="ds",
                named_queries={"s1": stage},
            )
            assert bundle is not None
