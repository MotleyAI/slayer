"""Naming and metadata of dotted saved-metric references.

An unnamed dotted reference surfaces under the dotted text as its implicit name,
yielding a result key of host prefix + dotted path (``orders.customers.aov``); an
explicit query ``name`` wins. The saved metric's declared ``type`` is inherited
with the same precedence as bare references (query-level type, then saved type,
then inference), and format/description derive from the bound tree — identical to
the hand-expanded form.

Result keys are read from the executed response's columns. Type / format parity
is observed the way the bare-name suites observe it
(``test_query_backed_typed_expansion``): a query-backed dataset whose backing stage
selects the metric exposes a virtual field whose ``type`` / ``format`` are the
inherited ones, compared against the hand-expanded twin.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1842_fixtures import dev1842_datasets, make_exec_engine, q


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request: pytest.FixtureRequest) -> AsyncIterator[tuple[str, SlayerQueryEngine]]:
    async for engine in make_exec_engine(request):
        yield request.param, engine


class TestImplicitAndExplicitName:
    async def test_implicit_name_is_the_dotted_path(self, exec_backend: tuple[str, SlayerQueryEngine]) -> None:
        """Unnamed ``customers.aov`` → result key ``orders.customers.aov``."""
        _, engine = exec_backend
        resp = await engine.execute(q(dimensions=["customers.tier"],
                                      metrics=[{"metric": "customers.aov"}]))
        assert "customers.aov" in resp.columns

    async def test_explicit_name_overrides_the_implicit_key(self, exec_backend: tuple[str, SlayerQueryEngine]) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            metrics=[{"metric": "customers.aov", "alias": "myaov"}]))
        assert "myaov" in resp.columns
        assert "customers.aov" not in resp.columns


class TestTypeAndFormatInheritance:
    """A dotted saved metric's type/format flow to the virtual field of a
    query-backed dataset whose backing stage selects it — identically to its
    hand-expanded twin."""

    async def _virtual_field(self, metric_spec, col_name: str):
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="sqlite", database=":memory:")
            )
            for m in dev1842_datasets():
                await storage.save_dataset(m, _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            qb = SlayerDataset(
                name="qb_typed", data_source="test",
                source_queries=[SlayerQuery(
                    population="orders",
                    dimensions=["orders.status"],
                    metrics=[metric_spec],
                )],
            )
            saved = await engine.save_dataset(qb)
            return next(c for c in saved.fields if c.name == col_name)

    async def test_saved_type_is_inherited(self) -> None:
        """``customers.typed_aov`` declares ``type=DOUBLE``; the virtual field
        carries it."""
        col = await self._virtual_field(
            QueryMetric(metric="customers.typed_aov", alias="ta"), "ta")
        assert col.datatype == DataType.FLOAT

    async def test_query_level_type_overrides_saved_type(self) -> None:
        col = await self._virtual_field(
            QueryMetric(metric="customers.typed_aov", alias="ti", datatype=DataType.INTEGER),
            "ti")
        assert col.datatype == DataType.INTEGER

    async def test_inferred_type_matches_hand_expanded(self) -> None:
        """``customers.aov`` (no declared type) infers the SAME type as the
        hand-expanded ``sum(customers.spend) / count(customers.*)``."""
        dotted = await self._virtual_field(
            QueryMetric(metric="customers.aov", alias="af"), "af")
        hand = await self._virtual_field(
            QueryMetric(metric="sum(customers.spend) / count(customers.*)", alias="af"),
            "af")
        assert dotted.datatype == hand.datatype

    async def test_format_matches_hand_expanded(self) -> None:
        dotted = await self._virtual_field(
            QueryMetric(metric="customers.aov", alias="af"), "af")
        hand = await self._virtual_field(
            QueryMetric(metric="sum(customers.spend) / count(customers.*)", alias="af"),
            "af")
        assert dotted.format == hand.format
