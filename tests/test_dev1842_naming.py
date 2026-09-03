"""DEV-1842 task 1.7 — naming and metadata of dotted saved-measure references.

An unnamed dotted reference surfaces under the dotted text as its implicit name,
yielding a result key of host prefix + dotted path (``orders.customers.aov``); an
explicit query ``name`` wins. The saved measure's declared ``type`` is inherited
with the same precedence as bare references (query-level type, then saved type,
then inference), and format/description derive from the bound tree — identical to
the hand-expanded form.

Result keys are read from the executed response's columns. Type / format parity
is observed the way the bare-name suites observe it
(``test_query_backed_typed_expansion``): a query-backed model whose backing stage
selects the measure exposes a virtual column whose ``type`` / ``format`` are the
inherited ones, compared against the hand-expanded twin.
"""

from __future__ import annotations

import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1842_fixtures import dev1842_models, make_exec_engine, q


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


class TestImplicitAndExplicitName:
    async def test_implicit_name_is_the_dotted_path(self, exec_backend) -> None:
        """Unnamed ``customers.aov`` → result key ``orders.customers.aov``."""
        _, engine = exec_backend
        resp = await engine.execute(q(dimensions=["customers.tier"],
                                      measures=[{"formula": "customers.aov"}]))
        assert "orders.customers.aov" in resp.columns

    async def test_explicit_name_overrides_the_implicit_key(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            measures=[{"formula": "customers.aov", "name": "myaov"}]))
        assert "orders.myaov" in resp.columns
        assert "orders.customers.aov" not in resp.columns


class TestTypeAndFormatInheritance:
    """A dotted saved measure's type/format flow to the virtual column of a
    query-backed model whose backing stage selects it — identically to its
    hand-expanded twin."""

    async def _virtual_column(self, measure_spec, col_name: str):
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="sqlite", database=":memory:")
            )
            for m in dev1842_models():
                await storage.save_model(m, _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            qb = SlayerModel(
                name="qb_typed", data_source="test",
                source_queries=[SlayerQuery(
                    source_model="orders",
                    dimensions=["status"],
                    measures=[measure_spec],
                )],
            )
            saved = await engine.save_model(qb)
            return next(c for c in saved.columns if c.name == col_name)

    async def test_saved_type_is_inherited(self) -> None:
        """``customers.typed_aov`` declares ``type=DOUBLE``; the virtual column
        carries it."""
        col = await self._virtual_column(
            ModelMeasure(formula="customers.typed_aov", name="ta"), "ta")
        assert col.type == DataType.DOUBLE

    async def test_query_level_type_overrides_saved_type(self) -> None:
        col = await self._virtual_column(
            ModelMeasure(formula="customers.typed_aov", name="ti", type=DataType.INT),
            "ti")
        assert col.type == DataType.INT

    async def test_inferred_type_matches_hand_expanded(self) -> None:
        """``customers.aov`` (no declared type) infers the SAME type as the
        hand-expanded ``customers.spend:sum / customers.*:count``."""
        dotted = await self._virtual_column(
            ModelMeasure(formula="customers.aov", name="af"), "af")
        hand = await self._virtual_column(
            ModelMeasure(formula="customers.spend:sum / customers.*:count", name="af"),
            "af")
        assert dotted.type == hand.type

    async def test_format_matches_hand_expanded(self) -> None:
        dotted = await self._virtual_column(
            ModelMeasure(formula="customers.aov", name="af"), "af")
        hand = await self._virtual_column(
            ModelMeasure(formula="customers.spend:sum / customers.*:count", name="af"),
            "af")
        assert dotted.format == hand.format
