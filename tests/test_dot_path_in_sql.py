"""DEV-1743 — the DOT_PATH_IN_SQL dotted→``__`` rewrite is RETIRED.

Mode-A free SQL (``Column.sql`` / ``Column.filter`` / ``SlayerModel.filters``)
is dotted-canonical: a dotted join path (``customers.regions.name``) is
PRESERVED verbatim through the save path and resolved structurally at
bind/generation time. Since DEV-1826 retired the last text-rewriting slack
rule (FUNC_STYLE_AGG), no normalization pass touches model text at all —
these tests pin verbatim save-path preservation and that ``normalize_query``
leaves Mode-B filter text alone.
"""

from __future__ import annotations

import tempfile

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.normalization import normalize_query
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def _orders_with_customers_join(**overrides) -> SlayerModel:
    fields = dict(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )
    fields.update(overrides)
    return SlayerModel(**fields)


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="region_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
        ],
    )


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions",
        data_source="prod",
        sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


async def _save_and_reload(model: SlayerModel) -> SlayerModel:
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(
            DatasourceConfig(name="prod", type="postgres")
        )
        await storage.save_model(_regions())
        await storage.save_model(_customers())
        engine = SlayerQueryEngine(storage=storage)
        await engine.save_model(model)
        stored = await storage.get_model("orders", data_source="prod")
    assert stored is not None
    return stored


class TestModeASurfacesPreservedVerbatim:
    async def test_column_sql_surface_preserved(self):
        m = _orders_with_customers_join()
        m.columns = list(m.columns) + [
            Column(
                name="region_name",
                type=DataType.TEXT,
                sql="customers.regions.name",
            )
        ]
        stored = await _save_and_reload(m)
        out_col = next(c for c in stored.columns if c.name == "region_name")
        assert out_col.sql == "customers.regions.name"

    async def test_column_filter_surface_preserved(self):
        m = _orders_with_customers_join()
        m.columns = list(m.columns) + [
            Column(
                name="region_amount",
                type=DataType.DOUBLE,
                sql="amount",
                filter="customers.regions.name = 'EU'",
            )
        ]
        stored = await _save_and_reload(m)
        out_col = next(c for c in stored.columns if c.name == "region_amount")
        # Verbatim: the dotted-canonical input is preserved exactly (operator +
        # literal too), not merely "contains the path / lacks the legacy token".
        assert out_col.filter == "customers.regions.name = 'EU'"

    async def test_model_filters_surface_preserved(self):
        m = _orders_with_customers_join(
            filters=["customers.regions.name IS NOT NULL"],
        )
        stored = await _save_and_reload(m)
        assert stored.filters == ["customers.regions.name IS NOT NULL"]

    async def test_model_measure_formula_preserved(self):
        # ModelMeasure.formula is Mode-B (DSL); the dotted colon form is a
        # join-path reference and must survive save verbatim.
        m = _orders_with_customers_join(
            measures=[
                ModelMeasure(
                    name="region_count",
                    formula="customers.regions.name:count",
                )
            ],
        )
        stored = await _save_and_reload(m)
        out_mm = next(x for x in stored.measures if x.name == "region_count")
        assert out_mm.formula == "customers.regions.name:count"


class TestNormalizeQueryLeavesModeBText:
    def test_query_filters_mode_b_not_rewritten(self):
        m = _orders_with_customers_join()
        q = SlayerQuery(
            source_model="orders",
            filters=["customers.regions.name = 'EU'"],
        )
        result = normalize_query(q, model=m)
        # Mode-B dotted form preserved verbatim, and no rewrite warning of any
        # kind (DOT_PATH_IN_SQL and FUNC_STYLE_AGG are both retired).
        assert result.query.filters[0] == "customers.regions.name = 'EU'"
        assert result.warnings == []
