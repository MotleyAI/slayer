"""The DOT_PATH_IN_SQL dotted→``__`` rewrite is RETIRED.

Mode-A free SQL (``DatasetField.sql`` / ``DatasetField.filter`` / ``SlayerDataset.filters``)
is dotted-canonical: a dotted relationship path (``customers.regions.name``) is
PRESERVED verbatim through the save path and resolved structurally at
bind/generation time. With the last text-rewriting slack
rule (FUNC_STYLE_AGG) retired, no normalization pass touches dataset text at all —
these tests pin verbatim save-path preservation and that ``normalize_query``
leaves Mode-B filter text alone.
"""

from __future__ import annotations

import tempfile

from slayer.core.enums import DataType
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.engine.normalization import normalize_query
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def _orders_with_customers_join(**overrides) -> SlayerDataset:
    fields = dict(
        name="orders",
        data_source="prod",
        sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
        ],
    )
    fields.update(overrides)
    return SlayerDataset(**fields)


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers",
        data_source="prod",
        sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
        ],
        relationships=[
            DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"]),
        ],
    )


def _regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions",
        data_source="prod",
        sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
        ],
    )


async def _save_and_reload(dataset: SlayerDataset) -> SlayerDataset:
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(
            DatasourceConfig(name="prod", type="postgres")
        )
        await storage.save_dataset(_regions())
        await storage.save_dataset(_customers())
        engine = SlayerQueryEngine(storage=storage)
        await engine.save_dataset(dataset)
        stored = await storage.get_dataset("orders", data_source="prod")
    assert stored is not None
    return stored


class TestModeASurfacesPreservedVerbatim:
    async def test_column_sql_surface_preserved(self):
        m = _orders_with_customers_join()
        m.fields = list(m.fields) + [
            DatasetField(
                name="region_name",
                datatype=DataType.STRING,
                expression="customers.regions.name",
            )
        ]
        stored = await _save_and_reload(m)
        out_col = next(c for c in stored.fields if c.name == "region_name")
        assert out_col.definition_sql == "customers.regions.name"

    async def test_column_filter_surface_preserved(self):
        m = _orders_with_customers_join()
        m.fields = list(m.fields) + [
            DatasetField(
                name="region_amount",
                datatype=DataType.FLOAT,
                sql="amount",
                filter="customers.regions.name = 'EU'",
            )
        ]
        stored = await _save_and_reload(m)
        out_col = next(c for c in stored.fields if c.name == "region_amount")
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
        # DatasetMetric.expression is Mode-B (DSL); the dotted colon form is a
        # relationship-path reference and must survive save verbatim.
        m = _orders_with_customers_join(
            metrics=[
                DatasetMetric(
                    name="region_count",
                    expression="count(customers.regions.name)",
                )
            ],
        )
        stored = await _save_and_reload(m)
        out_mm = next(x for x in stored.metrics if x.name == "region_count")
        assert out_mm.expression == "count(customers.regions.name)"


class TestNormalizeQueryLeavesModeBText:
    def test_query_filters_mode_b_not_rewritten(self):
        m = _orders_with_customers_join()
        q = SlayerQuery(
            population="orders",
            filter=["customers.regions.name = 'EU'"],
        )
        result = normalize_query(q, dataset=m)
        # Mode-B dotted form preserved verbatim, and no rewrite warning of any
        # kind (DOT_PATH_IN_SQL and FUNC_STYLE_AGG are both retired).
        assert result.query.filter[0] == "customers.regions.name = 'EU'"
        assert result.warnings == []
