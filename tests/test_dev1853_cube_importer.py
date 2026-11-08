"""DEV-1853 — the Cube importer dedups mutually-inverse join declarations.

Both directions of one relationship import as a single edge; contradicting
declarations import both edges and surface as an ambiguous hop.
"""

from __future__ import annotations

from slayer.core.enums import JoinCardinality
from slayer.core.join_walker import edges_between
from slayer.core.models import SlayerModel
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeCube, CubeDimension, CubeJoin, CubeProject

DS = "test_ds"


def _convert(project: CubeProject) -> dict[str, SlayerModel]:
    result = CubeToSlayerConverter(project=project, data_source=DS).convert()
    return {m.name: m for m in result.models}


def _project(*, customers_join_sql: str) -> CubeProject:
    return CubeProject(cubes=[
        CubeCube(
            name="orders", sql_table="public.orders",
            joins=[CubeJoin(name="customers", relationship="belongs_to",
                            sql="{CUBE}.customer_id = {customers.id}")],
            dimensions=[
                CubeDimension(name="id", sql="{CUBE}.id", type="number",
                              primary_key=True),
                CubeDimension(name="customer_id", sql="{CUBE}.customer_id",
                              type="number"),
                CubeDimension(name="alt_id", sql="{CUBE}.alt_id",
                              type="number"),
            ],
        ),
        CubeCube(
            name="customers", sql_table="public.customers",
            joins=[CubeJoin(name="orders", relationship="has_many",
                            sql=customers_join_sql)],
            dimensions=[
                CubeDimension(name="id", sql="{CUBE}.id", type="number",
                              primary_key=True),
            ],
        ),
    ])


class TestMutuallyInverseDeclarations:
    def test_exact_inverses_import_as_one_edge_traversable_both_ways(
        self,
    ) -> None:
        models = _convert(
            _project(customers_join_sql="{CUBE}.id = {orders.customer_id}"))
        orders_joins = models["orders"].joins
        customers_joins = models["customers"].joins
        assert len(orders_joins) + len(customers_joins) == 1
        assert len(edges_between(source=models["orders"],
                                 target=models["customers"])) == 1
        assert len(edges_between(source=models["customers"],
                                 target=models["orders"])) == 1

    def test_surviving_edge_is_the_to_one_declaration(self) -> None:
        models = _convert(
            _project(customers_join_sql="{CUBE}.id = {orders.customer_id}"))
        assert len(models["orders"].joins) == 1
        assert models["orders"].joins[0].cardinality == JoinCardinality.MANY_TO_ONE
        assert models["customers"].joins == []


class TestContradictingDeclarations:
    def test_different_columns_import_both_edges(self) -> None:
        models = _convert(
            _project(customers_join_sql="{CUBE}.id = {orders.alt_id}"))
        assert len(models["orders"].joins) == 1
        assert len(models["customers"].joins) == 1
