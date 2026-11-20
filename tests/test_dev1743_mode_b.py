"""Category 5 — Mode-B DSL over ``__``-named datasets.

Every test here drives a Mode-B surface (query ``dimensions`` / ``metrics`` or
a ``DatasetMetric`` expression) that touches a ``__``-bearing name. They fail today
for a FEATURE reason on one (or both) of two counts, all lifted by the flip:

* constructing a ``__``-named dataset still RAISES (``dunder_target()`` /
  ``dunder_target_datasets()``); and
* the Mode-B parser (``syntax._reject_dunder_in_ast``) rejects ANY ``__`` inside
  a query / expression identifier before binding ever runs.

Each test is a POSITIVE shape (no ``pytest.raises``): it fails today because a
build or a parse raises, and after the flip it must emit the asserted SQL —
mirroring ``tests/test_dev1743_resolution.py::TestDunderTargetResolves``.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery

from tests._dev1743_fixtures import (
    DS,
    dunder_target,
    dunder_target_datasets,
)
from tests._engine_helpers import _engine_generate


class TestModeBOverDunderModels:
    @pytest.mark.asyncio
    async def test_direct_join_target_dimension_resolves(self) -> None:
        """FAIL-FIRST: a Mode-B dimension referencing a ``__``-named DIRECT
        relationship target resolves to that dataset's table + field.

        Fails today: ``dunder_target_datasets()`` raises building the
        ``customer__region`` dataset, and the parser rejects the ``customer__
        region`` identifier in ``customer__region.label``.
        """
        host, dunder = dunder_target_datasets()
        sql = await _engine_generate(
            query=SlayerQuery(
                population="orders",
                dimensions=["customer__region.label"],
            ),
            dataset=host,
            extra_datasets=[dunder],
            validate=False,
        )
        assert "customer_region_dim" in sql
        assert "label" in sql

    @pytest.mark.asyncio
    async def test_cross_model_star_count_measure_resolves(self) -> None:
        """FAIL-FIRST: a Mode-B cross-dataset count ``count(customer__region.*)``
        over the ``__``-named dataset resolves and generates SQL.

        Fails today: the ``customer__region`` dataset build raises and the
        parser rejects the ``customer__region`` identifier in the expression.
        """
        host, dunder = dunder_target_datasets()
        sql = await _engine_generate(
            query=SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "count(customer__region.*)"}],
            }),
            dataset=host,
            extra_datasets=[dunder],
            validate=False,
        )
        assert isinstance(sql, str)
        assert sql
        # Post-flip the count reads the __-named dataset's physical table.
        assert "customer_region_dim" in sql

    @pytest.mark.asyncio
    async def test_model_measure_formula_over_dunder_model_resolves(self) -> None:
        """FAIL-FIRST: a host ``DatasetMetric`` whose expression references the
        ``__``-named dataset resolves when the metric is queried by name.

        The ``DatasetMetric`` itself constructs today (the ``__`` ban on
        expressions fires at parse time, not at construction); the test fails
        because ``dunder_target()`` raises building ``customer__region`` and,
        post-build, the parser rejects the expression's ``customer__region``.
        """
        host = SlayerDataset(
            name="orders", data_source=DS, sql_table="orders",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="cr_id", datatype=DataType.INTEGER),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ],
            relationships=[
                DatasetRelationship(to="customer__region",
                          from_columns=["cr_id"], to_columns=["id"]),
            ],
            metrics=[DatasetMetric(name="crc",
                                   expression="count(customer__region.*)")],
        )
        dunder = dunder_target()  # raises today — the flip must let it through
        sql = await _engine_generate(
            query=SlayerQuery(population="orders", metrics=["orders.crc"]),
            dataset=host,
            extra_datasets=[dunder],
            validate=False,
        )
        assert isinstance(sql, str)
        assert sql
        assert "customer_region_dim" in sql

    @pytest.mark.asyncio
    async def test_flat_dunder_column_referenceable_in_mode_b(self) -> None:
        """FAIL-FIRST (task-labelled INVARIANT LOCK / D5 carve-out).

        A field literally named ``stores__name`` must stay referenceable
        from a Mode-B query — the D5 carve-out. The task frames this as an
        invariant lock, but querying it does NOT pass today: the Mode-B parser
        rejects the ``stores__name`` identifier before the binder's
        exact-field-match escape can honour it. The flip relaxes the parser
        so the carve-out becomes reachable; construction of the field already
        works today (locked in ``test_dev1743_validators.py``).
        """
        model = SlayerDataset(
            name="stores", data_source=DS, sql_table="stores_t",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="stores__name", datatype=DataType.STRING),
            ],
        )
        sql = await _engine_generate(
            query=SlayerQuery(population="stores",
                              dimensions=["stores.stores__name"]),
            dataset=model,
            validate=False,
        )
        assert "stores__name" in sql
