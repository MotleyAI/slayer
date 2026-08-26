"""DEV-1743 category 5 — Mode-B DSL over ``__``-named models.

Every test here drives a Mode-B surface (query ``dimensions`` / ``measures`` or
a ``ModelMeasure`` formula) that touches a ``__``-bearing name. They fail today
for a FEATURE reason on one (or both) of two counts, all lifted by the flip:

* constructing a ``__``-named model still RAISES (``dunder_target()`` /
  ``dunder_target_models()``); and
* the Mode-B parser (``syntax._reject_dunder_in_ast``) rejects ANY ``__`` inside
  a query / formula identifier before binding ever runs.

Each test is a POSITIVE shape (no ``pytest.raises``): it fails today because a
build or a parse raises, and after the flip it must emit the asserted SQL —
mirroring ``tests/test_dev1743_resolution.py::TestDunderTargetResolves``.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery

from tests._dev1743_fixtures import (
    DS,
    dunder_target,
    dunder_target_models,
)
from tests._engine_helpers import _engine_generate


class TestModeBOverDunderModels:
    @pytest.mark.asyncio
    async def test_direct_join_target_dimension_resolves(self) -> None:
        """FAIL-FIRST: a Mode-B dimension referencing a ``__``-named DIRECT
        join target resolves to that model's table + column.

        Fails today: ``dunder_target_models()`` raises building the
        ``customer__region`` model, and the parser rejects the ``customer__
        region`` identifier in ``customer__region.label``.
        """
        host, dunder = dunder_target_models()
        sql = await _engine_generate(
            query=SlayerQuery(
                source_model="orders",
                dimensions=["customer__region.label"],
            ),
            model=host,
            extra_models=[dunder],
            validate=False,
        )
        assert "customer_region_dim" in sql
        assert "label" in sql

    @pytest.mark.asyncio
    async def test_cross_model_star_count_measure_resolves(self) -> None:
        """FAIL-FIRST: a Mode-B cross-model count ``customer__region.*:count``
        over the ``__``-named model resolves and generates SQL.

        Fails today: the ``customer__region`` model build raises and the
        parser rejects the ``customer__region`` identifier in the formula.
        """
        host, dunder = dunder_target_models()
        sql = await _engine_generate(
            query=SlayerQuery(
                source_model="orders",
                measures=[{"formula": "customer__region.*:count"}],
            ),
            model=host,
            extra_models=[dunder],
            validate=False,
        )
        assert isinstance(sql, str) and sql
        # Post-flip the count reads the __-named model's physical table.
        assert "customer_region_dim" in sql

    @pytest.mark.asyncio
    async def test_model_measure_formula_over_dunder_model_resolves(self) -> None:
        """FAIL-FIRST: a host ``ModelMeasure`` whose formula references the
        ``__``-named model resolves when the measure is queried by name.

        The ``ModelMeasure`` itself constructs today (the ``__`` ban on
        formulas fires at parse time, not at construction); the test fails
        because ``dunder_target()`` raises building ``customer__region`` and,
        post-build, the parser rejects the formula's ``customer__region``.
        """
        host = SlayerModel(
            name="orders", data_source=DS, sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="cr_id", type=DataType.INT),
                Column(name="amount", type=DataType.DOUBLE),
            ],
            joins=[
                ModelJoin(target_model="customer__region",
                          join_pairs=[["cr_id", "id"]]),
            ],
            measures=[ModelMeasure(name="crc",
                                   formula="customer__region.*:count")],
        )
        dunder = dunder_target()  # raises today — the flip must let it through
        sql = await _engine_generate(
            query=SlayerQuery(source_model="orders", measures=["crc"]),
            model=host,
            extra_models=[dunder],
            validate=False,
        )
        assert isinstance(sql, str) and sql
        assert "customer_region_dim" in sql

    @pytest.mark.asyncio
    async def test_flat_dunder_column_referenceable_in_mode_b(self) -> None:
        """FAIL-FIRST (task-labelled INVARIANT LOCK / D5 carve-out).

        A column literally named ``stores__name`` must stay referenceable
        from a Mode-B query — the D5 carve-out. The task frames this as an
        invariant lock, but querying it does NOT pass today: the Mode-B parser
        rejects the ``stores__name`` identifier before the binder's
        exact-column-match escape can honour it. The flip relaxes the parser
        so the carve-out becomes reachable; construction of the column already
        works today (locked in ``test_dev1743_validators.py``).
        """
        model = SlayerModel(
            name="stores", data_source=DS, sql_table="stores_t",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="stores__name", type=DataType.TEXT),
            ],
        )
        sql = await _engine_generate(
            query=SlayerQuery(source_model="stores",
                              dimensions=["stores__name"]),
            model=model,
            validate=False,
        )
        assert "stores__name" in sql
