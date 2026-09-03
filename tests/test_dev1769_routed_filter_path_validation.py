"""Multi-hop routed-filter path validation in the cross-model target-scope renderer.
Two-hop aggregate (``customers_v2.regions.population:sum``) → CTE target ``regions``, so a filter path can end at the target or the intermediate hop; reachable filters route into the CTE, unreachable ones drop from the producer with a warning.
"""

from __future__ import annotations

import warnings


from slayer.core.enums import DataType
from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.core.models import Column, ModelMeasure
from slayer.core.query import SlayerQuery

from tests._cross_model_chain import (
    _extract_cte_body,
    _gen,
    _norm,
)
from tests._engine_helpers import _assert_valid_sql

# Derived column on regions: its filter path ends AT the target ``regions``.
_REGIONS_EXTRA = [Column(name="pop_x2", sql="population * 2", type=DataType.DOUBLE)]

_TWO_HOP_AGG = ModelMeasure(formula="customers_v2.regions.population:sum")


class TestRoutedFilterPathE2E:
    """Accept cases: reachable filters land in the CTE WHERE. Reject cases: unreachable filters drop with a warning."""

    async def test_plain_column_path_ending_at_target_renders(self) -> None:
        query = SlayerQuery(
            source_model="orders_x",
            measures=[_TWO_HOP_AGG],
            filters=["customers_v2.regions.name = 'x'"],
        )
        sql = await _gen(query, regions_extra=_REGIONS_EXTRA)
        _assert_valid_sql(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "regions.name = 'x'" in cm_body, cm_body

    async def test_derived_column_path_ending_at_target_renders(self) -> None:
        query = SlayerQuery(
            source_model="orders_x",
            measures=[_TWO_HOP_AGG],
            filters=["customers_v2.regions.pop_x2 > 5"],
        )
        sql = await _gen(query, regions_extra=_REGIONS_EXTRA)
        _assert_valid_sql(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "CAST(regions.population * 2 AS DOUBLE PRECISION) > 5" in cm_body, cm_body

    async def test_intermediate_hop_plain_column_filter_pushes(self) -> None:
        """DEV-1840: a plain-column filter on the INTERMEDIATE hop
        (``customers_v2.status``) binds to the reverse-path node and pushes
        into the producer as a correlated EXISTS — silently, superseding the
        DEV-1836 drop+warn."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[_TWO_HOP_AGG],
            filters=["customers_v2.status = 'x'"],
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            sql = await _gen(query, regions_extra=_REGIONS_EXTRA)
        _assert_valid_sql(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "EXISTS" in cm_body.upper(), cm_body
        assert "status" in cm_body, cm_body
        assert not [
            w for w in caught
            if issubclass(w.category, UnreachableFilterDroppedWarning)
        ], [str(w.message) for w in caught]

    async def test_derived_column_owned_by_other_model_filter_pushes(self) -> None:
        """DEV-1840: a derived-column filter owned by the intermediate model
        (``customers_v2.ltv_x2``) expands at the reverse-path node inside the
        pushed EXISTS — silently, superseding the DEV-1836 drop+warn."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[_TWO_HOP_AGG],
            filters=["customers_v2.ltv_x2 > 5"],
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            sql = await _gen(query, regions_extra=_REGIONS_EXTRA)
        _assert_valid_sql(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "EXISTS" in cm_body.upper(), cm_body
        assert (
            "CAST(customers_v2.lifetime_value * 2 AS DOUBLE PRECISION) > 5"
            in cm_body
        ), cm_body
        assert not [
            w for w in caught
            if issubclass(w.category, UnreachableFilterDroppedWarning)
        ], [str(w.message) for w in caught]
