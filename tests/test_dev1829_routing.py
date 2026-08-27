"""DEV-1829 (F5) — routing of partitioned measures through the combined attach:
ORDER-BY-only targets, composites, in-filter rejection, duplicate aliases,
pagination, and the response-contract guarantee that no placeholder / producer
column leaks into result keys / columns / warnings."""

from __future__ import annotations

import re

import pytest

from tests._dev1739_fixtures import (
    ModelMeasure,
    SlayerQuery,
    gen,
    make_exec_engine,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _count_isolated_ctes(sql: str) -> int:
    return len({m.group(1) for m in re.finditer(r"(_cm_\w+)\s+AS\s*\(", sql)})


REGION_TOTAL = {"North": 100.0, "South": 50.0, None: 60.0}
CITY_REV = {
    ("North", "CityA"): 30.0, ("North", "CityB"): 40.0, ("North", None): 30.0,
    ("South", "CityC"): 50.0, (None, "CityD"): 60.0,
}
SHARE = {k: v / REGION_TOTAL[k[0]] for k, v in CITY_REV.items()}


class TestOrderOnlyPartitionedAggregate:
    async def test_order_only_partitioned_aggregate_isolates(self) -> None:
        # A partitioned aggregate referenced ONLY by ORDER BY must be discovered
        # and routed through a combined producer (fails on the current tree,
        # which silently sorts by the plain per-city total instead).
        sql = await gen(_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="cell")],
            order=[{"column": "amount:sum(partition_by=region)", "direction": "asc"}],
        ))
        assert _count_isolated_ctes(sql) == 1

    async def test_order_only_sorts_by_region_total(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="cell")],
            order=[{"column": "amount:sum(partition_by=region)", "direction": "asc"}],
        ))
        totals = [REGION_TOTAL[r["orders.region"]] for r in resp.data]
        assert totals == sorted(totals)
        # The hidden order target does not surface in the result.
        assert set(resp.data[0].keys()) == {
            "orders.region", "orders.city", "orders.cell",
        }

    async def test_composite_only_in_order_by_isolates(self) -> None:
        sql = await gen(_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="cell")],
            order=[{
                "column": "amount:sum / amount:sum(partition_by=region)",
                "direction": "asc",
            }],
        ))
        assert _count_isolated_ctes(sql) == 1

    async def test_composite_order_sorts_by_share(self, exec_engine) -> None:
        # ORDER BY share (cell / region_total): the composite consumes the
        # attached partitioned leaf, so rows come back in share order.
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="cell")],
            order=[{
                "column": "amount:sum / amount:sum(partition_by=region)",
                "direction": "asc",
            }],
        ))
        shares = [SHARE[(r["orders.region"], r["orders.city"])] for r in resp.data]
        assert shares == sorted(shares)
        assert set(resp.data[0].keys()) == {
            "orders.region", "orders.city", "orders.cell",
        }


class TestInFilterOverComposite:
    async def test_filter_over_partitioned_composite_raises(self) -> None:
        # An aggregate/post filter over a composite that contains a partitioned
        # aggregate is the deferred in-filter shape → DEV-1824.
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(_q(
                dimensions=["region", "city"],
                filters=["amount:sum / amount:sum(partition_by=region) > 0.5"],
                measures=[ModelMeasure(formula="amount:sum", name="cell")],
            ))


class TestDuplicateAliases:
    async def test_duplicate_partitioned_measures_project_both(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="a"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="b"),
            ],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        for key, row in by.items():
            assert float(row["orders.a"]) == pytest.approx(REGION_TOTAL[key[0]])
            assert float(row["orders.b"]) == pytest.approx(REGION_TOTAL[key[0]])
        keys = set(resp.data[0].keys())
        assert keys == {"orders.region", "orders.city", "orders.a", "orders.b"}


class TestPaginationWithHiddenOrdering:
    async def test_limit_offset_orders_by_hidden_partition(self, exec_engine) -> None:
        # ASC by region total, the grain rows are 50 (South,CityC), 60
        # (NULL,CityD), then three North (100). offset=1/limit=2 must yield the
        # 60 row then a 100 row — [60, 100]. The current mis-ordering (sort by
        # the plain per-city total) instead pages into two North rows → [100, 100].
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="cell")],
            order=[{"column": "amount:sum(partition_by=region)", "direction": "asc"}],
            limit=2, offset=1,
        ))
        assert len(resp.data) == 2
        totals = [REGION_TOTAL[r["orders.region"]] for r in resp.data]
        assert totals == [60.0, 100.0]
        assert set(resp.data[0].keys()) == {
            "orders.region", "orders.city", "orders.cell",
        }


class TestResponseContractNoLeak:
    async def test_no_placeholder_or_producer_column_leaks(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(
                formula="amount:sum(partition_by=region)", name="region_rev",
            )],
        ))
        data_keys = set(resp.data[0].keys())
        col_names = set(resp.columns)
        # meta: the per-column attribute maps must not carry a placeholder /
        # producer entry either.
        attr_keys = set(resp.attributes.dimensions) | set(resp.attributes.measures)
        for names in (data_keys, col_names, attr_keys):
            assert not any("__regroup__" in n for n in names)
            assert not any(n.endswith("amount_sum_partition_by_region") for n in names)
        assert data_keys == {"orders.region", "orders.city", "orders.region_rev"}
        # This shape drops no filter and synthesizes no warning.
        assert not resp.warnings
