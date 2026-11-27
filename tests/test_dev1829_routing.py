"""F5 — routing of partitioned metrics through the combined attach:
ORDER-BY-only targets, composites, in-filter rejection, duplicate aliases,
pagination, and the response-contract guarantee that no placeholder / producer
column leaks into result keys / columns / warnings."""

from __future__ import annotations

import os
import re

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.engine.plan import plan_stages
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

from slayer.core.query import QueryDimension, QueryMetric

from tests._dev1739_fixtures import (
    SlayerQuery,
    gen,
    make_exec_engine,
    rows_by,
)
import slayer.engine.bundle_builder
from slayer.engine.query_engine import SlayerQueryEngine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
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
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="cell")],
            order=[{"field": "sum(orders.amount, partition_by=orders.region)", "direction": "asc"}],
        ))
        assert _count_isolated_ctes(sql) == 1

    async def test_order_only_sorts_by_region_total(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="cell")],
            order=[{"field": "sum(orders.amount, partition_by=orders.region)", "direction": "asc"}],
        ))
        totals = [REGION_TOTAL[r["orders.region"]] for r in resp.data]
        assert totals == sorted(totals)
        # The hidden order target does not surface in the result.
        assert set(resp.data[0].keys()) == {
            "orders.region", "orders.city", "cell",
        }

    async def test_composite_only_in_order_by_isolates(self) -> None:
        sql = await gen(_q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="cell")],
            order=[{
                "field": "sum(orders.amount) / sum(orders.amount, partition_by=orders.region)",
                "direction": "asc",
            }],
        ))
        assert _count_isolated_ctes(sql) == 1

    async def test_composite_order_sorts_by_share(self, exec_engine) -> None:
        # ORDER BY share (cell / region_total): the composite consumes the
        # attached partitioned leaf, so rows come back in share order.
        resp = await exec_engine.execute(_q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="cell")],
            order=[{
                "field": "sum(orders.amount) / sum(orders.amount, partition_by=orders.region)",
                "direction": "asc",
            }],
        ))
        shares = [SHARE[(r["orders.region"], r["orders.city"])] for r in resp.data]
        assert shares == sorted(shares)
        assert set(resp.data[0].keys()) == {
            "orders.region", "orders.city", "cell",
        }


class TestInFilterOverComposite:
    async def test_filter_over_partitioned_composite_lifted(self) -> None:
        # A single predicate combining a plain
        # aggregate with a partitioned one resolves entirely at the combined
        # scope (both after aggregation + attachment); it renders at the outer
        # WHERE with no placeholder leak.
        q = _q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount) / sum(orders.amount, partition_by=orders.region) > 0.5"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="cell")],
        )
        assert "__regroup__" not in await gen(q)


class TestDuplicateAliases:
    async def test_duplicate_partitioned_measures_project_both(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["orders.region", "orders.city"],
            metrics=[
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="a"),
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="b"),
            ],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        for key, row in by.items():
            assert float(row["a"]) == pytest.approx(REGION_TOTAL[key[0]])
            assert float(row["b"]) == pytest.approx(REGION_TOTAL[key[0]])
        keys = set(resp.data[0].keys())
        assert keys == {"orders.region", "orders.city", "a", "b"}


class TestPaginationWithHiddenOrdering:
    async def test_limit_offset_orders_by_hidden_partition(self, exec_engine) -> None:
        # ASC by region total, the grain rows are 50 (South,CityC), 60
        # (NULL,CityD), then three North (100). offset=1/limit=2 must yield the
        # 60 row then a 100 row — [60, 100]. The current mis-ordering (sort by
        # the plain per-city total) instead pages into two North rows → [100, 100].
        resp = await exec_engine.execute(_q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="cell")],
            order=[{"field": "sum(orders.amount, partition_by=orders.region)", "direction": "asc"}],
            limit=2, offset=1,
        ))
        assert len(resp.data) == 2
        totals = [REGION_TOTAL[r["orders.region"]] for r in resp.data]
        assert totals == [60.0, 100.0]
        assert set(resp.data[0].keys()) == {
            "orders.region", "orders.city", "cell",
        }


class TestResponseContractNoLeak:
    async def test_no_placeholder_or_producer_column_leaks(self, exec_engine: SlayerQueryEngine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(
                metric="sum(orders.amount, partition_by=orders.region)", alias="region_rev",
            )],
        ))
        data_keys = set(resp.data[0].keys())
        col_names = set(resp.columns)
        # meta: the per-column attribute maps must not carry a placeholder /
        # producer entry either.
        attr_keys = set(resp.attributes.dimensions) | set(resp.attributes.metrics)
        for names in (data_keys, col_names, attr_keys):
            assert not any("__regroup__" in n for n in names)
            assert not any(n.endswith("amount_sum_partition_by_region") for n in names)
        assert data_keys == {"orders.region", "orders.city", "region_rev"}
        # This shape drops no filter and synthesizes no warning.
        assert not resp.warnings


class TestReservedPrefixAcrossStages:
    """An upstream ``__regroup__*`` column is an ordinary column beside a downstream regroup."""

    async def test_upstream_reserved_prefix_column_accepted(self, tmp_path) -> None:
        db_path = os.path.join(tmp_path, "t.db")
        with transaction(db_path) as con:
            con.execute("CREATE TABLE orders (region TEXT, amount REAL)")
            con.executemany(
                "INSERT INTO orders VALUES (?, ?)", [("North", 10.0), ("South", 20.0)],
            )

        storage = YAMLStorage(base_dir=os.path.join(tmp_path, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="prod", type="sqlite", database=db_path)
        )
        await storage.save_dataset(
            SlayerDataset(
                name="orders", sql_table="orders", data_source="prod",
                fields=[
                    DatasetField(name="region", datatype=DataType.STRING),
                    DatasetField(name="amount", datatype=DataType.FLOAT),
                ],
            ),
            _validate=False,
        )
        stage1 = SlayerQuery(
            name="stage1", population="orders", dimensions=["orders.region"],
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="__regroup__0__amount_sum"),
                QueryMetric(metric="sum(orders.amount)", alias="total"),
            ],
        )
        root = SlayerQuery(
            population="stage1",
            dimensions=[
                QueryDimension(field="stage1.region"),
                QueryDimension(field="CASE WHEN sum(stage1.total, partition_by=stage1.region) > 5 THEN 1 ELSE 0 END", alias="band"),
            ],
            metrics=[QueryMetric(metric="sum(stage1.total)", alias="s")],
        )
        bundle = await slayer.engine.bundle_builder.build_resolved_source_bundle(
            query=root, storage=storage, named_queries={"stage1": stage1},
        )
        assert plan_stages(queries=[stage1, root], bundle=bundle)
