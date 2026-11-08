"""DEV-1829 (F1 / D4) — the combined regroup producer names its output aggregate
by the CONSUMER's public alias when the partitioned aggregate is a directly-named
measure, and by the CANONICAL alias when it is a composite leaf. Byte-level across
the five golden dialects; this is the naming the byte-identical ``local/*`` goldens
depend on."""

from __future__ import annotations

import re

import pytest

from slayer.core.query import ModelMeasure, SlayerQuery

from tests._dev1739_fixtures import cm_cte_bodies, gen

DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _has_output_alias(body: str, leaf: str) -> bool:
    """Whether ``body`` projects ``orders.<leaf>`` as a column alias, robust to
    per-dialect quoting (``"orders.x"`` / ``[orders___x]`` / `` `orders___x` ``)."""
    return re.search(
        rf'AS\s+["\[`]orders[._]{{1,3}}{re.escape(leaf)}["\]`]', body,
    ) is not None


@pytest.mark.parametrize("dialect", DIALECTS)
class TestProducerOutputAlias:
    async def test_named_measure_uses_public_alias(self, dialect: str) -> None:
        sql = await gen(_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(
                formula="amount:sum(partition_by=region)", name="region_rev",
            )],
        ), dialect=dialect)
        body = cm_cte_bodies(sql)
        # Producer output alias IS the consumer's PUBLIC name, not the canonical.
        assert _has_output_alias(body, "region_rev")
        assert not _has_output_alias(body, "amount_sum_partition_by_region")

    async def test_composite_leaf_uses_canonical_alias(self, dialect: str) -> None:
        sql = await gen(_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(
                formula="amount:sum / amount:sum(partition_by=region)", name="sh",
            )],
        ), dialect=dialect)
        body = cm_cte_bodies(sql)
        # A composite LEAF producer output uses the CANONICAL alias; the public
        # composite name renders only at the combined SELECT.
        assert _has_output_alias(body, "amount_sum_partition_by_region")
        assert "sh" not in body


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_filtered_measure_predicate_lives_in_producer(dialect: str) -> None:
    # The measure's ``Column.filter`` (CASE WHEN status='ok') renders inside the
    # producer CTE, at the partition grain — unchanged from the DEV-1739 path.
    sql = await gen(_q(
        dimensions=["region", "city"],
        measures=[ModelMeasure(
            formula="ok_amount:sum(partition_by=region)", name="o",
        )],
    ), dialect=dialect)
    body = cm_cte_bodies(sql)
    assert "status" in body
    # Public alias ``o`` is used, not the canonical ``ok_amount_sum_...``.
    assert _has_output_alias(body, "o")
    assert not _has_output_alias(body, "ok_amount_sum_partition_by_region")
