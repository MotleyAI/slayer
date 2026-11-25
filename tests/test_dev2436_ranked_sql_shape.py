"""SQL shape of ``first`` / ``last`` over an aggregate on every Tier-1 dialect: the ranked ``ROW_NUMBER``
kernel over the operand cells, NULL ranking keys ordered by a flag, the consumed bucket out of the
partition, no ``FIRST_VALUE`` / ``LAST_VALUE`` (spec: queries/semantics, aggregations/functional-form)."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from tests._dev2006_fixtures import dev2006_datasets, m, month_td, snap_q
from tests._dev2215_fixtures import TIER1
from tests._engine_helpers import _engine_generate


async def _sql(metric: str, *, dialect: str) -> exp.Expr:
    first, *rest = dev2006_datasets()
    query = snap_q(dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(), metrics=[m(metric)])
    sql = await _engine_generate(query=query, dataset=first, extra_datasets=rest, dialect=dialect)
    return sqlglot.parse_one(sql, dialect=dialect)


def _row_numbers(tree: exp.Expr) -> list[exp.Window]:
    return [w for w in tree.find_all(exp.Window) if isinstance(w.this, exp.RowNumber) and w.args.get("order")]


def _has_null_flag(item: exp.Expr) -> bool:
    return any(isinstance(node.expression, exp.Null) for node in item.find_all(exp.Is))


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("op", ["first", "last"])
async def test_second_order_pick_is_the_ranked_kernel(dialect: str, op: str) -> None:
    tree = await _sql(f"{op}(sum(account_snapshots.balance))", dialect=dialect)
    assert not list(tree.find_all(exp.FirstValue, exp.LastValue)), tree.sql(dialect=dialect)
    windows = _row_numbers(tree)
    assert windows, tree.sql(dialect=dialect)
    for w in windows:
        flag, key, *_ = w.args["order"].expressions
        assert _has_null_flag(flag), w.sql(dialect=dialect)
        assert bool(key.args.get("desc")) == (op == "last"), w.sql(dialect=dialect)
        # the month bucket is the consumed ranking key: the pick partitions by the customer only
        assert len(w.args.get("partition_by") or []) == 1, w.sql(dialect=dialect)
