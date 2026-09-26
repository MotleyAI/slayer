"""Every ``_dev1966_fixtures`` model executes by name with its hand-computed values."""

from __future__ import annotations

from contextlib import nullcontext
from typing import AsyncIterator, List, Tuple

import pytest

from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1966_fixtures import (
    AMOUNT_BY_STATUS,
    AMOUNT_BY_TIER,
    CUST_REV,
    CUST_TOP,
    EMBEDDED_WITH,
    OK_REV,
    dev1966_engine,
    m,
    query,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev1966_engine(request.param) as e:
        yield e


def _values(rows) -> List[Tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


_BY_NAME = {
    "rev_by_status": [(s, v) for s, v in AMOUNT_BY_STATUS.items()],
    "status_share": [(s, v) for s, v in AMOUNT_BY_STATUS.items()],
    "cust_rev": [(c, CUST_REV[c], CUST_TOP[c]) for c in CUST_REV],
    "ok_rev": [(c, v) for c, v in OK_REV.items()],
    "qb_shadow": [(t, v) for t, v in AMOUNT_BY_TIER.items()],
    "bcast_qb": [(s, v) for s, v in AMOUNT_BY_STATUS.items()],
    "jt_qb": [(t, v) for t, v in AMOUNT_BY_TIER.items()],
}


@pytest.mark.parametrize("name", list(_BY_NAME))
async def test_query_backed_model_by_name(engine, name) -> None:
    with pytest.warns(UserWarning) if name == "bcast_qb" else nullcontext():
        resp = await engine.execute(name)
    assert _values(resp.data) == sorted(_BY_NAME[name], key=repr)


async def test_monthly_by_name(engine) -> None:
    resp = await engine.execute("monthly")
    assert len(resp.data) == 7
    assert sum(r[resp.columns[-1]] for r in resp.data) == 145.0


@pytest.mark.parametrize("name", list(EMBEDDED_WITH))
async def test_embedded_with_model(engine, name) -> None:
    resp = await engine.execute(query(
        source_model=name, dimensions=["customer_id"], measures=[m("amount:sum", "a")]))
    assert _values(resp.data) == sorted(EMBEDDED_WITH[name][1].items(), key=repr)



@pytest.mark.parametrize(("name", "measure", "total"), [
    ("orders", "amount:sum", 145.0),
    ("customers", "spend:sum", 430.0),
    ("clients", "spend:sum", 430.0),
])
async def test_storage_model(engine, name, measure, total) -> None:
    resp = await engine.execute(query(source_model=name, measures=[m(measure, "t")]))
    assert resp.data == [{f"{name}.t": total}]
