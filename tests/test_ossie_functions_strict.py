"""The strict binding admits exactly the Ossie functions that run (spec: queries/scalar-functions › The strict binding
admits exactly the Ossie functions that run)."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest

from slayer.core.errors import SlayerError
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2217_fixtures import rows, strict, strict_error
from tests._dev2446_strict_fixtures import counting_engine

#: Admitted filter → rows of ``tests._dev2215_fixtures``'s orders it keeps, strict and native alike.
ADMITTED = {
    "IF(orders.amount > 20, 1, 0) = 1": 3,
    "NVL(orders.note, 'x') = 'x'": 1,
    "TRUNC(orders.amount, 1) > 0": 5,
    "TO_DATE('2024-01-15') < orders.order_date": 6,
    "EXTRACT(MILLISECOND FROM orders.created_at) = 0": 6,
    "orders.created_at > TIMESTAMP_NTZ '2024-01-01 00:00:00'": 6,
    "IFF(orders.amount > 20, 1, 0) = 1": 3,
    "NVL2(orders.note, 1, 0) = 0": 1,
    "ZEROIFNULL(orders.amount) = 0": 1,
    "NULLIFZERO(orders.weight) IS NULL": 1,
    "STARTSWITH(orders.status, 'pa')": 4,
    "ENDSWITH(orders.status, 'id')": 5,
    "CHARINDEX('i', orders.status) = 3": 5,
    "TRUNCATE(orders.amount, -1) = 30": 1,
    "TO_TIMESTAMP('2025-01-01 00:00:00') < orders.created_at": 3,
    "RADIANS(orders.amount) > 1": 1,
    "DEGREES(orders.amount) > 2000": 2,
    "REGEXP_COUNT(orders.status, 'a') = 2": 0,
    "REGEXP_COUNT(orders.status, 'a') = 1": 4,
    "EXTRACT(DAYOFWEEK FROM orders.created_at) = 1": 3,
    "DATE_PART('dayofweek', orders.created_at) = 2": 1,
    "DATE_PART('millisecond', orders.created_at) = 0": 6,
}

#: Refused under strict → whether the default binding answers it (SLayer syntax).
REFUSED = {
    "TRUNC(orders.amount) = 0": True,
    "TRUNCATE(orders.amount) = 0": True,
    "TRUNCATE(orders.amount, 1, 2) = 0": False,
    "CHARINDEX('d', orders.status, 2) = 0": False,
    "REGEXP_COUNT(orders.status, 'n', 'i') = 2": True,
    "starts_with(orders.status, 'p')": True,
    "DAYOFWEEK(orders.created_at) = 1": False,
    "MILLISECOND(orders.created_at) = 0": False,
    "TO_DATE('2024-01-15', 'YYYY-MM-DD') < orders.order_date": False,
}


@pytest.fixture(scope="module")
async def engine() -> AsyncGenerator[SlayerQueryEngine]:
    async with counting_engine("duckdb") as e:
        yield e


def _query(filter_text: str) -> dict[str, object]:
    return {"metrics": ["orders.n"], "filter": filter_text}


@pytest.mark.parametrize("filter_text", sorted(ADMITTED))
async def test_admitted_at_ossie_counts(engine: SlayerQueryEngine, filter_text: str) -> None:
    expected = [(float(ADMITTED[filter_text]),)]
    assert rows(await strict(engine, _query(filter_text)), "n") == expected
    assert rows(await engine.execute(_query(filter_text)), "orders.n") == expected


@pytest.mark.parametrize("filter_text", sorted(REFUSED))
async def test_other_arities_and_spellings_refused(engine: SlayerQueryEngine, filter_text: str) -> None:
    await strict_error(engine, _query(filter_text), "E_INVALID_QUERY")
    if REFUSED[filter_text]:
        await engine.execute(_query(filter_text))


@pytest.mark.parametrize("filter_text", sorted(f for f, native in REFUSED.items() if not native))
async def test_other_arities_refused_natively(engine: SlayerQueryEngine, filter_text: str) -> None:
    query = _query(filter_text)
    with pytest.raises((SlayerError, ValueError)):
        await engine.execute(query)
