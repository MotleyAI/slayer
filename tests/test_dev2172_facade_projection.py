"""A plain facade projection returns one row per record; DISTINCT, GROUP BY, HAVING, aggregates and measure
references group (spec: facade/projection)."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from collections import Counter
from collections.abc import Iterator
from typing import Any

import pytest

from slayer.facade.catalog import FacadeCatalog, build_catalog
from slayer.facade.translator import QueryResult, translate
from tests._dev2120_fixtures import CONTRACTORS
from tests._dev2172_fixtures import (
    DS,
    ORDERS,
    STATUSES,
    FacadeStore,
    Servers,
    build_store,
    flight_query,
    pg_query,
    serve,
)

MONTHS = Counter((int(t[:4]), int(t[5:7])) for _, _, _, t in ORDERS)


@pytest.fixture(scope="module")
def store(tmp_path_factory: pytest.TempPathFactory) -> FacadeStore:
    return asyncio.run(build_store(tmp_path_factory.mktemp("dev2172-projection")))


@pytest.fixture(scope="module")
def servers(store: FacadeStore) -> Iterator[Servers]:
    with serve(store) as running:
        yield running


@pytest.fixture(scope="module")
def catalog(store: FacadeStore) -> FacadeCatalog:
    async def load() -> FacadeCatalog:
        names = await store.storage.list_models(data_source=DS)
        models = [m for n in names if (m := await store.storage.get_model(n, data_source=DS)) is not None]
        return build_catalog(models_by_datasource={DS: models})
    return asyncio.run(load())


def _month(value: Any) -> tuple[int, int]:
    assert isinstance(value, (dt.date, dt.datetime)), value
    return value.year, value.month


def _pg(servers: Servers, sql: str) -> list[tuple[Any, ...]]:
    return pg_query(servers.pg_port, sql).rows


def _flight(servers: Servers, sql: str) -> list[tuple[Any, ...]]:
    table = flight_query(servers.flight_port, sql)
    return [tuple(r.values()) for r in table.to_pylist()]


WIRES = {"pg": _pg, "flight": _flight}


# --------------------------------------------------------------------------- #
# Raw or grouped: the translator's classification
# --------------------------------------------------------------------------- #
TRUNC = "DATE_TRUNC('month', ordered_at)"
#: ``month(col)`` parses only without the Postgres dialect (which reads it as ``EXTRACT``).
BOTH, FLIGHT_ONLY = (None, "postgres"), (None,)
RAW = [
    ("SELECT status FROM orders", BOTH),
    ("SELECT status, ordered_at FROM orders WHERE status = 'paid'", BOTH),
    ("SELECT status FROM orders ORDER BY status LIMIT 3 OFFSET 2", BOTH),
    (f"SELECT {TRUNC} FROM orders", BOTH),
    (f"SELECT {TRUNC} AS m FROM orders ORDER BY m", BOTH),
    ("SELECT month(ordered_at) FROM orders", FLIGHT_ONLY),
    ("SELECT month(ordered_at) AS m FROM orders ORDER BY m", FLIGHT_ONLY),
    ("SELECT name, skills FROM contractors", BOTH),
]
GROUPED = [
    ("SELECT DISTINCT status FROM orders", BOTH),
    ("SELECT status FROM orders GROUP BY status", BOTH),
    ("SELECT status FROM orders GROUP BY status HAVING COUNT(*) > 2", BOTH),
    ("SELECT status, COUNT(*) FROM orders", BOTH),
    ("SELECT status, revenue_sum FROM orders", BOTH),
    ("SELECT status, revenue_total FROM orders", BOTH),
    ("SELECT status FROM orders WHERE revenue_sum > 150", BOTH),
    (f"SELECT DISTINCT {TRUNC} FROM orders", BOTH),
    (f"SELECT {TRUNC} FROM orders GROUP BY {TRUNC}", BOTH),
    ("SELECT DISTINCT month(ordered_at) FROM orders", FLIGHT_ONLY),
]


def _cases(entries: list[tuple[str, tuple[str | None, ...]]]) -> list[tuple[str, str | None]]:
    return [(sql, d) for sql, dialects in entries for d in dialects]


@pytest.mark.parametrize(("sql", "dialect"), _cases(RAW))
def test_plain_projection_is_raw_rows(catalog: FacadeCatalog, sql: str, dialect: str | None) -> None:
    result = translate(sql, catalog, dialect=dialect)
    assert isinstance(result, QueryResult), result
    assert result.query.distinct_dimension_values is False


@pytest.mark.parametrize(("sql", "dialect"), _cases(GROUPED))
def test_grouping_forms_stay_grouped(catalog: FacadeCatalog, sql: str, dialect: str | None) -> None:
    result = translate(sql, catalog, dialect=dialect)
    assert isinstance(result, QueryResult), result
    assert result.query.distinct_dimension_values is True


# --------------------------------------------------------------------------- #
# Executed over both wires
# --------------------------------------------------------------------------- #
#: ``(wire, grain)``: ``month(col)`` only over Flight (the Postgres dialect reads it as ``EXTRACT``).
GRAINS = [("pg", TRUNC), ("flight", TRUNC), ("flight", "month(ordered_at)")]


@pytest.mark.parametrize(("wire", "grain"), GRAINS)
@pytest.mark.parametrize("alias", ["", " AS m"])
def test_time_grain_without_an_aggregate_is_one_value_per_record(servers: Servers, wire: str, grain: str,
                                                                alias: str) -> None:
    rows = WIRES[wire](servers, f"SELECT {grain}{alias} FROM orders")
    assert Counter(_month(r[0]) for r in rows) == MONTHS


@pytest.mark.parametrize(("wire", "grain"), GRAINS)
@pytest.mark.parametrize("form", ["SELECT DISTINCT {g} FROM orders", "SELECT {g} FROM orders GROUP BY {g}"])
def test_time_grain_grouped_is_one_value_per_bucket(servers: Servers, wire: str, grain: str, form: str) -> None:
    rows = WIRES[wire](servers, form.format(g=grain))
    assert sorted(_month(r[0]) for r in rows) == sorted(MONTHS)


@pytest.mark.parametrize("wire", WIRES)
class TestRowCounts:
    def test_plain_projection_keeps_duplicates(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT status FROM orders")
        assert Counter(r[0] for r in rows) == Counter(s for _, s, _, _ in ORDERS)

    @pytest.mark.parametrize("sql", ["SELECT DISTINCT status FROM orders",
                                     "SELECT status FROM orders GROUP BY status"])
    def test_distinct_and_group_by_group(self, servers: Servers, wire: str, sql: str) -> None:
        rows = WIRES[wire](servers, sql)
        assert sorted(r[0] for r in rows) == sorted(STATUSES)

    def test_having_groups(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT status FROM orders GROUP BY status HAVING COUNT(*) > 2")
        counts = Counter(s for _, s, _, _ in ORDERS)
        assert sorted(r[0] for r in rows) == sorted(s for s, n in counts.items() if n > 2)

    def test_aggregate_in_select_groups(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT status, COUNT(*) FROM orders")
        assert {r[0]: r[1] for r in rows} == dict(Counter(s for _, s, _, _ in ORDERS))

    def test_measure_in_where_groups_and_filters_the_aggregate(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT status FROM orders WHERE revenue_sum > 150")
        sums = Counter[str]()
        for _, s, r, _ in ORDERS:
            sums[s] += int(r)
        assert sorted(r[0] for r in rows) == sorted(s for s, total in sums.items() if total > 150)

    def test_duplicate_timestamps_are_kept(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT ordered_at FROM orders")
        assert len(rows) == len(ORDERS)

    def test_ordered_and_paged_time_grain(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, f"SELECT {TRUNC} AS m FROM orders ORDER BY m LIMIT 3 OFFSET 4")
        per_record = sorted(m for m, n in MONTHS.items() for _ in range(n))
        assert [_month(r[0]) for r in rows] == per_record[4:7]

    def test_ordering_and_paging_a_plain_projection_with_a_list(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT name, skills FROM contractors ORDER BY name LIMIT 2 OFFSET 1")
        expected = sorted(CONTRACTORS, key=lambda c: c["name"])[1:3]
        assert [r[0] for r in rows] == [c["name"] for c in expected]
        assert [None if r[1] is None else json.loads(r[1]) for r in rows] == [c["skills"] for c in expected]

    def test_bi_table_preview_of_a_model_with_a_list(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT id, name, skills FROM contractors")
        got = {r[0]: (r[1], None if r[2] is None else json.loads(r[2])) for r in rows}
        assert got == {c["id"]: (c["name"], c["skills"]) for c in CONTRACTORS}
        assert len(rows) == len(CONTRACTORS)

    @pytest.mark.parametrize("sql", ["SELECT skills, COUNT(*) FROM contractors GROUP BY skills",
                                     "SELECT DISTINCT skills FROM contractors"])
    def test_grouping_on_a_list_names_its_elements(self, servers: Servers, wire: str, sql: str) -> None:
        with pytest.raises(Exception, match=r"skills\.value"):
            WIRES[wire](servers, sql)
