"""Facade ``MONTH(col)`` and its siblings, ``EXTRACT`` and ``date_part`` project the integer extraction;
``DATE_TRUNC`` keeps the bucket (specs: facade/projection, facade/time-filter-translation)."""

from __future__ import annotations

import asyncio
import datetime as dt
from collections import Counter
from collections.abc import Iterator
from typing import Any

import pytest

from tests._dev2172_fixtures import FacadeStore, Servers, build_store, flight_query, pg_query, serve

#: ``sales`` rows: March in two years, plus the 2024-12-30 10:15:42 extraction row.
SALES = (
    (1, "2024-03-05 08:00:00"), (2, "2024-04-10 09:30:00"), (3, "2024-12-30 10:15:42"),
    (4, "2025-03-17 12:00:00"), (5, "2025-03-28 23:00:00"), (6, "2025-06-02 07:45:00"),
)
STAMPS = [dt.datetime.fromisoformat(t) for _, t in SALES]
EXTRA = [
    "CREATE TABLE sales (id INTEGER PRIMARY KEY, ordered_at TIMESTAMP)",
    "INSERT INTO sales VALUES " + ", ".join(f"({i}, '{t}')" for i, t in SALES),
]
#: SQL function → Python extraction.
PARTS = {
    "YEAR": lambda t: t.year, "QUARTER": lambda t: (t.month - 1) // 3 + 1, "MONTH": lambda t: t.month,
    "WEEK": lambda t: t.isocalendar()[1], "DAY": lambda t: t.day, "HOUR": lambda t: t.hour,
    "MINUTE": lambda t: t.minute, "SECOND": lambda t: t.second,
}
MONTH_SPELLINGS = ["MONTH(ordered_at)", "month(ordered_at)", "EXTRACT(MONTH FROM ordered_at)",
                   "date_part('month', ordered_at)"]


@pytest.fixture(scope="module")
def store(tmp_path_factory: pytest.TempPathFactory) -> FacadeStore:
    return asyncio.run(build_store(tmp_path_factory.mktemp("dev2215-facade"), extra=EXTRA))


@pytest.fixture(scope="module")
def servers(store: FacadeStore) -> Iterator[Servers]:
    with serve(store) as running:
        yield running


def _pg(servers: Servers, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    result = pg_query(servers.pg_port, sql)
    return result.columns, result.rows


def _flight(servers: Servers, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    table = flight_query(servers.flight_port, sql)
    return table.column_names, [tuple(r.values()) for r in table.to_pylist()]


WIRES = {"pg": _pg, "flight": _flight}


def _ints(rows: list[tuple[Any, ...]]) -> list[tuple[int, ...]]:
    return [tuple(int(v) for v in r) for r in rows]


@pytest.mark.parametrize("wire", WIRES)
class TestExtractionProjection:
    @pytest.mark.parametrize("spelling", MONTH_SPELLINGS)
    def test_month_of_year(self, servers: Servers, wire: str, spelling: str) -> None:
        _, rows = WIRES[wire](servers, f"SELECT {spelling}, COUNT(*) FROM sales GROUP BY 1 ORDER BY 1")
        assert _ints(rows) == [(3, 3), (4, 1), (6, 1), (12, 1)]

    def test_spellings_share_one_column_name(self, servers: Servers, wire: str) -> None:
        names = {tuple(WIRES[wire](servers, f"SELECT {s}, COUNT(*) FROM sales GROUP BY 1")[0]) for s in MONTH_SPELLINGS}
        assert len(names) == 1, names

    @pytest.mark.parametrize("part", PARTS)
    def test_every_part(self, servers: Servers, wire: str, part: str) -> None:
        _, rows = WIRES[wire](servers, f"SELECT {part}(ordered_at), COUNT(*) FROM sales GROUP BY 1")
        assert {r[0]: r[1] for r in _ints(rows)} == dict(Counter(PARTS[part](t) for t in STAMPS))

    def test_order_by_alias(self, servers: Servers, wire: str) -> None:
        _, rows = WIRES[wire](servers, "SELECT MONTH(ordered_at) AS m, COUNT(*) FROM sales GROUP BY m ORDER BY m DESC")
        assert [r[0] for r in _ints(rows)] == [12, 6, 4, 3]

    def test_plain_projection_is_one_value_per_record(self, servers: Servers, wire: str) -> None:
        _, rows = WIRES[wire](servers, "SELECT MONTH(ordered_at) FROM sales")
        assert sorted(r[0] for r in _ints(rows)) == sorted(t.month for t in STAMPS)
        _, distinct = WIRES[wire](servers, "SELECT DISTINCT MONTH(ordered_at) FROM sales")
        assert sorted(r[0] for r in _ints(distinct)) == [3, 4, 6, 12]

    def test_date_trunc_keeps_the_bucket(self, servers: Servers, wire: str) -> None:
        _, rows = WIRES[wire](servers, "SELECT DATE_TRUNC('month', ordered_at), COUNT(*) FROM sales GROUP BY 1")
        assert sorted((r[0].year, r[0].month, int(r[1])) for r in rows) == [
            (2024, 3, 1), (2024, 4, 1), (2024, 12, 1), (2025, 3, 2), (2025, 6, 1)]

    def test_extraction_in_where(self, servers: Servers, wire: str) -> None:
        _, rows = WIRES[wire](servers, "SELECT COUNT(*) FROM sales WHERE MONTH(ordered_at) = 3")
        assert _ints(rows) == [(3,)]


@pytest.mark.parametrize("wire", WIRES)
def test_one_sided_time_filter_with_a_date_trunc_projection(servers: Servers, wire: str) -> None:
    _, rows = WIRES[wire](servers, "SELECT date_trunc('month', ordered_at), COUNT(*) FROM sales "
                                   "WHERE ordered_at >= '2025-01-01' GROUP BY 1")
    assert sorted((r[0].year, r[0].month, int(r[1])) for r in rows) == [(2025, 3, 2), (2025, 6, 1)]
