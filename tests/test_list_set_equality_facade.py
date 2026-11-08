"""BI facades group a list by its element set and advertise its distinct count (spec: facade/projection,
facade/list-values)."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from collections.abc import Iterator
from typing import Any, Optional

import pytest

from tests._dev2120_fixtures import CONTRACTORS
from tests._dev2172_fixtures import FacadeStore, Servers, build_store, flight_query, pg_query, serve

#: Fay and Gil: Ann's skills reordered without the repeat, and a list holding only NULL.
EXTRA_ROWS = ({"id": 6, "name": "Fay", "team": "C", "skills": ["sql", "go"]},
              {"id": 7, "name": "Gil", "team": "C", "skills": [None]})
EXTRA = ("INSERT INTO contractors (id, name, team, rate, skills) VALUES "
         "(6, 'Fay', 'C', 60, ['sql', 'go']::VARCHAR[]), (7, 'Gil', 'C', 70, [NULL]::VARCHAR[])",)
ROWS = [*({k: c[k] for k in ("id", "name", "team", "skills")} for c in CONTRACTORS), *EXTRA_ROWS]


def elset(value: Optional[list[Any]]) -> Optional[list[Any]]:
    return None if value is None else sorted({e for e in value if e is not None})


SETS = {r["id"]: elset(r["skills"]) for r in ROWS}


def _key(value: Optional[list[Any]]) -> Optional[tuple[Any, ...]]:
    return None if value is None else tuple(value)


GROUPS = Counter(_key(s) for s in SETS.values())


@pytest.fixture(scope="module")
def store(tmp_path_factory: pytest.TempPathFactory) -> FacadeStore:
    return asyncio.run(build_store(tmp_path_factory.mktemp("list-set-facade"), extra=EXTRA))


@pytest.fixture(scope="module")
def servers(store: FacadeStore) -> Iterator[Servers]:
    with serve(store) as running:
        yield running


def _pg(servers: Servers, sql: str) -> list[tuple[Any, ...]]:
    return pg_query(servers.pg_port, sql).rows


def _flight(servers: Servers, sql: str) -> list[tuple[Any, ...]]:
    return [tuple(r.values()) for r in flight_query(servers.flight_port, sql).to_pylist()]


WIRES = {"pg": _pg, "flight": _flight}


def _list(text: Optional[str]) -> Optional[list[Any]]:
    return None if text is None else json.loads(text)


def test_the_fixture_merges_sets() -> None:
    assert GROUPS == {("go", "sql"): 2, ("sql",): 1, (): 2, ("go",): 1, None: 1}


@pytest.mark.parametrize("wire", WIRES)
class TestGroupedPreviews:
    def test_preview_with_a_saved_measure(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT id, skills, row_count FROM contractors")
        assert {int(r[0]): (_list(r[1]), int(r[2])) for r in rows} == {i: (s, 1) for i, s in SETS.items()}
        assert len(rows) == len(ROWS)

    def test_grouping_on_a_list(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT skills, COUNT(*) FROM contractors GROUP BY skills")
        assert {_key(_list(r[0])): int(r[1]) for r in rows} == GROUPS
        assert len(rows) == len(GROUPS)

    def test_distinct_lists(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT DISTINCT skills FROM contractors")
        assert Counter(_key(_list(r[0])) for r in rows) == Counter(set(GROUPS))

    def test_ordering_by_a_scalar_beside_a_grouped_list(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT team, skills, COUNT(*) FROM contractors GROUP BY team, skills "
                                    "ORDER BY team")
        assert [r[0] for r in rows] == sorted(r[0] for r in rows)
        assert {(r[0], _key(_list(r[1]))): int(r[2]) for r in rows} == Counter(
            (r["team"], _key(SETS[r["id"]])) for r in ROWS)

    @pytest.mark.parametrize("sql", [
        "SELECT skills, COUNT(*) FROM contractors GROUP BY skills ORDER BY skills",
        "SELECT skills AS s, COUNT(*) FROM contractors GROUP BY skills ORDER BY s",
        "SELECT skills, COUNT(*) FROM contractors GROUP BY skills ORDER BY 1",
    ])
    def test_ordering_by_the_list_is_refused(self, servers: Servers, wire: str, sql: str) -> None:
        with pytest.raises(Exception, match=r"(?i)list.*order|order.*list"):
            WIRES[wire](servers, sql)

    def test_distinct_count_metric(self, servers: Servers, wire: str) -> None:
        rows = WIRES[wire](servers, "SELECT skills_count_distinct FROM contractors")
        assert [int(r[0]) for r in rows] == [len(set(GROUPS) - {None})]

    def test_metrics_listed(self, servers: Servers, wire: str) -> None:
        sql = "SELECT * FROM INFORMATION_SCHEMA.METRICS"
        if wire == "pg":
            result = pg_query(servers.pg_port, sql)
            rows = [dict(zip(result.columns, r)) for r in result.rows]
        else:
            rows = flight_query(servers.flight_port, sql).to_pylist()
        listed = {r["metric_name"] for r in rows
                  if r["table_name"] == "contractors" and r["metric_name"].startswith("skills_")}
        assert listed == {"skills_count", "skills_count_distinct"}
