"""Time zones executed on a throwaway Postgres under an ``Asia/Kolkata`` session (spec: queries/time-zones ›
Executed on Postgres; DST resolves by the Postgres rule; Converted time bounds stay prunable)."""

from __future__ import annotations

from collections import Counter
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest

from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2000_fixtures import (
    DS,
    DST_NAIVE,
    HOURLY_COUNT,
    HOURLY_START,
    NY,
    ROWS,
    Table,
    amount,
    by_id,
    day,
    default_datasets,
    default_tables,
    local,
    m,
    naive,
    q,
    to_zone,
)
from tests._pg_helpers import pg_database, pg_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

pg_proc = factories.postgresql_proc(port=None, postgres_options="-c unix_socket_directories=")

SESSION_ZONE = "Asia/Kolkata"
_PG_TYPES = {"INT": "INTEGER", "DOUBLE": "DOUBLE PRECISION", "TEXT": "TEXT", "DATE": "DATE",
             "TIMESTAMP": "TIMESTAMP", "TIMESTAMPTZ": "TIMESTAMPTZ"}


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def _table_statements(table: Table) -> list[str]:
    columns = ", ".join(f"{c} {_PG_TYPES[k]}" for c, k in table.columns)
    rows = ", ".join("(" + ", ".join(_literal(v) for v in row) + ")" for row in table.rows)
    return [f"CREATE TABLE {table.name} ({columns})", f"INSERT INTO {table.name} VALUES {rows}"]


def _statements() -> list[str]:
    session = (f"DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), "
               f"'{SESSION_ZONE}'); END $$")
    return [session, *(s for t in default_tables() for s in _table_statements(t))]


@pytest.fixture
async def pg(pg_proc: Any, tmp_path: Path) -> AsyncIterator[SlayerQueryEngine]:
    with pg_database(pg_proc, statements=_statements(), name=DS) as ds:
        with psycopg.connect(host=ds.host, port=ds.port, user=ds.username, dbname=ds.database) as conn:
            row = conn.execute("SHOW timezone").fetchone()
        assert row is not None
        assert row[0] == SESSION_ZONE
        zoned = DatasourceConfig.model_validate({**ds.model_dump(), "time_zone": "UTC"})
        async with pg_query_engine(zoned, base_dir=str(tmp_path / "store"), datasets=default_datasets()) as engine:
            yield engine


@pytest.mark.parametrize("field", ["ordered_at", "paid_at"])
async def test_local_day_bucket(pg: SlayerQueryEngine, field: str) -> None:
    resp = await pg.execute(q(
        time_zone=NY, metrics=[m("sum(orders.amount)", "s")],
        time_dimensions=[{"field": f"orders.{field}", "granularity": "day"}],
    ))
    got = {day(r[f"orders.{field}"]): float(r["s"]) for r in resp.data}
    assert got == {local(i, NY).date(): amount(i) for i in ROWS}, resp.sql


async def test_utc_timestamp_near_midnight_lands_in_the_local_day(pg: SlayerQueryEngine) -> None:
    resp = await pg.execute(q(time_zone=NY, dimensions=["orders.id"], filter=["orders.ordered_at in '2024-12-31'"]))
    assert {int(r["orders.id"]) for r in resp.data} == {1}, resp.sql


async def test_skipped_and_repeated_local_times(pg: SlayerQueryEngine) -> None:
    resp = await pg.execute(SlayerQuery.model_validate({
        "population": "dst", "time_zone": "UTC", "dimensions": ["dst.id", "dst.local_at"],
    }))
    got = {i: naive(v) for i, v in by_id(resp, "dst.local_at", id_key="dst.id").items()}
    expected = {i: to_zone(naive(text), "UTC", source=NY) for i, text in DST_NAIVE.items()}
    assert got == expected, resp.sql
    assert {str(v) for v in got.values()} == {"2025-03-09 07:30:00", "2024-11-03 06:30:00"}


async def test_a_25_hour_local_day(pg: SlayerQueryEngine) -> None:
    resp = await pg.execute(SlayerQuery.model_validate({
        "population": "hourly", "time_zone": NY, "metrics": [m("count(hourly.*)", "n")],
        "time_dimensions": [{"field": "hourly.ts", "granularity": "day"}],
    }))
    got = {day(r["hourly.ts"]): int(r["n"]) for r in resp.data}
    expected = Counter(to_zone(HOURLY_START + timedelta(hours=h), NY).date() for h in range(HOURLY_COUNT))
    assert got == dict(expected), resp.sql
    assert got[naive("2024-11-03").date()] == 25


@pytest.mark.parametrize("field", ["ordered_at", "paid_at"])
async def test_widened_bounds_keep_the_result(pg: SlayerQueryEngine, field: str) -> None:
    resp = await pg.execute(q(
        time_zone=NY, dimensions=["orders.id"],
        filter=[f"orders.{field} >= '2025-01-01'", f"orders.{field} < '2025-02-01'"],
    ))
    assert {int(r["orders.id"]) for r in resp.data} == {2, 3}, resp.sql
    assert resp.sql is not None
    assert "2024-12-30" in resp.sql, resp.sql
    assert "2025-02-03" in resp.sql, resp.sql
