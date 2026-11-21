"""Shared seeds, models, oracles and checks for one logical bucket type and DATE-meets-TIMESTAMP as midnight."""

from __future__ import annotations

import contextlib
import re
from collections.abc import AsyncGenerator, Iterable
from datetime import date, datetime, time, timedelta
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1737_fixtures import SERVER_TYPES, TableSpec, seed_backend, seed_statements, server_table_statements
from tests._engine_helpers import seeded_exec_engine
from tests._pg_helpers import pg_database, pg_query_engine
from tests._time_points_fixtures import PinnedClock

IN_PROCESS = ("sqlite", "duckdb")
WITH_POSTGRES = ("sqlite", "duckdb", "postgres")
PG_SESSION_ZONE = "America/Sao_Paulo"

GRANULARITIES: list[dict[str, Any]] = [
    {"name": "fiscal_year", "base": "month", "multiple": 12, "origin": "2000-04-01"},
    {"name": "quarter_hour", "base": "minute", "multiple": 15},
]
DS_GRANULARITIES = {"granularities": GRANULARITIES}
BUILT_IN = ("second", "minute", "hour", "day", "week", "week_sunday", "month", "quarter", "year")
SUB_DAY = frozenset({"second", "minute", "hour", "quarter_hour"})
ALL_UNITS = (*BUILT_IN, "fiscal_year", "quarter_hour")

# ---------------------------------------------------------------------------
# Seeds
# ---------------------------------------------------------------------------

# Bucket matrix: a Sunday, a Monday midnight, a fiscal-year boundary, sub-second time, NULLs.
BK = TableSpec(
    name="bk",
    columns=[("id", "INT"), ("d", "DATE"), ("ts", "TIMESTAMP"), ("amount", "DOUBLE")],
    rows=[
        (1, "2024-06-15", "2024-06-15 10:37:21", 1.0),
        (2, "2024-12-29", "2024-12-29 23:59:59", 2.0),
        (3, "2024-12-30", "2024-12-30 00:00:00", 3.0),
        (4, "2025-03-31", "2025-04-01 00:07:00", 4.0),
        (5, "2025-04-01", "2025-03-31 23:52:30", 5.0),
        (6, None, None, 6.0),
    ],
)

# DATE meeting TIMESTAMP: midnight, same day, next day, previous evening, NULLs.
MX = TableSpec(
    name="mx",
    columns=[("id", "INT"), ("d", "DATE"), ("ts", "TIMESTAMP"), ("shipped_at", "TIMESTAMP")],
    rows=[
        (1, "2025-03-01", "2025-03-01 00:00:00", None),
        (2, "2025-03-01", "2025-03-01 10:00:00", "2025-03-02 00:00:00"),
        (3, "2025-03-01", "2025-03-02 00:00:00", None),
        (4, "2025-03-02", "2025-03-01 23:00:00", "2025-03-01 23:00:00"),
        (5, None, "2025-03-01 00:00:00", None),
        (6, "2025-03-03", None, "2025-03-03 00:00:00"),
        (7, None, None, None),
    ],
)

# SQLite storage spellings of one instant pair per row; ``ts2`` spells the same instant differently.
SP = TableSpec(
    name="sp",
    columns=[("id", "INT"), ("d", "DATE"), ("ts", "TIMESTAMP"), ("ts2", "TIMESTAMP")],
    rows=[
        (1, "2025-03-01", "2025-03-01T00:00:00", "2025-03-01 00:00:00"),
        (2, "2025-03-01", "2025-03-01 00:00:00", "2025-03-01T00:00:00"),
        (3, "2025-03-01", "2025-03-01", "2025-03-01 00:00:00"),
        (4, "2025-03-01", "2025-03-01 10:00:00.250", "2025-03-01T10:00:00.250"),
        (5, "2025-03-01", "2025-03-01T10:00:00", "2025-03-01 10:00:00"),
        (6, "2025-03-01", "garbage", "2025-03-01"),
        (7, "2025-03-01", "2025-03-01 10:00:00", "2025-03-01T10:00:00.000"),
    ],
)
SP_VALID = frozenset({1, 2, 3, 4, 5, 7})
SP_MIDNIGHT = frozenset({1, 2, 3})

# DATE / TIMESTAMP parity: ``ts`` is midnight of ``d``; month-ends, a Sunday/Monday pair, a gap month.
PX_DAYS = [
    "2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-15", "2024-05-16",
    "2024-05-17", "2024-07-01", "2024-12-29", "2024-12-30", "2025-01-05", "2025-01-06",
]
PX = TableSpec(
    name="px",
    columns=[("id", "INT"), ("d", "DATE"), ("ts", "TIMESTAMP"), ("amount", "DOUBLE")],
    rows=[(i + 1, day, f"{day} 00:00:00", float(i + 1)) for i, day in enumerate(PX_DAYS)],
)

TABLES = [BK, MX, SP, PX]


def _fields(*spec: tuple[str, DataType]) -> list[DatasetField]:
    return [DatasetField(name=n, datatype=t, primary_key=(n == "id")) for n, t in spec]


def bk_dataset(*, data_source: str = "test") -> SlayerDataset:
    return SlayerDataset(
        name="bk", sql_table="bk", data_source=data_source, default_time_dimension="ts",
        fields=_fields(("id", DataType.INTEGER), ("d", DataType.DATE), ("ts", DataType.DATETIME),
                      ("amount", DataType.FLOAT)),
    )


def mx_dataset(*, data_source: str = "test") -> SlayerDataset:
    return SlayerDataset(
        name="mx", sql_table="mx", data_source=data_source, default_time_dimension="ts",
        fields=_fields(("id", DataType.INTEGER), ("d", DataType.DATE), ("ts", DataType.DATETIME),
                      ("shipped_at", DataType.DATETIME)),
    )


def sp_dataset(*, data_source: str = "test") -> SlayerDataset:
    return SlayerDataset(
        name="sp", sql_table="sp", data_source=data_source, default_time_dimension="ts",
        fields=_fields(("id", DataType.INTEGER), ("d", DataType.DATE), ("ts", DataType.DATETIME),
                      ("ts2", DataType.DATETIME)),
    )


def px_dataset(*, data_source: str = "test", axis: str = "d", name: str = "px") -> SlayerDataset:
    return SlayerDataset(
        name=name, sql_table="px", data_source=data_source, default_time_dimension=axis,
        fields=_fields(("id", DataType.INTEGER), ("d", DataType.DATE), ("ts", DataType.DATETIME),
                      ("amount", DataType.FLOAT)),
    )


def all_datasets(*, data_source: str = "test") -> list[SlayerDataset]:
    return [
        bk_dataset(data_source=data_source), mx_dataset(data_source=data_source), sp_dataset(data_source=data_source),
        px_dataset(data_source=data_source), px_dataset(data_source=data_source, axis="ts", name="px_ts"),
    ]


# ---------------------------------------------------------------------------
# Engines
# ---------------------------------------------------------------------------

SNOWFLAKE_TYPES = {
    "INT": "NUMBER(38,0)", "DOUBLE": "FLOAT", "TEXT": "VARCHAR", "BOOLEAN": "BOOLEAN",
    "DATE": "DATE", "TIMESTAMP": "TIMESTAMP_NTZ(6)",
}
SERVER_TABLES = [BK, MX]


def server_statements(backend: str, *, tables: Iterable[TableSpec] = SERVER_TABLES) -> list[str]:
    """CREATE + INSERT for ``tables`` on a server ``backend`` (``postgres``, the integration servers, ``snowflake``)."""
    out: list[str] = []
    for table in tables:
        if backend == "snowflake":
            out += seed_statements(table, types=SNOWFLAKE_TYPES)
        elif backend == "postgres":
            out += seed_statements(table, types=SERVER_TYPES[backend])
        else:
            out += server_table_statements(table, backend=backend)
    return out


def with_granularities(cfg: DatasourceConfig, *, name: Optional[str] = None) -> DatasourceConfig:
    return DatasourceConfig.model_validate({**cfg.model_dump(), "name": name or cfg.name, **DS_GRANULARITIES})


def _pg_zone_statement() -> str:
    return (
        "DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), "
        f"'{PG_SESSION_ZONE}'); END $$"
    )


@contextlib.asynccontextmanager
async def lt_engine(
    backend: str, *, request: Optional[pytest.FixtureRequest] = None, clock: Optional[PinnedClock] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine over every seed, the datasource defining the custom granularities.

    ``postgres`` runs under a non-UTC session time zone and needs ``request`` for ``postgresql_proc``.
    """
    clock = clock if clock is not None else PinnedClock()
    if backend == "postgres":
        assert request is not None
        info = request.getfixturevalue("postgresql_proc")
        base_dir = str(request.getfixturevalue("tmp_path") / "store")
        tables = [BK, MX, PX]
        with pg_database(info, statements=[*server_statements("postgres", tables=tables), _pg_zone_statement()],
                         name="test") as ds:
            ds = with_granularities(ds)
            async with pg_query_engine(ds, base_dir=base_dir, datasets=all_datasets(), validate=False,
                                       clock=clock) as engine:
                yield engine
        return
    tables = TABLES if backend == "sqlite" else [BK, MX, PX]
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, tables), datasets=all_datasets(),
        clock=clock, datasource_fields=DS_GRANULARITIES,
    ) as (engine, _db):
        yield engine


# ---------------------------------------------------------------------------
# Oracle
# ---------------------------------------------------------------------------

def parse(value: Optional[str]) -> date | datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value) if len(value) > 10 else date.fromisoformat(value)


def midnight(value: date) -> datetime:
    return datetime.combine(value, time())


def bucket_type(source: DataType, unit: str) -> DataType:
    """The logical type of ``date_trunc(unit, <source>)``."""
    return DataType.DATE if source is DataType.DATE and unit not in SUB_DAY else DataType.DATETIME


def _months_back(v: datetime, *, origin_month: int, multiple: int) -> datetime:
    months = (v.year * 12 + v.month - 1) - (2000 * 12 + origin_month - 1)
    k = months // multiple * multiple
    total = 2000 * 12 + origin_month - 1 + k
    return datetime(total // 12, total % 12 + 1, 1)


def oracle_bucket(value: date | datetime | None, unit: str) -> date | datetime | None:
    """``date_trunc(unit, value)`` with the bucket's logical type."""
    if value is None:
        return None
    is_date = not isinstance(value, datetime)
    v = midnight(value) if is_date else value.replace(microsecond=0)
    day = v.replace(hour=0, minute=0, second=0)
    out = {
        "second": lambda: v,
        "minute": lambda: v.replace(second=0),
        "quarter_hour": lambda: v.replace(minute=v.minute // 15 * 15, second=0),
        "hour": lambda: v.replace(minute=0, second=0),
        "day": lambda: day,
        "week": lambda: day - timedelta(days=day.weekday()),
        "week_sunday": lambda: day - timedelta(days=(day.weekday() + 1) % 7),
        "month": lambda: day.replace(day=1),
        "quarter": lambda: day.replace(month=(day.month - 1) // 3 * 3 + 1, day=1),
        "year": lambda: day.replace(month=1, day=1),
        "fiscal_year": lambda: _months_back(day, origin_month=4, multiple=12),
    }[unit]()
    source = DataType.DATE if is_date else DataType.DATETIME
    return out.date() if bucket_type(source, unit) is DataType.DATE else out


_SQLITE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SQLITE_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def physical_type_error(value: Any, expected: DataType, *, backend: str) -> Optional[str]:
    """Why ``value`` (as ``backend`` returned it) is not a ``expected`` value, else ``None``."""
    if value is None:
        return None
    if backend == "sqlite":
        pattern = _SQLITE_DATE if expected is DataType.DATE else _SQLITE_TIMESTAMP
        return None if isinstance(value, str) and pattern.match(value) else f"{value!r} is not {pattern.pattern}"
    if expected is DataType.DATE:
        return None if type(value) is date else f"{value!r} is not a date"
    if not isinstance(value, datetime):
        return f"{value!r} is not a datetime"
    return None if value.tzinfo is None else f"{value!r} is zoned"


def as_temporal(value: Any) -> date | datetime | None:
    """A returned value as ``date`` / naive ``datetime`` (SQLite text parsed; a midnight-free date kept)."""
    if value is None or isinstance(value, date):
        return value.replace(tzinfo=None) if isinstance(value, datetime) else value
    text = str(value).replace("T", " ")
    return datetime.fromisoformat(text) if len(text) > 10 else date.fromisoformat(text)


def same_instant(actual: Any, expected: date | datetime | None) -> bool:
    got = as_temporal(actual)
    if got is None or expected is None:
        return got is None and expected is None
    return _instant(got) == _instant(expected)


def _instant(value: date | datetime) -> datetime:
    return value if isinstance(value, datetime) else midnight(value)


def check_bucket_values(
    rows: Iterable[tuple[int, Any]], *, source: DataType, unit: str, table: TableSpec, column: str, backend: str,
) -> list[str]:
    """Problems with ``(id, bucket)`` rows against the oracle: wrong instant or wrong physical type."""
    col = [c for c, _ in table.columns].index(column)
    raw = {row[0]: parse(row[col]) for row in table.rows}
    expected_type = bucket_type(source, unit)
    rows = list(rows)
    returned = sorted(i for i, _ in rows)
    problems = [] if returned == sorted(raw) else [f"{column}/{unit}: ids {returned} != {sorted(raw)}"]
    for i, got in rows:
        want = oracle_bucket(raw[i], unit)
        if not same_instant(got, want):
            problems.append(f"{column}/{unit} id={i}: {got!r} != {want!r}")
        why = physical_type_error(got, expected_type, backend=backend)
        if why is not None:
            problems.append(f"{column}/{unit} id={i}: {why} (expected {expected_type.value})")
    return problems


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

async def ids(engine: SlayerQueryEngine, *filters: str, model: str = "mx", data_source: Optional[str] = None) -> set[int]:
    resp = await engine.execute(SlayerQuery.model_validate({
        "population": model, "dimensions": [f"{model}.id"], "filter": list(filters),
    }), data_source=data_source)
    return {int(r[f"{model}.id"]) for r in resp.data}


async def by_id(engine: SlayerQueryEngine, expression: str, *, model: str = "mx",
                data_source: Optional[str] = None) -> dict[int, Any]:
    """``{id: value}`` of an expression dimension."""
    resp = await engine.execute(SlayerQuery.model_validate({
        "population": model, "dimensions": [f"{model}.id", {"field": expression, "alias": "v"}],
    }), data_source=data_source)
    return {int(r[f"{model}.id"]): r["v"] for r in resp.data}


async def bucket_rows(engine: SlayerQueryEngine, *, column: str, unit: str, position: str, model: str = "bk",
                      data_source: Optional[str] = None) -> list[tuple[int, Any]]:
    """``(id, bucket)`` rows for ``date_trunc(unit, column)`` as a time dimension or an expression dimension."""
    if position == "time_dimension":
        payload: dict[str, Any] = {
            "population": model, "dimensions": [f"{model}.id"],
            "time_dimensions": [{"field": f"{model}.{column}", "granularity": unit}],
        }
        key = f"{model}.{column}"
    else:
        payload = {
            "population": model,
            "dimensions": [f"{model}.id", {"field": f"date_trunc('{unit}', {model}.{column})", "alias": "b"}],
        }
        key = "b"
    resp = await engine.execute(SlayerQuery.model_validate(payload), data_source=data_source)
    return [(int(r[f"{model}.id"]), r[key]) for r in resp.data]


# ---------------------------------------------------------------------------
# Mixed DATE / TIMESTAMP oracle over MX
# ---------------------------------------------------------------------------

MX_ROWS = {row[0]: (parse(row[1]), parse(row[2]), parse(row[3])) for row in MX.rows}
MX_IDS = frozenset(MX_ROWS)


def mx_where(pred) -> set[int]:
    """Ids whose ``(d, ts, shipped_at)`` satisfy ``pred`` (NULL-free; ``pred`` sees ``d`` as midnight)."""
    out = set()
    for i, (d, ts, shipped) in MX_ROWS.items():
        dm = None if d is None else midnight(d)
        if pred(dm, ts, shipped):
            out.add(i)
    return out


def known(*values: Any) -> bool:
    return all(v is not None for v in values)


MIXED_FILTER_CASES: list[tuple[str, Any]] = [
    ("mx.d = mx.ts", lambda d, ts, s: known(d, ts) and d == ts),
    ("mx.d < mx.ts", lambda d, ts, s: known(d, ts) and d < ts),
    ("mx.d > mx.ts", lambda d, ts, s: known(d, ts) and d > ts),
    ("mx.ts >= mx.d", lambda d, ts, s: known(d, ts) and ts >= d),
    ("mx.d != mx.ts", lambda d, ts, s: known(d, ts) and d != ts),
    ("date_trunc('day', mx.ts) = mx.ts", lambda d, ts, s: known(ts) and ts.time() == time()),
    ("date_trunc('day', mx.ts) = mx.d", lambda d, ts, s: known(d, ts) and ts.date() == d.date()),
    ("mx.d = date_trunc('hour', mx.ts)", lambda d, ts, s: known(d, ts) and d == ts.replace(minute=0, second=0)),
    ("date_trunc('hour', mx.d) = mx.ts", lambda d, ts, s: known(d, ts) and d == ts),
    ("date_trunc('month', mx.d) <= date_trunc('day', mx.ts)",
     lambda d, ts, s: known(d, ts) and d.replace(day=1) <= ts.replace(hour=0, minute=0, second=0)),
    ("mx.d is not distinct from mx.ts", lambda d, ts, s: (d is None and ts is None) or (known(d, ts) and d == ts)),
    ("mx.d is distinct from mx.ts", lambda d, ts, s: not ((d is None and ts is None) or (known(d, ts) and d == ts))),
    ("coalesce(mx.shipped_at, mx.d) = mx.ts", lambda d, ts, s: known(s or d, ts) and (s or d) == ts),
    ("ifnull(mx.shipped_at, mx.d) = mx.ts", lambda d, ts, s: known(s or d, ts) and (s or d) == ts),
    ("iif(mx.shipped_at is null, mx.d, mx.shipped_at) = mx.ts", lambda d, ts, s: known(s or d, ts) and (s or d) == ts),
    ("CASE WHEN mx.shipped_at IS NULL THEN mx.d ELSE mx.shipped_at END = mx.ts",
     lambda d, ts, s: known(s or d, ts) and (s or d) == ts),
    ("greatest(mx.d, mx.ts) = mx.ts and mx.d is not null and mx.ts is not null", lambda d, ts, s: known(d, ts) and max(d, ts) == ts),
    ("least(mx.d, mx.ts) = mx.d and mx.d is not null and mx.ts is not null", lambda d, ts, s: known(d, ts) and min(d, ts) == d),
    ("mx.d = timestamp '2025-03-01 06:00:00'", lambda d, ts, s: known(d) and d == datetime(2025, 3, 1, 6)),
    ("mx.d != timestamp '2025-03-01 06:00:00'", lambda d, ts, s: known(d) and d != datetime(2025, 3, 1, 6)),
    ("mx.d is not distinct from timestamp '2025-03-01 00:00:00'", lambda d, ts, s: d == datetime(2025, 3, 1)),
    ("mx.d is distinct from timestamp '2025-03-01 06:00:00'", lambda d, ts, s: True),
    ("mx.ts is not distinct from '2025-03-02'", lambda d, ts, s: ts == datetime(2025, 3, 2)),
    ("mx.d in ('2025-03-02', '2025-03-01 06:00:00')",
     lambda d, ts, s: known(d) and d in {datetime(2025, 3, 2), datetime(2025, 3, 1, 6)}),
]


# ---------------------------------------------------------------------------
# Server checks (the integration type matrices)
# ---------------------------------------------------------------------------

def server_datasets(*, data_source: str) -> list[SlayerDataset]:
    return [bk_dataset(data_source=data_source), mx_dataset(data_source=data_source)]


async def check_bucket_matrix(engine: SlayerQueryEngine, *, backend: str, data_source: str) -> None:
    """Every unit over DATE and TIMESTAMP, as a time dimension and an expression: values and physical types."""
    problems: list[str] = []
    for column, source in (("d", DataType.DATE), ("ts", DataType.DATETIME)):
        for position in ("time_dimension", "dimension"):
            for unit in ALL_UNITS:
                rows = await bucket_rows(engine, column=column, unit=unit, position=position, data_source=data_source)
                problems += check_bucket_values(rows, source=source, unit=unit, table=BK, column=column,
                                                backend=backend)
    assert not problems, "\n".join(problems)


async def check_mixed(engine: SlayerQueryEngine, *, backend: str, data_source: str) -> None:
    """A DATE meeting a TIMESTAMP reads as midnight in filters, conditionals and ``nullif``."""
    wrong = [
        filt for filt, pred in MIXED_FILTER_CASES
        if await ids(engine, filt, data_source=data_source) != mx_where(pred)
    ]
    assert not wrong, wrong
    nullif = await by_id(engine, "nullif(mx.d, mx.ts)", data_source=data_source)
    assert {i: as_temporal(v) for i, v in nullif.items()} == {
        1: None, 2: date(2025, 3, 1), 3: date(2025, 3, 1), 4: date(2025, 3, 2), 5: None, 6: date(2025, 3, 3), 7: None,
    }
    assert all(physical_type_error(v, DataType.DATE, backend=backend) is None for v in nullif.values()), nullif
    problems = await conditional_value_problems(engine, backend=backend, data_source=data_source)
    assert not problems, "\n".join(problems)


def _first_known(*values: Any) -> Any:
    return next((v for v in values if v is not None), None)


BOTH_KNOWN = frozenset(i for i, (d, ts, _s) in MX_ROWS.items() if d is not None and ts is not None)
# expression → (oracle over (d as midnight, ts, shipped_at), ids checked); every value is a TIMESTAMP.
CONDITIONAL_VALUE_CASES: list[tuple[str, Any, frozenset[int]]] = [
    ("coalesce(mx.shipped_at, mx.d)", lambda d, ts, s: _first_known(s, d), MX_IDS),
    ("ifnull(mx.shipped_at, mx.d)", lambda d, ts, s: _first_known(s, d), MX_IDS),
    ("iif(mx.shipped_at is null, mx.d, mx.shipped_at)", lambda d, ts, s: _first_known(s, d), MX_IDS),
    ("CASE WHEN mx.shipped_at IS NULL THEN mx.d ELSE mx.shipped_at END", lambda d, ts, s: _first_known(s, d), MX_IDS),
    ("greatest(mx.d, mx.ts)", lambda d, ts, s: max(d, ts), BOTH_KNOWN),
    ("least(mx.d, mx.ts)", lambda d, ts, s: min(d, ts), BOTH_KNOWN),
]


async def conditional_value_problems(engine: SlayerQueryEngine, *, backend: str,
                                     data_source: Optional[str] = None) -> list[str]:
    """DATE arms of TIMESTAMP conditionals: wrong instants or non-TIMESTAMP values."""
    problems = []
    for expression, oracle, checked in CONDITIONAL_VALUE_CASES:
        got = await by_id(engine, expression, data_source=data_source)
        for i in sorted(checked):
            d, ts, s = MX_ROWS[i]
            want = oracle(None if d is None else midnight(d), ts, s)
            if not same_instant(got[i], want):
                problems.append(f"{expression} id={i}: {got[i]!r} != {want!r}")
            why = physical_type_error(got[i], DataType.DATETIME, backend=backend)
            if why is not None:
                problems.append(f"{expression} id={i}: {why}")
    return problems
