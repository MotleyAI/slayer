"""Shared fixtures for the Mode-B date functions: seeded tables, models, a Python oracle, and the executed case matrix."""

from __future__ import annotations

import calendar
import contextlib
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional

import pytest
from pydantic import BaseModel

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")

PARTS = (
    "year", "iso_year", "quarter", "month", "week", "day",
    "day_of_week", "day_of_year", "hour", "minute", "second",
)
UNITS = ("second", "minute", "hour", "day", "week", "week_sunday", "month", "quarter", "year")

# Logical column kind → storage type per in-process backend (SQLite keeps dates as text).
_SQLITE_TYPE = {
    "INT": "INTEGER", "DOUBLE": "REAL", "TEXT": "TEXT", "BOOLEAN": "INTEGER",
    "DATE": "TEXT", "TIMESTAMP": "TEXT",
}
_DUCKDB_TYPE = {
    "INT": "INTEGER", "DOUBLE": "DOUBLE", "TEXT": "VARCHAR", "BOOLEAN": "BOOLEAN",
    "DATE": "DATE", "TIMESTAMP": "TIMESTAMP",
}


class TableSpec(BaseModel):
    name: str
    columns: list[tuple[str, str]]
    rows: list[tuple]


# ---------------------------------------------------------------------------
# Spec-scenario tables: orders → customers, plus a clock-relative table.
# ---------------------------------------------------------------------------

ORDERS = TableSpec(
    name="orders",
    columns=[
        ("id", "INT"), ("customer_id", "INT"), ("order_date", "DATE"),
        ("created_at", "TIMESTAMP"), ("shipped_at", "TIMESTAMP"), ("amount", "DOUBLE"),
        ("status", "TEXT"), ("sla_days", "DOUBLE"), ("is_paid", "BOOLEAN"), ("raw_dt", "TEXT"),
    ],
    rows=[
        (1, 1, "2024-03-01", "2024-03-01 05:30:00", "2024-03-02 00:01:00", 10.0, "ok", 2.7, True, "2024-03-01"),
        (2, 1, "2024-03-01", "2024-06-02 10:00:00", "2024-06-07 09:00:00", 20.0, "ok", -2.7, False, "x"),
        (3, 2, "2024-06-03", "2024-06-03 23:59:00", None, 30.0, "hold", None, False, None),
        (4, 2, "2024-12-30", "2024-12-30 12:00:00", "2025-01-04 12:00:00", 40.0, "ok", 1.0, True, None),
        (5, 3, "2024-01-31", "2024-01-31 08:00:00", "2024-02-05 07:59:59", 50.0, "ok", 0.0, True, None),
    ],
)

CUSTOMERS = TableSpec(
    name="customers",
    columns=[("id", "INT"), ("name", "TEXT"), ("signup_date", "DATE")],
    rows=[(1, "A", "2023-12-31"), (2, "B", "2024-02-29"), (3, "C", None)],
)


def recent_table(*, today: date) -> TableSpec:
    """Two rows relative to ``today``: one inside a 30-day look-back, one outside."""
    return TableSpec(
        name="recent",
        columns=[("id", "INT"), ("created_at", "TIMESTAMP")],
        rows=[
            (1, f"{today - timedelta(days=1)} 12:00:00"),
            (2, f"{today - timedelta(days=40)} 12:00:00"),
        ],
    )


def orders_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source=data_source,
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="order_date", type=DataType.DATE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="shipped_at", type=DataType.TIMESTAMP),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="sla_days", type=DataType.DOUBLE),
            Column(name="is_paid", type=DataType.BOOLEAN),
            Column(name="raw_dt"),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def customers_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="signup_date", type=DataType.DATE),
        ],
    )


def recent_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="recent", sql_table="recent", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
    )


def scenario_models(*, data_source: str = "test") -> list[SlayerModel]:
    return [
        orders_model(data_source=data_source),
        customers_model(data_source=data_source),
        recent_model(data_source=data_source),
    ]


def scenario_tables(*, today: date) -> list[TableSpec]:
    return [ORDERS, CUSTOMERS, recent_table(today=today)]


# ---------------------------------------------------------------------------
# The date matrix table ``dt``: boundary pairs per row.
# ---------------------------------------------------------------------------

DT = TableSpec(
    name="dt",
    columns=[
        ("id", "INT"), ("d1", "DATE"), ("d2", "DATE"),
        ("t1", "TIMESTAMP"), ("t2", "TIMESTAMP"), ("n", "DOUBLE"),
    ],
    rows=[
        (1, "2024-01-31", "2024-02-01", "2024-01-31 23:59:00", "2024-02-01 00:01:00", 2.7),
        (2, "2024-02-01", "2024-01-31", "2024-02-01 00:01:00", "2024-01-31 23:59:00", -2.7),
        (3, "2024-12-31", "2025-01-01", "2024-12-31 23:59:59", "2025-01-01 00:00:01", 1.0),
        (4, "2024-06-02", "2024-06-03", "2024-06-02 10:00:00", "2024-06-03 09:00:00", -1.0),
        (5, "2024-02-01", "2024-02-29", "2024-02-01 10:15:30", "2024-02-29 10:15:29", 0.0),
        (6, "2024-03-01", "2024-03-01", "2024-03-01 05:30:00", "2024-03-01 05:30:00", None),
        (7, None, "2024-01-01", None, "2024-01-01 00:00:00", 3.0),
        (8, "2024-12-30", "2021-01-03", "2024-03-01 10:00:00.750000", "2024-03-01 10:00:01.250000", 12.0),
        (9, "2024-02-29", "2023-03-31", "2024-01-31 10:15:00", "2024-05-31 23:00:00", -13.0),
        (10, "2023-01-01", "2024-12-29", "2023-01-01 00:00:00", "2024-12-29 23:59:59", 5.5),
    ],
)


def dt_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="dt", sql_table="dt", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="d1", type=DataType.DATE),
            Column(name="d2", type=DataType.DATE),
            Column(name="t1", type=DataType.TIMESTAMP),
            Column(name="t2", type=DataType.TIMESTAMP),
            Column(name="n", type=DataType.DOUBLE),
        ],
    )


# ---------------------------------------------------------------------------
# Python oracle — the spec's semantics, independent of any backend.
# ---------------------------------------------------------------------------

def parse_temporal(value: Optional[str]) -> date | datetime | None:
    if value is None:
        return None
    return date.fromisoformat(value) if len(value) == 10 else datetime.fromisoformat(value)


def _as_datetime(v: date | datetime) -> datetime:
    return v if isinstance(v, datetime) else datetime(v.year, v.month, v.day)


def _day(v: date | datetime) -> date:
    return v.date() if isinstance(v, datetime) else v


def oracle_part(part: str, v: date | datetime | None) -> Optional[int]:
    if v is None:
        return None
    d = _day(v)
    iso = d.isocalendar()
    sub_day = {"hour": "hour", "minute": "minute", "second": "second"}
    if part in sub_day:
        return getattr(v, sub_day[part]) if isinstance(v, datetime) else 0
    return {
        "year": d.year, "iso_year": iso.year, "quarter": (d.month - 1) // 3 + 1,
        "month": d.month, "week": iso.week, "day": d.day,
        "day_of_week": d.isoweekday(), "day_of_year": d.timetuple().tm_yday,
    }[part]


def _week_start(d: date, *, sunday: bool) -> date:
    back = d.isoweekday() % 7 if sunday else d.isoweekday() - 1
    return d - timedelta(days=back)


def _trunc_seconds(v: datetime, step: int) -> int:
    epoch = int((v.replace(microsecond=0) - datetime(1970, 1, 1)).total_seconds())
    return epoch - epoch % step


def oracle_diff(unit: str, start: date | datetime | None, end: date | datetime | None) -> Optional[int]:
    if start is None or end is None:
        return None
    s, e = _day(start), _day(end)
    if unit == "year":
        return e.year - s.year
    if unit == "quarter":
        return 4 * (e.year - s.year) + (e.month - 1) // 3 - (s.month - 1) // 3
    if unit == "month":
        return 12 * (e.year - s.year) + e.month - s.month
    if unit in ("week", "week_sunday"):
        sunday = unit == "week_sunday"
        return (_week_start(e, sunday=sunday) - _week_start(s, sunday=sunday)).days // 7
    if unit == "day":
        return (e - s).days
    step = {"hour": 3600, "minute": 60, "second": 1}[unit]
    return (_trunc_seconds(_as_datetime(end), step) - _trunc_seconds(_as_datetime(start), step)) // step


def _add_months(v: date | datetime, months: int) -> date | datetime:
    total = v.month - 1 + months
    year, month = v.year + total // 12, total % 12 + 1
    return v.replace(year=year, month=month, day=min(v.day, calendar.monthrange(year, month)[1]))


def oracle_add(v: date | datetime | None, n: float | int | None, unit: str) -> date | datetime | None:
    if v is None or n is None:
        return None
    k = int(n)  # truncates toward zero
    if unit in ("second", "minute", "hour"):
        return _as_datetime(v) + timedelta(**{f"{unit}s": k})
    if unit == "day":
        return v + timedelta(days=k)
    if unit in ("week", "week_sunday"):
        return v + timedelta(days=7 * k)
    return _add_months(v, k * {"month": 1, "quarter": 3, "year": 12}[unit])


# ---------------------------------------------------------------------------
# The executed case matrix over ``dt``.
# ---------------------------------------------------------------------------

class DateCase(BaseModel):
    case_id: str
    expr: str
    expected: dict[int, Any]


def _rows() -> list[dict[str, Any]]:
    names = [c for c, _ in DT.columns]
    out = []
    for row in DT.rows:
        rec = dict(zip(names, row))
        for col in ("d1", "d2", "t1", "t2"):
            rec[col] = parse_temporal(rec[col])
        out.append(rec)
    return out


def _case(case_id: str, expr: str, fn: Callable[[dict[str, Any]], Any]) -> DateCase:
    return DateCase(case_id=case_id, expr=expr, expected={r["id"]: fn(r) for r in _rows()})


def matrix_cases() -> list[DateCase]:
    cases: list[DateCase] = []
    for part in PARTS:
        for col in ("d1", "t1"):
            cases.append(_case(
                f"part|{part}|{col}", f"date_part('{part}', {col})",
                lambda r, p=part, c=col: oracle_part(p, r[c]),
            ))
    for unit in UNITS:
        for s, e in (("d1", "d2"), ("t1", "t2"), ("d1", "t2"), ("t1", "d2")):
            cases.append(_case(
                f"diff|{unit}|{s}-{e}", f"date_diff('{unit}', {s}, {e})",
                lambda r, u=unit, a=s, b=e: oracle_diff(u, r[a], r[b]),
            ))
    for unit in UNITS:
        for col in ("d1", "t1"):
            for label, count in (("plus1", "1"), ("minus13", "-13"), ("computed", "n")):
                cases.append(_case(
                    f"add|{unit}|{col}|{label}", f"date_add({col}, {count}, '{unit}')",
                    lambda r, u=unit, c=col, k=count: oracle_add(
                        r[c], r["n"] if k == "n" else int(k), u),
                ))
    cases += [
        _case("interval|plus|d1", "d1 + interval(1, 'month')", lambda r: oracle_add(r["d1"], 1, "month")),
        _case("interval|commuted|d1", "interval(2, 'week') + d1", lambda r: oracle_add(r["d1"], 2, "week")),
        _case("interval|minus_computed|t1", "t1 - interval(n, 'hour')",
              lambda r: oracle_add(r["t1"], None if r["n"] is None else -r["n"], "hour")),
        _case("interval|chain|t1", "t1 + interval(1, 'month') + interval(2, 'hour')",
              lambda r: oracle_add(oracle_add(r["t1"], 1, "month"), 2, "hour")),
    ]
    return cases


def matrix_query(case: DateCase) -> SlayerQuery:
    return SlayerQuery.model_validate({
        "source_model": "dt",
        "dimensions": ["id", {"expression": case.expr, "name": "v"}],
    })


def as_temporal(value: Any) -> date | datetime | None:
    """A backend's date/timestamp result as a Python value (SQLite returns ISO text)."""
    if value is None or isinstance(value, (date, datetime)):
        return value
    assert isinstance(value, str), f"not a temporal value: {value!r}"
    text = value.replace("T", " ")
    return date.fromisoformat(text) if len(text) == 10 else datetime.fromisoformat(text)


def assert_value(actual: Any, expected: Any, *, where: str) -> None:
    if expected is None:
        assert actual is None, f"{where}: expected NULL, got {actual!r}"
    elif isinstance(expected, (date, datetime)):
        got = as_temporal(actual)
        assert type(got) is type(expected), f"{where}: expected {type(expected).__name__} {expected}, got {actual!r}"
        assert got == expected, f"{where}: expected {expected}, got {actual!r}"
    else:
        assert not isinstance(actual, (str, bool)), f"{where}: expected an integer, got {actual!r}"
        assert actual is not None, f"{where}: expected {expected}, got NULL"
        assert Decimal(str(actual)) == Decimal(expected), f"{where}: expected {expected}, got {actual!r}"


def assert_case(data: list[dict[str, Any]], case: DateCase, *, model: str = "dt") -> None:
    got = {int(r[f"{model}.id"]): r[f"{model}.v"] for r in data}
    assert set(got) == set(case.expected), f"{case.case_id}: row ids {sorted(got)}"
    for rid, expected in case.expected.items():
        assert_value(got[rid], expected, where=f"{case.case_id} id={rid}")


# ---------------------------------------------------------------------------
# Seeding.
# ---------------------------------------------------------------------------

def _seed_with(con: Any, *, tables: Iterable[TableSpec], types: Mapping[str, str]) -> None:
    for t in tables:
        con.execute(f"CREATE TABLE {t.name} ({', '.join(f'{c} {types[k]}' for c, k in t.columns)})")
        if t.rows:
            con.executemany(f"INSERT INTO {t.name} VALUES ({', '.join('?' * len(t.columns))})", t.rows)


def seed_backend(backend: str, db_path: str, tables: Iterable[TableSpec]) -> None:
    if backend == "sqlite":
        with transaction(db_path) as con:
            _seed_with(con, tables=tables, types=_SQLITE_TYPE)
        return
    duckdb = pytest.importorskip("duckdb")
    with contextlib.closing(duckdb.connect(db_path)) as con:
        _seed_with(con, tables=tables, types=_DUCKDB_TYPE)


@contextlib.asynccontextmanager
async def exec_engine(
    backend: str, *, tables: list[TableSpec], models: list[SlayerModel],
) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, tables), models=models,
    ) as (engine, _db):
        yield engine


def sql_literal(value: Any, *, bits: bool = False) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(int(value)) if bits else str(value).upper()
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def seed_statements(
    table: TableSpec, *, types: Mapping[str, str], suffix: str = "", bits: bool = False,
) -> list[str]:
    """CREATE + one multi-row INSERT for a server backend (``types``: kind → column type)."""
    cols = ", ".join(f"{c} {types[k]}" for c, k in table.columns)
    values = ", ".join("(" + ", ".join(sql_literal(v, bits=bits) for v in row) + ")" for row in table.rows)
    return [f"CREATE TABLE {table.name} ({cols}){suffix}", f"INSERT INTO {table.name} VALUES {values}"]


_NULLABLE_CH = {
    "INT": "Nullable(Int32)", "DOUBLE": "Nullable(Float64)", "TEXT": "Nullable(String)",
    "BOOLEAN": "Nullable(Bool)", "DATE": "Nullable(Date)", "TIMESTAMP": "Nullable(DateTime64(6))",
}
SERVER_TYPES: dict[str, dict[str, str]] = {
    "postgres": {
        "INT": "INTEGER", "DOUBLE": "DOUBLE PRECISION", "TEXT": "TEXT", "BOOLEAN": "BOOLEAN",
        "DATE": "DATE", "TIMESTAMP": "TIMESTAMP(6)",
    },
    "mysql": {
        "INT": "INTEGER", "DOUBLE": "DOUBLE", "TEXT": "VARCHAR(255)", "BOOLEAN": "BOOLEAN",
        "DATE": "DATE", "TIMESTAMP": "DATETIME(6)",
    },
    "clickhouse": _NULLABLE_CH,
    "tsql": {
        "INT": "INT", "DOUBLE": "FLOAT", "TEXT": "NVARCHAR(255)", "BOOLEAN": "BIT",
        "DATE": "DATE", "TIMESTAMP": "DATETIME2(6)",
    },
}
_SERVER_SUFFIX = {"clickhouse": " ENGINE = MergeTree ORDER BY tuple()"}


def server_seed_statements(backend: str, *, today: date) -> list[str]:
    """Every statement seeding ``dt`` plus the scenario tables on a server backend."""
    out: list[str] = []
    for table in [DT, *scenario_tables(today=today)]:
        out += seed_statements(
            table, types=SERVER_TYPES[backend], suffix=_SERVER_SUFFIX.get(backend, ""), bits=backend == "tsql",
        )
    return out


def all_models(*, data_source: str) -> list[SlayerModel]:
    return [dt_model(data_source=data_source), *scenario_models(data_source=data_source)]


async def check_server_scenarios(engine: SlayerQueryEngine) -> None:
    """Spec scenarios whose values need a real backend (clock, joins, literals)."""
    async def by_id(expr: str, model: str = "orders") -> dict[int, Any]:
        resp = await engine.execute(SlayerQuery.model_validate(
            {"source_model": model, "dimensions": ["id", {"expression": expr, "name": "v"}]}))
        return {int(r[f"{model}.id"]): r[f"{model}.v"] for r in resp.data}

    async def count(filt: str, model: str = "orders") -> int:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": model, "measures": [{"formula": "count(*)", "name": "n"}], "filters": [filt]}))
        return int(resp.data[0][f"{model}.n"])

    assert await count("created_at >= date_add(current_date(), -30, 'day')", model="recent") == 1
    assert await count("created_at <= now()") == 5
    assert await count("date_diff('day', created_at, shipped_at) > 3") == 3
    assert await count("date_diff('day', '2024-05-01', created_at) >= 0") == 3
    assert await by_id("date_diff('day', customers.signup_date, order_date)") == {1: 61, 2: 61, 3: 95, 4: 305, 5: None}
    for expr, expected in (
        ("date_add('2024-03-31', -1, 'month')", date(2024, 2, 29)),
        ("date_add('2024-01-31 10:15:00', 1, 'month')", datetime(2024, 2, 29, 10, 15)),
        ("date_add(order_date, 2, 'hour')", datetime(2024, 3, 1, 2, 0)),
        ("date_add(order_date, sla_days, 'day')", date(2024, 3, 3)),
        ("date_part('day_of_week', created_at)", 5),
        ("date_part('hour', order_date)", 0),
        ("date_diff('hour', order_date, created_at)", 5),
    ):
        assert_value((await by_id(expr))[1], expected, where=expr)


def sqlite_malformed_seed(db_path: str) -> None:
    """``orders`` with one stored ``order_date`` that is not a date (SQLite only)."""
    malformed = (9, 1, "not a date", "garbage", None, 1.0, "ok", 1.0, True, None)
    with transaction(db_path) as con:
        _seed_with(
            con, tables=[ORDERS.model_copy(update={"rows": [ORDERS.rows[0], malformed]}), CUSTOMERS],
            types=_SQLITE_TYPE,
        )
