"""The Ossie compliance gate's shared core: corpus root, Tier-1 engines, cell states, logical-type comparison."""

from __future__ import annotations

import datetime
import os
import re
from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, NoReturn, Optional

import pytest
from pydantic import BaseModel, ConfigDict

VENDORED = Path(__file__).parent / "fixtures" / "ossie_corpus"
CORPUS_ENV = "SLAYER_OSSIE_CORPUS"
#: Comma-separated server engines a run requires; unset runs every server engine available.
ENGINES_ENV = "SLAYER_GATE_ENGINES"


def corpus_root() -> Path:
    """``$SLAYER_OSSIE_CORPUS`` when set, else the vendored snapshot."""
    return Path(os.environ.get(CORPUS_ENV) or VENDORED)


IN_PROCESS = ("sqlite", "duckdb")
SERVER = ("postgres", "mysql", "clickhouse", "tsql", "trino", "snowflake", "bigquery")
TIER1 = (*IN_PROCESS, *SERVER)
#: The compliance page's name for each engine.
DISPLAY = {"sqlite": "SQLite", "duckdb": "DuckDB", "postgres": "PostgreSQL", "mysql": "MySQL",
           "clickhouse": "ClickHouse", "tsql": "SQL Server", "trino": "Trino", "snowflake": "Snowflake",
           "bigquery": "BigQuery"}
#: Engines whose boolean results arrive as 1 / 0.
INT_BOOLEANS = frozenset({"sqlite", "mysql", "tsql"})


def required_servers() -> tuple[str, ...]:
    """The server engines ``$SLAYER_GATE_ENGINES`` names; a named engine that cannot start fails the run."""
    named = tuple(e.strip() for e in os.environ.get(ENGINES_ENV, "").split(",") if e.strip())
    unknown = set(named) - set(SERVER)
    if unknown:
        raise ValueError(f"{ENGINES_ENV} names unknown engines: {sorted(unknown)}")
    return named


def selected_servers() -> tuple[str, ...]:
    return required_servers() or SERVER


def unavailable(engine: str, reason: str) -> NoReturn:
    """Fail when the run requires ``engine``, else skip."""
    if engine in required_servers():
        pytest.fail(f"{engine} is required by {ENGINES_ENV} but unavailable: {reason}")
    pytest.skip(f"{engine} unavailable: {reason}")


# --------------------------------------------------------------------------- #
# Cell states
# --------------------------------------------------------------------------- #
State = Literal["pass", "refused", "gap"]


class Declared(BaseModel):
    """A cell state on ``engines`` (every Tier-1 engine when None); refused and gap carry their error class."""

    model_config = ConfigDict(frozen=True)

    state: State = "pass"
    engines: Optional[frozenset[str]] = None
    error: Optional[type[BaseException]] = None
    issue: Optional[str] = None


PASS = Declared()


def passes(*engines: str) -> Declared:
    return Declared(engines=frozenset(engines) or None)


def refused(error: type[BaseException], *engines: str) -> Declared:
    return Declared(state="refused", error=error, engines=frozenset(engines) or None)


def gap(issue: str, error: type[BaseException], *engines: str) -> Declared:
    return Declared(state="gap", issue=issue, error=error, engines=frozenset(engines) or None)


def expand(item: str, declarations: Sequence[Declared]) -> dict[str, Declared]:
    """Engine → state for ``item``; an engine declared twice raises."""
    out: dict[str, Declared] = {}
    for declared in declarations:
        for engine in declared.engines if declared.engines is not None else TIER1:
            if engine in out:
                raise ValueError(f"{item}: {engine} declared twice")
            out[engine] = declared
    return out


def declaration_problems(declared: Mapping[str, Sequence[Declared]], discovered: Iterable[str]) -> list[str]:
    """Why the declarations are not exactly one state per (discovered item, Tier-1 engine)."""
    found = set(discovered)
    problems = [f"stale declaration: {item}" for item in sorted(set(declared) - found)]
    problems += [f"undeclared: {item}" for item in sorted(found - set(declared))]
    for item in sorted(set(declared) & found):
        try:
            cells = expand(item, declared[item])
        except ValueError as exc:
            problems.append(str(exc))
            continue
        problems += [f"{item}: no state on {e}" for e in TIER1 if e not in cells]
        problems += [f"{item}: unknown engine {e}" for e in cells if e not in TIER1]
        for state in declared[item]:
            if state.state != "pass" and state.error is None:
                problems.append(f"{item}: {state.state} names no error class")
            if state.state == "gap" and not state.issue:
                problems.append(f"{item}: gap names no issue")
            if state.state != "gap" and state.issue:
                problems.append(f"{item}: only a gap names an issue")
    return problems


def cell_marks(declared: Declared) -> tuple[Any, ...]:
    """A gap is a strict expected failure raising its error class."""
    if declared.state == "gap":
        return (pytest.mark.xfail(strict=True, raises=declared.error, reason=declared.issue or ""),)
    return ()


def mark_cell(request: pytest.FixtureRequest, declared: Declared) -> None:
    """Mark the running test with its cell's state (the engine is a fixture parameter)."""
    for mark in cell_marks(declared):
        request.applymarker(mark)


# --------------------------------------------------------------------------- #
# Logical-type comparison
# --------------------------------------------------------------------------- #
class Approx(BaseModel):
    """An approximate value: equal within ``tolerance``."""

    model_config = ConfigDict(frozen=True)

    value: float
    tolerance: float = 1e-9


class Now(BaseModel):
    """The database clock's current date, timestamp or time of day."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["date", "timestamp", "time"]


class Bracket(BaseModel):
    """UTC instants captured immediately before and after a query."""

    model_config = ConfigDict(frozen=True)

    start: datetime.datetime
    end: datetime.datetime


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


_DAY = datetime.timedelta(days=1)
_TIME_TEXT = re.compile(r"^\d{2}:\d{2}:\d{2}(\.\d+)?$")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _exact(value: Any) -> Decimal:
    return Decimal(repr(value)) if isinstance(value, float) else Decimal(value)


def _as_datetime(value: Any) -> Optional[datetime.datetime]:
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.UTC).replace(tzinfo=None)
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        try:
            return _as_datetime(datetime.datetime.fromisoformat(value.strip()))
        except ValueError:
            return None
    return None


def _as_date(value: Any) -> Optional[datetime.date]:
    moment = _as_datetime(value)
    if moment is None or moment.time() != datetime.time(0):
        return None
    return moment.date()


def _as_time(value: Any) -> Optional[datetime.time]:
    if isinstance(value, datetime.time):
        return value.replace(tzinfo=None)
    if isinstance(value, datetime.timedelta) and datetime.timedelta(0) <= value < _DAY:
        return (datetime.datetime.min + value).time()
    if isinstance(value, str) and _TIME_TEXT.match(value.strip()):
        return datetime.time.fromisoformat(value.strip())
    return None


def _now_problem(got: Any, want: Now, bracket: Optional[Bracket]) -> Optional[str]:
    if bracket is None:
        return "a clock row needs the bracket captured around its query"
    if want.kind == "time":
        return None if _as_time(got) is not None else f"expected a time of day, got {got!r}"
    low, high = bracket.start - _DAY, bracket.end + _DAY
    if want.kind == "date":
        day = _as_date(got)
        if day is None or not low.date() <= day <= high.date():
            return f"expected a date within a day of {bracket.start:%Y-%m-%d}, got {got!r}"
        return None
    moment = _as_datetime(got)
    if moment is None or not low <= moment <= high:
        return f"expected a timestamp within a day of {bracket.start:%Y-%m-%d %H:%M:%S}, got {got!r}"
    return None


def logical_problem(got: Any, want: Any, *, engine: str, bracket: Optional[Bracket] = None) -> Optional[str]:
    """Why ``got`` is not ``want`` by its logical type, else None."""
    if isinstance(want, Now):
        return _now_problem(got, want, bracket)
    if want is None or got is None:
        return None if got is want else f"expected {want!r}, got {got!r}"
    if isinstance(want, bool):
        if isinstance(got, bool):
            return None if got == want else f"expected {want}, got {got}"
        if engine in INT_BOOLEANS and type(got) is int and got in (0, 1):
            return None if bool(got) == want else f"expected {want}, got {got}"
        return f"expected the boolean {want}, got {got!r}"
    if isinstance(want, Approx):
        if not _is_number(got):
            return f"expected a number near {want.value}, got {got!r}"
        return None if abs(float(got) - want.value) <= want.tolerance else f"expected ~{want.value}, got {got!r}"
    if _is_number(want):
        if not _is_number(got):
            return f"expected the number {want}, got {got!r}"
        return None if _exact(got) == _exact(want) else f"expected {want}, got {got!r}"
    if isinstance(want, str):
        return None if isinstance(got, str) and got == want else f"expected {want!r}, got {got!r}"
    if isinstance(want, datetime.datetime):
        return None if _as_datetime(got) == want else f"expected {want}, got {got!r}"
    if isinstance(want, datetime.date):
        return None if _as_date(got) == want else f"expected {want}, got {got!r}"
    if isinstance(want, datetime.time):
        return None if _as_time(got) == want else f"expected {want}, got {got!r}"
    raise TypeError(f"no logical comparison for {want!r}")


def assert_logical(got: Any, want: Any, *, engine: str, bracket: Optional[Bracket] = None) -> None:
    problem = logical_problem(got, want, engine=engine, bracket=bracket)
    assert problem is None, problem


def multiset_problem(got: Sequence[Sequence[Any]], want: Sequence[Sequence[Any]], *, engine: str) -> Optional[str]:
    """Why ``got`` is not ``want`` as a multiset of rows compared cell by cell by logical type, else None."""
    unmatched = list(got)
    missing: list[Sequence[Any]] = []
    for expected in want:
        for index, row in enumerate(unmatched):
            if len(row) == len(expected) and all(
                    logical_problem(g, w, engine=engine) is None for g, w in zip(row, expected)):
                del unmatched[index]
                break
        else:
            missing.append(expected)
    if missing or unmatched:
        return f"missing rows {missing}; unexpected rows {unmatched}"
    return None
