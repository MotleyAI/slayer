"""Shared fixtures for the Ossie Layer 1 scalar functions (specs: queries/scalar-functions, queries/date-functions,
queries/regex).

fn (id, status, amount, sla_days, shipped_at, created_at, order_date, code, x, n, ratio):
   1  pending  30    NULL  NULL                 2024-12-30 10:15:42.123     2024-12-30  abc   2.789   2     1.5
   2  aaaa     10    0     2024-12-31 09:00:00  2024-12-30 10:15:42.001999  2024-12-31  xyz   -2.789  NULL  0.5
   3  NULL     NULL  NULL  NULL                 2024-12-30 10:15:42.999999  2024-12-29  NULL  1234.5  NULL  NULL
   4  paid     20    3     2025-01-02 00:00:00  2024-12-30 10:15:42         2025-01-01  a     NULL    NULL  NULL
   5  void     5     1     NULL                 1969-07-20 20:17:40.5       1969-07-20  NULL  NULL    NULL  NULL
   6  sunday   1     2     NULL                 2024-12-29 12:00:00         2024-12-29  NULL  NULL    NULL  NULL
2024-12-30 is a Monday; 2024-12-29 and 1969-07-20 are Sundays.
"""

from __future__ import annotations

import contextlib
import tempfile
from collections.abc import AsyncGenerator, Iterable
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.models import Aggregation, DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2101_fixtures import emit
from tests._engine_helpers import owned_query_engine, seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")

FN = TableSpec(
    name="fn",
    columns=[
        ("id", "INT"), ("status", "TEXT"), ("amount", "DOUBLE"), ("sla_days", "INT"), ("shipped_at", "TIMESTAMP"),
        ("created_at", "TIMESTAMP"), ("order_date", "DATE"), ("code", "TEXT"), ("x", "DOUBLE"), ("n", "INT"),
        ("ratio", "DOUBLE"),
    ],
    rows=[
        (1, "pending", 30.0, None, None, "2024-12-30 10:15:42.123", "2024-12-30", "abc", 2.789, 2, 1.5),
        (2, "aaaa", 10.0, 0, "2024-12-31 09:00:00", "2024-12-30 10:15:42.001999", "2024-12-31", "xyz", -2.789, None,
         0.5),
        (3, None, None, None, None, "2024-12-30 10:15:42.999999", "2024-12-29", None, 1234.5, None, None),
        (4, "paid", 20.0, 3, "2025-01-02 00:00:00", "2024-12-30 10:15:42", "2025-01-01", "a", None, None, None),
        (5, "void", 5.0, 1, None, "1969-07-20 20:17:40.5", "1969-07-20", None, None, None, None),
        (6, "sunday", 1.0, 2, None, "2024-12-29 12:00:00", "2024-12-29", None, None, None, None),
    ],
)
_TYPES = {"INT": DataType.INTEGER, "DOUBLE": DataType.FLOAT, "TEXT": DataType.STRING, "TIMESTAMP": DataType.DATETIME,
          "DATE": DataType.DATE}


def fn_dataset(*, data_source: str = "test", aggregations: Iterable[Aggregation] = (),
               extra: Iterable[DatasetField] = ()) -> SlayerDataset:
    return SlayerDataset(
        name="fn", sql_table="fn", data_source=data_source, default_time_dimension="created_at",
        fields=[*(DatasetField(name=c, datatype=_TYPES[k], primary_key=c == "id") for c, k in FN.columns), *extra],
        aggregations=list(aggregations),
    )


@contextlib.asynccontextmanager
async def fn_engine(backend: str, *, datasets: Optional[list[SlayerDataset]] = None,
                    validate: bool = False) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, [FN]), datasets=datasets or [fn_dataset()],
        validate=validate,
    ) as (engine, _db):
        yield engine


def fn_body(*dims: Any, **kw: Any) -> dict[str, Any]:
    """A row-level ``fn`` query body: ``id`` plus ``dims`` (an expression string becomes the ``v`` dimension); the
    engine reports a parse error in it as the error itself."""
    dimensions: list[Any] = ["fn.id"] + [{"field": d, "alias": "v"} if isinstance(d, str) else d for d in dims]
    return {"population": "fn", "distinct_dimension_values": False, "dimensions": dimensions, **kw}


def fn_query(*dims: Any, **kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(fn_body(*dims, **kw))


def named(expr: str, alias: str) -> dict[str, str]:
    return {"field": expr, "alias": alias}


def unnamed_query(expr: str) -> SlayerQuery:
    """``expr`` as an unaliased dimension beside ``id``."""
    return SlayerQuery.model_validate({"population": "fn", "distinct_dimension_values": False,
                                       "dimensions": ["fn.id", expr]})


def by_id(resp: SlayerResponse, name: str = "v") -> dict[int, Any]:
    return {int(r["fn.id"]): r[name] for r in resp.data}


def ids(resp: SlayerResponse) -> set[int]:
    return {int(r["fn.id"]) for r in resp.data}


async def emit_fn(query: SlayerQuery, *, dialect: str, datasets: Optional[list[SlayerDataset]] = None) -> str:
    """Dry-run SQL for an ``fn`` query on ``dialect`` (asserts it parses there)."""
    return await emit(query, dialect=dialect, datasets=datasets or [fn_dataset()])


async def execute_offline(query: SlayerQuery, *, dialect: str) -> SlayerResponse:
    """Execute (not dry-run) ``query`` against a ``dialect`` datasource with no server behind it, so only a plan-time
    refusal can answer."""
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(DatasourceConfig.model_validate({"name": "test", "type": dialect}))
        await storage.save_dataset(fn_dataset(), _validate=False)
        async with owned_query_engine(storage=storage) as engine:
            return await engine.execute(query)


async def refusal(engine: SlayerQueryEngine, query: Any) -> SlayerError | ValueError:
    """The error ``query`` fails with."""
    with pytest.raises((SlayerError, ValueError)) as info:
        await engine.execute(query)
    return info.value


async def assert_twins(engine: SlayerQueryEngine, written: str, twin: str) -> SlayerResponse:
    """``written`` and ``twin`` as unaliased dimensions: same result keys, SQL and rows."""
    a = await engine.execute(unnamed_query(written))
    b = await engine.execute(unnamed_query(twin))
    assert a.columns == b.columns, (a.columns, b.columns)
    assert a.sql == b.sql, (a.sql, b.sql)
    assert sorted(map(str, a.data)) == sorted(map(str, b.data))
    return a
