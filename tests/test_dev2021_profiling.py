"""Numeric and temporal profiling reads the model's own columns with plain ``min`` / ``max``, no extension.

Spec: queries/extensions (profiling is no longer an extension producer); usage-reporting/collection.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import duckdb
import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.profiling import ensure_samples_fresh
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

# (id, amount, quantity, "Order Total", ordered_at, ordered_ts)
_ROWS = [
    (1, 10.0, 4, 1.5, "2024-01-05", "2024-01-05 10:00:00"),
    (2, 20.0, 1, 2.5, "2024-02-05", "2024-02-05 11:00:00"),
    (3, 40.0, 3, 0.5, "2024-04-05", "2024-04-05 12:00:00"),
]
_COLUMNS = 'id INTEGER, amount DOUBLE, quantity INTEGER, "Order Total" DOUBLE, ordered_at DATE, ordered_ts TIMESTAMP'


def _seed(db_path: str) -> None:
    if db_path.endswith(".duckdb"):
        con = duckdb.connect(db_path)
        try:
            con.execute(f"CREATE TABLE t ({_COLUMNS}, tags INTEGER[])")
            con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?)",
                            [(*r, tags) for r, tags in zip(_ROWS, ([3, 1], [7], [5, 2]))])
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        cur.execute(f"CREATE TABLE t ({_COLUMNS})")
        cur.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)", _ROWS)


def _model(*, arrays: bool) -> SlayerModel:
    columns = [
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="amount", type=DataType.DOUBLE),
        Column(name="double_amount", sql="amount * 2", type=DataType.DOUBLE),
        Column(name="qty", sql="quantity", type=DataType.INT),
        Column(name="order_total", sql='"Order Total"', type=DataType.DOUBLE),
        Column(name="ordered_at", type=DataType.DATE),
        Column(name="ordered_ts", type=DataType.TIMESTAMP),
    ]
    if arrays:
        columns.append(Column(name="tags", type=DataType.ARRAY, element_type=DataType.INT))
    return SlayerModel(name="t", sql_table="t", data_source="test", columns=columns)


_EXPECTED = {
    "amount": "10.0 .. 40.0",
    "double_amount": "20.0 .. 80.0",
    "qty": "1 .. 4",
    "order_total": "0.5 .. 2.5",
    "ordered_at": "2024-01-05 .. 2024-04-05",
    "ordered_ts": "2024-01-05 10:00:00 .. 2024-04-05 12:00:00",
}


class _Run:
    def __init__(self, *, engine: SlayerQueryEngine, dialect: str) -> None:
        self.engine = engine
        self.dialect = dialect


@pytest.fixture(params=["sqlite", "duckdb"])
async def run(request) -> AsyncIterator[_Run]:
    async with seeded_exec_engine(dialect=request.param, seed=_seed,
                                  models=[_model(arrays=request.param == "duckdb")]) as (engine, _db):
        yield _Run(engine=engine, dialect=request.param)


def _record(*, engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch) -> list[SlayerQuery]:
    log: list[SlayerQuery] = []
    real = engine.execute

    async def recording(*args, **kwargs):
        q = kwargs.get("query", args[0] if args else None)
        log.append(q if isinstance(q, SlayerQuery) else SlayerQuery.model_validate(q))
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine, "execute", recording)
    return log


async def _profile(run: _Run, monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, str | None], list[SlayerQuery]]:
    model = await run.engine.storage.get_model("t", data_source="test")
    assert model is not None
    targets = [c for c in model.columns if c.name != "id"]
    log = _record(engine=run.engine, monkeypatch=monkeypatch)
    outcome = await ensure_samples_fresh(model=model, columns=targets, engine=run.engine, storage=run.engine.storage,
                                         force=True)
    assert outcome.errors == []
    return {c.name: c.sampled for c in outcome.columns}, log


async def test_min_max_over_every_column_kind(run: _Run, monkeypatch: pytest.MonkeyPatch) -> None:
    sampled, _ = await _profile(run, monkeypatch)
    expected = dict(_EXPECTED)
    if run.dialect == "duckdb":
        expected["tags"] = "1 .. 7"
    assert {k: sampled[k] for k in expected} == expected


async def test_profiling_sends_no_extension(run: _Run, monkeypatch: pytest.MonkeyPatch) -> None:
    _, log = await _profile(run, monkeypatch)
    assert log, "profiling ran no query"
    assert [q for q in log if q.extensions] == []
