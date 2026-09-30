"""Model-level sample profiling: caching, failure bounding, failure cache, logging, RLS scoping."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

import slayer.engine.profiling as profiling
import slayer.inspect.model_render as model_render
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.core.query import ColumnRef, ModelExtension, SlayerQuery
from slayer.engine.profiling import (
    ProfileOutcome,
    ensure_samples_fresh,
    refresh_all_table_backed_sampled,
    refresh_table_backed_model_sampled,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.inspect.model_render import render_model_inspection
from slayer.inspect.service import InspectService
from slayer.search.service import SearchService, handle_edit_refresh
from slayer.storage.base import StorageBackend, resolve_storage
from slayer.storage.sqlite_conn import transaction

DS = "ds"
BAD_SQL = "no_such_fn(status)"
BAD_NUM_SQL = "no_such_fn(amount)"
TTL = 3600.0

ROWS = [
    (1, "a", "paid", "web", None, 10.0, 1, None, "2024-01-01"),
    (2, "a", "paid", "app", None, 20.0, 2, None, "2024-02-01"),
    (3, "b", "refunded", "web", None, 5.0, 3, None, "2024-03-01"),
    (4, "b", "cancelled", "store", None, 99.0, 4, None, "2024-04-01"),
]


def _orders(*, extra: list[Column] | None = None, sql_table: str = "t", name: str = "orders") -> SlayerModel:
    return SlayerModel(
        name=name, sql_table=sql_table, data_source=DS,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
            Column(name="channel", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="qty", type=DataType.INT),
            Column(name="ordered_at", type=DataType.DATE),
            *(extra or []),
        ],
    )


def _ghost() -> SlayerModel:
    """Every query fails: the table does not exist."""
    return _orders(name="ghost", sql_table="no_such_table")


def _all_bad() -> SlayerModel:
    """``count(*)`` succeeds but every column expression errors."""
    return SlayerModel(
        name="allbad", sql_table="t", data_source=DS,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            *[Column(name=f"b{i}", sql=f"no_such_fn(status, {i})", type=DataType.TEXT) for i in range(5)],
        ],
    )


@pytest_asyncio.fixture
async def env(tmp_path) -> AsyncIterator[tuple[SlayerQueryEngine, StorageBackend]]:
    db_file = str(tmp_path / "data.db")
    with transaction(db_file) as conn:
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, org TEXT, status TEXT, channel TEXT, "
            "note TEXT, amount REAL, qty INTEGER, empty_num REAL, ordered_at DATE)"
        )
        conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ROWS)
    storage = resolve_storage(str(tmp_path / "storage"))
    await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite", database=db_file))
    yield SlayerQueryEngine(storage=storage), storage


async def _save(storage: StorageBackend, model: SlayerModel) -> SlayerModel:
    await storage.save_model(model)
    loaded = await storage.get_model(model.name, data_source=DS)
    assert loaded is not None
    return loaded


def _record(*, engine: SlayerQueryEngine, monkeypatch) -> list[SlayerQuery]:
    """Record every query sent through ``engine.execute``."""
    log: list[SlayerQuery] = []
    real = engine.execute

    async def recording(*args, **kwargs):
        q = kwargs.get("query", args[0] if args else None)
        log.append(q if isinstance(q, SlayerQuery) else SlayerQuery.model_validate(q))
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine, "execute", recording)
    return log


def _numeric_batches(log: list[SlayerQuery]) -> list[SlayerQuery]:
    return [q for q in log if isinstance(q.source_model, ModelExtension)]


def _categorical(log: list[SlayerQuery]) -> list[SlayerQuery]:
    return [q for q in log if q.dimensions]


def _probes(log: list[SlayerQuery]) -> list[SlayerQuery]:
    return [q for q in log if not q.dimensions and not isinstance(q.source_model, ModelExtension)]


def _count_persists(*, storage: StorageBackend, monkeypatch) -> list[str]:
    calls: list[str] = []
    real = storage.update_column_sampled

    async def counting(**kwargs):
        calls.append(kwargs["column_name"])
        return await real(**kwargs)

    monkeypatch.setattr(storage, "update_column_sampled", counting)
    return calls


def _profiling_warnings(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == profiling.__name__ and r.levelno >= logging.WARNING]


def _triple(col: Column) -> tuple:
    return (col.sampled, col.sampled_values, col.distinct_count)


def _cols(model: SlayerModel | None, *names: str) -> list[Column]:
    assert model is not None
    out: list[Column] = []
    for name in names:
        col = model.get_column(name)
        assert col is not None, name
        out.append(col)
    return out


async def _stored(storage: StorageBackend, *, model: str = "orders", column: str) -> Column:
    m = await storage.get_model(model, data_source=DS)
    assert m is not None
    col = m.get_column(column)
    assert col is not None
    return col


async def _profile(*, engine, storage, model: SlayerModel, force: bool = False) -> ProfileOutcome:
    return await ensure_samples_fresh(
        model=model, columns=list(model.columns), engine=engine, storage=storage, force=force,
    )


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    c = _Clock()
    monkeypatch.setattr(profiling, "_clock", c)
    return c


# ---------------------------------------------------------------------------
# Failure-cache fingerprints
# ---------------------------------------------------------------------------


def test_fingerprints_ignore_sample_fields() -> None:
    model = _orders()
    sampled = model.model_copy(update={"columns": [
        c.model_copy(update={"sampled": "x", "sampled_values": ["x"], "distinct_count": 1}) for c in model.columns
    ]})
    assert profiling._model_fingerprint(sampled) == profiling._model_fingerprint(model)
    assert profiling._column_fingerprint(sampled.columns[1]) == profiling._column_fingerprint(model.columns[1])


def test_fingerprints_change_on_definition_edits() -> None:
    model = _orders()
    status = _cols(model, "status")[0]
    assert profiling._model_fingerprint(model.model_copy(update={"sql_table": "u"})) != profiling._model_fingerprint(model)
    assert profiling._column_fingerprint(status.model_copy(update={"sql": "channel"})) != profiling._column_fingerprint(status)


def test_model_fingerprint_ignores_column_order() -> None:
    model = _orders()
    reordered = model.model_copy(update={"columns": list(reversed(model.columns))})
    assert profiling._model_fingerprint(reordered) == profiling._model_fingerprint(model)


# ---------------------------------------------------------------------------
# One profiling path
# ---------------------------------------------------------------------------


async def test_owner_returns_input_columns_in_order(env) -> None:
    engine, storage = env
    model = await _save(storage, _orders())
    cols = _cols(model, "status", "id", "amount")
    outcome = await ensure_samples_fresh(model=model, columns=cols, engine=engine, storage=storage)
    assert isinstance(outcome, ProfileOutcome)
    assert [c.name for c in outcome.columns] == ["status", "id", "amount"]
    assert outcome.columns[0].sampled_values == ["paid", "cancelled", "refunded"]
    assert outcome.columns[1] == cols[1]
    assert outcome.columns[2].sampled == "5.0 .. 99.0"
    assert outcome.errors == []


async def test_hidden_identifier_and_opaque_columns_are_never_profiled(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _orders(extra=[
        Column(name="secret", sql="status", type=DataType.TEXT, hidden=True),
        Column(name="blob", sql="status", type=DataType.UNKNOWN),
    ]))
    log = _record(engine=engine, monkeypatch=monkeypatch)
    cols = _cols(model, "id", "secret", "blob")
    outcome = await ensure_samples_fresh(model=model, columns=cols, engine=engine, storage=storage)
    assert log == []
    assert outcome.columns == cols


async def test_numeric_columns_share_one_batched_query(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _orders())
    log = _record(engine=engine, monkeypatch=monkeypatch)
    await _profile(engine=engine, storage=storage, model=model)
    assert len(_numeric_batches(log)) == 1
    assert len(_categorical(log)) == 2  # status, channel
    assert _probes(log) == []


async def test_inspect_model_profiles_numeric_columns_with_one_query(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _orders())
    log = _record(engine=engine, monkeypatch=monkeypatch)
    out = await render_model_inspection(
        model=model, storage=storage, engine=engine, format="json", sections=["columns"],
    )
    assert len(_numeric_batches(log)) == 1
    rendered = {c["name"]: c["sampled"] for c in json.loads(out)["columns"]}
    for name in ("amount", "qty", "ordered_at"):
        assert rendered[name] == (await _stored(storage, column=name)).sampled
    assert rendered["ordered_at"] == "2024-01-01 .. 2024-04-01"


async def _reset_samples(storage: StorageBackend, *, names: tuple[str, ...]) -> None:
    for name in names:
        await storage.update_column_sampled(
            data_source=DS, model_name="orders", column_name=name,
            sampled=None, sampled_values=None, distinct_count=None,
        )


async def test_healthy_model_parity_across_read_paths(env) -> None:
    engine, storage = env
    names = ("status", "note", "amount", "empty_num", "ordered_at")
    model = await _save(storage, _orders(extra=[
        Column(name="note", type=DataType.TEXT),
        Column(name="empty_num", type=DataType.DOUBLE),
    ]))

    await render_model_inspection(model=model, storage=storage, engine=engine, sections=["columns"])
    via_inspect_model = {n: _triple(await _stored(storage, column=n)) for n in names}

    await _reset_samples(storage, names=names)
    inspect_svc = InspectService(storage=storage, engine=SlayerQueryEngine(storage=storage))
    for n in names:
        await inspect_svc.inspect(reference=f"{DS}.orders.{n}", entity_type="column", compact=False)
    via_inspect_column = {n: _triple(await _stored(storage, column=n)) for n in names}

    await _reset_samples(storage, names=names)
    search_svc = SearchService(storage=storage, engine=SlayerQueryEngine(storage=storage))
    await search_svc.search(entities=[f"{DS}.orders.{n}" for n in names], max_results=20, compact=False)
    via_search = {n: _triple(await _stored(storage, column=n)) for n in names}

    assert via_inspect_model == via_inspect_column == via_search
    assert via_inspect_model["empty_num"] == ("all NULL", None, None)
    assert via_inspect_model["note"] == ("", [], 0)


async def test_forced_refresh_skips_non_table_backed_models(env, monkeypatch) -> None:
    engine, storage = env
    await _save(storage, _orders())
    sql_model = SlayerModel(
        name="sql_orders", sql="SELECT * FROM t", data_source=DS,
        columns=[Column(name="status", type=DataType.TEXT), Column(name="amount", type=DataType.DOUBLE)],
    )
    query_backed = SlayerModel(
        name="by_status", data_source=DS,
        source_queries=[SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="status")])],
        columns=[Column(name="status", type=DataType.TEXT)],
    )
    log = _record(engine=engine, monkeypatch=monkeypatch)
    for model in (sql_model, query_backed):
        assert await refresh_table_backed_model_sampled(model=model, engine=engine, storage=storage) == []
    assert log == []


async def test_forced_refresh_only_columns(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _orders())
    log = _record(engine=engine, monkeypatch=monkeypatch)
    errors = await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage, only_columns={"status"},
    )
    assert errors == []
    assert len(log) == 1
    assert (await _stored(storage, column="status")).sampled_values == ["paid", "cancelled", "refunded"]
    assert (await _stored(storage, column="amount")).sampled is None


async def test_forced_refresh_reprofiles_cached_columns(env) -> None:
    engine, storage = env
    model = await _save(storage, _orders())
    await storage.update_column_sampled(
        data_source=DS, model_name="orders", column_name="status",
        sampled="STALE", sampled_values=["STALE"], distinct_count=1,
    )
    errors = await refresh_table_backed_model_sampled(model=model, engine=engine, storage=storage)
    assert errors == []
    assert (await _stored(storage, column="status")).sampled_values == ["paid", "cancelled", "refunded"]


# ---------------------------------------------------------------------------
# A successful profile always caches the column
# ---------------------------------------------------------------------------


async def test_all_null_numeric_is_cached(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _orders(extra=[Column(name="empty_num", type=DataType.DOUBLE)]))
    outcome = await ensure_samples_fresh(
        model=model, columns=_cols(model, "empty_num"), engine=engine, storage=storage,
    )
    assert outcome.columns[0].sampled == "all NULL"
    stored = await _stored(storage, column="empty_num")
    assert stored.sampled == "all NULL"
    log = _record(engine=engine, monkeypatch=monkeypatch)
    reloaded = await storage.get_model("orders", data_source=DS)
    await ensure_samples_fresh(
        model=reloaded, columns=_cols(reloaded, "empty_num"), engine=engine, storage=storage,
    )
    assert log == []


async def test_all_null_numeric_is_cached_via_inspect_column(env, monkeypatch) -> None:
    engine, storage = env
    await _save(storage, _orders(extra=[Column(name="empty_num", type=DataType.DOUBLE)]))
    svc = InspectService(storage=storage, engine=engine)
    await svc.inspect(reference=f"{DS}.orders.empty_num", entity_type="column", compact=False)
    assert (await _stored(storage, column="empty_num")).sampled == "all NULL"
    log = _record(engine=engine, monkeypatch=monkeypatch)
    await svc.inspect(reference=f"{DS}.orders.empty_num", entity_type="column", compact=False)
    assert log == []


async def test_empty_categorical_is_cached(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _orders(extra=[Column(name="note", type=DataType.TEXT)]))
    await ensure_samples_fresh(model=model, columns=_cols(model, "note"), engine=engine, storage=storage)
    assert _triple(await _stored(storage, column="note")) == ("", [], 0)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    reloaded = await storage.get_model("orders", data_source=DS)
    await ensure_samples_fresh(model=reloaded, columns=_cols(reloaded, "note"), engine=engine, storage=storage)
    assert log == []


# ---------------------------------------------------------------------------
# Classification and bounding
# ---------------------------------------------------------------------------


async def test_model_whose_every_query_fails(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _ghost())
    log = _record(engine=engine, monkeypatch=monkeypatch)
    persists = _count_persists(storage=storage, monkeypatch=monkeypatch)
    outcome = await _profile(engine=engine, storage=storage, model=model)
    assert len(log) == 2
    assert len(_probes(log)) == 1
    assert log[1] is _probes(log)[0]
    assert persists == []
    assert all(_triple(c) == (None, None, None) for c in outcome.columns)
    assert len(outcome.errors) == 1
    assert "ghost" in outcome.errors[0]


async def test_inspect_model_on_failing_model_is_bounded(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _ghost())
    log = _record(engine=engine, monkeypatch=monkeypatch)
    await render_model_inspection(model=model, storage=storage, engine=engine, sections=["columns"])
    assert len(log) == 3  # row count + first profiling query + probe


async def test_forced_filter_error_is_classified_via_probe(env, monkeypatch) -> None:
    _, storage = env
    model = await _save(storage, _orders())
    engine = SlayerQueryEngine(
        storage=storage, policy=SessionPolicy(ruleset=ColumnFilterRuleset(column="tenant_zz", value="x")),
    )
    log = _record(engine=engine, monkeypatch=monkeypatch)
    outcome = await _profile(engine=engine, storage=storage, model=model)
    assert len(log) == 2
    assert len(outcome.errors) == 1


async def test_one_bad_column_on_healthy_model(env, monkeypatch, caplog) -> None:
    engine, storage = env
    model = await _save(storage, _orders(extra=[Column(name="bad", sql=BAD_SQL, type=DataType.TEXT)]))
    model = model.model_copy(update={"columns": [
        model.get_column(n) for n in ("id", "status", "bad", "channel", "amount", "qty", "ordered_at")
    ]})
    log = _record(engine=engine, monkeypatch=monkeypatch)
    with caplog.at_level(logging.WARNING):
        outcome = await _profile(engine=engine, storage=storage, model=model)
    assert len(_probes(log)) == 1
    for name in ("status", "channel", "amount", "qty", "ordered_at"):
        assert (await _stored(storage, column=name)).sampled is not None, name
    assert _triple(await _stored(storage, column="bad")) == (None, None, None)
    assert len(outcome.errors) == 1
    assert "bad" in outcome.errors[0]

    warnings = _profiling_warnings(caplog)
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert all(part in msg for part in (DS, "orders", "bad", "no such function"))
    assert "[SQL:" not in msg

    log.clear()
    caplog.clear()
    reloaded = await storage.get_model("orders", data_source=DS)
    with caplog.at_level(logging.DEBUG):
        await _profile(engine=engine, storage=storage, model=reloaded)
    assert log == []
    assert _profiling_warnings(caplog) == []


async def test_non_consecutive_bad_columns_probe_once(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, SlayerModel(
        name="orders", sql_table="t", data_source=DS,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="bad1", sql=BAD_SQL, type=DataType.TEXT),
            Column(name="status", type=DataType.TEXT),
            Column(name="bad2", sql="no_such_fn(channel)", type=DataType.TEXT),
            Column(name="channel", type=DataType.TEXT),
        ],
    ))
    log = _record(engine=engine, monkeypatch=monkeypatch)
    outcome = await _profile(engine=engine, storage=storage, model=model)
    assert len(_probes(log)) == 1
    assert len(_categorical(log)) == 4
    assert (await _stored(storage, column="status")).sampled_values is not None
    assert (await _stored(storage, column="channel")).sampled_values is not None
    assert len(outcome.errors) == 2


async def test_probe_succeeds_but_every_column_fails(env, monkeypatch, caplog) -> None:
    engine, storage = env
    model = await _save(storage, _all_bad())
    log = _record(engine=engine, monkeypatch=monkeypatch)
    with caplog.at_level(logging.WARNING):
        outcome = await _profile(engine=engine, storage=storage, model=model)
    assert len(_probes(log)) == 1
    assert len(_categorical(log)) == 3
    assert len(log) == 4
    assert outcome.errors
    assert any("allbad" in r.getMessage() for r in _profiling_warnings(caplog))

    log.clear()
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        await _profile(engine=engine, storage=storage, model=model)
    assert log == []
    assert _profiling_warnings(caplog) == []


async def test_failing_numeric_batch_on_healthy_model(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, SlayerModel(
        name="orders", sql_table="t", data_source=DS,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="badnum", sql=BAD_NUM_SQL, type=DataType.DOUBLE),
            Column(name="qty", type=DataType.INT),
        ],
    ))
    log = _record(engine=engine, monkeypatch=monkeypatch)
    outcome = await _profile(engine=engine, storage=storage, model=model)
    assert len(_probes(log)) == 1
    assert len(_numeric_batches(log)) == 4  # the batch + one per column
    assert (await _stored(storage, column="amount")).sampled == "5.0 .. 99.0"
    assert (await _stored(storage, column="qty")).sampled == "1 .. 4"
    assert (await _stored(storage, column="badnum")).sampled is None
    assert len(outcome.errors) == 1
    assert "badnum" in outcome.errors[0]

    log.clear()
    reloaded = await storage.get_model("orders", data_source=DS)
    await _profile(engine=engine, storage=storage, model=reloaded)
    assert log == []


async def test_search_hits_on_failing_model(env, monkeypatch) -> None:
    engine, storage = env
    await _save(storage, _ghost())
    svc = SearchService(storage=storage, engine=engine)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    resp = await svc.search(
        entities=[f"{DS}.ghost.status", f"{DS}.ghost.channel", f"{DS}.ghost.amount"],
        max_results=10, compact=False,
    )
    assert {h.id for h in resp.results if h.kind == "column"} >= {
        f"{DS}.ghost.status", f"{DS}.ghost.channel", f"{DS}.ghost.amount",
    }
    assert len(log) == 2


async def test_inspect_service_model_makes_one_owner_call(env, monkeypatch) -> None:
    engine, storage = env
    await _save(storage, _orders())
    calls: list[list[str]] = []
    real = model_render.ensure_samples_fresh

    async def spy(**kwargs):
        calls.append([c.name for c in kwargs["columns"]])
        return await real(**kwargs)

    monkeypatch.setattr(model_render, "ensure_samples_fresh", spy)
    await InspectService(storage=storage, engine=engine).inspect(
        reference=f"{DS}.orders", entity_type="model", compact=False,
    )
    assert len(calls) == 1
    assert {"status", "channel", "amount", "qty", "ordered_at"} <= set(calls[0])


async def test_edit_refresh_on_failing_model_is_bounded(env, monkeypatch) -> None:
    engine, storage = env
    await _save(storage, _ghost())
    await storage.update_column_sampled(
        data_source=DS, model_name="ghost", column_name="status",
        sampled="KEEP", sampled_values=["KEEP"], distinct_count=1,
    )
    log = _record(engine=engine, monkeypatch=monkeypatch)
    warnings = await handle_edit_refresh(
        engine=engine, storage=storage, data_source=DS, model_name="ghost",
        changed_columns=set(), model_level_change=True,
    )
    assert len(log) == 2
    assert any("ghost" in w for w in warnings)
    assert (await _stored(storage, model="ghost", column="status")).sampled_values == ["KEEP"]


async def test_search_groups_a_models_hits_into_one_numeric_query(env, monkeypatch) -> None:
    engine, storage = env
    await _save(storage, _orders())
    svc = SearchService(storage=storage, engine=engine)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    await svc.search(entities=[f"{DS}.orders.amount", f"{DS}.orders.qty"], max_results=10)
    assert len(_numeric_batches(log)) == 1


# ---------------------------------------------------------------------------
# Failure cache
# ---------------------------------------------------------------------------


async def test_second_read_within_ttl(env, monkeypatch, clock) -> None:
    engine, storage = env
    model = await _save(storage, _ghost())
    log = _record(engine=engine, monkeypatch=monkeypatch)
    await _profile(engine=engine, storage=storage, model=model)
    log.clear()
    clock.now += TTL - 1
    await _profile(engine=engine, storage=storage, model=model)
    assert log == []


async def test_retry_after_ttl(env, monkeypatch, clock) -> None:
    engine, storage = env
    model = await _save(storage, _ghost())
    log = _record(engine=engine, monkeypatch=monkeypatch)
    await _profile(engine=engine, storage=storage, model=model)
    log.clear()
    clock.now += TTL + 1
    await _profile(engine=engine, storage=storage, model=model)
    assert len(log) == 2


async def test_column_failure_retried_after_ttl(env, monkeypatch, clock) -> None:
    engine, storage = env
    model = await _save(storage, _orders(extra=[Column(name="bad", sql=BAD_SQL, type=DataType.TEXT)]))
    await _profile(engine=engine, storage=storage, model=model)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    clock.now += TTL + 1
    reloaded = await storage.get_model("orders", data_source=DS)
    await _profile(engine=engine, storage=storage, model=reloaded)
    assert len(_categorical(log)) == 1


async def test_retry_after_model_edit(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _ghost())
    await _profile(engine=engine, storage=storage, model=model)
    edited = await _save(storage, model.model_copy(update={"sql_table": "t"}))
    log = _record(engine=engine, monkeypatch=monkeypatch)
    outcome = await _profile(engine=engine, storage=storage, model=edited)
    assert log
    assert outcome.errors == []
    assert (await _stored(storage, model="ghost", column="status")).sampled_values is not None


async def test_retry_after_column_edit(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _orders(extra=[Column(name="bad", sql=BAD_SQL, type=DataType.TEXT)]))
    await _profile(engine=engine, storage=storage, model=model)
    reloaded = await storage.get_model("orders", data_source=DS)
    fixed = await _save(storage, reloaded.model_copy(update={"columns": [
        c.model_copy(update={"sql": "channel"}) if c.name == "bad" else c for c in reloaded.columns
    ]}))
    log = _record(engine=engine, monkeypatch=monkeypatch)
    await _profile(engine=engine, storage=storage, model=fixed)
    assert len(_categorical(log)) == 1
    assert (await _stored(storage, column="bad")).sampled_values == ["web", "app", "store"]


async def test_engines_do_not_share_failures(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _ghost())
    await _profile(engine=engine, storage=storage, model=model)
    other = SlayerQueryEngine(storage=storage)
    log = _record(engine=other, monkeypatch=monkeypatch)
    await _profile(engine=other, storage=storage, model=model)
    assert len(log) == 2


async def test_failures_are_not_persisted(env) -> None:
    engine, storage = env
    model = await _save(storage, _orders(extra=[Column(name="bad", sql=BAD_SQL, type=DataType.TEXT)]))
    before = (await storage.get_model("orders", data_source=DS)).get_column("bad")
    await _profile(engine=engine, storage=storage, model=model)
    assert await _stored(storage, column="bad") == before


async def test_forced_refresh_of_failing_model(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _ghost())
    for name in ("status", "amount"):
        await storage.update_column_sampled(
            data_source=DS, model_name="ghost", column_name=name,
            sampled="KEEP", sampled_values=["KEEP"] if name == "status" else None, distinct_count=None,
        )
    before = {n: _triple(await _stored(storage, model="ghost", column=n)) for n in ("status", "amount")}
    model = await storage.get_model("ghost", data_source=DS)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    errors = await refresh_table_backed_model_sampled(model=model, engine=engine, storage=storage)
    assert len(log) == 2
    assert len(errors) == 1
    assert "ghost" in errors[0]
    after = {n: _triple(await _stored(storage, model="ghost", column=n)) for n in ("status", "amount")}
    assert after == before


async def test_forced_refresh_ignores_failure_cache_and_records(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _ghost())
    await _profile(engine=engine, storage=storage, model=model)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    await refresh_table_backed_model_sampled(model=model, engine=engine, storage=storage)
    assert len(log) == 2
    log.clear()
    await _profile(engine=engine, storage=storage, model=model)
    assert log == []


async def test_forced_refresh_keeps_sample_of_failing_column(env) -> None:
    engine, storage = env
    model = await _save(storage, _orders(extra=[Column(name="bad", sql=BAD_SQL, type=DataType.TEXT)]))
    await storage.update_column_sampled(
        data_source=DS, model_name="orders", column_name="bad",
        sampled="KEEP", sampled_values=["KEEP"], distinct_count=1,
    )
    errors = await refresh_table_backed_model_sampled(model=model, engine=engine, storage=storage)
    assert len(errors) == 1
    assert "bad" in errors[0]
    assert _triple(await _stored(storage, column="bad")) == ("KEEP", ["KEEP"], 1)
    assert (await _stored(storage, column="status")).sampled_values is not None


async def test_forced_refresh_records_model_failure_for_lazy_reads(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _ghost())
    await refresh_table_backed_model_sampled(model=model, engine=engine, storage=storage)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    await _profile(engine=engine, storage=storage, model=model)
    assert log == []


async def test_refresh_all_bounds_a_failing_model(env, monkeypatch) -> None:
    engine, storage = env
    await _save(storage, _ghost())
    await _save(storage, _orders())
    log = _record(engine=engine, monkeypatch=monkeypatch)
    errors = await refresh_all_table_backed_sampled(engine=engine, storage=storage, data_source=DS)
    assert len(errors) == 1
    assert "ghost" in errors[0]
    ghost_queries = [q for q in log if q.source_model == "ghost" or getattr(q.source_model, "source_name", None) == "ghost"]
    assert len(ghost_queries) == 2
    assert (await _stored(storage, column="status")).sampled_values is not None


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


async def test_model_level_failure_logs_once(env, caplog) -> None:
    engine, storage = env
    model = await _save(storage, _ghost())
    with caplog.at_level(logging.DEBUG):
        await _profile(engine=engine, storage=storage, model=model)
        await _profile(engine=engine, storage=storage, model=model)
    warnings = _profiling_warnings(caplog)
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert DS in msg
    assert "ghost" in msg
    assert re.search(r"\d+ columns", msg)
    assert "no such table" in msg
    assert "[SQL:" not in msg


async def test_persist_failure_keeps_fresh_value(env, monkeypatch, caplog) -> None:
    engine, storage = env
    model = await _save(storage, _orders())

    async def boom(**_kwargs):
        raise RuntimeError("disk on fire\nsecond line detail")

    monkeypatch.setattr(storage, "update_column_sampled", boom)
    [status] = _cols(model, "status")
    with caplog.at_level(logging.WARNING):
        outcome = await ensure_samples_fresh(model=model, columns=[status], engine=engine, storage=storage)
    assert outcome.columns[0].sampled_values == ["paid", "cancelled", "refunded"]
    warnings = _profiling_warnings(caplog)
    assert len(warnings) == 1
    assert all(part in warnings[0].getMessage() for part in (DS, "orders", "status", "disk on fire"))
    assert "second line detail" not in warnings[0].getMessage()
    assert len(outcome.errors) == 1
    assert "disk on fire" in outcome.errors[0]

    log = _record(engine=engine, monkeypatch=monkeypatch)
    await ensure_samples_fresh(model=model, columns=[status], engine=engine, storage=storage)
    assert len(log) == 1


async def test_persist_failure_on_one_column_only(env, monkeypatch, caplog) -> None:
    engine, storage = env
    model = await _save(storage, _orders())
    real = storage.update_column_sampled

    async def flaky(**kwargs):
        if kwargs["column_name"] == "amount":
            raise RuntimeError("disk on fire")
        return await real(**kwargs)

    monkeypatch.setattr(storage, "update_column_sampled", flaky)
    with caplog.at_level(logging.WARNING):
        outcome = await _profile(engine=engine, storage=storage, model=model)
    assert len(_profiling_warnings(caplog)) == 1
    assert len(outcome.errors) == 1
    assert "amount" in outcome.errors[0]
    assert next(c for c in outcome.columns if c.name == "amount").sampled == "5.0 .. 99.0"
    assert (await _stored(storage, column="status")).sampled_values is not None


async def test_persist_failure_renders_fresh_value_in_inspect(env, monkeypatch) -> None:
    engine, storage = env
    await _save(storage, _orders())

    async def boom(**_kwargs):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(storage, "update_column_sampled", boom)
    out = await InspectService(storage=storage, engine=engine).inspect(
        reference=f"{DS}.orders.status", entity_type="column", compact=False,
    )
    assert "paid" in out


async def test_forced_refresh_reports_persist_failure(env, monkeypatch) -> None:
    engine, storage = env
    model = await _save(storage, _orders())

    async def boom(**_kwargs):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(storage, "update_column_sampled", boom)
    errors = await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage, only_columns={"status", "amount"},
    )
    assert len(errors) == 2
    assert all("disk on fire" in e for e in errors)


# ---------------------------------------------------------------------------
# Row-level-security policy
# ---------------------------------------------------------------------------


def _policy_engine(storage: StorageBackend, *, org: str = "a") -> SlayerQueryEngine:
    return SlayerQueryEngine(
        storage=storage, policy=SessionPolicy(ruleset=ColumnFilterRuleset(column="org", value=org)),
    )


async def _store_status(storage: StorageBackend, *, model: str = "orders") -> None:
    await storage.update_column_sampled(
        data_source=DS, model_name=model, column_name="status",
        sampled="STORED", sampled_values=["STORED"], distinct_count=1,
    )


async def test_policy_engine_ignores_persisted_samples(env, monkeypatch) -> None:
    _, storage = env
    await _save(storage, _orders())
    await _store_status(storage)
    model = await storage.get_model("orders", data_source=DS)
    engine = _policy_engine(storage)
    persists = _count_persists(storage=storage, monkeypatch=monkeypatch)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    outcome = await _profile(engine=engine, storage=storage, model=model)
    assert len(_categorical(log)) == 2
    status = next(c for c in outcome.columns if c.name == "status")
    assert _triple(status) == ("paid", ["paid"], 1)
    assert next(c for c in outcome.columns if c.name == "amount").sampled == "10.0 .. 20.0"
    assert persists == []
    assert (await _stored(storage, column="status")).sampled_values == ["STORED"]


async def test_policy_engine_reuses_its_own_samples(env, monkeypatch, clock) -> None:
    _, storage = env
    model = await _save(storage, _orders())
    engine = _policy_engine(storage)
    await _profile(engine=engine, storage=storage, model=model)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    clock.now += TTL - 1
    outcome = await _profile(engine=engine, storage=storage, model=model)
    assert log == []
    assert next(c for c in outcome.columns if c.name == "status").sampled_values == ["paid"]
    clock.now += 2
    await _profile(engine=engine, storage=storage, model=model)
    assert log


async def test_policy_model_failure_still_returns_scoped_samples(env, monkeypatch) -> None:
    _, storage = env
    bad = [Column(name=f"bad{i}", sql=f"no_such_fn(status, {i})", type=DataType.TEXT) for i in range(3)]
    model = await _save(storage, _orders(extra=bad))
    engine = _policy_engine(storage)
    first = await _profile(engine=engine, storage=storage, model=model)
    assert any("profiling unavailable" in e for e in first.errors)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    outcome = await _profile(engine=engine, storage=storage, model=model)
    assert log == []
    assert next(c for c in outcome.columns if c.name == "status").sampled_values == ["paid"]


async def test_policy_engines_do_not_share_samples(env) -> None:
    _, storage = env
    model = await _save(storage, _orders())
    await _profile(engine=_policy_engine(storage, org="a"), storage=storage, model=model)
    outcome = await _profile(engine=_policy_engine(storage, org="b"), storage=storage, model=model)
    assert next(c for c in outcome.columns if c.name == "status").sampled_values == ["cancelled", "refunded"]


async def test_policy_failure_never_returns_persisted_samples(env) -> None:
    _, storage = env
    await _save(storage, _ghost())
    await _store_status(storage, model="ghost")
    model = await storage.get_model("ghost", data_source=DS)
    outcome = await _profile(engine=_policy_engine(storage), storage=storage, model=model)
    assert _triple(next(c for c in outcome.columns if c.name == "status")) == (None, None, None)


async def test_policy_never_returns_persisted_samples_on_unprofiled_columns(env) -> None:
    _, storage = env
    await _save(storage, _orders(extra=[
        Column(name="secret", sql="status", type=DataType.TEXT, hidden=True),
        Column(name="bad", sql=BAD_SQL, type=DataType.TEXT),
    ]))
    for name in ("id", "secret", "bad"):
        await storage.update_column_sampled(
            data_source=DS, model_name="orders", column_name=name,
            sampled="STORED", sampled_values=None, distinct_count=None,
        )
    model = await storage.get_model("orders", data_source=DS)
    engine = _policy_engine(storage)
    for _ in range(2):  # second call serves the failure cache
        outcome = await _profile(engine=engine, storage=storage, model=model)
        for name in ("id", "secret", "bad"):
            assert next(c for c in outcome.columns if c.name == name).sampled is None, name


async def test_inspect_model_under_policy_renders_scoped_samples(env) -> None:
    _, storage = env
    await _save(storage, _orders())
    await _store_status(storage)
    model = await storage.get_model("orders", data_source=DS)
    out = await render_model_inspection(
        model=model, storage=storage, engine=_policy_engine(storage), format="json", sections=["columns"],
    )
    rendered = {c["name"]: c["sampled"] for c in json.loads(out)["columns"]}
    assert rendered["status"] == "paid"
    assert "STORED" not in out


async def test_inspect_column_under_policy_renders_scoped_samples(env) -> None:
    _, storage = env
    await _save(storage, _orders())
    await _store_status(storage)
    out = await InspectService(storage=storage, engine=_policy_engine(storage)).inspect(
        reference=f"{DS}.orders.status", entity_type="column", compact=False,
    )
    assert "paid" in out
    assert "STORED" not in out


async def test_search_hit_text_under_policy(env) -> None:
    _, storage = env
    await _save(storage, _orders())
    await _store_status(storage)
    svc = SearchService(storage=storage, engine=_policy_engine(storage))
    resp = await svc.search(entities=[f"{DS}.orders.status"], max_results=10, compact=False)
    hits = [h for h in resp.results if h.kind == "column" and h.id == f"{DS}.orders.status"]
    assert hits
    assert all("STORED" not in h.text for h in hits)
    assert any("paid" in h.text for h in hits)


async def test_search_hit_text_under_policy_when_profiling_fails(env) -> None:
    _, storage = env
    await _save(storage, _ghost())
    await _store_status(storage, model="ghost")
    svc = SearchService(storage=storage, engine=_policy_engine(storage))
    resp = await svc.search(entities=[f"{DS}.ghost.status"], max_results=10, compact=False)
    hits = [h for h in resp.results if h.kind == "column" and h.id == f"{DS}.ghost.status"]
    assert hits
    assert all("STORED" not in h.text for h in hits)
