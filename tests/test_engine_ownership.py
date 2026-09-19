"""DEV-1943 L2 — one disposing owner per engine.

``SlayerSQLClient.close()`` disposes the client's *private* in-memory engine
(and never a factory-owned one), a finalizer disposes it if the client is
collected unclosed, and ``SlayerQueryEngine.close()`` closes every client,
survives a failing one, clears its cache, is idempotent and stays reusable.
``aclose()`` still spares synchronous engines.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import sys
import tempfile
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient
from slayer.sql.engine_factory import _sql_client_cache_key
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

_ON_313 = sys.version_info >= (3, 13)


def _mem_ds(name: str = "mem") -> DatasourceConfig:
    return DatasourceConfig(name=name, type="sqlite", database=":memory:")


@pytest.fixture
def workspace() -> Iterator[Path]:
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _seed_counter(db: Path, rows: int) -> None:
    with transaction(db) as con:
        con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        con.executemany("INSERT INTO t (v) VALUES (?)", [(i,) for i in range(rows)])


def _file_engine(db: Path) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=str(db.parent / "store"))
    asyncio.run(storage.save_datasource(
        DatasourceConfig(name="lite", type="sqlite", database=str(db)),
    ))
    asyncio.run(storage.save_model(SlayerModel(
        name="t", data_source="lite", sql_table="t",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="v", sql="v", type=DataType.INT),
        ],
    )))
    return SlayerQueryEngine(storage=storage)


def _seeded_in_memory_engine(
    workspace: Path, *, rows: int,
) -> tuple[SlayerQueryEngine, SlayerSQLClient]:
    """Query engine over an in-memory datasource whose db is seeded through the
    client's own (StaticPool-shared) engine, registered under its real key."""
    ds = _mem_ds("mem")
    storage = YAMLStorage(base_dir=str(workspace / "mem_store"))
    asyncio.run(storage.save_datasource(ds))
    asyncio.run(storage.save_model(
        SlayerModel(
            name="t", data_source="mem", sql_table="t",
            columns=[Column(name="v", sql="v", type=DataType.INT)],
        ),
        _validate=False,
    ))
    engine = SlayerQueryEngine(storage=storage)
    client = SlayerSQLClient(datasource=ds)
    raw = client._get_sync_engine_for_client()
    assert raw is not None
    with raw.connect() as conn:
        conn.exec_driver_sql("CREATE TABLE t (v INTEGER)")
        conn.exec_driver_sql(
            "INSERT INTO t (v) VALUES " + ", ".join(f"({i})" for i in range(rows))
        )
        conn.commit()
    engine._sql_clients[_sql_client_cache_key(ds)] = client
    return engine, client


@contextmanager
def _dispose_spy():
    """Class-level spy on ``Engine.dispose``, installed BEFORE engine construction
    so it records disposal whether ``close()`` / the finalizer calls dispose
    directly or through a callback that captured the (now-mocked) bound method —
    an instance-level patch applied afterwards would miss the latter (D4)."""
    with patch.object(sa.engine.base.Engine, "dispose", autospec=True) as spy:
        yield spy


def _disposed(spy) -> list:
    return [call.args[0] for call in spy.call_args_list if call.args]


def _count_disposed(spy, engine) -> int:
    return sum(1 for disposed in _disposed(spy) if disposed is engine)


# --------------------------------------------------------------------------- #
# SlayerSQLClient.close()
# --------------------------------------------------------------------------- #
class TestClientClose:

    def test_close_disposes_the_private_in_memory_engine_once(self) -> None:
        with _dispose_spy() as spy:
            client = SlayerSQLClient(datasource=_mem_ds())
            engine = client._get_sync_engine_for_client()
            assert engine is not None
            client.close()
            assert _count_disposed(spy, engine) == 1
            client.close()  # idempotent
            assert _count_disposed(spy, engine) == 1

    def test_close_never_disposes_a_factory_engine(self) -> None:
        engine_factory.reset_cache()
        ds = DatasourceConfig(name="file", type="sqlite", database="/tmp/slayer-own.db")
        with _dispose_spy() as spy:
            client = SlayerSQLClient(datasource=ds)
            factory_engine = client._get_sync_engine_for_client()
            assert factory_engine is engine_factory.get_engine(ds)
            client.close()
            assert _count_disposed(spy, factory_engine) == 0
            assert engine_factory.get_engine(ds) is factory_engine
        engine_factory.reset_cache()

    def test_private_engine_rebuilt_on_discard_is_still_disposed(self) -> None:
        """D4 regression: a finalizer bound to the first engine must be detached
        on discard and re-registered for the rebuilt one, so neither leaks."""
        with _dispose_spy() as spy:
            client = SlayerSQLClient(datasource=_mem_ds())
            first = client._get_sync_engine_for_client()
            assert first is not None
            client._discard_sync_engine_on_auth_failure(Exception("authentication failed"))
            assert _count_disposed(spy, first) == 1
            second = client._get_sync_engine_for_client()
            assert second is not None
            assert second is not first
            client.close()
            assert _count_disposed(spy, second) == 1
            assert _count_disposed(spy, first) == 1  # not disposed twice

    def test_private_engine_disposed_on_collection(self) -> None:
        with _dispose_spy() as spy:
            client = SlayerSQLClient(datasource=_mem_ds())
            engine = client._get_sync_engine_for_client()
            del client
            gc.collect()
            assert _count_disposed(spy, engine) >= 1, (
                "finalizer did not dispose the private engine on collection"
            )

    @pytest.mark.skipif(not _ON_313, reason="`unclosed database` ResourceWarning is 3.13+")
    def test_unreferenced_client_leaks_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client = SlayerSQLClient(datasource=_mem_ds())
            client._get_sync_engine_for_client()
            del client
            gc.collect()
        unclosed = [w for w in caught if "unclosed database" in str(w.message).lower()]
        assert not unclosed, [str(w.message) for w in unclosed]

    def test_close_disposes_a_fresh_empty_in_memory_db_on_reuse(self) -> None:
        client = SlayerSQLClient(datasource=_mem_ds())
        first = client._get_sync_engine_for_client()
        assert first is not None
        with first.connect() as conn:
            conn.exec_driver_sql("CREATE TABLE seeded (v INTEGER)")
            conn.exec_driver_sql("INSERT INTO seeded (v) VALUES (1)")
            conn.commit()
        client.close()
        second = client._get_sync_engine_for_client()
        assert second is not None
        assert second is not first
        with second.connect() as conn:
            tables = conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        assert tables == [], "reused in-memory datasource must start empty"

    def test_close_warns_and_retains_a_loop_bound_async_engine(self, caplog) -> None:
        """A loop-bound async engine can only be disposed by aclose() inside its
        event loop, so a synchronous close() must NOT fake-dispose it (which
        would swallow a MissingGreenlet) — it warns and leaves the reference in
        place so aclose() can still reach it."""
        client = SlayerSQLClient(
            datasource=DatasourceConfig(name="pg", type="postgres", host="h", database="db"),
        )
        fake_async = MagicMock()
        client._async_engine = fake_async
        with caplog.at_level(logging.WARNING, logger="slayer.sql.client"):
            client.close()
        fake_async.sync_engine.dispose.assert_not_called()
        fake_async.dispose.assert_not_called()
        assert client._async_engine is fake_async, "async engine must not be dropped by sync close()"
        assert any("async" in r.message.lower() for r in caplog.records)


# --------------------------------------------------------------------------- #
# SlayerQueryEngine.close()
# --------------------------------------------------------------------------- #
class TestQueryEngineClose:

    def _engine(self, workspace: Path) -> SlayerQueryEngine:
        return SlayerQueryEngine(storage=YAMLStorage(base_dir=str(workspace)))

    def test_close_closes_every_client_and_clears(self, workspace: Path) -> None:
        engine = self._engine(workspace)
        c1, c2 = MagicMock(), MagicMock()
        engine._sql_clients = {("a", "x", ""): c1, ("b", "y", ""): c2}
        engine.close()
        c1.close.assert_called_once()
        c2.close.assert_called_once()
        assert engine._sql_clients == {}

    def test_close_continues_past_a_failing_client(self, workspace: Path) -> None:
        engine = self._engine(workspace)
        bad, good = MagicMock(), MagicMock()
        bad.close.side_effect = RuntimeError("boom")
        engine._sql_clients = {("a", "x", ""): bad, ("b", "y", ""): good}
        engine.close()  # must not raise
        good.close.assert_called_once()
        assert engine._sql_clients == {}

    def test_close_is_idempotent(self, workspace: Path) -> None:
        engine = self._engine(workspace)
        engine._sql_clients = {("a", "x", ""): MagicMock()}
        engine.close()
        engine.close()  # no error the second time
        assert engine._sql_clients == {}

    def test_close_disposes_only_private_engines_end_to_end(self, workspace: Path) -> None:
        """Scenario: closing a query engine that used an in-memory *and* a
        file-backed datasource disposes the in-memory client's private engine,
        not the factory-owned file engine; a second close is a no-op."""
        engine_factory.reset_cache()
        with _dispose_spy() as spy:
            engine, mem_client = _seeded_in_memory_engine(workspace, rows=2)
            mem_engine = mem_client._sync_engine
            assert mem_engine is not None
            file_db = workspace / "own.db"
            _seed_counter(file_db, rows=1)
            file_ds = DatasourceConfig(name="lite", type="sqlite", database=str(file_db))
            file_client = SlayerSQLClient(datasource=file_ds)
            file_engine = file_client._get_sync_engine_for_client()
            assert file_engine is engine_factory.get_engine(file_ds)
            engine._sql_clients[_sql_client_cache_key(file_ds)] = file_client
            engine.close()
            engine.close()  # second close is a no-op
            assert _count_disposed(spy, mem_engine) == 1
            assert _count_disposed(spy, file_engine) == 0
            assert engine._sql_clients == {}
        engine_factory.reset_cache()

    def test_closed_engine_is_reusable(self, workspace: Path) -> None:
        db = workspace / "reuse.db"
        _seed_counter(db, rows=3)
        engine = _file_engine(db)
        first = engine.execute_sync(SlayerQuery(source_model="t", measures=["*:count"]))  # type: ignore[arg-type]
        assert first.data[0]["t._count"] == 3
        engine.close()
        again = engine.execute_sync(SlayerQuery(source_model="t", measures=["*:count"]))  # type: ignore[arg-type]
        assert again.data[0]["t._count"] == 3

    def test_in_memory_query_engine_rebuilds_empty_after_close(self, workspace: Path) -> None:
        """Scenario: after close the in-memory datasource starts from an empty
        database, and the query engine stays reusable — a fresh client sees no
        trace of the seeded table, then a re-seeded query succeeds."""
        engine_factory.reset_cache()
        engine, _client = _seeded_in_memory_engine(workspace, rows=2)
        seeded = asyncio.run(engine.execute(SlayerQuery(source_model="t", measures=["*:count"])))  # type: ignore[arg-type]
        assert seeded.data[0]["t._count"] == 2
        engine.close()
        assert engine._sql_clients == {}
        # The fresh in-memory db is empty — the seeded table is specifically gone.
        empty_probe = engine.execute(SlayerQuery(source_model="t", measures=["*:count"]))  # type: ignore[arg-type]
        with pytest.raises(Exception) as excinfo:
            asyncio.run(empty_probe)
        assert "no such table" in str(excinfo.value).lower()
        # Reusable: re-seed through a fresh client and the query succeeds again.
        ds = _mem_ds("mem")
        fresh = SlayerSQLClient(datasource=ds)
        raw = fresh._get_sync_engine_for_client()
        assert raw is not None
        with raw.connect() as conn:
            conn.exec_driver_sql("CREATE TABLE t (v INTEGER)")
            conn.exec_driver_sql("INSERT INTO t (v) VALUES (5)")
            conn.commit()
        engine._sql_clients[_sql_client_cache_key(ds)] = fresh
        reused = asyncio.run(engine.execute(SlayerQuery(source_model="t", measures=["*:count"])))  # type: ignore[arg-type]
        assert reused.data[0]["t._count"] == 1
        engine_factory.reset_cache()

    def test_unreferenced_query_engine_disposes_private_engines(self, workspace: Path) -> None:
        with _dispose_spy() as spy:
            engine, client = _seeded_in_memory_engine(workspace, rows=1)
            private = client._sync_engine
            assert private is not None
            del engine, client
            gc.collect()
            assert _count_disposed(spy, private) >= 1, (
                "collecting an unreferenced query engine must dispose its private engines"
            )

    @pytest.mark.skipif(not _ON_313, reason="`unclosed database` ResourceWarning is 3.13+")
    def test_unreferenced_query_engine_leaks_no_warning(self, workspace: Path) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            engine, client = _seeded_in_memory_engine(workspace, rows=1)
            del engine, client
            gc.collect()
        unclosed = [w for w in caught if "unclosed database" in str(w.message).lower()]
        assert not unclosed, [str(w.message) for w in unclosed]

    async def test_aclose_still_spares_the_sync_engine(self, workspace: Path) -> None:
        engine = self._engine(workspace)
        ds = DatasourceConfig(name="file", type="sqlite", database=str(workspace / "s.db"))
        client = SlayerSQLClient(datasource=ds)
        sync_engine = client._get_sync_engine_for_client()
        assert sync_engine is not None
        engine._sql_clients[_sql_client_cache_key(ds)] = client
        with patch.object(sync_engine, "dispose") as disposed:
            await engine.aclose()
        disposed.assert_not_called()
        assert client._sync_engine is sync_engine
        engine_factory.reset_cache()
