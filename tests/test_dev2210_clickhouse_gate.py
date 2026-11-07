"""A model filter read by association needs ClickHouse 25.4+, gated like a pushed-down query filter."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Callable, Iterator

import pytest
import sqlglot

from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import ExecutionResult, SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage
from tests._ch_fake_http import CORRELATED_SETTING, FakeClickHouse, fake_ch_engine, route_engine
from tests._dev2210_fixtures import table_models

UPS = SlayerQuery.model_validate({"source_model": "ups_orders", "dimensions": ["id"]})
DONE = SlayerQuery.model_validate({"source_model": "returns", "dimensions": ["id", "done_orders.status"]})


@pytest.fixture
def serve(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[str], FakeClickHouse]]:
    """``serve(version)`` routes every ClickHouse client to a fake server reporting ``version``."""
    with contextlib.ExitStack() as stack:
        async def no_execute(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async
            return ExecutionResult(rows=[])

        monkeypatch.setattr(SlayerSQLClient, "execute", no_execute)

        def start(version: str) -> FakeClickHouse:
            fake = FakeClickHouse(version=version)
            route_engine(monkeypatch, stack.enter_context(fake_ch_engine(fake)))
            return fake

        yield start


@pytest.fixture
async def engine(tmp_path) -> AsyncIterator[SlayerQueryEngine]:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(DatasourceConfig(name="test", type="clickhouse"))
    for model in table_models():
        await storage.save_model(model, _validate=False)
    eng = SlayerQueryEngine(storage=storage)
    try:
        yield eng
    finally:
        eng.close()


async def test_below_min_version_fails_closed(serve, engine):
    serve("25.3.1.100")
    with pytest.raises(SlayerError, match="25.4"):
        await engine.execute(UPS, dry_run=True)


async def test_supported_version_attaches_the_setting(serve, engine):
    serve("25.4.1.100")
    sql = (await engine.execute(UPS, dry_run=True)).sql
    assert sql is not None
    settings = sqlglot.parse_one(sql, dialect="clickhouse").args.get("settings") or []
    assert any(CORRELATED_SETTING in s.sql() for s in settings), sql


async def test_inlined_filter_needs_no_probe(serve, engine):
    fake = serve("25.3.1.100")
    sql = (await engine.execute(DONE, dry_run=True)).sql
    assert sql is not None
    assert CORRELATED_SETTING not in sql
    assert fake.permission_checks() == 0
