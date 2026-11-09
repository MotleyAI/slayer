"""Forms removed in SLayer 2.0 are gone; the deprecation helper stays for forms removed in a later major.

Spec: compatibility/removal-tripwire, database/client-contract.
"""

from __future__ import annotations

import sys
import warnings
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

import slayer
from slayer.core.deprecation import deprecations_on_one_line, warn_deprecated
from slayer.core.errors import MemoryNotFoundError
from slayer.core.models import DatasourceConfig
from slayer.database import client as client_module
from slayer.database import engine_factory
from slayer.database.client import DelegatingSqlClient, ExecutionResult, SlayerSQLClient, SqlClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.sql.dialects.base import ExplainStatements, SqlDialect
from slayer.storage.base import default_storage_path
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import run_cli_in_process
from tests._compat_helpers import future_warnings, recorded
from tests._gateway_harness import backend_of, orders_model, seed_shop, shop_datasource, shop_storage

COUNT_ORDERS = {"source_model": "orders", "measures": [{"formula": "*:count", "name": "n"}]}
_MESSAGE = "old() is deprecated; use new(). It works until SLayer 3.0."


def _old() -> None:
    warn_deprecated(_MESSAGE, removal="3.0")


def test_a_deprecation_names_its_callers_line() -> None:
    with recorded() as caught:
        line = sys._getframe().f_lineno + 1
        _old()
    assert [(str(w.message), w.filename, w.lineno) for w in future_warnings(caught)] == [(_MESSAGE, __file__, line)]


def test_a_deprecation_prints_on_one_stderr_line(capsys: pytest.CaptureFixture[str]) -> None:
    with deprecations_on_one_line():
        warnings.simplefilter("always")
        _old()
    assert capsys.readouterr().err.splitlines() == [f"Deprecated: {_MESSAGE}"]


def test_the_package_is_2_0_0_dev0() -> None:
    assert slayer.__version__ == "2.0.0.dev0"


def test_models_dir_is_not_a_cli_option(tmp_path: Path) -> None:
    result = run_cli_in_process(["datasources", "--models-dir", str(tmp_path), "list"])
    assert result.returncode != 0
    assert "unrecognized arguments: --models-dir" in result.stderr


def test_slayer_models_dir_does_not_decide_the_storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SLAYER_STORAGE", raising=False)
    monkeypatch.delenv("SLAYER_MODELS_DIR", raising=False)
    expected = default_storage_path()
    monkeypatch.setenv("SLAYER_MODELS_DIR", str(tmp_path / "legacy"))
    with recorded() as caught:
        assert default_storage_path() == expected
    assert future_warnings(caught) == []


async def test_inspect_model_is_not_an_mcp_tool(tmp_path: Path) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path / "store")), _seed_help=False)
    names = {tool.name for tool in await server.list_tools()}
    assert "inspect" in names
    assert "inspect_model" not in names


def test_memory_not_found_has_no_identifier() -> None:
    error = MemoryNotFoundError("m-1")
    assert error.memory_id == "m-1"
    assert not hasattr(error, "identifier")


@pytest.mark.parametrize(("owner", "name"), [
    (client_module, "get_column_types_sync"),
    (SlayerSQLClient, "get_column_types"),
    (DelegatingSqlClient, "get_column_types"),
    (SlayerQueryEngine, "get_column_types"),
    (SqlDialect, "build_explain_sql"),
])
def test_the_removed_probes_and_builders_are_gone(owner: object, name: str) -> None:
    assert not hasattr(owner, name)


class _Bare:
    """A client serving neither ``explain`` nor ``get_result_column_types``."""

    def __init__(self, datasource: DatasourceConfig) -> None:
        self.datasource = datasource

    async def aclose(self) -> None:
        pass

    def close(self) -> None:
        pass


async def test_the_engine_uses_its_factorys_client_as_built(tmp_path: Path) -> None:
    built: list[_Bare] = []

    def factory(datasource: DatasourceConfig) -> _Bare:
        built.append(_Bare(datasource))
        return built[-1]

    ds = DatasourceConfig(name="shop", type="sqlite", database=str(tmp_path / "shop.db"))
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "store")), client_factory=factory)  # pyright: ignore[reportArgumentType]
    try:
        assert engine.client_for(ds) is built[0]
    finally:
        await engine.aclose()
        engine.close()


class _StatementsOnly(DelegatingSqlClient):
    executed: list[str]

    def __init__(self, inner: SqlClient) -> None:
        super().__init__(inner)
        self.executed = []

    async def execute(self, sql: str) -> ExecutionResult:
        self.executed.append(sql)
        return await self.inner.execute(sql)


@pytest.fixture
async def shop(tmp_path: Path) -> AsyncIterator[tuple[DatasourceConfig, YAMLStorage]]:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    ds = shop_datasource(db)
    storage = await shop_storage(str(tmp_path / "store"), datasource=ds, models=[orders_model()])
    try:
        yield ds, storage
    finally:
        engine_factory.invalidate_engine(ds)
        engine_factory.invalidate_engine(backend_of(ds))


async def test_an_execute_only_gateway_explains_through_the_wrapped_client(
    shop: tuple[DatasourceConfig, YAMLStorage], monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, storage = shop
    gateways: list[_StatementsOnly] = []
    explained: list[str] = []
    real_explain = SlayerSQLClient.explain

    async def _spy(self: SlayerSQLClient, statements: ExplainStatements, *args: Any, **kwargs: Any) -> ExecutionResult:
        explained.append(statements.statement)
        return await real_explain(self, statements, *args, **kwargs)

    monkeypatch.setattr(SlayerSQLClient, "explain", _spy)

    def factory(datasource: DatasourceConfig) -> _StatementsOnly:
        gateways.append(_StatementsOnly(SlayerSQLClient(datasource=backend_of(datasource))))
        return gateways[-1]

    engine = SlayerQueryEngine(storage=storage, client_factory=factory)
    try:
        with recorded() as caught:
            response = await engine.execute(COUNT_ORDERS, explain=True)
    finally:
        await engine.aclose()
        engine.close()
    assert response.data
    assert len(explained) == 1
    assert "EXPLAIN" in explained[0].upper()
    assert not any("EXPLAIN" in sql.upper() for gateway in gateways for sql in gateway.executed)
    assert future_warnings(caught) == []
