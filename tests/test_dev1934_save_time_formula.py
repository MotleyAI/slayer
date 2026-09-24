"""Save-time aggregation-formula parse check at every engine create/edit door."""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.async_utils import run_sync
from slayer.cli import main as cli_main
from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.models import Aggregation, Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.sql.client import SlayerSQLClient
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

_DS = "ds"
_BROKEN = "SUM({value}"


def _seed(db_path: str) -> None:
    with transaction(db_path) as conn:
        conn.executescript(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL NOT NULL);"
            "INSERT INTO orders VALUES (1, 2.0), (2, 3.0);"
        )


def _model(formula: str, *, name: str = "orders", data_source: str = _DS,
           agg: str = "custom_agg", sql: str | None = None) -> SlayerModel:
    return SlayerModel(
        name=name,
        sql_table=None if sql else "orders",
        sql=sql,
        data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
        aggregations=[Aggregation(name=agg, formula=formula)],
    )


@pytest.fixture
async def seeded(tmp_path: Path) -> AsyncIterator[tuple[SlayerQueryEngine, YAMLStorage]]:
    db_path = str(tmp_path / "live.db")
    _seed(db_path)
    store = YAMLStorage(base_dir=str(tmp_path / "store"))
    await store.save_datasource(DatasourceConfig(name=_DS, type="sqlite", database=db_path))
    engine = SlayerQueryEngine(storage=store)
    try:
        yield engine, store
    finally:
        engine.close()


class TestEngineCheck:
    async def test_unparseable_formula_rejected_naming_model_and_aggregation(self, seeded) -> None:
        engine, store = seeded
        with pytest.raises(SlayerError) as ei:
            await engine.save_model(_model(_BROKEN))
        assert "orders" in str(ei.value) and "custom_agg" in str(ei.value)
        assert await store.get_model("orders", data_source=_DS) is None

    async def test_qualifier_position_placeholder_rejected(self, seeded) -> None:
        engine, store = seeded
        with pytest.raises(SlayerError, match="custom_agg"):
            await engine.save_model(_model("SUM({t}.amount)"))
        assert await store.get_model("orders", data_source=_DS) is None

    async def test_query_time_only_placeholder_accepted(self, seeded) -> None:
        engine, store = seeded
        await engine.save_model(_model("SUM({value} * {scale})"))
        assert await store.get_model("orders", data_source=_DS) is not None

    async def test_formula_without_value_accepted(self, seeded) -> None:
        engine, store = seeded
        await engine.save_model(_model("COUNT(*)"))
        assert await store.get_model("orders", data_source=_DS) is not None

    async def test_unresolvable_datasource_uses_generic_dialect(self, seeded) -> None:
        engine, store = seeded
        with pytest.raises(SlayerError, match="custom_agg"):
            await engine.save_model(_model(_BROKEN, data_source="nowhere"))
        await engine.save_model(_model("SUM({value})", data_source="nowhere"))
        assert await store.get_model("orders", data_source="nowhere") is not None

    async def test_datasource_dialect_is_used(self, tmp_path: Path) -> None:
        store = YAMLStorage(base_dir=str(tmp_path / "store"))
        await store.save_datasource(
            DatasourceConfig(name="duck", type="duckdb", database=str(tmp_path / "x.duckdb")),
        )
        engine = SlayerQueryEngine(storage=store)
        try:
            await engine.save_model(_model(
                "SUM({value}) + STRUCT_EXTRACT({'a': 1}, 'a')", data_source="duck",
            ))
        finally:
            engine.close()
        assert await store.get_model("orders", data_source="duck") is not None

    async def test_check_runs_before_storage_or_trial_execute(
        self, seeded, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        engine, store = seeded
        executed: list[str] = []
        saved: list[str] = []

        async def _spy_exec(self, sql: str) -> dict[str, str]:  # noqa: ANN001
            executed.append(sql)
            return {}

        original_save = YAMLStorage.save_model

        async def _spy_save(self, model, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            saved.append(model.name)
            return await original_save(self, model, *args, **kwargs)

        monkeypatch.setattr(SlayerSQLClient, "get_column_types", _spy_exec)
        monkeypatch.setattr(YAMLStorage, "save_model", _spy_save)
        with pytest.raises(SlayerError):
            await engine.save_model(_model(_BROKEN, sql="SELECT id, amount FROM orders"))
        assert executed == []
        assert saved == []


@pytest.fixture
def rest(seeded) -> Iterator[tuple[TestClient, YAMLStorage]]:
    _engine, store = seeded
    yield TestClient(create_app(storage=store)), store


class TestRest:
    async def test_create_rejected(self, rest) -> None:
        client, store = rest
        resp = client.post("/models", json=_model(_BROKEN).model_dump(mode="json"))
        assert resp.status_code == 400
        assert "orders" in resp.json()["detail"] and "custom_agg" in resp.json()["detail"]
        assert await store.get_model("orders", data_source=_DS) is None

    async def test_update_rejected_leaves_original(self, rest) -> None:
        client, store = rest
        ok = client.post("/models", json=_model("SUM({value})").model_dump(mode="json"))
        assert ok.status_code == 200
        resp = client.put("/models/orders", json=_model(_BROKEN).model_dump(mode="json"))
        assert resp.status_code == 400
        kept = await store.get_model("orders", data_source=_DS)
        assert kept.aggregations[0].formula == "SUM({value})"


class TestCli:
    def test_create_rejected(self, seeded, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        _engine, store = seeded
        path = tmp_path / "m.yaml"
        path.write_text(yaml.safe_dump(_model(_BROKEN).model_dump(mode="json", exclude_none=True)))
        argv = sys.argv
        sys.argv = ["slayer", "models", "--storage", store.base_dir, "create", str(path)]
        try:
            with pytest.raises(SystemExit) as ei:
                cli_main()
        finally:
            sys.argv = argv
        assert ei.value.code == 1
        out = capsys.readouterr().out
        assert "orders" in out and "custom_agg" in out
        assert run_sync(store.get_model("orders", data_source=_DS)) is None


@pytest.fixture
def mcp(seeded) -> tuple[Any, SlayerQueryEngine, YAMLStorage]:
    engine, store = seeded
    return create_mcp_server(storage=store), engine, store


async def _call(server: Any, name: str, arguments: dict[str, Any]) -> str:
    blocks, _ = await server.call_tool(name=name, arguments=arguments)
    return blocks[0].text


_COLUMNS = [
    {"name": "id", "sql": "id", "type": "number", "primary_key": True},
    {"name": "amount", "sql": "amount", "type": "number"},
]


class TestMcp:
    async def test_create_model_rejects_broken_formula(self, mcp) -> None:
        server, _engine, store = mcp
        out = await _call(server, "create_model", {
            "name": "orders", "sql_table": "orders", "data_source": _DS, "columns": _COLUMNS,
            "aggregations": [{"name": "custom_agg", "formula": _BROKEN}],
        })
        assert "Error" in out and "orders" in out and "custom_agg" in out
        assert await store.get_model("orders", data_source=_DS) is None

    async def test_create_model_with_aggregation_is_queryable(self, mcp) -> None:
        server, engine, store = mcp
        out = await _call(server, "create_model", {
            "name": "orders", "sql_table": "orders", "data_source": _DS, "columns": _COLUMNS,
            "aggregations": [{"name": "sum_sq", "formula": "SUM({value} * {value})"}],
        })
        assert "created" in out, out
        saved = await store.get_model("orders", data_source=_DS)
        assert [a.name for a in saved.aggregations] == ["sum_sq"]
        resp = await engine.execute(SlayerQuery(
            source_model="orders", measures=[ModelMeasure(formula="amount:sum_sq", name="m")],
        ))
        assert resp.data[0]["orders.m"] == pytest.approx(13.0)

    async def test_create_model_aggregations_with_query_rejected(self, mcp) -> None:
        server, _engine, _store = mcp
        out = await _call(server, "create_model", {
            "name": "qb", "query": {"source_model": "orders", "measures": ["*:count"]},
            "aggregations": [{"name": "sum_sq", "formula": "SUM({value} * {value})"}],
        })
        assert "Error" in out and "aggregations" in out

    async def test_edit_model_broken_formula_leaves_original(self, mcp) -> None:
        server, engine, store = mcp
        await engine.save_model(_model("SUM({value})"))
        out = await _call(server, "edit_model", {
            "model_name": "orders", "data_source": _DS,
            "aggregations": [{"name": "custom_agg", "formula": _BROKEN}],
        })
        assert "Error" in out or "error" in out
        kept = await store.get_model("orders", data_source=_DS)
        assert kept.aggregations[0].formula == "SUM({value})"
