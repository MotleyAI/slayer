"""``statement_timeout_skipped``: the payload, its carrier, and its place on ``SlayerResponse``."""

from __future__ import annotations

import pytest
from pydantic import TypeAdapter, ValidationError

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.warnings import (
    AnySlayerWarning,
    SlayerStatementTimeoutSkippedWarning,
    StatementTimeoutSkippedWarning,
)
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.sql.client import ExecutionResult, SlayerSQLClient
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

_READONLY = StatementTimeoutSkippedWarning(datasource="ch_prod", timeout_seconds=30, reason="readonly_user")
_REJECTED = StatementTimeoutSkippedWarning(datasource="pg_prod", timeout_seconds=120, reason="timeout_rejected")


class TestPayload:
    def test_kind(self) -> None:
        assert _READONLY.kind == "statement_timeout_skipped"

    @pytest.mark.parametrize("payload", [_READONLY, _REJECTED])
    def test_round_trips_through_union(self, payload: StatementTimeoutSkippedWarning) -> None:
        back = TypeAdapter(AnySlayerWarning).validate_python(payload.model_dump(mode="json"))
        assert type(back) is StatementTimeoutSkippedWarning
        assert back == payload

    def test_round_trips_on_response(self) -> None:
        response = SlayerResponse(data=[], warnings=[_READONLY])
        back = SlayerResponse.model_validate_json(response.model_dump_json())
        assert back.warnings == [_READONLY]

    def test_unknown_reason_rejected(self) -> None:
        with pytest.raises(ValidationError):
            StatementTimeoutSkippedWarning.model_validate(
                {"datasource": "d", "timeout_seconds": 1, "reason": "other"},
            )

    def test_readonly_message_names_remedy(self) -> None:
        message = _READONLY.human_message()
        assert "ch_prod" in message
        assert "30" in message
        assert "readonly = 2" in message

    def test_rejected_message_names_datasource_and_timeout(self) -> None:
        message = _REJECTED.human_message()
        assert "pg_prod" in message
        assert "120" in message


class TestCarrier:
    def test_is_user_warning_with_one_wording(self) -> None:
        carrier = SlayerStatementTimeoutSkippedWarning(_READONLY)
        assert isinstance(carrier, UserWarning)
        assert carrier.payload is _READONLY
        assert str(carrier) == _READONLY.human_message()


# ---------------------------------------------------------------------------
# Engine: a skipped timeout reaches SlayerResponse.warnings
# ---------------------------------------------------------------------------


@pytest.fixture
async def engine(tmp_path, monkeypatch) -> SlayerQueryEngine:
    db_path = tmp_path / "orders.sqlite"
    with transaction(db_path) as conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL)")
        conn.executemany("INSERT INTO orders VALUES (?, ?)", [(1, "a"), (2, "b")])
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=str(db_path)))
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
        ],
    ))

    real_execute = SlayerSQLClient.execute

    async def _skipping_execute(self: SlayerSQLClient, sql: str, timeout_seconds: int = 120) -> ExecutionResult:
        result = await real_execute(self, sql, timeout_seconds=timeout_seconds)
        return result.model_copy(update={"warnings": [*result.warnings, _REJECTED]})

    monkeypatch.setattr(SlayerSQLClient, "execute", _skipping_execute)
    return SlayerQueryEngine(storage=storage)


_QUERY = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="*:count")])  # pyright: ignore[reportArgumentType]


async def test_data_query_response_carries_skip(engine: SlayerQueryEngine) -> None:
    response = await engine.execute(query=_QUERY)
    assert response.data == [{"orders._count": 2}]
    assert response.warnings.count(_REJECTED) == 1


async def test_explain_response_carries_skip(engine: SlayerQueryEngine) -> None:
    response = await engine.execute(query=_QUERY, explain=True)
    assert response.warnings.count(_REJECTED) == 1
