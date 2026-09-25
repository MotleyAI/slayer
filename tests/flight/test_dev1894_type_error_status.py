"""A checker type error raised while planning a Flight SQL query reaches the client as invalid-argument."""

from __future__ import annotations

import threading
from collections.abc import AsyncIterator

import pyarrow as pa
import pyarrow.flight as fl
import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.flight import _flight_sql_pb2 as fsql_pb
from slayer.flight.handlers import FlightHandlers
from slayer.flight.server import build_server
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import seeded_exec_engine


def _seed(db_path: str) -> None:
    with transaction(db_path) as con:
        con.execute("CREATE TABLE orders (id INT, amount REAL, status TEXT, created_at TIMESTAMP)")


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
        measures=[
            ModelMeasure(name="bad_window", formula="amount:sum(window='90x')"),
            ModelMeasure(name="no_axis", formula="sum(cumsum(amount:sum(partition_by=status)))"),
        ],
    )


@pytest.fixture
async def flight_client() -> AsyncIterator[fl.FlightClient]:  # pyright: ignore[reportPrivateImportUsage]
    async with seeded_exec_engine(dialect="sqlite", seed=_seed, models=[_orders()]) as (engine, _):
        server = build_server(
            host="127.0.0.1", port=0,
            handlers=FlightHandlers(engine=engine, storage=engine.storage),
        )
        thread = threading.Thread(target=server.serve, daemon=True)
        thread.start()
        try:
            with fl.FlightClient(f"grpc://127.0.0.1:{server.port}") as client:  # pyright: ignore[reportPrivateImportUsage]
                client.wait_for_available()
                yield client
        finally:
            server.shutdown()
            server.wait()


def _prepare(client: fl.FlightClient, sql: str) -> None:  # pyright: ignore[reportPrivateImportUsage]
    req = fsql_pb.ActionCreatePreparedStatementRequest()  # pyright: ignore[reportAttributeAccessIssue]
    req.query = sql
    action = fl.Action("CreatePreparedStatement", req.SerializeToString())  # pyright: ignore[reportPrivateImportUsage]
    list(client.do_action(action))


@pytest.mark.parametrize(("sql", "cls_name"), [
    ("SELECT DATE_TRUNC('month', created_at) AS m, no_axis FROM orders GROUP BY 1", "TimeAxisError"),
    ("SELECT status, bad_window FROM orders GROUP BY status", "WindowDurationError"),
])
def test_type_error_is_invalid_argument(flight_client, sql: str, cls_name: str) -> None:
    with pytest.raises(pa.ArrowInvalid) as ei:
        _prepare(flight_client, sql)
    assert not isinstance(ei.value, pa.ArrowNotImplementedError)
    assert f"{cls_name}: " in str(ei.value)
