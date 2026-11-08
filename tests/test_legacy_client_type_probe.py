"""A SQL client written for 1.6.0, whose type probe is ``get_column_types``, keeps probing and warns.

Spec: database/client-contract.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import validate_datasource
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import OSIDocument
from slayer.sql import engine_factory
from slayer.sql.client import (
    DelegatingSqlClient,
    ExecutionResult,
    ObjectListing,
    ResultColumnType,
    SlayerSQLClient,
    SqlClient,
    adapt_client,
    type_categories,
)
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.base import ServerProfile
from slayer.sql.dialects.reflection import ReflectedColumn, TableKeys, TableLocation
from tests._gateway_harness import DS_NAME, paid_sql_model, seed_shop, shop_datasource, shop_storage

DEPRECATION = r"get_column_types\(\) is the 1\.6\.0 type probe.*SLayer 2\.0.*get_result_column_types\(\)"
BIG_OSI = OSIDocument.model_validate({
    "version": "0.2.0.dev0", "name": "shop",
    "datasets": [{"name": "orders", "source": "orders",
                  "fields": [{"name": "big", "expression": {"sql": "amount * 2"}}]}],
})


class FullGateway160:
    """Every member of the 1.6.0 contract, written from scratch; its probe answers 1.6.0 categories."""

    def __init__(self, inner: SlayerSQLClient) -> None:
        self.inner = inner
        self.probes: list[str] = []

    @property
    def datasource(self) -> DatasourceConfig:
        return self.inner.datasource

    async def execute(self, sql: str) -> ExecutionResult:
        return await self.inner.execute(sql)

    async def get_column_types(self, sql: str) -> dict[str, str]:
        self.probes.append(sql)
        return type_categories(await self.inner.get_result_column_types(sql),
                               dialect=dialect_for_ds_type(self.inner.datasource.type))

    async def server_profile(self) -> ServerProfile:
        return await self.inner.server_profile()

    async def ping(self) -> None:
        await self.inner.ping()

    async def list_schemas(self) -> list[str]:
        return await self.inner.list_schemas()

    async def default_schema(self) -> str | None:
        return await self.inner.default_schema()

    async def current_catalog(self) -> str | None:
        return await self.inner.current_catalog()

    async def list_objects(self, schema: str | None) -> ObjectListing:
        return await self.inner.list_objects(schema)

    async def reflect_columns(self, location: TableLocation) -> list[ReflectedColumn]:
        return await self.inner.reflect_columns(location)

    async def reflect_table_keys(self, location: TableLocation) -> TableKeys:
        return await self.inner.reflect_table_keys(location)

    async def table_comment(self, location: TableLocation) -> str | None:
        return await self.inner.table_comment(location)

    async def schema_description(self, schema: str | None, configured_schema: str | None) -> str | None:
        return await self.inner.schema_description(schema=schema, configured_schema=configured_schema)

    async def aclose(self) -> None:
        await self.inner.aclose()

    def close(self) -> None:
        self.inner.close()


class ProbingGateway160(DelegatingSqlClient):
    """A 1.6.0 gateway that routes its type probe through its own ``get_column_types``."""

    def __init__(self, inner: SlayerSQLClient) -> None:
        super().__init__(inner)
        self.probes: list[str] = []

    async def get_column_types(self, sql: str) -> dict[str, str]:
        self.probes.append(sql)
        return type_categories(await self.inner.get_result_column_types(sql),
                               dialect=dialect_for_ds_type(self.inner.datasource.type))


class SuperGateway160(DelegatingSqlClient):
    """A 1.6.0 gateway whose probe defers to ``DelegatingSqlClient.get_column_types``."""

    def __init__(self, inner: SlayerSQLClient) -> None:
        super().__init__(DelegatingSqlClient(inner))
        self.probes: list[str] = []

    async def get_column_types(self, sql: str) -> dict[str, str]:
        self.probes.append(sql)
        return await super().get_column_types(sql)


class CurrentGateway(DelegatingSqlClient):
    async def get_result_column_types(self, sql: str) -> dict[str, ResultColumnType]:
        return await self.inner.get_result_column_types(sql)

    async def get_column_types(self, sql: str) -> dict[str, str]:
        raise AssertionError("never called")


class CategoryStub:
    async def get_column_types(self, sql: str) -> dict[str, str]:
        return {"n": "number", "s": "string", "t": "time", "b": "boolean", "x": "opaque"}


class ProtocolSubclass160(SqlClient):
    """A 1.6.0 client subclassing the protocol: it inherits the typed probe's stub."""

    async def get_column_types(self, sql: str) -> dict[str, str]:
        return {"n": "number"}


@pytest.fixture
def shop_db(tmp_path: Path) -> str:
    path = str(tmp_path / "shop.db")
    seed_shop(path)
    return path


@pytest.mark.parametrize("gateway_class", [FullGateway160, ProbingGateway160, SuperGateway160])
async def test_a_1_6_0_client_probes_through_its_get_column_types(
    tmp_path: Path, shop_db: str, gateway_class: type[FullGateway160 | ProbingGateway160 | SuperGateway160],
) -> None:
    ds = shop_datasource(shop_db)
    storage = await shop_storage(str(tmp_path / "store"), datasource=ds, models=[paid_sql_model()])
    gateways: list[FullGateway160 | ProbingGateway160 | SuperGateway160] = []

    def factory(datasource: DatasourceConfig) -> FullGateway160 | ProbingGateway160 | SuperGateway160:
        gateways.append(gateway_class(SlayerSQLClient(datasource=datasource)))
        return gateways[-1]

    engine = SlayerQueryEngine(storage=storage, client_factory=factory)  # pyright: ignore[reportArgumentType]
    try:
        with pytest.warns(FutureWarning, match=DEPRECATION):
            types = await engine.get_result_column_types("paid", data_source=DS_NAME)
    finally:
        await engine.aclose()
        engine_factory.invalidate_engine(ds)
    assert types["amount"].type == DataType.DOUBLE
    assert gateways
    assert gateways[0].probes


async def test_osi_import_probes_through_a_1_6_0_client(shop_db: str) -> None:
    ds = shop_datasource(shop_db)
    gateway = FullGateway160(SlayerSQLClient(datasource=ds))
    converter = OsiToSlayerConverter(documents=[BIG_OSI], data_source=DS_NAME, target_dialect="sqlite")
    try:
        with pytest.warns(FutureWarning, match=DEPRECATION):
            await converter.collect_metadata(client=gateway)  # pyright: ignore[reportArgumentType]
    finally:
        await gateway.aclose()
        engine_factory.invalidate_engine(ds)
    [model] = converter.convert().models
    assert {c.name: c.type for c in model.columns}["big"] == DataType.DOUBLE
    assert gateway.probes


async def test_drift_validation_probes_through_a_1_6_0_client(shop_db: str) -> None:
    ds = shop_datasource(shop_db)
    gateway = FullGateway160(SlayerSQLClient(datasource=ds))
    models = [paid_sql_model()]
    try:
        with pytest.warns(FutureWarning, match=DEPRECATION):
            await validate_datasource(datasource=ds, models=models, client=gateway)  # pyright: ignore[reportArgumentType]
    finally:
        await gateway.aclose()
        engine_factory.invalidate_engine(ds)
    assert gateway.probes


async def test_the_delegating_category_probe_reads_the_typed_probe(shop_db: str) -> None:
    ds = shop_datasource(shop_db)
    client = DelegatingSqlClient(SlayerSQLClient(datasource=ds))
    try:
        with pytest.warns(FutureWarning, match=r"DelegatingSqlClient\.get_column_types\(\).*SLayer 2\.0"):
            categories = await client.get_column_types("SELECT id, status FROM orders")
    finally:
        await client.aclose()
        engine_factory.invalidate_engine(ds)
    assert categories == {"id": "number", "status": "string"}


async def test_1_6_0_categories_read_as_types() -> None:
    client = adapt_client(CategoryStub())  # pyright: ignore[reportArgumentType]
    with pytest.warns(FutureWarning, match=DEPRECATION):
        types = await client.get_result_column_types("SELECT 1")
    assert types == {
        "n": ResultColumnType(type=DataType.DOUBLE), "s": ResultColumnType(type=DataType.TEXT),
        "t": ResultColumnType(type=DataType.TIMESTAMP), "b": ResultColumnType(type=DataType.BOOLEAN),
        "x": ResultColumnType(type=DataType.TEXT),
    }


async def test_an_inherited_protocol_stub_is_no_typed_probe() -> None:
    client = adapt_client(ProtocolSubclass160())  # pyright: ignore[reportAbstractUsage]
    with pytest.warns(FutureWarning, match=DEPRECATION):
        types = await client.get_result_column_types("SELECT 1")
    assert types == {"n": ResultColumnType(type=DataType.DOUBLE)}


class ProxyClient:
    """Serves every member, the typed probe included, by forwarding to ``inner``."""

    def __init__(self, inner: SlayerSQLClient) -> None:
        self.inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


@pytest.mark.parametrize("make", [
    lambda ds: SlayerSQLClient(datasource=ds),
    lambda ds: DelegatingSqlClient(SlayerSQLClient(datasource=ds)),
    lambda ds: CurrentGateway(SlayerSQLClient(datasource=ds)),
    lambda ds: ProxyClient(SlayerSQLClient(datasource=ds)),
], ids=["default", "delegating", "current-gateway", "proxy"])
def test_a_client_serving_the_typed_probe_is_used_as_is(shop_db: str, make) -> None:
    client = make(shop_datasource(shop_db))
    assert adapt_client(client) is client
