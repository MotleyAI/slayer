"""A seeded database per server Tier-1 engine for the Ossie compliance gate.

Postgres runs on the ``pytest-postgresql`` server; MySQL, ClickHouse, SQL Server and Trino in testcontainers;
Snowflake through ``~/.snowflake/connections.toml`` and BigQuery through Application Default Credentials and
``$GCP_PROJECT_ID``. An engine ``$SLAYER_GATE_ENGINES`` names fails the run when it cannot start; others skip.
"""

from __future__ import annotations

import contextlib
import os
import pathlib
import tomllib
from collections.abc import Generator, Sequence
from typing import Any, Callable

import docker
import psycopg
import pyodbc
import pytest
import snowflake.connector
from google.cloud import bigquery
from psycopg import sql as pg_sql
from pydantic import BaseModel, ConfigDict
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.trino import TrinoContainer

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from tests._engine_helpers import disposable_engine
from tests._ossie_gate import unavailable
from tests._ossie_gate_fixtures import Placement, gate_schemas, run_token, seed_statements

#: The gate's SQL Server; 2025 adds the regular-expression functions.
SQLSERVER_2022 = "mcr.microsoft.com/mssql/server:2022-latest"
SQLSERVER_2025 = "mcr.microsoft.com/mssql/server:2025-latest"
_SA_PASSWORD = "YourStrong@Passw0rd1"  # NOSONAR(S2068) — testcontainer credentials, not real secrets
_ODBC = "ODBC Driver 18 for SQL Server"
_SNOWFLAKE_TOML = pathlib.Path("~/.snowflake/connections.toml").expanduser()
_SNOWFLAKE_CONNECTION = os.environ.get("SLAYER_SNOWFLAKE_CONNECTION", "slayer_test")


class ServerProvider(BaseModel):
    """A seeded server database: statements run through ``executor``, reads through SLayer's engine factory."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    placement: Placement
    fields: dict[str, Any]
    executor: Callable[[Sequence[str]], None]

    @property
    def engine(self) -> str:
        return self.placement.engine

    def run(self, statements: Sequence[str]) -> None:
        self.executor(statements)

    def fetch(self, sql: str) -> list[tuple[Any, ...]]:
        datasource = self.datasource("readback")
        try:
            with engine_factory.get_engine(datasource).connect() as connection:
                return [tuple(row) for row in connection.exec_driver_sql(sql).fetchall()]
        finally:
            engine_factory.invalidate_engine(datasource)

    def datasource(self, name: str) -> DatasourceConfig:
        return DatasourceConfig.model_validate({"name": name, **self.fields})


def _sqlalchemy_executor(url: str, **engine_kwargs: Any) -> Callable[[Sequence[str]], None]:
    def execute(statements: Sequence[str]) -> None:
        with disposable_engine(url, **engine_kwargs) as engine, engine.begin() as connection:
            for statement in statements:
                connection.exec_driver_sql(statement)
    return execute


def _seeded(provider: ServerProvider) -> ServerProvider:
    provider.run(seed_statements(provider.placement))
    return provider


def _require_docker(engine: str) -> None:
    try:
        docker.from_env().ping()
    except Exception as exc:  # noqa: BLE001 — any failure means no Docker
        unavailable(engine, f"Docker not available: {exc}")


@contextlib.contextmanager
def postgres_provider(info: Any) -> Generator[ServerProvider]:
    """A fresh database on the ``pytest-postgresql`` server ``info``."""
    token = run_token()
    database = f"gate_{token}"
    with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres", autocommit=True) as admin:
        admin.execute(pg_sql.SQL("CREATE DATABASE {}").format(pg_sql.Identifier(database)))
    url = f"postgresql+psycopg://{info.user}@{info.host}:{info.port}/{database}"
    fields = {"type": "postgres", "host": info.host, "port": info.port, "database": database,
              "username": info.user, "password": ""}
    try:
        yield _seeded(ServerProvider(placement=Placement(engine="postgres", token=token), fields=fields,
                                     executor=_sqlalchemy_executor(url)))
    finally:
        engine_factory.reset_cache()
        with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres",
                             autocommit=True) as admin:
            admin.execute(pg_sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(pg_sql.Identifier(database)))


@contextlib.contextmanager
def mysql_provider() -> Generator[ServerProvider]:
    _require_docker("mysql")
    token = run_token()
    database = f"gate_{token}"
    with MySqlContainer("mysql:8.0", username="slayer", password="slayer", root_password="root") as container:
        host, port = container.get_container_host_ip(), int(container.get_exposed_port(3306))
        _sqlalchemy_executor(f"mysql+pymysql://root:root@{host}:{port}/mysql")([f"CREATE DATABASE {database}"])
        fields = {"type": "mysql", "host": host, "port": port, "database": database, "username": "root",
                  "password": "root"}
        yield _seeded(ServerProvider(placement=Placement(engine="mysql", token=token), fields=fields,
                                     executor=_sqlalchemy_executor(f"mysql+pymysql://root:root@{host}:{port}/{database}")))
        engine_factory.reset_cache()


@contextlib.contextmanager
def clickhouse_provider() -> Generator[ServerProvider]:
    _require_docker("clickhouse")
    token = run_token()
    database = f"gate_{token}"
    container = ClickHouseContainer("clickhouse/clickhouse-server:25.12").with_env(
        "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
    with container as running:
        host, port = running.get_container_host_ip(), int(running.get_exposed_port(8123))
        user = getattr(running, "username", "default") or "default"
        password = getattr(running, "password", "") or ""
        auth = f"{user}:{password}@" if password else f"{user}@"
        _sqlalchemy_executor(f"clickhouse+http://{auth}{host}:{port}/default")([f"CREATE DATABASE {database}"])
        fields = {"type": "clickhouse", "host": host, "port": port, "database": database, "username": user,
                  "password": password}
        yield _seeded(ServerProvider(placement=Placement(engine="clickhouse", token=token), fields=fields,
                                     executor=_sqlalchemy_executor(f"clickhouse+http://{auth}{host}:{port}/{database}")))
        engine_factory.reset_cache()


@contextlib.contextmanager
def sqlserver_provider(image: str = SQLSERVER_2022) -> Generator[ServerProvider]:
    _require_docker("tsql")
    if _ODBC not in pyodbc.drivers():
        unavailable("tsql", f"no {_ODBC!r} visible to pyodbc (installed: {pyodbc.drivers()!r})")
    token = run_token()
    database = f"gate_{token}"
    with SqlServerContainer(image, password=_SA_PASSWORD) as container:
        host, port = container.get_container_host_ip(), int(container.get_exposed_port(1433))

        def url(db: str) -> str:
            return (f"mssql+pyodbc://sa:{_SA_PASSWORD.replace('@', '%40')}@{host}:{port}/{db}"
                    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")

        _sqlalchemy_executor(url("master"), isolation_level="AUTOCOMMIT")([f"CREATE DATABASE [{database}]"])
        fields = {"type": "mssql", "host": host, "port": port, "database": database, "username": "sa",
                  "password": _SA_PASSWORD}
        yield _seeded(ServerProvider(placement=Placement(engine="tsql", token=token), fields=fields,
                                     executor=_sqlalchemy_executor(url(database), isolation_level="AUTOCOMMIT")))
        engine_factory.reset_cache()


@contextlib.contextmanager
def trino_provider() -> Generator[ServerProvider]:
    _require_docker("trino")
    token = run_token()
    schema = f"gate_{token}"
    with TrinoContainer("trinodb/trino:483", container_start_timeout=180) as container:
        host, port = container.get_container_host_ip(), int(container.get_exposed_port(8080))

        def execute(statements: Sequence[str]) -> None:
            with disposable_engine(f"trino://{container.user}@{host}:{port}/memory/{schema}") as engine:
                raw = engine.raw_connection()
                try:
                    cursor = raw.cursor()
                    for statement in statements:
                        cursor.execute(statement)
                        cursor.fetchall()
                finally:
                    raw.close()

        with disposable_engine(f"trino://{container.user}@{host}:{port}/memory") as engine:
            raw = engine.raw_connection()
            try:
                cursor = raw.cursor()
                cursor.execute(f"CREATE SCHEMA memory.{schema}")
                cursor.fetchall()
            finally:
                raw.close()
        fields = {"type": "trino", "host": host, "port": port, "database": f"memory/{schema}",
                  "username": container.user}
        yield _seeded(ServerProvider(placement=Placement(engine="trino", token=token), fields=fields,
                                     executor=execute))
        engine_factory.reset_cache()


def _snowflake_profile() -> dict[str, Any]:
    if not _SNOWFLAKE_TOML.is_file():
        unavailable("snowflake", f"{_SNOWFLAKE_TOML} not present")
    with open(_SNOWFLAKE_TOML, "rb") as f:
        profile = tomllib.load(f).get(_SNOWFLAKE_CONNECTION)
    if not isinstance(profile, dict):
        unavailable("snowflake", f"no [{_SNOWFLAKE_CONNECTION}] profile in {_SNOWFLAKE_TOML}")
    return profile


@contextlib.contextmanager
def snowflake_provider() -> Generator[ServerProvider]:
    """Transient schemas on the profile's database: the gate's own, and one per wide-table schema."""
    profile = _snowflake_profile()
    token = run_token()
    schema = f"GATE_{token.upper()}"
    placement = Placement(engine="snowflake", token=token)
    try:
        connection = snowflake.connector.connect(connection_name=_SNOWFLAKE_CONNECTION)
    except Exception as exc:  # noqa: BLE001 — any failure means no Snowflake
        unavailable("snowflake", f"cannot connect via {_SNOWFLAKE_CONNECTION!r}: {exc}")
    schemas = [schema, *(placement.schema(s).upper() for s in gate_schemas())]

    def execute(statements: Sequence[str]) -> None:
        cursor = connection.cursor()
        try:
            if profile.get("warehouse"):
                cursor.execute(f"USE WAREHOUSE {profile['warehouse']}")
            if profile.get("database"):
                cursor.execute(f"USE DATABASE {profile['database']}")
            cursor.execute(f"USE SCHEMA {schema}")
            for statement in statements:
                cursor.execute(statement)
                if statement.startswith("CREATE SCHEMA"):  # it also makes the new schema current
                    cursor.execute(f"USE SCHEMA {schema}")
        finally:
            cursor.close()

    try:
        cursor = connection.cursor()
        if profile.get("database"):
            cursor.execute(f"USE DATABASE {profile['database']}")
        cursor.execute(f"CREATE TRANSIENT SCHEMA {schema}")
        cursor.close()
        fields = {"type": "snowflake", "connection_name": _SNOWFLAKE_CONNECTION, "schema_name": schema}
        yield _seeded(ServerProvider(placement=placement, fields=fields, executor=execute))
    finally:
        engine_factory.reset_cache()
        cursor = connection.cursor()
        try:
            if profile.get("database"):
                cursor.execute(f"USE DATABASE {profile['database']}")
            for name in schemas:
                cursor.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
        finally:
            cursor.close()
            connection.close()


@contextlib.contextmanager
def bigquery_provider() -> Generator[ServerProvider]:
    """Datasets on ``$GCP_PROJECT_ID``: the gate's own, and one per wide-table schema."""
    project = os.environ.get("GCP_PROJECT_ID")
    if not project:
        unavailable("bigquery", "GCP_PROJECT_ID not set")
    try:
        client = bigquery.Client(project=project)
    except Exception as exc:  # noqa: BLE001 — any failure means no BigQuery
        unavailable("bigquery", f"no Google credentials: {exc}")
    token = run_token()
    dataset = f"gate_{token}"
    placement = Placement(engine="bigquery", token=token)
    datasets = [dataset, *(placement.schema(s) for s in gate_schemas())]

    def execute(statements: Sequence[str]) -> None:
        config = bigquery.QueryJobConfig(default_dataset=f"{project}.{dataset}")
        for statement in statements:
            client.query(statement, job_config=config).result()

    try:
        client.create_dataset(f"{project}.{dataset}")
        fields = {"type": "bigquery", "connection_string": f"bigquery://{project}/{dataset}"}
        yield _seeded(ServerProvider(placement=placement, fields=fields, executor=execute))
    finally:
        engine_factory.reset_cache()
        for name in datasets:
            client.delete_dataset(f"{project}.{name}", delete_contents=True, not_found_ok=True)
        client.close()


@contextlib.contextmanager
def server_provider(engine: str, request: pytest.FixtureRequest) -> Generator[ServerProvider]:
    """A seeded database on server ``engine``."""
    if engine == "postgres":
        with postgres_provider(request.getfixturevalue("postgresql_proc")) as provider:
            yield provider
        return
    providers = {"mysql": mysql_provider, "clickhouse": clickhouse_provider, "tsql": sqlserver_provider,
                 "trino": trino_provider, "snowflake": snowflake_provider, "bigquery": bigquery_provider}
    with providers[engine]() as provider:
        yield provider
