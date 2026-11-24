"""Enforced keys: dialect capability, ingestion, drift and save validation (specs: models/fk-ingestion › Ingestion
records enforced keys; models/schema-drift › Drift clears a vanished enforced key; models/save-validation › Enforced
keys name declared keys of a table-backed dataset)."""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import duckdb
import pytest
from pytest_postgresql import factories

from slayer.core.errors import EnforcedKeyError
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import DriftReportEntry, IdempotentIngestResult
from slayer.sql.dialects import SQLGLOT_NAMES, SqlDialect, dialect_for_ds_type, get_dialect
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2437_fixtures import (
    CLEAN,
    DS,
    DUP_C1,
    REGION,
    REVENUE,
    customers_dataset,
    engine,
    joins_customers_inline,
    orders_dataset,
    region_revenue,
    seed_datasets,
    tables,
)
from tests._engine_helpers import owned_query_engine, seeded_exec_engine
from tests._pg_helpers import pg_database

pg_proc = factories.postgresql_proc(port=None, postgres_options="-c unix_socket_directories=")

CAPABILITY = "enforces_unique_keys"
#: sqlglot name (or datasource type) → whether the database enforces primary-key and unique constraints.
ENFORCES = {
    "sqlite": True, "postgres": True, "duckdb": True, "mysql": True, "tsql": True, "oracle": True,
    "snowflake": False, "bigquery": False, "redshift": False, "databricks": False, "clickhouse": False,
    "trino": False, "presto": False, "spark": False,
}

SHOP = "shop"
KEYED_DDL = (
    "CREATE TABLE customers(customer_id VARCHAR PRIMARY KEY, region VARCHAR)",
    "CREATE TABLE orders(order_id VARCHAR PRIMARY KEY, customer_id VARCHAR REFERENCES customers(customer_id), "
    "amount DOUBLE)",
    "INSERT INTO customers VALUES ('C1', 'EAST'), ('C2', 'EAST'), ('C3', 'WEST')",
    "INSERT INTO orders VALUES ('O1', 'C1', 100), ('O2', 'C1', 200), ('O3', 'C2', 50), ('O4', 'C3', 300)",
)
REGION_SUM = {"dimensions": ["customers.region"], "metrics": ["sum(orders.amount)"]}


def _enforces(dialect: SqlDialect) -> bool:
    return getattr(dialect, CAPABILITY)


def _non_enforcing(monkeypatch: pytest.MonkeyPatch, dialect: SqlDialect) -> None:
    """Make ``dialect`` report that it does not enforce unique keys."""
    if CAPABILITY in dialect.__dict__:
        monkeypatch.setitem(dialect.__dict__, CAPABILITY, False)
    else:
        monkeypatch.setattr(type(dialect), CAPABILITY, False)


def _duck(path: Path, statements: tuple[str, ...]) -> None:
    with contextlib.closing(duckdb.connect(str(path))) as con:
        for statement in statements:
            con.execute(statement)


@contextlib.asynccontextmanager
async def _shop(tmp_path: Path, ds: DatasourceConfig) -> AsyncGenerator[SlayerQueryEngine]:
    async with owned_query_engine(storage=YAMLStorage(base_dir=str(tmp_path / "store")), datasources=[ds]) as eng:
        yield eng


async def _ingest(eng: SlayerQueryEngine, ds: DatasourceConfig) -> IdempotentIngestResult:
    result = await eng.ingest_idempotent(ds)
    assert not result.errors, result.errors
    return result


async def _stored(eng: SlayerQueryEngine, name: str, *, data_source: str) -> SlayerDataset:
    dataset = await eng.storage.get_dataset(name, data_source=data_source)
    assert dataset is not None
    return dataset


def _entries_for(entries: list[DriftReportEntry], dataset: str) -> list[str]:
    """The JSON of each drift entry on ``dataset``."""
    return [json.dumps(e.model_dump(mode="json")) for e in entries if e.dataset_name == dataset]


def _reports_enforced_key(entries: list[DriftReportEntry]) -> bool:
    return any("enforced" in text and "customer_id" in text for text in _entries_for(entries, "customers"))


def _with_key_field(dataset: SlayerDataset, *, expression: str) -> SlayerDataset:
    """``dataset`` with field ``customer_id`` defined as ``expression``."""
    return dataset.model_copy(update={"fields": [
        f.model_copy(update={"expression": expression}) if f.name == "customer_id" else f for f in dataset.fields]})


# --------------------------------------------------------------------------- #
# Dialect capability
# --------------------------------------------------------------------------- #
class TestDialectCapability:
    def test_table_covers_every_dialect(self) -> None:
        assert set(ENFORCES) == set(SQLGLOT_NAMES)

    @pytest.mark.parametrize(("name", "enforces"), sorted(ENFORCES.items()))
    def test_each_dialect_states_it(self, name: str, enforces: bool) -> None:
        assert _enforces(get_dialect(name)) is enforces

    def test_mariadb_enforces(self) -> None:
        assert _enforces(dialect_for_ds_type("mariadb")) is True


# --------------------------------------------------------------------------- #
# Ingestion
# --------------------------------------------------------------------------- #
class TestIngestion:
    async def test_duckdb_primary_key_is_enforced(self, tmp_path: Path) -> None:
        """Spec: models/fk-ingestion › A DuckDB primary key is enforced."""
        _duck(tmp_path / "shop.duckdb", KEYED_DDL)
        ds = DatasourceConfig(name=SHOP, type="duckdb", database=str(tmp_path / "shop.duckdb"))
        async with _shop(tmp_path, ds) as eng:
            await _ingest(eng, ds)
            customers = await _stored(eng, "customers", data_source=SHOP)
            resp = await eng.execute(SlayerQuery.model_validate(REGION_SUM))
        assert customers.enforced_keys == [["customer_id"]]
        assert joins_customers_inline(resp), resp.sql

    async def test_non_enforcing_dialect_records_no_enforced_key(self, tmp_path: Path,
                                                                 monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec: models/fk-ingestion › A Snowflake primary key is not enforced (decided by the dialect capability)."""
        assert _enforces(get_dialect("snowflake")) is False
        _duck(tmp_path / "shop.duckdb", KEYED_DDL)
        ds = DatasourceConfig(name=SHOP, type="duckdb", database=str(tmp_path / "shop.duckdb"))
        _non_enforcing(monkeypatch, get_dialect("duckdb"))
        async with _shop(tmp_path, ds) as eng:
            await _ingest(eng, ds)
            customers = await _stored(eng, "customers", data_source=SHOP)
        assert [f.name for f in customers.fields if f.primary_key] == ["customer_id"]
        assert customers.enforced_keys == []

    async def test_an_unconditional_unique_key_is_enforced(self, tmp_path: Path) -> None:
        """A non-primary unconditional UNIQUE constraint is recorded beside the primary key."""
        _duck(tmp_path / "shop.duckdb", ("CREATE TABLE people(id VARCHAR PRIMARY KEY, email VARCHAR UNIQUE)",))
        ds = DatasourceConfig(name=SHOP, type="duckdb", database=str(tmp_path / "shop.duckdb"))
        async with _shop(tmp_path, ds) as eng:
            await _ingest(eng, ds)
            people = await _stored(eng, "people", data_source=SHOP)
        assert [f.name for f in people.fields if f.unique] == ["email"]
        assert sorted(people.enforced_keys) == [["email"], ["id"]]

    async def test_sqlite_partial_and_expression_unique_indexes_are_not_enforced(self, tmp_path: Path) -> None:
        """Partial and expression unique indexes are never recorded; a primary key beside them is."""
        path = tmp_path / "shop.db"
        with transaction(path) as con:
            con.executescript(
                "CREATE TABLE keyed (id TEXT PRIMARY KEY, name TEXT);"
                "CREATE TABLE partial (customer_id TEXT, active INTEGER);"
                "CREATE UNIQUE INDEX ux_partial ON partial (customer_id) WHERE active = 1;"
                "CREATE TABLE expr (customer_id TEXT, name TEXT);"
                "CREATE UNIQUE INDEX ux_expr ON expr (lower(name));"
            )
        ds = DatasourceConfig(name=SHOP, type="sqlite", database=str(path))
        async with _shop(tmp_path, ds) as eng:
            await _ingest(eng, ds)
            stored = {n: await _stored(eng, n, data_source=SHOP) for n in ("keyed", "partial", "expr")}
        assert stored["keyed"].enforced_keys == [["id"]]
        assert stored["partial"].enforced_keys == []
        assert stored["expr"].enforced_keys == []

    async def test_postgres_partial_unique_index_is_not_enforced(self, pg_proc: Any, tmp_path: Path) -> None:
        """Spec: models/fk-ingestion › A partial unique index is not enforced."""
        statements = [
            "CREATE TABLE keyed (id TEXT PRIMARY KEY)",
            "CREATE TABLE customers (customer_id TEXT, active BOOLEAN)",
            "CREATE UNIQUE INDEX ux_active ON customers (customer_id) WHERE active",
        ]
        with pg_database(pg_proc, statements=statements, name="dev2437") as ds:
            async with _shop(tmp_path, ds) as eng:
                await _ingest(eng, ds)
                keyed = await _stored(eng, "keyed", data_source=ds.name)
                customers = await _stored(eng, "customers", data_source=ds.name)
        assert keyed.enforced_keys == [["id"]]
        assert customers.enforced_keys == []

    async def test_a_primary_key_with_an_unrepresented_member_is_not_enforced(self, tmp_path: Path) -> None:
        """A key member that cannot be a field (a dotted name) leaves the rest of the key unenforced."""
        _duck(tmp_path / "shop.duckdb", (
            'CREATE TABLE partial_pk(id VARCHAR, "a.b" VARCHAR, region VARCHAR, PRIMARY KEY (id, "a.b"))',
            "CREATE TABLE whole_pk(id VARCHAR, part VARCHAR, PRIMARY KEY (id, part))",
        ))
        ds = DatasourceConfig(name=SHOP, type="duckdb", database=str(tmp_path / "shop.duckdb"))
        async with _shop(tmp_path, ds) as eng:
            await _ingest(eng, ds)
            partial = await _stored(eng, "partial_pk", data_source=SHOP)
            whole = await _stored(eng, "whole_pk", data_source=SHOP)
        assert [f.name for f in partial.fields] == ["id", "region"]
        assert partial.enforced_keys == []
        assert [sorted(k) for k in whole.enforced_keys] == [["id", "part"]]

    async def test_reingestion_fills_an_enforced_key(self, tmp_path: Path) -> None:
        """Spec: models/fk-ingestion › Re-ingestion fills an enforced key."""
        _duck(tmp_path / "shop.duckdb", KEYED_DDL)
        ds = DatasourceConfig(name=SHOP, type="duckdb", database=str(tmp_path / "shop.duckdb"))
        async with _shop(tmp_path, ds) as eng:
            await _ingest(eng, ds)
            first = await _stored(eng, "customers", data_source=SHOP)
            await eng.storage.save_dataset(first.model_copy(update={"enforced_keys": []}), _validate=False)
            assert (await _stored(eng, "customers", data_source=SHOP)).enforced_keys == []
            result = await _ingest(eng, ds)
            customers = await _stored(eng, "customers", data_source=SHOP)
        assert customers.enforced_keys == [["customer_id"]]
        (addition,) = [a for a in result.additions if a.dataset_name == "customers"]
        assert addition.metadata_changed

    async def test_reingestion_never_removes_an_enforced_key(self) -> None:
        """Re-ingestion of a table without the constraint keeps the stored enforced key."""
        customers = customers_dataset(enforced_keys=[["customer_id"]])
        async with seeded_exec_engine(dialect="duckdb", seed=lambda p: seed_backend("duckdb", p, tables(CLEAN)),
                                      datasets=seed_datasets(customers=customers)) as (eng, db_path):
            ds = DatasourceConfig(name=DS, type="duckdb", database=db_path)
            await _ingest(eng, ds)
            stored = await _stored(eng, "customers", data_source=DS)
        assert stored.enforced_keys == [["customer_id"]]


# --------------------------------------------------------------------------- #
# Schema drift
# --------------------------------------------------------------------------- #
class TestDrift:
    async def test_a_dropped_constraint_clears_the_enforced_key(self) -> None:
        """Spec: models/schema-drift › A dropped constraint clears the enforced key."""
        datasets = seed_datasets(customers=customers_dataset(enforced_keys=[["customer_id"]]))
        async with engine("duckdb", DUP_C1, datasets) as eng:
            entries = await eng.validate_datasets(data_source=DS)
            assert _reports_enforced_key(entries), entries
            result = await eng.apply_drift_deletes(entries)
            assert not result.errors, result.errors
            customers = await _stored(eng, "customers", data_source=DS)
            orders = await _stored(eng, "orders", data_source=DS)
            resp = await eng.execute(SlayerQuery.model_validate({"dimensions": [REGION], "metrics": [REVENUE]}))
        assert customers.enforced_keys == []
        assert [f.name for f in customers.fields if f.primary_key] == ["customer_id"]
        assert orders.relationships == datasets[1].relationships
        assert region_revenue(resp)["EAST"] == 350.0

    async def test_an_authors_trusted_flag_is_never_cleared(self) -> None:
        """Spec: models/schema-drift › An author's trusted flag is never cleared."""
        async with engine("duckdb", CLEAN, trusted=True) as eng:
            entries = await eng.validate_datasets(data_source=DS)
            assert _entries_for(entries, "orders") == []
            assert not any("trusted" in text for text in _entries_for(entries, "customers"))
            await eng.apply_drift_deletes(entries)
            orders = await _stored(eng, "orders", data_source=DS)
        assert orders.relationships[0].trusted is True

    async def test_non_enforcing_dialect_clears_every_enforced_key_and_keeps_trusted(
            self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec: models/schema-drift › Drift clears a vanished enforced key (non-enforcing dialect; trusted kept)."""
        path = tmp_path / "live.duckdb"
        _duck(path, (
            "CREATE TABLE customers(customer_id VARCHAR PRIMARY KEY, region VARCHAR, manager_id VARCHAR, "
            "tier VARCHAR, weight DOUBLE)",
            "CREATE TABLE orders(order_id VARCHAR, customer_id VARCHAR, seller_id VARCHAR, store_id VARCHAR, "
            "manager_id VARCHAR, amount DOUBLE, status VARCHAR, ordered_at DATE)",
        ))
        ds = DatasourceConfig(name=DS, type="duckdb", database=str(path))
        async with _shop(tmp_path, ds) as eng:
            await eng.storage.save_dataset(customers_dataset(enforced_keys=[["customer_id"]]), _validate=False)
            await eng.storage.save_dataset(orders_dataset(trusted=True), _validate=False)
            assert _reports_enforced_key(await eng.validate_datasets(data_source=DS)) is False
            _non_enforcing(monkeypatch, get_dialect("duckdb"))
            entries = await eng.validate_datasets(data_source=DS)
            assert _reports_enforced_key(entries), entries
            assert _entries_for(entries, "orders") == []
            await eng.apply_drift_deletes(entries)
            customers = await _stored(eng, "customers", data_source=DS)
            orders = await _stored(eng, "orders", data_source=DS)
        assert customers.enforced_keys == []
        assert orders.relationships[0].trusted is True


# --------------------------------------------------------------------------- #
# Save validation
# --------------------------------------------------------------------------- #
class TestSaveValidation:
    @pytest.mark.parametrize("key", [["customer_id"], ["tier"]], ids=["primary_key", "unique_field"])
    async def test_a_declared_key_is_accepted(self, key: list[str]) -> None:
        """A primary key or a unique field may be recorded as enforced."""
        async with engine("duckdb", CLEAN) as eng:
            customers = customers_dataset(enforced_keys=[key])
            customers = customers.model_copy(update={"fields": [
                f.model_copy(update={"unique": True}) if f.name == "tier" else f for f in customers.fields]})
            await eng.storage.save_dataset(customers)
            stored = await _stored(eng, "customers", data_source=DS)
        assert stored.enforced_keys == [key]

    async def test_an_undeclared_enforced_key_is_refused(self) -> None:
        """Spec: models/save-validation › An undeclared enforced key is refused."""
        async with engine("duckdb", CLEAN) as eng:
            await eng.storage.save_dataset(customers_dataset(enforced_keys=[["customer_id"]]))
            undeclared = customers_dataset(enforced_keys=[["region"]])
            with pytest.raises(Exception, match="region"):  # noqa: PT011 — any typed refusal naming the entry
                await eng.storage.save_dataset(undeclared)
            stored = await _stored(eng, "customers", data_source=DS)
        assert stored.enforced_keys == [["customer_id"]]

    @pytest.mark.parametrize("backing", [
        pytest.param({"sql": "SELECT * FROM customers"}, id="sql"),
        pytest.param({"source_queries": [{"population": "customers",
                                          "dimensions": ["customers.customer_id", "customers.region"]}]},
                     id="backing-query"),
    ])
    async def test_a_query_backed_dataset_cannot_carry_enforced_keys(self, backing: dict[str, Any]) -> None:
        """Spec: models/save-validation › A query-backed dataset cannot carry enforced keys."""
        async with engine("duckdb", CLEAN) as eng:
            await eng.storage.save_dataset(customers_dataset(enforced_keys=[["customer_id"]]))
            backed = customers_dataset(name="customers_sql", sql_table=None, **backing, enforced_keys=[["customer_id"]])
            with pytest.raises(Exception):  # noqa: B017, PT011 — any typed refusal
                await eng.storage.save_dataset(backed)
            assert await eng.storage.get_dataset("customers_sql", data_source=DS) is None

    @pytest.mark.parametrize("update", [
        pytest.param({"sql_table": "customers_v2"}, id="sql_table"),
        pytest.param({"sql_table": None, "sql": "SELECT * FROM customers_v2"}, id="sql"),
        pytest.param({"sql_table": None, "source_queries": [
            {"population": "customers", "dimensions": ["customers.customer_id", "customers.region"]}]},
            id="backing-query"),
    ])
    async def test_repointing_drops_enforced_keys(self, update: dict[str, Any]) -> None:
        """Spec: models/save-validation › Repointing drops enforced keys."""
        copy = TableSpec(name="customers_v2", columns=tables(CLEAN)[0].columns, rows=CLEAN)
        async with seeded_exec_engine(dialect="duckdb",
                                      seed=lambda p: seed_backend("duckdb", p, [*tables(CLEAN), copy]),
                                      datasets=[]) as (eng, _):
            await eng.storage.save_dataset(customers_dataset(enforced_keys=[["customer_id"]]))
            stored = await _stored(eng, "customers", data_source=DS)
            assert stored.enforced_keys == [["customer_id"]]
            await eng.storage.save_dataset(SlayerDataset.model_validate({**stored.model_dump(), **update}))
            repointed = await _stored(eng, "customers", data_source=DS)
        assert (repointed.sql_table, repointed.sql) == (update.get("sql_table"), update.get("sql"))
        assert bool(repointed.source_queries) == ("source_queries" in update)
        assert repointed.enforced_keys == []

    async def test_an_enforced_key_on_a_computed_field_is_refused(self) -> None:
        """Spec: models/save-validation › An enforced key on a computed field is refused."""
        async with engine("duckdb", CLEAN, datasets=[]) as eng:
            computed = _with_key_field(customers_dataset(enforced_keys=[["customer_id"]]), expression="lower(region)")
            with pytest.raises(EnforcedKeyError, match="customer_id"):
                await eng.storage.save_dataset(computed)
            assert await eng.storage.get_dataset("customers", data_source=DS) is None

    @pytest.mark.parametrize("expression", ["region", "lower(region)"], ids=["another_column", "computed"])
    async def test_repointing_a_key_field_drops_its_enforced_key(self, expression: str) -> None:
        """Spec: models/save-validation › Re-pointing a key field drops its enforced key."""
        async with engine("duckdb", CLEAN, datasets=[]) as eng:
            await eng.storage.save_dataset(customers_dataset(enforced_keys=[["customer_id"]]))
            stored = await _stored(eng, "customers", data_source=DS)
            assert stored.enforced_keys == [["customer_id"]]
            await eng.storage.save_dataset(_with_key_field(stored, expression=expression))
            repointed = await _stored(eng, "customers", data_source=DS)
        key_field = repointed.get_field("customer_id")
        assert repointed.enforced_keys == []
        assert key_field is not None
        assert key_field.expression == expression
