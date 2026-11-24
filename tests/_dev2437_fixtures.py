"""Seeds and builders for the trusted-cardinality tests (spec: queries/semantics › Declared cardinality never changes
a value; models/join-cardinality › Trusted hops).

The Appendix B seed of ``tests/_dev2277_fixtures.py`` (datasource ``test``) with ``customers`` widened::

    customers (customer_id, region, manager_id, tier, weight):
        C1 EAST M1 gold 1 | C2 EAST M1 silver 2 | C3 WEST M2 gold 1 | C4 NORTH M2 silver 1
    orders: O1 C1 100 | O2 C1 200 | O3 C2 50 | O4 C3 300

Dirty variants: ``DUP_C1`` adds ``C1 EAST M1 gold 3``; ``SPLIT_C1`` makes C1 ``EAST gold`` and ``WEST silver``;
``C3_TWO_GROUPS`` adds ``C3 SOUTH M2 gold 1``. Clean EAST is 350, WEST 300.

``trusted`` and ``enforced_keys`` are reached through builders, so a model lacking them fails only the test that
needs them.
"""

from __future__ import annotations

import contextlib
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, Optional

import duckdb

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2243_fixtures import CORPUS, make_storage, store_document
from tests._dev2277_fixtures import BACKENDS, DS, MANAGERS, ORDERS, RETURNS, STORES, by, returns_dataset
from tests._engine_helpers import owned_query_engine, seeded_exec_engine

__all__ = ["BACKENDS", "DS"]

REGION, REVENUE = "customers.region", "orders.total_revenue"

_CUSTOMER_COLUMNS = [("customer_id", "TEXT"), ("region", "TEXT"), ("manager_id", "TEXT"), ("tier", "TEXT"),
                     ("weight", "DOUBLE")]
CLEAN = [("C1", "EAST", "M1", "gold", 1.0), ("C2", "EAST", "M1", "silver", 2.0),
         ("C3", "WEST", "M2", "gold", 1.0), ("C4", "NORTH", "M2", "silver", 1.0)]
DUP_C1 = [*CLEAN, ("C1", "EAST", "M1", "gold", 3.0)]
SPLIT_C1 = [("C1", "EAST", "M1", "gold", 1.0), ("C1", "WEST", "M1", "silver", 1.0), *CLEAN[1:]]
C3_TWO_GROUPS = [*CLEAN, ("C3", "SOUTH", "M2", "gold", 1.0)]

#: Appendix B orders plus ``ordered_at``: O1 2025-01-10 | O2 2025-02-10 | O3 2025-01-20 | O4 2025-03-05.
DATED_ORDERS = TableSpec(
    name="orders", columns=[*ORDERS.columns, ("ordered_at", "DATE")],
    rows=[(*row, day) for row, day in zip(ORDERS.rows, ("2025-01-10", "2025-02-10", "2025-01-20", "2025-03-05"))],
)


def tables(customers: list[tuple]) -> list[TableSpec]:
    return [TableSpec(name="customers", columns=_CUSTOMER_COLUMNS, rows=customers), DATED_ORDERS, RETURNS, STORES,
            MANAGERS]


def _field(name: str, datatype: DataType = DataType.STRING, *, pk: bool = False) -> DatasetField:
    return DatasetField(name=name, datatype=datatype, primary_key=pk)


def to_customers(*, trusted: bool = False, cardinality: Optional[RelationshipCardinality] = (
        RelationshipCardinality.MANY_TO_ONE), to: str = "customers", **extra: Any) -> DatasetRelationship:
    """``orders.customer_id → customers.customer_id``; ``trusted`` is passed only when set."""
    data: dict[str, Any] = {"to": to, "from_columns": ["customer_id"], "to_columns": ["customer_id"],
                            "cardinality": cardinality, **extra}
    if trusted:
        data["trusted"] = True
    return DatasetRelationship.model_validate(data)


def customers_dataset(*, keyed: bool = True, enforced_keys: Optional[list[list[str]]] = None,
                      **extra: Any) -> SlayerDataset:
    data: dict[str, Any] = {
        "name": "customers", "sql_table": "customers", "data_source": DS,
        "fields": [_field("customer_id", pk=keyed), _field("region"), _field("manager_id"), _field("tier"),
                   _field("weight", DataType.FLOAT), *extra.pop("fields", [])],
        **extra,
    }
    if enforced_keys is not None:
        data["enforced_keys"] = enforced_keys
    return SlayerDataset.model_validate(data)


def orders_dataset(*, relationship: Optional[DatasetRelationship] = None, trusted: bool = False,
                   relationships: Optional[list[DatasetRelationship]] = None, **extra: Any) -> SlayerDataset:
    """``relationships`` replaces the default ``to_customers`` edge (``relationship``) entirely."""
    return SlayerDataset(
        name="orders", sql_table="orders", data_source=DS,
        fields=[_field("order_id", pk=True), _field("customer_id"), _field("seller_id"), _field("store_id"),
                _field("manager_id"), _field("amount", DataType.FLOAT), _field("status"),
                _field("ordered_at", DataType.DATE), *extra.pop("fields", [])],
        relationships=(relationships if relationships is not None
                       else [relationship or to_customers(trusted=trusted)]),
        metrics=[DatasetMetric(name="total_revenue", expression="sum(amount)"), *extra.pop("metrics", [])],
        **extra,
    )


def seed_datasets(*, trusted: bool = False, customers: Optional[SlayerDataset] = None,
                  orders: Optional[SlayerDataset] = None) -> list[SlayerDataset]:
    return [customers or customers_dataset(), orders or orders_dataset(trusted=trusted), returns_dataset()]


@contextlib.asynccontextmanager
async def engine(backend: str, customers: list[tuple], datasets: Optional[list[SlayerDataset]] = None, *,
                 trusted: bool = False) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine over the seed with ``customers`` rows."""
    chosen = datasets if datasets is not None else seed_datasets(trusted=trusted)
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, tables(customers)),
                                  datasets=chosen) as (eng, _):
        yield eng


def region_revenue(resp: SlayerResponse) -> dict[Any, Any]:
    return by(resp, REGION, REVENUE)


def sql_of(resp: SlayerResponse) -> str:
    assert resp.sql is not None
    return " ".join(resp.sql.split())


def joins_customers_inline(resp: SlayerResponse) -> bool:
    """The statement joins the ``customers`` table itself, not a derived projection of it."""
    return "JOIN customers AS customers ON" in sql_of(resp)


# --------------------------------------------------------------------------- #
# The oracle's retail fixture with C1 duplicated and the customers primary key dropped from the DDL
# --------------------------------------------------------------------------- #
RETAIL = CORPUS / "compliance" / "wide_table" / "retail"
RETAIL_DS = "wide_table"


def dirty_retail_sql() -> str:
    text = (RETAIL / "retail.sql").read_text(encoding="utf-8")
    clean_ddl = "customer_id VARCHAR NOT NULL PRIMARY KEY,\n  region      VARCHAR NOT NULL"
    assert clean_ddl in text
    text = text.replace(clean_ddl, "customer_id VARCHAR NOT NULL,\n  region      VARCHAR NOT NULL")
    seed_rows = "  ('C4', 'NORTH');"
    assert seed_rows in text
    return text.replace(seed_rows, "  ('C4', 'NORTH'),\n  ('C1', 'EAST');")


def retail_model_text() -> str:
    return (RETAIL / "retail_model.ossie.yaml").read_text(encoding="utf-8")


@contextlib.asynccontextmanager
async def retail_engine(*, model_text: Optional[str] = None, kind: str = "yaml",
                        ) -> AsyncGenerator[SlayerQueryEngine]:
    """DuckDB over the dirty retail data with the retail document (or ``model_text``) stored."""
    with tempfile.TemporaryDirectory() as tmp:
        database = Path(tmp) / "oracle_retail.duckdb"
        connection = duckdb.connect(str(database))
        try:
            connection.execute(dirty_retail_sql())
        finally:
            connection.close()
        storage = make_storage(kind, Path(tmp))
        ds = DatasourceConfig(name=RETAIL_DS, type="duckdb", database=str(database))
        async with owned_query_engine(storage=storage, datasources=[ds]) as eng:
            await store_document(storage, model_text or retail_model_text(), data_source=RETAIL_DS)
            yield eng
