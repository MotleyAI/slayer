"""Seeds, engines and helpers for the qualified-reference tests (spec: queries/references).

Ossie Layer 3 Appendix B seed, datasource ``test``::

    customers (customer_id, region, manager_id): C1 EAST M1 | C2 EAST M1 | C3 WEST M2 | C4 NORTH M2
    orders (order_id, customer_id, seller_id, store_id, manager_id, amount, status):
        O1 C1 C2 S1 M1 100 completed | O2 C1 C3 S2 M1 200 open | O3 C2 C4 S1 M2 50 completed | O4 C3 C1 S2 M2 300 open
    returns (return_id, customer_id, amount): R1 C1 25 | R2 C4 10
    stores (store_id, customer_id): S1 C1 | S2 C3
    managers (manager_id, name): M1 mia | M2 max

``orders → customers`` (unnamed, or ``buyer``), ``returns → customers`` many-to-one; the metric
``orders.total_revenue = sum(amount)``. Optional: ``seller`` (``orders → customers``), ``stores``
(``orders → stores → customers``, both many-to-one), ``managers`` (``orders → managers`` many-to-one,
``managers → customers`` one-to-many).

Cycle seed: ``x → y → z → x``, every edge many-to-one::

    x (id, y_id, v, amount): 1 1 x1 1 | 2 2 x2 2
    y (id, z_id, v):         1 1 y1   | 2 2 y2
    z (id, x_id, v, amount): 1 2 z1 10 | 2 2 z2 20
"""

from __future__ import annotations

import contextlib
import warnings
from collections.abc import AsyncGenerator
from typing import Any, Optional

from slayer.core.deprecation import DeprecatedInputFutureWarning
from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.core.warnings import DeprecatedInputWarning
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.access_filtered import AccessFilteredStorage

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")
DS = "test"

CUSTOMERS = TableSpec(
    name="customers", columns=[("customer_id", "TEXT"), ("region", "TEXT"), ("manager_id", "TEXT")],
    rows=[("C1", "EAST", "M1"), ("C2", "EAST", "M1"), ("C3", "WEST", "M2"), ("C4", "NORTH", "M2")],
)
ORDERS = TableSpec(
    name="orders",
    columns=[("order_id", "TEXT"), ("customer_id", "TEXT"), ("seller_id", "TEXT"), ("store_id", "TEXT"),
             ("manager_id", "TEXT"), ("amount", "DOUBLE"), ("status", "TEXT")],
    rows=[
        ("O1", "C1", "C2", "S1", "M1", 100.0, "completed"), ("O2", "C1", "C3", "S2", "M1", 200.0, "open"),
        ("O3", "C2", "C4", "S1", "M2", 50.0, "completed"), ("O4", "C3", "C1", "S2", "M2", 300.0, "open"),
    ],
)
RETURNS = TableSpec(
    name="returns", columns=[("return_id", "TEXT"), ("customer_id", "TEXT"), ("amount", "DOUBLE")],
    rows=[("R1", "C1", 25.0), ("R2", "C4", 10.0)],
)
STORES = TableSpec(name="stores", columns=[("store_id", "TEXT"), ("customer_id", "TEXT")],
                   rows=[("S1", "C1"), ("S2", "C3")])
MANAGERS = TableSpec(name="managers", columns=[("manager_id", "TEXT"), ("name", "TEXT")],
                     rows=[("M1", "mia"), ("M2", "max")])
SEED_TABLES = [CUSTOMERS, ORDERS, RETURNS, STORES, MANAGERS]

X = TableSpec(name="x", columns=[("id", "INT"), ("y_id", "INT"), ("v", "TEXT"), ("amount", "DOUBLE")],
              rows=[(1, 1, "x1", 1.0), (2, 2, "x2", 2.0)])
Y = TableSpec(name="y", columns=[("id", "INT"), ("z_id", "INT"), ("v", "TEXT")], rows=[(1, 1, "y1"), (2, 2, "y2")])
Z = TableSpec(name="z", columns=[("id", "INT"), ("x_id", "INT"), ("v", "TEXT"), ("amount", "DOUBLE")],
              rows=[(1, 2, "z1", 10.0), (2, 2, "z2", 20.0)])
CYCLE_TABLES = [X, Y, Z]


def _field(name: str, datatype: DataType, *, pk: bool = False) -> DatasetField:
    return DatasetField(name=name, datatype=datatype, primary_key=pk)


def rel(to: str, frm: str, to_col: str, *, name: Optional[str] = None,
        cardinality: RelationshipCardinality = RelationshipCardinality.MANY_TO_ONE) -> DatasetRelationship:
    return DatasetRelationship(to=to, name=name, from_columns=[frm], to_columns=[to_col], cardinality=cardinality)


def customers_dataset(**extra: Any) -> SlayerDataset:
    return SlayerDataset(
        name="customers", sql_table="customers", data_source=DS,
        fields=[_field("customer_id", DataType.STRING, pk=True), _field("region", DataType.STRING),
                _field("manager_id", DataType.STRING), *extra.pop("fields", [])],
        **extra,
    )


def orders_dataset(*, buyer: bool = False, seller: bool = False, stores: bool = False, managers: bool = False,
                   **extra: Any) -> SlayerDataset:
    rels = [rel("customers", "customer_id", "customer_id", name="buyer" if buyer else None)]
    if seller:
        rels.append(rel("customers", "seller_id", "customer_id", name="seller"))
    if stores:
        rels.append(rel("stores", "store_id", "store_id"))
    if managers:
        rels.append(rel("managers", "manager_id", "manager_id"))
    return SlayerDataset(
        name="orders", sql_table="orders", data_source=DS,
        fields=[_field("order_id", DataType.STRING, pk=True), _field("customer_id", DataType.STRING),
                _field("seller_id", DataType.STRING), _field("store_id", DataType.STRING),
                _field("manager_id", DataType.STRING), _field("amount", DataType.FLOAT),
                _field("status", DataType.STRING), *extra.pop("fields", [])],
        relationships=rels,
        metrics=[DatasetMetric(name="total_revenue", expression="sum(amount)"), *extra.pop("metrics", [])],
        **extra,
    )


def returns_dataset(**extra: Any) -> SlayerDataset:
    return SlayerDataset(
        name="returns", sql_table="returns", data_source=DS,
        fields=[_field("return_id", DataType.STRING, pk=True), _field("customer_id", DataType.STRING),
                _field("amount", DataType.FLOAT)],
        relationships=[rel("customers", "customer_id", "customer_id")],
        **extra,
    )


def stores_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="stores", sql_table="stores", data_source=DS,
        fields=[_field("store_id", DataType.STRING, pk=True), _field("customer_id", DataType.STRING)],
        relationships=[rel("customers", "customer_id", "customer_id")],
    )


def managers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="managers", sql_table="managers", data_source=DS,
        fields=[_field("manager_id", DataType.STRING, pk=True), _field("name", DataType.STRING)],
        relationships=[rel("customers", "manager_id", "manager_id", cardinality=RelationshipCardinality.ONE_TO_MANY)],
    )


def seed_datasets(*, buyer: bool = False, seller: bool = False, stores: bool = False, managers: bool = False,
                  customers: Optional[SlayerDataset] = None, orders: Optional[SlayerDataset] = None,
                  returns: Optional[SlayerDataset] = None) -> list[SlayerDataset]:
    """The seed datasets; ``stores`` / ``managers`` add those datasets and their edges."""
    out = [
        customers or customers_dataset(),
        orders or orders_dataset(buyer=buyer, seller=seller, stores=stores, managers=managers),
        returns or returns_dataset(),
    ]
    if stores:
        out.append(stores_dataset())
    if managers:
        out.append(managers_dataset())
    return out


def _cycle_dataset(name: str, to: str, fk: str, *, amount: bool) -> SlayerDataset:
    fields = [_field("id", DataType.INTEGER, pk=True), _field(fk, DataType.INTEGER), _field("v", DataType.STRING)]
    if amount:
        fields.append(_field("amount", DataType.FLOAT))
    return SlayerDataset(name=name, sql_table=name, data_source=DS, fields=fields, relationships=[rel(to, fk, "id")])


def cycle_datasets() -> list[SlayerDataset]:
    return [
        _cycle_dataset("x", "y", "y_id", amount=True),
        _cycle_dataset("y", "z", "z_id", amount=False),
        _cycle_dataset("z", "x", "x_id", amount=True),
    ]


@contextlib.asynccontextmanager
async def seed_engine(backend: str, *, datasets: Optional[list[SlayerDataset]] = None, buyer: bool = False,
                      seller: bool = False, stores: bool = False, managers: bool = False,
                      ) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine over the Appendix B seed (flags as in ``seed_datasets``)."""
    chosen = datasets if datasets is not None else seed_datasets(
        buyer=buyer, seller=seller, stores=stores, managers=managers)
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, SEED_TABLES), datasets=chosen,
    ) as (engine, _db):
        yield engine


@contextlib.asynccontextmanager
async def cycle_engine(backend: str) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, CYCLE_TABLES), datasets=cycle_datasets(),
    ) as (engine, _db):
        yield engine


async def add_other_datasource(engine: SlayerQueryEngine, *, dataset: str = "events") -> None:
    """A dataset ``dataset`` that exists only in datasource ``other``."""
    await engine.storage.save_datasource(DatasourceConfig(name="other", type="sqlite", database=":memory:"))
    await engine.storage.save_dataset(SlayerDataset(
        name=dataset, sql_table=dataset, data_source="other", fields=[_field("id", DataType.INTEGER, pk=True)],
    ))


@contextlib.asynccontextmanager
async def scoped(engine: SlayerQueryEngine, *, tags: frozenset[str] = frozenset()) -> AsyncGenerator[SlayerQueryEngine]:
    """``engine``'s store as a caller holding ``tags`` sees it."""
    eng = SlayerQueryEngine(storage=AccessFilteredStorage(engine.storage, tags=tags, bypass=False))
    try:
        yield eng
    finally:
        eng.close()


def hidden(dataset: SlayerDataset) -> SlayerDataset:
    return dataset.model_copy(update={"access_tags": ["secret"]})


# --------------------------------------------------------------------------- #
# Results and warnings
# --------------------------------------------------------------------------- #
def table(resp: SlayerResponse, *keys: str) -> set[tuple]:
    """The rows of ``resp`` projected on exact result ``keys``, numbers as floats."""
    def norm(v: Any) -> Any:
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v
    return {tuple(norm(r[k]) for k in keys) for r in resp.data}


def by(resp: SlayerResponse, key: str, value_key: str) -> dict[Any, Any]:
    """``{row[key]: row[value_key]}`` with numbers as floats; ``key`` values must be unique."""
    out: dict[Any, Any] = {}
    for r in resp.data:
        assert r[key] not in out, (key, r[key])
        v = r[value_key]
        out[r[key]] = float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v
    return out


async def execute_recording(engine: SlayerQueryEngine, query: Any, **kwargs: Any,
                            ) -> tuple[SlayerResponse, list[warnings.WarningMessage]]:
    """``engine.execute(query)`` and every Python warning it raised."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        resp = await engine.execute(query, **kwargs)
    return resp, list(caught)


def future_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    return [str(w.message) for w in caught if issubclass(w.category, FutureWarning)]


def conversions(caught: list[warnings.WarningMessage]) -> list[tuple[str, str]]:
    """``(old, new)`` of every recorded ``deprecated_input`` conversion."""
    return [(w.message.payload.old, w.message.payload.new) for w in caught
            if isinstance(w.message, DeprecatedInputFutureWarning)]


def response_conversions(resp: SlayerResponse) -> list[tuple[str, str]]:
    """``(old, new)`` of the response's structured ``deprecated_input`` warnings."""
    return [(w.old, w.new) for w in resp.warnings or [] if isinstance(w, DeprecatedInputWarning)]


def notices(resp: SlayerResponse, *needles: str) -> list[str]:
    """Human messages of non-deprecation response warnings mentioning every needle."""
    return [msg for w in resp.warnings or []
            if getattr(w, "kind", None) != "deprecated_input"
            and all(n in (msg := w.human_message()) for n in needles)]
