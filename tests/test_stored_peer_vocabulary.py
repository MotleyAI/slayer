"""A stored dataset's load reads its peers in one vocabulary, whatever version each peer is stored at.

The pre-2.0 spellings below are deliberate stored documents: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.storage.document_keys import CURRENT_KEYS, LEGACY_KEYS, dataset_in
from tests._stored_upgrade_fixtures import (
    AMOUNT_BY_REGION,
    BACKENDS,
    DS,
    by_dimension,
    customers_v10,
    orders_v10,
    raw_store,
    run,
    seed_shop,
)

LEGACY: dict[str, Any] = {
    "version": 10, "name": "orders",
    "columns": [{"name": "id", "type": "INT"}],
    "measures": [{"name": "rev", "formula": "sum(amount)", "type": "DOUBLE"}],
    "aggregations": [{"name": "doubled", "formula": "SUM({value}) * 2"}],
    "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
}
CURRENT: dict[str, Any] = {
    "version": 10, "name": "orders",
    "fields": [{"name": "id", "datatype": "INT"}],
    "metrics": [{"name": "rev", "expression": "sum(amount)", "datatype": "DOUBLE"}],
    "aggregations": [{"name": "doubled", "expression": "SUM({value}) * 2"}],
    "relationships": [{"to": "customers", "join_pairs": [["customer_id", "id"]]}],
}
REGION_QUERY = {"population": "orders", "dimensions": ["customers.region"], "metrics": ["sum(amount)"]}


@pytest.mark.parametrize(("raw", "keys", "expected"), [
    (LEGACY, CURRENT_KEYS, CURRENT), (CURRENT, LEGACY_KEYS, LEGACY),
    (CURRENT, CURRENT_KEYS, CURRENT), (LEGACY, LEGACY_KEYS, LEGACY),
], ids=["legacy-to-current", "current-to-legacy", "current-kept", "legacy-kept"])
def test_a_raw_dataset_is_spelled_in_the_asked_vocabulary(raw: dict, keys: Any, expected: dict) -> None:
    assert dataset_in(raw, keys) == expected


def test_the_raw_document_is_not_mutated() -> None:
    raw = {**LEGACY, "joins": [dict(j) for j in LEGACY["joins"]]}
    dataset_in(raw, CURRENT_KEYS)
    assert raw["joins"][0] == {"target_model": "customers", "join_pairs": [["customer_id", "id"]]}


@pytest.fixture
def shop_ds(tmp_path: Path) -> Iterator[DatasourceConfig]:
    db = str(tmp_path / "shop.db")
    seed_shop(db, dialect="sqlite")
    ds = DatasourceConfig(name=DS, type="sqlite", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


def orders_v26() -> dict[str, Any]:
    """``orders_v10`` as this release stores it."""
    return {"version": 26, **dataset_in({k: v for k, v in orders_v10().items() if k != "version"}, CURRENT_KEYS)}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("orders", [orders_v10, orders_v26], ids=["legacy-peer", "current-peer"])
async def test_a_key_declared_only_by_a_peer_relationship_is_declared_on_load(
    tmp_path: Path, shop_ds: DatasourceConfig, backend: str, orders: Any,
) -> None:
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                              models=[orders(), customers_v10(declare_pk=False)])
    customers = await storage.get_dataset("customers", data_source=DS)
    assert customers is not None
    assert "id" in [f.name for f in customers.fields]
    assert by_dimension(await run(storage, REGION_QUERY), dim_suffix="region") == AMOUNT_BY_REGION
