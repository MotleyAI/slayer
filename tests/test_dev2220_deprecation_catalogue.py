"""Every form deprecated until 3.0 still converts on its surface, with the warning its kind promises."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from slayer.api.server import create_app
from slayer.core.deprecation import DeprecatedInputFutureWarning
from slayer.core.models import DatasetMetric
from slayer.core.query import SlayerQuery
from slayer.memories.help_seed import seed_help_memories
from slayer.storage.base import resolve_storage
from tests._deprecated_input_helpers import orders_engine, recorded
from tests._dev2137_fixtures import list_engine
from tests._dev2187_fixtures import orders_engine as references_engine
from tests._dev2220_deprecation_catalogue import CASES, Case, Ctx, Env, seeded_shop
from tests._dev2277_fixtures import cycle_engine
from tests._ossie_vocabulary_fixtures import refine_vocab_engine, vocab_engine
from tests.search_helpers import seed_warehouse_datasets
from tests.test_law_removal_tripwire import PINNED

DEPRECATIONS = ("deprecated_input", "deprecated_spelling")


@pytest.fixture(scope="module")
async def env(tmp_path_factory: pytest.TempPathFactory) -> AsyncGenerator[Env]:
    async with AsyncExitStack() as stack:
        shop_base = tmp_path_factory.mktemp("shop")
        shop = await seeded_shop(shop_base)
        await seed_help_memories(shop)
        search = resolve_storage(str(tmp_path_factory.mktemp("search")))
        await seed_warehouse_datasets(search)
        warehouse_orders = await search.get_dataset("orders", data_source="warehouse")
        assert warehouse_orders is not None
        paid = DatasetMetric.model_validate({"name": "paid_revenue", "expression": "sum(amount_paid)"})
        await search.save_dataset(warehouse_orders.model_copy(update={"metrics": [*warehouse_orders.metrics, paid]}))
        yield Env(
            vocab=await stack.enter_async_context(vocab_engine("sqlite")),
            refine=await stack.enter_async_context(refine_vocab_engine("sqlite")),
            orders=await stack.enter_async_context(orders_engine()),
            refs=await stack.enter_async_context(references_engine("sqlite", edge_name="buyer")),
            lists=await stack.enter_async_context(list_engine()),
            cycle=await stack.enter_async_context(cycle_engine("sqlite")),
            shop=shop, shop_base=shop_base, rest=TestClient(create_app(storage=shop)), search=search,
        )


def _get(item: Any, key: str) -> Any:
    return item.get(key) if isinstance(item, dict) else getattr(item, key, None)


def _reports(item: Any, *, kind: str) -> bool:
    """``item`` is a structured warning of ``kind`` removed in 3.0, or a reply line naming 3.0."""
    if isinstance(item, str):
        return "3.0" in item
    return _get(item, "kind") == kind and _get(item, "removal") == "3.0"


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.id)
async def test_a_deprecated_form_converts_with_its_warning(
    case: Case, env: Env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    with recorded() as caught:
        observed = await case.check(Ctx(env=env, tmp_path=tmp_path, monkeypatch=monkeypatch))
    futures = [w for w in caught if issubclass(w.category, FutureWarning)]
    if case.channel == "stderr":
        assert futures == []
        [line] = observed.stderr_deprecations
        assert "3.0" in line
    elif case.kind == "deprecated_input":
        [w] = futures
        assert isinstance(w.message, DeprecatedInputFutureWarning)
        assert w.message.payload.removal == "3.0"
    elif case.kind == "deprecated_spelling":
        [w] = futures
        assert "3.0" in str(w.message)
    else:
        assert futures == []
    structured = [w for w in observed.warnings if isinstance(w, str) or _get(w, "kind") in DEPRECATIONS]
    if case.kind in DEPRECATIONS:
        assert all(_reports(w, kind=case.kind) for w in structured), structured
    else:
        assert structured == []
    if case.surfaced:
        assert any(_reports(w, kind=case.notice or case.kind) for w in observed.warnings), observed.warnings
    assert observed.deprecation_header == case.deprecation_header


def test_every_pinned_key_has_a_case() -> None:
    covered = {c.pinned for c in CASES}
    assert covered <= set(PINNED)
    assert sorted(k for k in PINNED if not k[1].startswith("slayer.") and k not in covered) == []


def test_case_ids_are_unique() -> None:
    ids = [c.id for c in CASES]
    assert len(ids) == len(set(ids))


@pytest.mark.parametrize("key", ["time_dimensions", "order"])
def test_a_reference_object_with_model_and_dataset_is_refused(key: str) -> None:
    ref = {"name": "created_at", "model": "orders", "dataset": "orders"}
    item = {"dimension": ref, "granularity": "month"} if key == "time_dimensions" else {"column": ref}
    with pytest.raises(ValidationError, match="keep only `dataset`"):
        SlayerQuery.model_validate({"population": "orders", key: [item]})
