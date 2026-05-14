"""Per-bucket ranking invariance (DEV-1414).

For a fixed ``(question, datasource, max_X)``, the user-visible list of
``X`` (``memories`` / ``example_queries`` / ``entities``) must be a pure
function of the corpus + question + that one cap. Changing the OTHER
two caps must not move any id in or out of the returned ``X`` list,
nor reorder it.

These tests exercise the bug reported in DEV-1414: the previous
``over_fetch_budget = max(max_memories + max_example_queries,
max_entities) * 5`` shared one candidate-pool cap across all three
channels, so changing ``max_entities`` or ``max_example_queries`` would
push memories in or out of the bottom of each channel's per-kind
ranking — and the membership/order at the top of the fused memory list
would shift even though the question and ``max_memories`` were fixed.
"""

from __future__ import annotations

import hashlib
import tempfile
from typing import AsyncIterator, List, Optional

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.embeddings import client as embedding_client
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Corpus fixture
# ---------------------------------------------------------------------------


_LEARNING_TOPICS = [
    "amount_paid is gross of refunds",
    "filter status='paid' for net revenue",
    "customer email may be NULL for anonymous checkouts",
    "shipping rates apply only to physical goods",
    "tax is computed at checkout, not at order placement",
    "refund window is 30 days from order placement",
    "loyalty points accrue on net revenue not gross",
    "warehouse code 'EU1' is the default Europe warehouse",
    "order status 'cancelled' excludes from revenue rollups",
    "amount_paid in cents, divide by 100 for dollars",
    "customer_id is FK to customers.id",
    "checkout sessions older than 24h are abandoned",
    "premium customers have customer_tier='gold'",
    "free shipping over $50 net of tax",
    "anonymous checkouts have NULL customer_id",
    "discount_code applies before tax computation",
    "order id is monotonic and never reused",
    "subscription orders have recurring=true",
    "fraud_check is required for orders over $1000",
    "currency is always USD for the warehouse dataset",
    "customer tier upgrades trigger on $5000 lifetime spend",
    "refunded orders retain their original amount_paid",
    "shipping warehouse selection is FIFO by region",
    "email bounces flip the customer to inactive",
    "discount stacking is capped at 30 percent",
    "gold tier customers skip the fraud queue",
    "warehouse closures move orders to backup region",
    "abandoned checkouts older than 7 days are purged",
    "customer email change requires re-verification",
    "tax exemption applies to gold tier government accounts",
    "amount_paid excludes shipping and tax",
    "anonymous orders cannot have loyalty points",
    "duplicate customer rows are merged on email match",
    "warehouse capacity is in physical units not value",
    "order amount totals always agree with payment ledger",
    "customer_tier is set on first paid order",
    "EU2 warehouse opened in Q2 2024",
    "refund processing time is 5-7 business days",
    "free shipping promo requires registered customer",
    "discount_code expiry is checked at checkout",
]


def _make_models() -> List[SlayerModel]:
    return [
        SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="warehouse",
            description=(
                "Checkout orders fact table including shipping, refund, "
                "and tax detail."
            ),
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(
                    name="customer_id", type=DataType.INT,
                    description="FK to customers.id, NULL for anonymous.",
                ),
                Column(
                    name="amount_paid", type=DataType.DOUBLE,
                    description="Net paid in cents.",
                ),
                Column(
                    name="status", type=DataType.TEXT,
                    description="paid|refunded|cancelled|abandoned.",
                ),
                Column(
                    name="shipped_at", type=DataType.TIMESTAMP,
                    description="When the order shipped from warehouse.",
                ),
                Column(
                    name="discount_code", type=DataType.TEXT,
                    description="Optional promotional discount code.",
                ),
            ],
        ),
        SlayerModel(
            name="customers",
            sql_table="public.customers",
            data_source="warehouse",
            description="Customer master data.",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(
                    name="email", type=DataType.TEXT,
                    description="Customer email; NULL for anonymous.",
                ),
                Column(
                    name="customer_tier", type=DataType.TEXT,
                    description="Tier: gold|silver|standard.",
                ),
            ],
        ),
        SlayerModel(
            name="warehouses",
            sql_table="public.warehouses",
            data_source="warehouse",
            description="Physical warehouses for fulfilment.",
            columns=[
                Column(name="code", type=DataType.TEXT, primary_key=True),
                Column(name="region", type=DataType.TEXT),
            ],
        ),
    ]


async def _seed_invariance_corpus(storage: StorageBackend) -> None:
    """Seed a corpus large enough to exercise the bottom-cliff cases that
    used to leak through the shared over_fetch budget."""
    await storage.save_datasource(DatasourceConfig(
        name="warehouse", type="sqlite", database=":memory:",
    ))
    for model in _make_models():
        await storage.save_model(model)

    # 20 learning-only memories.
    for i, topic in enumerate(_LEARNING_TOPICS):
        # Spread entity tags so different memories surface for different
        # questions.
        entities: List[str]
        if "amount_paid" in topic or "paid" in topic or "revenue" in topic:
            entities = ["warehouse.orders.amount_paid"]
        elif "email" in topic or "anonymous" in topic:
            entities = ["warehouse.customers.email"]
        elif "ship" in topic or "warehouse" in topic:
            entities = ["warehouse.warehouses"]
        elif "customer" in topic and "tier" in topic:
            entities = ["warehouse.customers.customer_tier"]
        elif "customer" in topic:
            entities = ["warehouse.customers"]
        elif "status" in topic:
            entities = ["warehouse.orders.status"]
        elif "discount" in topic:
            entities = ["warehouse.orders.discount_code"]
        elif "checkout" in topic or "fraud" in topic:
            entities = ["warehouse.orders"]
        else:
            entities = ["warehouse"]
        await storage.save_memory(
            learning=f"KB{i:02d}: {topic}.",
            entities=entities,
        )

    # 8 query-bearing memories — drive the example_queries bucket.
    for i in range(8):
        await storage.save_memory(
            learning=f"Example query {i}: revenue rollup pattern.",
            entities=["warehouse.orders.amount_paid"],
            query=SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="amount_paid:sum")],
            ),
        )


@pytest_asyncio.fixture
async def storage_with_invariance_corpus() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await _seed_invariance_corpus(storage)
        yield storage


@pytest_asyncio.fixture
async def service_invariance(
    storage_with_invariance_corpus: YAMLStorage,
) -> SearchService:
    return SearchService(storage=storage_with_invariance_corpus)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _ids(service: SearchService, **kwargs) -> dict[str, list]:
    response = await service.search(**kwargs)
    return {
        "memories": [h.id for h in response.memories],
        "example_queries": [h.id for h in response.example_queries],
        "entities": [h.id for h in response.entities],
    }


# ---------------------------------------------------------------------------
# Memory-bucket invariance under entity / example-query caps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_memories_invariant_under_max_entities(
    service_invariance: SearchService,
) -> None:
    """Varying ``max_entities`` (with question + datasource +
    max_memories + max_example_queries fixed) must not change the
    `memories` id list or its order. Tight caps exercise the bottom
    cliff in the legacy ``over_fetch_budget``."""
    base = await _ids(
        service_invariance,
        question="amount paid refund revenue customer email warehouse",
        datasource="warehouse",
        max_memories=3,
        max_example_queries=0,
        max_entities=2,
    )
    for max_entities in (0, 1, 5, 50, 200):
        other = await _ids(
            service_invariance,
            question="amount paid refund revenue customer email warehouse",
            datasource="warehouse",
            max_memories=3,
            max_example_queries=0,
            max_entities=max_entities,
        )
        assert other["memories"] == base["memories"], (
            f"memories order changed when max_entities went 2 -> "
            f"{max_entities}: {base['memories']} vs {other['memories']}"
        )


@pytest.mark.asyncio
async def test_memories_invariant_under_max_example_queries(
    service_invariance: SearchService,
) -> None:
    base = await _ids(
        service_invariance,
        question="amount paid refund revenue customer email warehouse",
        datasource="warehouse",
        max_memories=3,
        max_example_queries=0,
        max_entities=2,
    )
    for max_example_queries in (0, 1, 5, 20, 100):
        other = await _ids(
            service_invariance,
            question="amount paid refund revenue customer email warehouse",
            datasource="warehouse",
            max_memories=3,
            max_example_queries=max_example_queries,
            max_entities=2,
        )
        assert other["memories"] == base["memories"], (
            f"memories order changed when max_example_queries went 0 -> "
            f"{max_example_queries}: {base['memories']} vs "
            f"{other['memories']}"
        )


# ---------------------------------------------------------------------------
# example_queries-bucket invariance under memory / entity caps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_example_queries_invariant_under_max_memories(
    service_invariance: SearchService,
) -> None:
    base = await _ids(
        service_invariance,
        question="revenue rollup amount paid",
        datasource="warehouse",
        max_memories=5,
        max_example_queries=5,
        max_entities=5,
    )
    for max_memories in (0, 1, 10, 50):
        other = await _ids(
            service_invariance,
            question="revenue rollup amount paid",
            datasource="warehouse",
            max_memories=max_memories,
            max_example_queries=5,
            max_entities=5,
        )
        assert other["example_queries"] == base["example_queries"], (
            f"example_queries order changed when max_memories went 5 -> "
            f"{max_memories}: {base['example_queries']} vs "
            f"{other['example_queries']}"
        )


@pytest.mark.asyncio
async def test_example_queries_invariant_under_max_entities(
    service_invariance: SearchService,
) -> None:
    base = await _ids(
        service_invariance,
        question="revenue rollup amount paid",
        datasource="warehouse",
        max_memories=5,
        max_example_queries=5,
        max_entities=5,
    )
    for max_entities in (0, 1, 20, 100):
        other = await _ids(
            service_invariance,
            question="revenue rollup amount paid",
            datasource="warehouse",
            max_memories=5,
            max_example_queries=5,
            max_entities=max_entities,
        )
        assert other["example_queries"] == base["example_queries"], (
            f"example_queries order changed when max_entities went 5 -> "
            f"{max_entities}: {base['example_queries']} vs "
            f"{other['example_queries']}"
        )


# ---------------------------------------------------------------------------
# entities-bucket invariance under memory / example-query caps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_entities_invariant_under_max_memories(
    service_invariance: SearchService,
) -> None:
    base = await _ids(
        service_invariance,
        question="amount paid refund customer email warehouse shipping",
        datasource="warehouse",
        max_memories=2,
        max_example_queries=0,
        max_entities=3,
    )
    for max_memories in (0, 1, 20, 100):
        other = await _ids(
            service_invariance,
            question="amount paid refund customer email warehouse shipping",
            datasource="warehouse",
            max_memories=max_memories,
            max_example_queries=0,
            max_entities=3,
        )
        assert other["entities"] == base["entities"], (
            f"entities order changed when max_memories went 2 -> "
            f"{max_memories}: {base['entities']} vs {other['entities']}"
        )


@pytest.mark.asyncio
async def test_entities_invariant_under_max_example_queries(
    service_invariance: SearchService,
) -> None:
    base = await _ids(
        service_invariance,
        question="amount paid refund customer email warehouse shipping",
        datasource="warehouse",
        max_memories=2,
        max_example_queries=0,
        max_entities=3,
    )
    for max_example_queries in (0, 1, 5, 30):
        other = await _ids(
            service_invariance,
            question="amount paid refund customer email warehouse shipping",
            datasource="warehouse",
            max_memories=2,
            max_example_queries=max_example_queries,
            max_entities=3,
        )
        assert other["entities"] == base["entities"], (
            f"entities order changed when max_example_queries went 0 -> "
            f"{max_example_queries}: {base['entities']} vs "
            f"{other['entities']}"
        )


# ---------------------------------------------------------------------------
# DEV-1414 repro tuples
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dev_1414_repro_tuples_yield_same_top_memories(
    service_invariance: SearchService,
) -> None:
    """The exact three call shapes from DEV-1414 (max_memories fixed at
    the smaller of the two values, varying entity / example-query caps)
    must yield identical top-``min(max_memories)`` memory ids.

    Original repro held max_memories=10 across A and B, then bumped to
    15 in C. Compare the prefix of length 10 across all three."""
    call_a = await _ids(
        service_invariance,
        question="amount paid refund revenue customer email",
        datasource="warehouse",
        max_memories=10,
        max_entities=10,
        max_example_queries=5,
    )
    call_b = await _ids(
        service_invariance,
        question="amount paid refund revenue customer email",
        datasource="warehouse",
        max_memories=10,
        max_entities=0,
        max_example_queries=0,
    )
    call_c = await _ids(
        service_invariance,
        question="amount paid refund revenue customer email",
        datasource="warehouse",
        max_memories=15,
        max_entities=5,
        max_example_queries=2,
    )
    # A and B share max_memories=10 → full equality.
    assert call_a["memories"] == call_b["memories"]
    # C asks for 15 memories; the first 10 must match A and B.
    assert call_c["memories"][:10] == call_a["memories"]


# ---------------------------------------------------------------------------
# Channel-3 active path (embedding) — invariance must hold there too
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def storage_with_embeddings(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[YAMLStorage]:
    """Same corpus as the base fixture, plus a deterministic embedding
    backend stubbed in so channel 3 actually fires."""
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await _seed_invariance_corpus(storage)

        embedding_client._reset_query_cache()
        monkeypatch.setattr(embedding_client, "is_available", lambda: True)

        # Deterministic embeddings: hash the rendered text into a tiny
        # vector so ranks vary across docs but are reproducible across
        # interpreter runs (Python's built-in ``hash`` is randomised
        # per process, so use sha256 here).
        def _vec(text: str) -> List[float]:
            out: List[float] = []
            for i in range(8):
                digest = hashlib.sha256(
                    f"{text}|{i}".encode("utf-8"),
                ).digest()
                # First two bytes give a stable 16-bit unsigned int.
                out.append(((digest[0] << 8) | digest[1]) / 65535.0)
            return out

        async def stub_embed_batch(  # NOSONAR(S7503) — stub matches embed_batch async signature
            texts: List[str], *, model: Optional[str] = None,
        ) -> List[Optional[List[float]]]:
            return [_vec(t) for t in texts]

        async def stub_embed_query(  # NOSONAR(S7503) — stub matches embed_query async signature
            text: str, *, model: Optional[str] = None,
        ) -> List[float]:
            return _vec(text)

        monkeypatch.setattr(
            "slayer.embeddings.service.embed_batch", stub_embed_batch,
        )
        monkeypatch.setattr(
            embedding_client, "embed_query", stub_embed_query,
        )

        from slayer.embeddings.service import EmbeddingService
        emb_service = EmbeddingService(storage=storage)
        persisted_models = []
        for m in _make_models():
            persisted = await storage.get_model(
                m.name, data_source="warehouse",
            )
            assert persisted is not None
            persisted_models.append(persisted)
            await emb_service.refresh_model_subtree(persisted)
        await emb_service.refresh_datasource(
            name="warehouse", models=persisted_models,
        )
        for mem in await storage.list_memories(entities=None):
            await emb_service.refresh_memory(mem)

        yield storage


@pytest_asyncio.fixture
async def service_with_embeddings(
    storage_with_embeddings: YAMLStorage,
) -> SearchService:
    return SearchService(storage=storage_with_embeddings)


@pytest.mark.asyncio
async def test_memories_invariant_under_max_entities_with_channel_3_active(
    service_with_embeddings: SearchService,
) -> None:
    base = await _ids(
        service_with_embeddings,
        question="amount paid refund revenue customer email",
        datasource="warehouse",
        max_memories=10,
        max_example_queries=2,
        max_entities=5,
    )
    for max_entities in (0, 20, 50):
        other = await _ids(
            service_with_embeddings,
            question="amount paid refund revenue customer email",
            datasource="warehouse",
            max_memories=10,
            max_example_queries=2,
            max_entities=max_entities,
        )
        assert other["memories"] == base["memories"], (
            f"channel-3 active: memories changed when max_entities went "
            f"5 -> {max_entities}"
        )


@pytest.mark.asyncio
async def test_entities_invariant_under_max_memories_with_channel_3_active(
    service_with_embeddings: SearchService,
) -> None:
    base = await _ids(
        service_with_embeddings,
        question="amount paid refund customer email warehouse",
        datasource="warehouse",
        max_memories=5,
        max_example_queries=2,
        max_entities=10,
    )
    for max_memories in (0, 20, 50):
        other = await _ids(
            service_with_embeddings,
            question="amount paid refund customer email warehouse",
            datasource="warehouse",
            max_memories=max_memories,
            max_example_queries=2,
            max_entities=10,
        )
        assert other["entities"] == base["entities"], (
            f"channel-3 active: entities changed when max_memories went "
            f"5 -> {max_memories}"
        )
