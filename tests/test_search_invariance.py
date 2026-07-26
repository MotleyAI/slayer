"""Per-result ranking invariance.

For a fixed ``(question, datasource)``, the ranked order within each
kind-category (memories / example_queries / entities) must not change
when ``max_results`` is increased. Items surfaced at a smaller cap must
appear in the same order at a larger cap — only new items may be appended
at the bottom.

These tests replace the pre-flat-API per-bucket invariance suite. The old
tests exercised that changing ``max_entities`` didn't perturb ``memories``,
etc.; that property is a consequence of independent per-kind ranking and
is validated here by checking order-stability as ``max_results`` grows.
"""

from __future__ import annotations

import hashlib
import tempfile
from collections.abc import AsyncIterator

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


def _make_models() -> list[SlayerModel]:
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


def _entities_for_topic(topic: str) -> list[str]:
    """Pick canonical entity tags for a learning-topic string. Pulled
    out of ``_seed_invariance_corpus`` so each branch stays separate
    from the seeding loop's control flow."""
    if "amount_paid" in topic or "paid" in topic or "revenue" in topic:
        return ["warehouse.orders.amount_paid"]
    if "email" in topic or "anonymous" in topic:
        return ["warehouse.customers.email"]
    if "ship" in topic or "warehouse" in topic:
        return ["warehouse.warehouses"]
    if "customer" in topic and "tier" in topic:
        return ["warehouse.customers.customer_tier"]
    if "customer" in topic:
        return ["warehouse.customers"]
    if "status" in topic:
        return ["warehouse.orders.status"]
    if "discount" in topic:
        return ["warehouse.orders.discount_code"]
    if "checkout" in topic or "fraud" in topic:
        return ["warehouse.orders"]
    return ["warehouse"]


async def _seed_invariance_corpus(storage: StorageBackend) -> None:
    """Seed a corpus large enough to exercise the bottom-cliff cases that
    used to leak through the shared over_fetch budget."""
    await storage.save_datasource(DatasourceConfig(
        name="warehouse", type="sqlite", database=":memory:",
    ))
    for model in _make_models():
        await storage.save_model(model)

    # 20+ learning-only memories tagged by topic.
    for i, topic in enumerate(_LEARNING_TOPICS):
        await storage.save_memory(
            learning=f"KB{i:02d}: {topic}.",
            entities=_entities_for_topic(topic),
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


async def _ids_by_kind(service: SearchService, **kwargs) -> dict[str, list]:
    """Return per-kind id lists from a search response."""
    response = await service.search(**kwargs)
    return {
        "memories": [h.id for h in response.results if h.kind == "memory" and h.query is None],
        "example_queries": [h.id for h in response.results if h.kind == "memory" and h.query is not None],
        "entities": [h.id for h in response.results if h.kind != "memory"],
    }


def _is_prefix(shorter: list, longer: list) -> bool:
    """Return True if ``shorter`` is a prefix of ``longer``."""
    return longer[:len(shorter)] == shorter


# ---------------------------------------------------------------------------
# Memory-bucket order stability as max_results grows
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_memories_order_stable_as_max_results_grows(
    service_invariance: SearchService,
) -> None:
    """Items surfaced at a smaller max_results must appear in the same order
    at a larger cap — only new items may be appended at the bottom.

    With a flat ranked list the invariant is: the ids returned at cap N are
    a prefix of those returned at cap N+k (within the same kind)."""
    small = await _ids_by_kind(
        service_invariance,
        question="amount paid refund revenue customer email warehouse",
        datasource="warehouse",
        max_results=5,
    )
    large = await _ids_by_kind(
        service_invariance,
        question="amount paid refund revenue customer email warehouse",
        datasource="warehouse",
        max_results=30,
    )
    assert _is_prefix(small["memories"], large["memories"]), (
        f"memory order changed as max_results grew: "
        f"{small['memories']} is not a prefix of {large['memories']}"
    )


@pytest.mark.asyncio
async def test_example_queries_order_stable_as_max_results_grows(
    service_invariance: SearchService,
) -> None:
    small = await _ids_by_kind(
        service_invariance,
        question="revenue rollup amount paid",
        datasource="warehouse",
        max_results=5,
    )
    large = await _ids_by_kind(
        service_invariance,
        question="revenue rollup amount paid",
        datasource="warehouse",
        max_results=30,
    )
    assert _is_prefix(small["example_queries"], large["example_queries"]), (
        f"example_queries order changed as max_results grew: "
        f"{small['example_queries']} is not a prefix of {large['example_queries']}"
    )


@pytest.mark.asyncio
async def test_entities_order_stable_as_max_results_grows(
    service_invariance: SearchService,
) -> None:
    small = await _ids_by_kind(
        service_invariance,
        question="amount paid refund customer email warehouse shipping",
        datasource="warehouse",
        max_results=5,
    )
    large = await _ids_by_kind(
        service_invariance,
        question="amount paid refund customer email warehouse shipping",
        datasource="warehouse",
        max_results=30,
    )
    assert _is_prefix(small["entities"], large["entities"]), (
        f"entities order changed as max_results grew: "
        f"{small['entities']} is not a prefix of {large['entities']}"
    )


# ---------------------------------------------------------------------------
# DEV-1414 repro: same question, same question, different max_results
# Top items must be stable across calls
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dev_1414_repro_tuples_yield_same_top_memories(
    service_invariance: SearchService,
) -> None:
    """With a flat list, three calls with increasing max_results must
    yield the same top-N ids in the same order (prefix property)."""
    call_a = await _ids_by_kind(
        service_invariance,
        question="amount paid refund revenue customer email",
        datasource="warehouse",
        max_results=10,
    )
    call_b = await _ids_by_kind(
        service_invariance,
        question="amount paid refund revenue customer email",
        datasource="warehouse",
        max_results=20,
    )
    call_c = await _ids_by_kind(
        service_invariance,
        question="amount paid refund revenue customer email",
        datasource="warehouse",
        max_results=30,
    )
    # A is a prefix of B.
    assert _is_prefix(call_a["memories"], call_b["memories"]), (
        f"memories prefix violated: {call_a['memories']} vs {call_b['memories']}"
    )
    # A is a prefix of C.
    assert _is_prefix(call_a["memories"], call_c["memories"]), (
        f"memories prefix violated: {call_a['memories']} vs {call_c['memories']}"
    )


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
        def _vec(text: str) -> list[float]:
            out: list[float] = []
            for i in range(8):
                digest = hashlib.sha256(
                    f"{text}|{i}".encode("utf-8"),
                ).digest()
                # First two bytes give a stable 16-bit unsigned int.
                out.append(((digest[0] << 8) | digest[1]) / 65535.0)
            return out

        async def stub_embed_batch(  # NOSONAR(S7503) — stub matches embed_batch async signature
            texts: list[str], *, model: str | None = None,
        ) -> list[list[float] | None]:
            return [_vec(t) for t in texts]

        async def stub_embed_query(  # NOSONAR(S7503) — stub matches embed_query async signature
            text: str, *, model: str | None = None,
        ) -> list[float]:
            return _vec(text)

        monkeypatch.setattr(
            "slayer.search.retrievers.embeddings.embed_batch",
            stub_embed_batch,
        )
        monkeypatch.setattr(
            embedding_client, "embed_query", stub_embed_query,
        )

        from slayer.search.retrievers.embeddings import EmbeddingRetriever
        emb_retriever = EmbeddingRetriever(storage=storage)
        persisted_models = []
        for m in _make_models():
            persisted = await storage.get_model(
                m.name, data_source="warehouse",
            )
            assert persisted is not None
            persisted_models.append(persisted)
            await emb_retriever.refresh_model_subtree(persisted)
        await emb_retriever.refresh_datasource(
            name="warehouse", models=persisted_models,
        )
        for mem in await storage.list_memories(entities=None):
            await emb_retriever.upsert_memory(mem)

        yield storage


@pytest_asyncio.fixture
async def service_with_embeddings(
    storage_with_embeddings: YAMLStorage,
) -> SearchService:
    return SearchService(storage=storage_with_embeddings)


@pytest.mark.asyncio
async def test_memories_order_stable_with_channel_3_active(
    service_with_embeddings: SearchService,
) -> None:
    small = await _ids_by_kind(
        service_with_embeddings,
        question="amount paid refund revenue customer email",
        datasource="warehouse",
        max_results=10,
    )
    large = await _ids_by_kind(
        service_with_embeddings,
        question="amount paid refund revenue customer email",
        datasource="warehouse",
        max_results=30,
    )
    assert _is_prefix(small["memories"], large["memories"]), (
        "channel-3 active: memories order changed as max_results grew"
    )


@pytest.mark.asyncio
async def test_entities_order_stable_with_channel_3_active(
    service_with_embeddings: SearchService,
) -> None:
    small = await _ids_by_kind(
        service_with_embeddings,
        question="amount paid refund customer email warehouse",
        datasource="warehouse",
        max_results=10,
    )
    large = await _ids_by_kind(
        service_with_embeddings,
        question="amount paid refund customer email warehouse",
        datasource="warehouse",
        max_results=30,
    )
    assert _is_prefix(small["entities"], large["entities"]), (
        "channel-3 active: entities order changed as max_results grew"
    )


# ---------------------------------------------------------------------------
# DEV-1513: channel-1 entity ranking — order stability with named entities
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_memories_order_stable_with_channel_1_named(
    service_invariance: SearchService,
) -> None:
    """DEV-1513: with ``entities=[X]`` supplied (channel 1 entity ranking
    active), increasing ``max_results`` must not reorder the memories."""
    small = await _ids_by_kind(
        service_invariance,
        entities=["warehouse.orders.amount_paid"],
        datasource="warehouse",
        max_results=5,
    )
    large = await _ids_by_kind(
        service_invariance,
        entities=["warehouse.orders.amount_paid"],
        datasource="warehouse",
        max_results=20,
    )
    assert _is_prefix(small["memories"], large["memories"]), (
        "channel-1 named active: memories order changed as max_results grew"
    )


@pytest.mark.asyncio
async def test_entities_order_stable_with_channel_1_named(
    service_invariance: SearchService,
) -> None:
    """DEV-1513: with ``entities=[X]`` supplied, increasing ``max_results``
    must not reorder the entities returned at the smaller cap."""
    small = await _ids_by_kind(
        service_invariance,
        entities=["warehouse.orders.amount_paid"],
        datasource="warehouse",
        max_results=3,
    )
    large = await _ids_by_kind(
        service_invariance,
        entities=["warehouse.orders.amount_paid"],
        datasource="warehouse",
        max_results=15,
    )
    assert _is_prefix(small["entities"], large["entities"]), (
        "channel-1 named active: entities order changed as max_results grew"
    )
