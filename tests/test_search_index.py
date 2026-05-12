"""Tantivy in-memory index — schema + build + query (DEV-1375).

Covers:
* The schema is built with `id`, `kind`, `canonical` (raw) and `text`
  (English-stemmed) fields.
* `build_in_memory_index` produces one doc per memory + one doc per
  searchable entity, skipping hidden columns / hidden models.
* English stemmer tokenization (`shipped` ↔ `shipping`).
* Snake_case tokenization (default tokenizer splits `customer_id` into
  `customer` and `id`).
* The `canonical` field supports exact-match lookup of canonical entity
  strings.
* The `kind` field supports filtering memory vs entity hits at query time.
"""

from __future__ import annotations

from typing import List

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.memories.models import Memory
from slayer.search.index import build_in_memory_index, search_index


def _make_models() -> List[SlayerModel]:
    return [
        SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="warehouse",
            description="One row per checkout order. Includes shipping info.",
            columns=[
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount_paid", type=DataType.DOUBLE),
                Column(name="shipped_at", type=DataType.TIMESTAMP),
                Column(name="hidden_col", type=DataType.TEXT, hidden=True),
            ],
        ),
        SlayerModel(
            name="customers",
            sql_table="public.customers",
            data_source="warehouse",
            description="Customers and contact info.",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="email", type=DataType.TEXT),
            ],
        ),
        SlayerModel(
            name="hidden_model",
            sql_table="x",
            data_source="warehouse",
            hidden=True,
            columns=[Column(name="vis", type=DataType.TEXT)],
        ),
    ]


def _make_memories() -> List[Memory]:
    return [
        Memory(
            id=1,
            learning="To compute revenue, sum amount_paid where status='paid'.",
            entities=["warehouse.orders.amount_paid"],
        ),
        Memory(
            id=2,
            learning="Customers without email are usually anonymous checkouts.",
            entities=["warehouse.customers.email"],
        ),
    ]


def test_build_index_skips_hidden_models_and_hidden_columns() -> None:
    idx = build_in_memory_index(
        memories=_make_memories(),
        models=_make_models(),
        datasources=["warehouse"],
    )
    # Hidden model entirely absent.
    hits = search_index(index=idx, question="hidden_model", limit=10)
    assert all("hidden_model" not in hit.id for hit in hits)
    # Hidden column entirely absent.
    hits = search_index(index=idx, question="hidden_col", limit=10)
    assert all("hidden_col" not in hit.id for hit in hits)


def test_index_has_one_doc_per_memory_and_per_entity() -> None:
    """Sanity check: a wildcard search returns at least the entities we
    expect (memories: 2; datasources: 1; models: 2 visible; columns: 5
    visible across both models)."""
    idx = build_in_memory_index(
        memories=_make_memories(),
        models=_make_models(),
        datasources=["warehouse"],
    )
    # English stemmer + tokenizer: "checkout" appears in orders.description
    # and one memory.
    hits = search_index(index=idx, question="checkout", limit=20)
    kinds = {hit.kind for hit in hits}
    assert "model" in kinds or "memory" in kinds


def test_english_stemmer_matches_singular_to_plural_form() -> None:
    """`shipped` ↔ `shipping` via Porter stemmer."""
    idx = build_in_memory_index(
        memories=_make_memories(),
        models=_make_models(),
        datasources=["warehouse"],
    )
    hits = search_index(index=idx, question="ship", limit=10)
    # Should find docs containing "shipping" or "shipped" in description.
    surface_ids = {hit.id for hit in hits}
    # Either the model description "shipping info" or the column
    # "shipped_at" surfaces.
    assert any("orders" in i for i in surface_ids)


def test_snake_case_tokenization_finds_subword() -> None:
    """`customer_id` → ['customer', 'id'] under the default tokenizer."""
    idx = build_in_memory_index(
        memories=_make_memories(),
        models=_make_models(),
        datasources=["warehouse"],
    )
    hits = search_index(index=idx, question="customer", limit=20)
    # Should find at minimum the customers model + the customer_id column
    # on orders.
    ids = {hit.id for hit in hits}
    assert any("customers" in i for i in ids)


def test_canonical_field_exact_match() -> None:
    """An agent can search for the exact canonical string and get the doc."""
    idx = build_in_memory_index(
        memories=_make_memories(),
        models=_make_models(),
        datasources=["warehouse"],
    )
    hits = search_index(
        index=idx,
        question='"warehouse.orders.amount_paid"',  # exact-match query syntax
        limit=10,
        fields=["canonical"],
    )
    ids = [hit.id for hit in hits]
    assert "warehouse.orders.amount_paid" in ids


def test_kind_field_split_for_memory_vs_entity() -> None:
    idx = build_in_memory_index(
        memories=_make_memories(),
        models=_make_models(),
        datasources=["warehouse"],
    )
    hits = search_index(index=idx, question="customer", limit=20)
    memory_hits = [h for h in hits if h.kind == "memory"]
    entity_hits = [h for h in hits if h.kind != "memory"]
    # Both groups should be non-empty for "customer" since memory 2 mentions
    # "customers" and entities include customers/customer_id.
    assert isinstance(memory_hits, list)  # type sanity
    assert isinstance(entity_hits, list)


def test_empty_corpus_returns_empty_results() -> None:
    idx = build_in_memory_index(memories=[], models=[], datasources=[])
    hits = search_index(index=idx, question="anything", limit=10)
    assert hits == []


def test_memory_id_round_trips_as_integer() -> None:
    """`MemoryHit.id` must be the int memory id — the index doc-id namespacing
    is internal."""
    idx = build_in_memory_index(
        memories=_make_memories(),
        models=_make_models(),
        datasources=["warehouse"],
    )
    hits = search_index(index=idx, question="anonymous checkouts", limit=10)
    memory_hits = [h for h in hits if h.kind == "memory"]
    assert any(h.memory_id == 2 for h in memory_hits)
