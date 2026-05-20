"""Unit tests for the BM25 ranker (DEV-1365)."""

from __future__ import annotations

from slayer.memories.models import Memory
from slayer.memories.ranker import bm25_rank


def _mem(memory_id: int, entities: list[str]) -> Memory:
    # DEV-1428: memory ids are str; tests still pass int for readability.
    # The Memory model stringifies via its ``id`` before-validator.
    return Memory(id=memory_id, learning=f"mem-{memory_id}", entities=entities)


def test_empty_corpus_returns_empty():
    assert bm25_rank(memories=[], query_entities=["x"]) == []


def test_empty_query_returns_empty():
    assert bm25_rank(
        memories=[_mem(memory_id=1, entities=["x"])],
        query_entities=[],
    ) == []


def test_single_doc_single_term_match():
    m = _mem(memory_id=1, entities=["mydb.orders.amount"])
    ranked = bm25_rank(memories=[m], query_entities=["mydb.orders.amount"])
    assert len(ranked) == 1
    assert ranked[0][0].id == "1"
    assert ranked[0][1] > 0


def test_dev_1365_fix_precise_outranks_overbroad():
    # The old ranker (raw overlap count) tied these at 1; with
    # length-normalised BM25, the precisely-tagged memory must rank
    # above the over-broad one.
    precise = _mem(
        memory_id=1,
        entities=["mydb.orders.amount", "mydb.orders.qty"],
    )
    broad = _mem(
        memory_id=2,
        entities=["mydb.orders.amount"]
        + [f"mydb.x.col{i}" for i in range(50)],
    )
    ranked = bm25_rank(
        memories=[precise, broad],
        query_entities=["mydb.orders.amount"],
    )
    ids_in_order = [m.id for m, _ in ranked]
    assert ids_in_order[0] == "1", (
        "precise memory must outrank over-broad memory; "
        f"got order {ids_in_order}"
    )


def test_strict_superset_still_scores_positive():
    # Memory entity set is a strict superset of the query — BM25 should
    # still keep it (TF=1 on the matched term, length-normalised).
    m = _mem(memory_id=1, entities=["a", "b", "c", "d"])
    ranked = bm25_rank(memories=[m], query_entities=["a"])
    assert len(ranked) == 1
    assert ranked[0][1] > 0


def test_term_in_every_doc_still_returned():
    # When every document contains the query term, the term loses
    # discriminative power. BM25Plus (used here) keeps IDF positive,
    # avoiding the negative-IDF case BM25Okapi would have hit; either
    # way the overlap-based pre-filter must surface every matching
    # memory regardless of the variant's IDF behaviour — silently
    # returning nothing would be wrong.
    a = _mem(memory_id=1, entities=["x"])
    b = _mem(memory_id=2, entities=["x"])
    c = _mem(memory_id=3, entities=["x"])
    ranked = bm25_rank(memories=[a, b, c], query_entities=["x"])
    assert {m.id for m, _ in ranked} == {"1", "2", "3"}


def test_memory_with_empty_entities_does_not_crash_and_is_dropped():
    empty_mem = _mem(memory_id=1, entities=[])
    matched = _mem(memory_id=2, entities=["mydb.orders.amount"])
    ranked = bm25_rank(
        memories=[empty_mem, matched],
        query_entities=["mydb.orders.amount"],
    )
    ids = [m.id for m, _ in ranked]
    assert "1" not in ids, "memory with no entities cannot match anything"
    assert "2" in ids


def test_defensive_dedup_on_memory_entities():
    # A row with duplicated entries should rank identically to a row
    # with a single occurrence of the same entity.
    dup = _mem(memory_id=1, entities=["x", "x", "x"])
    single = _mem(memory_id=2, entities=["x"])
    # We need at least one OTHER memory in the corpus so IDF for "x"
    # stays positive (it's in only some docs, not all).
    other = _mem(memory_id=3, entities=["y"])
    ranked = bm25_rank(memories=[dup, single, other], query_entities=["x"])
    score_by_id = {m.id: s for m, s in ranked}
    assert score_by_id["1"] == score_by_id["2"], (
        "duplicate entities must not change BM25 score; "
        f"got {score_by_id}"
    )


def test_stability_repeated_calls_same_order():
    a = _mem(memory_id=1, entities=["x", "y"])
    b = _mem(memory_id=2, entities=["x"])
    c = _mem(memory_id=3, entities=["z"])
    first = bm25_rank(memories=[a, b, c], query_entities=["x"])
    second = bm25_rank(memories=[a, b, c], query_entities=["x"])
    assert [m.id for m, _ in first] == [m.id for m, _ in second]
    assert [s for _, s in first] == [s for _, s in second]


def test_query_entity_dedup_does_not_change_score():
    a = _mem(memory_id=1, entities=["x"])
    b = _mem(memory_id=2, entities=["y"])
    once = bm25_rank(memories=[a, b], query_entities=["x"])
    twice = bm25_rank(memories=[a, b], query_entities=["x", "x"])
    assert [m.id for m, _ in once] == [m.id for m, _ in twice]
    assert [s for _, s in once] == [s for _, s in twice]
