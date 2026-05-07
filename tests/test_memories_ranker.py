"""Unit tests for the BM25 ranker (DEV-1365)."""

from __future__ import annotations

from slayer.memories.models import Memory
from slayer.memories.ranker import bm25_rank


def _mem(memory_id: int, entities: list[str]) -> Memory:
    return Memory(id=memory_id, learning=f"mem-{memory_id}", entities=entities)


def test_empty_corpus_returns_empty():
    assert bm25_rank([], ["x"]) == []


def test_empty_query_returns_empty():
    assert bm25_rank([_mem(1, ["x"])], []) == []


def test_single_doc_single_term_match():
    m = _mem(1, ["mydb.orders.amount"])
    ranked = bm25_rank([m], ["mydb.orders.amount"])
    assert len(ranked) == 1
    assert ranked[0][0].id == 1
    assert ranked[0][1] > 0


def test_dev_1365_fix_precise_outranks_overbroad():
    # The old ranker (raw overlap count) tied these at 1; with
    # length-normalised BM25, the precisely-tagged memory must rank
    # above the over-broad one.
    precise = _mem(1, ["mydb.orders.amount", "mydb.orders.qty"])
    broad = _mem(
        2,
        ["mydb.orders.amount"] + [f"mydb.x.col{i}" for i in range(50)],
    )
    ranked = bm25_rank([precise, broad], ["mydb.orders.amount"])
    ids_in_order = [m.id for m, _ in ranked]
    assert ids_in_order[0] == 1, (
        "precise memory must outrank over-broad memory; "
        f"got order {ids_in_order}"
    )


def test_strict_superset_still_scores_positive():
    # Memory entity set is a strict superset of the query — BM25 should
    # still keep it (TF=1 on the matched term, length-normalised).
    m = _mem(1, ["a", "b", "c", "d"])
    ranked = bm25_rank([m], ["a"])
    assert len(ranked) == 1
    assert ranked[0][1] > 0


def test_term_in_every_doc_still_returned():
    # When every document contains the query term, BM25Okapi assigns
    # a non-positive score (IDF goes negative because the term is not
    # discriminative). Those memories DO overlap the query, so the
    # ranker keeps them — the agent asked about that entity, surfacing
    # nothing would be silently wrong.
    a = _mem(1, ["x"])
    b = _mem(2, ["x"])
    c = _mem(3, ["x"])
    ranked = bm25_rank([a, b, c], ["x"])
    assert {m.id for m, _ in ranked} == {1, 2, 3}


def test_memory_with_empty_entities_does_not_crash_and_is_dropped():
    empty_mem = _mem(1, [])
    matched = _mem(2, ["mydb.orders.amount"])
    ranked = bm25_rank([empty_mem, matched], ["mydb.orders.amount"])
    ids = [m.id for m, _ in ranked]
    assert 1 not in ids, "memory with no entities cannot match anything"
    assert 2 in ids


def test_defensive_dedup_on_memory_entities():
    # A row with duplicated entries should rank identically to a row
    # with a single occurrence of the same entity.
    dup = _mem(1, ["x", "x", "x"])
    single = _mem(2, ["x"])
    # We need at least one OTHER memory in the corpus so IDF for "x"
    # stays positive (it's in only some docs, not all).
    other = _mem(3, ["y"])
    ranked = bm25_rank([dup, single, other], ["x"])
    score_by_id = {m.id: s for m, s in ranked}
    assert score_by_id[1] == score_by_id[2], (
        "duplicate entities must not change BM25 score; "
        f"got {score_by_id}"
    )


def test_stability_repeated_calls_same_order():
    a = _mem(1, ["x", "y"])
    b = _mem(2, ["x"])
    c = _mem(3, ["z"])
    first = bm25_rank([a, b, c], ["x"])
    second = bm25_rank([a, b, c], ["x"])
    assert [m.id for m, _ in first] == [m.id for m, _ in second]
    assert [s for _, s in first] == [s for _, s in second]


def test_query_entity_dedup_does_not_change_score():
    a = _mem(1, ["x"])
    b = _mem(2, ["y"])
    once = bm25_rank([a, b], ["x"])
    twice = bm25_rank([a, b], ["x", "x"])
    assert [m.id for m, _ in once] == [m.id for m, _ in twice]
    assert [s for _, s in once] == [s for _, s in twice]
