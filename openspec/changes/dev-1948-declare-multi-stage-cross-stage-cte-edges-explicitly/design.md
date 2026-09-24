## Context

- `engine/stage_ordering.topologically_order_stages` walks every sibling edge
  (`source_model` name / `ModelExtension.source_name`, spec `joins[].target_model`,
  inline-model nested `source_queries`) and is input-order invariant (alphabetical
  Kahn frontier; pinned by `tests/test_topologically_order_stages.py`). The engine
  calls it at every entry point before `plan_stages`.
- `plan_stages` re-sorts with `compile/stages._topo_sort` (source-only edges, FIFO
  seeded by input order), which can push a join-reading stage ahead of its target.
- `generate_planned_stages` renders stages in plan order under one generation scope
  (shared allocator, global producer dedup via `_gen_rendered_producers`), hoists
  each stage's CTEs, and assembles one flat `WITH` via `assemble_with_chain`. Stage
  relations declare only their hoisted producers; stage→sibling and root→stage reads
  ride on insertion order; a stage body's reuse of an earlier stage's producer
  records no reuse edge (`_gen_split_consumers` never carries the stage relation).
- Principles: engine P1 (typed pipeline — the plan carries the edges, the generator
  never re-derives them), engine P5, sql P6 (declared, never scanned), sql P11.

## Goals / Non-Goals

**Goals:**
- One stage-edge extractor and one ordering function for ordering, planning and emission.
- Every cross-stage `WITH` edge declared from typed plan data.
- Byte-identical SQL for every currently-working query.

**Non-Goals:**
- Per-CTE-exact sibling-read declaration (would need a "current CTE" context
  threaded through every CTE builder — the real reader is usually a producer's
  hoisted `_base`, not a consumer-stack name).
- Accepting a planned list that is not in dependency order.
- Changing `topologically_order_stages`' tie-break (its input-order invariance is a
  pinned contract).

## Decisions

1. **One extractor.** `stage_ordering._extract_sibling_refs` becomes public
   `stage_sibling_reads(query, siblings)`; `topologically_order_stages` and
   `plan_stages` both use it.
2. **One sorter.** `plan_stages` orders with `topologically_order_stages`;
   `compile/stages._topo_sort` is deleted (with its two raise-ledger rows);
   `query_engine`'s warning alignment (`ordered_stages = …`) calls the same pure
   function on the same input. Engine paths already pass its fixpoint, so their
   order (and SQL) is unchanged; a direct caller with non-canonical independent
   stages gets the canonical order. `plan_stages` inherits its validation (unnamed
   non-root, self-reference, root referenced, duplicate, cycle).
3. **Typed edges.** `PlannedQuery.stage_reads: List[str]` — the sibling stage
   relations a stage's statement reads. `plan_stages` computes one immutable set of
   all non-root stage names up front (never the incrementally-filled
   `stage_schemas`) and sets `stage_reads` for every stage, root included, listed in
   plan order. Empty for single-stage plans and producer sub-plans.
4. **Declared assembly.** In `generate_planned_stages`:
   - each non-root stage's `relation_name` is pushed onto `_gen_split_consumers`
     around its render (popped in `finally`), so a body reuse records a reuse edge;
   - stage relation `depends_on = [*hoisted, *_reuse_deps_of(relation_name), *stage_reads]`;
   - every CTE hoisted from a stage's statement, and every root entry, adds that
     statement's `stage_reads` — a statement-level conservative prerequisite;
   - `CteEntry.depends_on` is documented as "CTEs that must precede this one".
5. **Order is a checked precondition.** Before rendering, `generate_planned_stages`
   raises `ValueError` if any stage's `stage_reads` names a relation not planned
   earlier in the list. Under that precondition the statement-level edges are
   acyclic: a producer first rendered in stage S carries S's reads, all of which
   precede S, and only later stages can reuse it. A permuted list would instead
   manufacture a false cycle (producer → sibling via over-approximation, sibling →
   producer via reuse), so it is rejected up front.
6. **Stored joins never read siblings.** A reference through a stored model's own
   join resolves to the physical model (probed), so the spec-level walk is complete.

## Risks / Trade-offs

- [Golden drift] Declared edges must reproduce insertion order → any golden or
  notebook-output change is a stop-and-ask, never a silent re-bless.
- [Direct `plan_stages` callers] Test callers passing independent stages in a
  non-canonical order now get the canonical order → a golden change there is also
  stop-and-ask.
- [Over-approximation] Statement-level edges are sound only under Decision 5's
  precondition → enforced by the entry check and its test.
