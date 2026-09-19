# DEV-1935 design — boolean-total semi-join pushdown

## Context

See proposal.md — Why. Substrate on current main (HEAD `51743422`, the DEV-1909 merge):

- `_conjunct_push_plan` (`slayer/engine/compile/stages.py`) resolves every column ref of a conjunct
  into a correlation tree (`nodes`: node_path → `SemiJoinHop`), rewrites the ref paths to tree-node
  paths, and raises `_PushBlocked` for (a) `_reject_mixed_or_not` — an OR/NOT subtree mixing a
  root-local ref with a cross-path ref, (b) `len(first_hops) != 1`. `_conjunct_disposition` turns a
  block into `UnreachableFilterDroppedWarning`. `_semi_join_groups_from_pushes` groups pushes by
  `(first hop target_model, join_pairs)`, ANDing a group's conjuncts.
- `PopulationFilters` (DEV-1909 D1/D2) records inline / semi_join / excluded per population conjunct;
  `host_split` moves a `semi_join` conjunct to the EXISTS unless the consumer's grain materialises
  every fanning path of the conjunct; the host base residue-checks an `excluded` conjunct with
  `check_population_filter_in_pushdown_scope`; a producer drops it with the warning.
- `_build_semi_join_exists` (`slayer/sql/generator.py`) renders one group as
  `EXISTS(SELECT 1 FROM <first hop> [INNER JOIN deeper hops ON parent pairs] WHERE <correlation
  eqs> AND <conjuncts>)`, conjunct refs resolving to hop aliases through the allocator and root-local
  refs correlating to the outer body. Seven call sites append the EXISTS conditions to a WHERE.
- Node paths keep the user's spelling; `resolve_hop` accepts an edge's name or its target model, so
  one physical edge can register two nodes (a latent DEV-1840 same-row hazard).
- Verified on the DEV-1900 dataset (raw SQL, SQLite): every oracle in the specs; the null-extended
  and strict-INNER readings differ only where a root row has no related row (`tier = 'gold' or
  orders.status = 'ok'`: 475 vs 420); the inline LEFT-join product and the EXISTS agree on every shape.
- Outer references inside a JOIN ON of a correlated EXISTS work on PostgreSQL, MySQL ≥ 8.0.20,
  SQLite, DuckDB, SQL Server, Snowflake and ClickHouse 25.4+; BigQuery may fail to decorrelate.
  Snowflake rejects a correlated EXISTS used as an operand of OR in the outer WHERE.

## Goals / Non-Goals

**Goals:** one semantic for every consumer (host base inline, association arm, EXISTS), stated once;
pushdown total over the boolean shape with byte-identical SQL for every existing shape; same-row
binding per shared branch, including across edge spellings and inside a partially materialised
multi-branch conjunct; the population's residue machinery and its checker deleted.

**Non-Goals:** Mode-A `SlayerModel.filters` / column `filter=` fragments crossing fanning hops;
window association (DEV-1914); the strict-mode topological contract (DEV-1932 → DEV-1873);
partitioned association cases (DEV-1932); a DNF rewrite that pulls root-local literals outside the
EXISTS (rejected: Snowflake forbids EXISTS under OR, and it would keep two semantics for branch
subtrees).

## Decisions

1. **Semantic = the inline product, existential.** A root row survives iff the conjunct holds on at
   least one row of its join product over the referenced branches, each hop joined as declared (LEFT
   by default → NULL-extended when absent). The EXISTS is a lowering of that product (Law 6), so the
   host base's inline path, the association arm's inline-plus-dedup and the EXISTS agree by
   construction. `NOT B` = some related row fails B (DEV-1840 pin, dice–slice Law 5); `x is null` on a
   related column = absence. *Rejected:* the issue's literal `NOT B → NOT EXISTS(B)` (flips the pin,
   breaks dice–slice for NOT, not spelling-invariant).

2. **Pushability total.** Delete `_reject_mixed_or_not` and the single-first-hop block. The
   three-way `_conjunct_disposition` stays for target-rooted producers (`excluded` = unreachable only);
   the population disposition is two-way — a bound population conjunct resolves from the host by
   construction, so the dropped arm is asserted impossible (like the D6 backstop), and
   `_DisposedConjunct.disposition`, `host_split(drop_excluded=)`, `producer_view`'s dropped list,
   `dropped_warnings` and the host-base residue check go. `check_population_filter_in_pushdown_scope`
   and its ledger row are deleted; `guards` baseline unchanged.

3. **Canonical hop identity.** `_forward_hops` / `_reverse_hops` register a node under the resolved
   edge's canonical token (its `name` when named, else its target model), and `_remap_ref_path`
   rewrites refs to that path, so `purchases.status` and `orders.channel` share one node and one
   alias; parallel named edges stay distinct (unnamed parallel edges already raise). The golden corpus
   is the byte-identity tripwire (no existing golden names an edge by a non-canonical spelling).

4. **Grouping by shared branch, first-appearance order.** Union-find over each push's first-level
   hops; a push spanning several branches merges their groups; group order is the order of the first
   push in each group and sibling hops keep registration order (`sorted` by depth is stable) — the
   union-find never reorders, so two disjoint single-branch pushes still yield two EXISTS in today's
   order. A group can now hold several first-level hops (a product).

5. **Null-rejection analysis (planner) and `SemiJoinHop.null_extended`.** Per group and per hop h, a
   conservative three-valued evaluation of the AND-ed predicate with every column of h and its
   descendants NULL, values ∈ {TRUE, FALSE, UNKNOWN, DEPENDS}:
   - an operand is *null-valued for h* when it is a `ColumnKey`/`ColumnSqlKey`/`TimeTruncKey` whose
     path lies under h (node-path prefix), or a Mode-A `ColumnSqlKey` whose dependency paths (from
     `_register_dep_hops`) lie under h and whose parsed fragment is a bare column, arithmetic or
     comparison (no function call, CASE or literal-only fragment); arithmetic over a null-valued operand
     is null-valued; anything containing a `ScalarCallKey` or `StarKey` is DEPENDS;
   - comparison (`PREDICATE_COMPARISON_OPS` minus `is`/`is not`), `InKey`, `BetweenKey` with a
     null-valued operand → UNKNOWN; `is` with one NULL-literal operand and one null-valued operand →
     TRUE, `is not` → FALSE; `is` with a non-NULL literal and a null-valued operand → FALSE, `is not` →
     TRUE; either operand order; anything else → DEPENDS;
   - AND: FALSE if any FALSE, else UNKNOWN if any UNKNOWN, else DEPENDS if any DEPENDS, else TRUE;
     OR: TRUE if any TRUE, else DEPENDS if any DEPENDS, else UNKNOWN if any UNKNOWN, else FALSE;
     NOT: TRUE↔FALSE, UNKNOWN and DEPENDS unchanged;
   - rejects(h) iff the value is FALSE or UNKNOWN; `null_extended(h) = declared LEFT ∧ ¬rejects(h)`.
   Rejecting h's NULLs covers the parent-absent row too (its h columns are NULL), so an INNER child
   under a LEFT parent is exact; a declared-INNER hop stays INNER and kills the parent's NULL row
   exactly as the inline product does. DEPENDS never rejects, so a wrong answer can only choose LEFT
   (always correct), never INNER. Every DEV-1840/1909/1747/1900 golden shape rejects every hop, hence
   byte-identical. *Rejected:* deciding at render time (engine P6: placement derives from the plan).

6. **Per-branch materialisation on the host base (D2 generalised).** `host_split` computes, per
   `semi_join` conjunct, which of its fanning branches the consumer's grain materialises: all →
   inline (unchanged); none → the push as planned; some → a reduced push whose hop registry drops the
   materialised branches, their refs keeping their (canonical) paths so the allocator resolves them to
   the outer query's own join aliases, and the null-rejection analysis runs over the remaining hops
   only. Host-rooted and target-rooted producers never materialise a fanning branch (DEV-1909 D2), so
   only the host base takes this arm. *Rejected:* the whole conjunct in one EXISTS (breaks
   filter/dimension same-row binding on the materialised branch); failing closed (a residue).

7. **Emission — one builder, two shapes.** `_build_semi_join_exists`: when no first-level hop is
   null-extended, today's shape — `FROM <first hop>` with its correlation in WHERE, further first-level
   hops CROSS-joined with their correlations in WHERE, deeper hops `INNER JOIN` (or `LEFT JOIN` when
   null-extended) ON their parent pairs. Otherwise the spine shape — `FROM (SELECT 1 AS one) AS
   <alias>` (SQL Server needs the column name), every hop `LEFT JOIN`/`INNER JOIN` per its flag with its
   correlation or parent pairs in the ON. The EXISTS stays a conjunct of the outer WHERE in both
   shapes. The spine alias comes from the allocator under a reserved `__slayer_` token; the
   scope-closure validator accepts it. Refs on a materialised branch (decision 6) render against the
   outer query's alias for that path, exactly as root-local refs do today.

8. **Association arm.** `_association_inline_filters` is unchanged in shape: with the push plan no
   longer blocking mixed or multi-branch conjuncts, they inline through the association producer's
   declared joins (discovered from the rerooted refs, as every inline filter is) before the per-entity
   dedup, and report as pushed. No plan-level assertion: the scope-closure validator backstops emission.

9. **Reporting.** One `semi_join_pushed` entry per conjunct text (the issue's "each pushed leg" — a
   conjunct is not split into legs under decision 1). Collectors unchanged.

10. **Normative harness (needs per-change approval; exact diff shown before applying).**
    `architecture/semantics.arc42.md`: Axiom 3, Axiom 14, Law 5 and Law 6 gain
    `[enforced: test:tests/test_dev1935_boolean_lowering.py]`.

11. **Test-impact protocol.** Consented: the residue pins in `test_dev1840_disposition.py` (4),
    `test_dev1840_strict_metadata.py` (3), `test_dev1841_association_filters.py` (1),
    `test_dev1841_error_mode.py` (1), `test_dev1836_broadcast_strict.py` (1),
    `test_dev1909_population_pushdown.py::TestOutOfScopeResidue` (5) and
    `test_dev1747_reroot_filter_routing.py` (8) flip to pushed / reported / executed-value; the
    dropped-filter dedup tests in `test_dev1745_warning_contract.py`, `test_dev1836_warning_collector.py`
    and `test_dev1838_interning.py` re-point to `semi_join_pushed` entry dedup (one entry per
    `(location, measure, text)`, never one per consumer); the ledger row goes; `excluded/mixed_or` in
    `dev1840_sql_baseline.json` re-blesses with a recorded reason. The declared-INNER-descendant pin
    needs a fixture variant with an event-less region (every DEV-1900 region has events).

12. **Producer genuinely-unreachable arm is dead — delete it (Egor, 2026-09-19, spec-tests Codex
    review).** Once pushdown is total (decision 2), the producer's `excluded`/genuinely-unreachable
    disposition has no reachable trigger, exactly as the population's dropped arm does not: a
    target-rooted producer joins back to the host to attach its grain, so its root reaches — by the
    same bidirectional traversal every push uses (root → host → N) — every model the host reaches;
    and a reference that does *not* resolve from the query root fails earlier at dimension routing
    with `UnresolvableDimensionJoinError`, in **every** mode, before the producer disposition runs.
    So the only feeders of the producer `UNREACHABLE_NO_PATH` → `UnreachableFilterDroppedWarning`
    arm (`stages.py` ~1125/1287) were the mixed-OR / multi-branch `_PushBlocked` reasons decision 2
    removes. **Do at implement:** (a) enumerate every feeder of the producer
    `UnreachableFilterDroppedWarning` / `excluded` disposition and prove none survives (grep callers;
    confirm no reroot/traversal edge case yields `UNREACHABLE_NO_PATH` for a host-resolvable ref) —
    if a live trigger is found, STOP and flag (do not silently keep the arm); (b) delete the
    producer `excluded` disposition, `dropped_warnings`, and the producer `UnreachableFilterDropped`
    emission as dead code (the two-way disposition mirrors decision 2's population arm); (c) reword
    the `queries/cross-model-aggregates` requirement sentence and its *"Genuinely unreachable filter
    keeps the established behavior"* scenario: a reference with no resolvable join path is **refused
    at resolution in every mode with a typed error, never routed as if it crossed nothing** — there
    is no producer-level silent drop. *Rejected:* keeping the arm defensively (leaves dead, untested
    code and a spec scenario describing an unreachable state — the option-B fixture is topologically
    inconstructible, since any producer that attaches to the host reaches everything the host does).

## Risks / Trade-offs

- [MySQL < 8.0.20, BigQuery, ClickHouse reject the spine shape] → reached only by shapes that error or
  drop today; the failure is the engine's own error, never a wrong value; documented in one sentence.
- [Null-rejection misjudged] → conservative by construction: DEPENDS never rejects, so the only
  possible mistake is LEFT where INNER would do; unit tests pin every rule and the golden corpus pins
  byte-identity.
- [Canonical tokens re-alias an existing golden] → none found; the golden suite trips if one exists.
- [Union-find reorders groups] → first-appearance ordering rule plus a generate-twice determinism pin.
- [Product EXISTS cost on wide branches] → accepted; correctness first.
- [The population's asserted-impossible dropped arm fires] → an assertion with a message, failing
  closed, never a silent drop.

## Migration Plan

Pure planner / renderer change; no stored-artifact migration. PR base is main; merge only, never
rebase. Rollback = revert the PR.
